"""轨道交通振动数据湖 API。

FastAPI + SQLite（WAL）+ 本地 JSONL 分区。两个 API 进程共享同一数据目录，
通过 SQLite 写锁串行化“保全”与“删除资格认领”，保证确定先后。
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import config
from .cleanup import CleanupRunGate, cleanup_loop
from .db import init_db
from .routes import cleanup as cleanup_routes
from .routes import partitions as partitions_routes
from .routes import preservations as preservations_routes
from .seed import reconcile_files, seed_from_manifest
from .timeutil import ms_to_iso, now_ms

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("datalake.main")

DESCRIPTION = """
轨道交通车辆振动数据湖：到期数据清理 + 故障调查波形保全。

**演示流程（/docs 内可直接执行）**

1. `GET /partitions` 查看 8 个种子分区（部分已到期）；
2. `GET /samples` 按车号 + 时间闭区间查询波形（如 T203 的 00:30–00:35 故障冲击段）；
3. `POST /preservations` 保全 T203 `[00:30, 00:35]` —— 覆盖含故障段的 P0006；
4. `POST /cleanup/run` 触发清理：到期且无保全的分区被回收，P0006 因保全存活；
5. 对已清理分区再次 `POST /preservations` 会得到 **409**（整次拒绝）；
6. `POST /preservations/{id}/release` 解除保全，再次 `POST /cleanup/run`，P0006 被回收；
7. `GET /cleanup/status` 查看状态统计、在途删除、失败记录与 `in_progress` 闸门状态。

**任务交接与取消等待（本服务修复点）**

- 每次删除认领带围栏 `(owner, attempts, lease)`；租约到期被接管后，旧任务迟到的
  成功/失败落库零行命中，结果记 `superseded`，不计本轮 cleaned/failed，
  绝不覆盖接管者的状态、租约、错误与完成结果；
- 取消 `/cleanup/run` 等待只放弃响应，删除工作继续；同进程下一轮在实际工作
  结束前持续排队，重复取消也不能提前放行。
- 可控演示（docker compose profile）：`handover`（慢删除跨租约 + 迟到报错）、
  `cancel-demo`（取消等待），脚本见 `scripts/demo_handover.sh`、`scripts/demo_cancel.sh`。

两个 API 进程（api-a:8001 / api-b:8002）共享同一 SQLite 与分区目录，
任一进程都可执行上述操作，状态一致。
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    seed_stats = seed_from_manifest()
    reconcile_stats = reconcile_files()
    log.info("启动完成 seed=%s reconcile=%s", seed_stats, reconcile_stats)

    # 进程内清理串行闸门：取消 HTTP 等待不取消实际删除，
    # 闸门在实际工作结束后才放行下一轮（详见 app/cleanup.py）
    app.state.cleanup_gate = CleanupRunGate(app)
    app.state.last_cleanup_run = None
    app.state.seed_stats = seed_stats

    task = None
    if config.BACKGROUND_CLEANUP_ENABLED:
        task = asyncio.create_task(cleanup_loop(app))
    try:
        yield
    finally:
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        # 等待“被取消等待但仍在跑”的清理工作真正结束，再关闭进程
        try:
            await asyncio.wait_for(app.state.cleanup_gate.aclose(), timeout=120)
        except asyncio.TimeoutError:
            log.warning("等待在途清理工作超时，强制关闭（租约机制保证其他进程可接管）")


app = FastAPI(
    title="轨道交通振动数据湖",
    description=DESCRIPTION,
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(preservations_routes.router)
app.include_router(partitions_routes.router)
app.include_router(cleanup_routes.router)


@app.get("/health", tags=["meta"], summary="健康检查")
def health() -> dict:
    return {"status": "ok", "process": config.OWNER, "now": ms_to_iso(now_ms())}


@app.get("/meta/info", tags=["meta"], summary="进程与存储信息")
def meta_info() -> dict:
    return {
        "process": config.OWNER,
        "data_dir": str(config.DATA_DIR),
        "db_path": str(config.DB_PATH),
        "seed_dir": str(config.SEED_DIR),
        "background_cleanup_enabled": config.BACKGROUND_CLEANUP_ENABLED,
        "cleanup_interval_seconds": config.CLEANUP_INTERVAL_SECONDS,
        "delete_lease_seconds": config.DELETE_LEASE_SECONDS,
        "delete_fail_partitions": sorted(config.DELETE_FAIL_PARTITIONS),
        "slow_delete_partitions": sorted(config.SLOW_DELETE_PARTITIONS),
        "slow_delete_seconds": config.SLOW_DELETE_SECONDS,
    }
