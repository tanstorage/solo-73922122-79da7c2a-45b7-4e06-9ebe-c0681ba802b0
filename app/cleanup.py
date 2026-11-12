"""后台清理：仅处理“已到期且无生效保全”的分区，逐分区认领删除资格。

关键语义：
- 认领（claim）在 BEGIN IMMEDIATE 事务内完成：与保全创建互斥串行，
  谁先提交谁生效；条件 UPDATE + rowcount 校验保证同一分区只有一个赢家。
- 拿到资格后释放数据库锁再删文件：清理某分区期间，其他分区照常可操作。
- 文件删除失败 -> 状态置 delete_failed 并记录错误，绝不标记为已回收；
  后续 pass 会重试。
- 进程中断 -> deleting 行的租约到期后，任一存活进程可重新认领并继续删除；
  文件已删但状态未更新的情况通过“文件不存在即视为删除成功”收敛到 cleaned。
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from . import config
from .db import immediate_txn
from .timeutil import ms_to_iso, now_ms

log = logging.getLogger("datalake.cleanup")

# 可认领条件：活跃 / 删除失败且已到重试时刻 / 删除中但租约已过期（持有进程已崩溃）
CLAIM_SQL = """
SELECT p.partition_id, p.file_path
FROM partitions p
WHERE p.expires_ms <= :now
  AND (
      p.status = 'active'
      OR (p.status = 'delete_failed' AND p.next_retry_ms <= :now)
      OR (p.status = 'deleting' AND p.delete_lease_ms <= :now)
  )
  AND NOT EXISTS (
      SELECT 1
      FROM preservation_partitions pp
      JOIN preservations pr ON pr.preservation_id = pp.preservation_id
      WHERE pp.partition_id = p.partition_id
        AND pr.status = 'active'
  )
ORDER BY p.expires_ms ASC, p.partition_id ASC
LIMIT :lim
"""

MARK_CLAIMED_SQL = """
UPDATE partitions
SET status = 'deleting',
    delete_owner = :owner,
    delete_lease_ms = :lease,
    delete_attempts = delete_attempts + 1
WHERE partition_id = :pid
  AND (
      status = 'active'
      OR (status = 'delete_failed' AND next_retry_ms <= :now)
      OR (status = 'deleting' AND delete_lease_ms <= :now)
  )
"""


def claim_one(exclude: set[str] | None = None) -> dict[str, Any] | None:
    """在一个 IMMEDIATE 事务内挑选并认领一个可删除分区。

    exclude：本轮 pass 已尝试过的分区，避免持续失败的分区在同一轮里被
    反复认领而饿死其他分区（与 next_retry_ms 退避共同保证公平）。
    """
    now = now_ms()
    exclude = exclude or set()
    with immediate_txn() as conn:
        rows = conn.execute(CLAIM_SQL, {"now": now, "lim": len(exclude) + 1}).fetchall()
        row = next((r for r in rows if r["partition_id"] not in exclude), None)
        if row is None:
            return None
        cur = conn.execute(
            MARK_CLAIMED_SQL,
            {
                "owner": config.OWNER,
                "lease": now + int(config.DELETE_LEASE_SECONDS * 1000),
                "pid": row["partition_id"],
                "now": now,
            },
        )
        if cur.rowcount != 1:
            # 理论上被 IMMEDIATE 串行化后不会发生；兜底视为认领失败
            return None
        return {"partition_id": row["partition_id"], "file_path": row["file_path"]}


def execute_delete(partition_id: str, file_path: str) -> dict[str, str]:
    """删文件并按结果更新索引；失败绝不报已回收。"""
    path = config.DATA_DIR / file_path
    try:
        if partition_id in config.DELETE_FAIL_PARTITIONS:
            raise OSError("injected delete failure (DELETE_FAIL_PARTITIONS)")
        os.remove(path)
        detail = "file deleted"
    except FileNotFoundError:
        detail = "file already absent (crash-resume converge)"
    except OSError as exc:
        err = f"{type(exc).__name__}: {exc}"
        log.warning("分区 %s 删除失败：%s", partition_id, err)
        with immediate_txn() as conn:
            conn.execute(
                "UPDATE partitions SET status = 'delete_failed', last_error = ?, "
                "delete_lease_ms = NULL, next_retry_ms = ? WHERE partition_id = ?",
                (err, now_ms() + int(config.DELETE_RETRY_SECONDS * 1000), partition_id),
            )
        return {"partition_id": partition_id, "result": "failed", "detail": err}

    with immediate_txn() as conn:
        conn.execute(
            "UPDATE partitions SET status = 'cleaned', cleaned_ms = ?, "
            "delete_owner = NULL, delete_lease_ms = NULL, next_retry_ms = NULL, "
            "last_error = NULL WHERE partition_id = ?",
            (now_ms(), partition_id),
        )
    log.info("分区 %s 已回收（%s）", partition_id, detail)
    return {"partition_id": partition_id, "result": "cleaned", "detail": detail}


def cleanup_pass(reason: str) -> dict[str, Any]:
    """同步执行一轮清理：逐个认领并删除，直到没有合格分区或达到上限。"""
    started = now_ms()
    outcomes: list[dict[str, str]] = []
    attempted: set[str] = set()
    for _ in range(config.CLEANUP_MAX_PER_PASS):
        claimed = claim_one(exclude=attempted)
        if claimed is None:
            break
        attempted.add(claimed["partition_id"])
        outcomes.append(execute_delete(claimed["partition_id"], claimed["file_path"]))
    finished = now_ms()
    result = {
        "process": config.OWNER,
        "reason": reason,
        "started_at": ms_to_iso(started),
        "finished_at": ms_to_iso(finished),
        "outcomes": outcomes,
        "message": (
            f"claimed {len(outcomes)} partition(s): "
            f"{sum(1 for o in outcomes if o['result'] == 'cleaned')} cleaned, "
            f"{sum(1 for o in outcomes if o['result'] == 'failed')} failed"
        ),
    }
    log.info("清理 pass（%s）完成：%s", reason, result["message"])
    return result


async def run_cleanup_once(app: Any, reason: str) -> dict[str, Any]:
    """异步包装：同进程内串行（锁），跨进程由 SQLite 事务串行。"""
    async with app.state.cleanup_lock:
        result = await asyncio.to_thread(cleanup_pass, reason)
        app.state.last_cleanup_run = result
    return result


async def cleanup_loop(app: Any) -> None:
    """后台循环：到期的删除资格认领（含崩溃遗留的租约过期分区）在这里被持续拾起。

    先等待一个周期间隔再执行首轮，给演示/保全操作留出时间窗；
    手动 POST /cleanup/run 不受此延迟影响。
    """
    log.info(
        "后台清理循环已启动：interval=%ss lease=%ss owner=%s",
        config.CLEANUP_INTERVAL_SECONDS, config.DELETE_LEASE_SECONDS, config.OWNER,
    )
    while True:
        await asyncio.sleep(config.CLEANUP_INTERVAL_SECONDS)
        try:
            await run_cleanup_once(app, "timer")
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - 循环不允许因单次失败退出
            log.exception("清理 pass 异常，下个周期重试")
