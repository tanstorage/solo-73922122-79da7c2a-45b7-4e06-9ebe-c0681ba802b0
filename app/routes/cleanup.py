"""清理接口：手动触发一轮清理、查看清理状态。"""
from __future__ import annotations

from fastapi import APIRouter, Request

from .. import config
from ..cleanup import run_cleanup_once
from ..db import read_conn
from ..models import CleanupRunResult, CleanupStatus, DeleteFailure, InFlightDelete
from ..timeutil import ms_to_iso, now_ms

router = APIRouter(prefix="/cleanup", tags=["清理"])


@router.post(
    "/run",
    response_model=CleanupRunResult,
    summary="立即执行一轮清理（仅处理已到期且无生效保全的分区）",
)
async def run_cleanup(request: Request) -> CleanupRunResult:
    """逐分区认领删除资格并删除文件；与后台循环共用同一把进程内锁。

    删除失败的分区在返回中标记为 failed（索引中为 delete_failed），
    不会被报告为已回收。
    """
    result = await run_cleanup_once(request.app, "manual")
    return CleanupRunResult(**result)


@router.get("/status", response_model=CleanupStatus, summary="清理状态总览")
def cleanup_status(request: Request) -> CleanupStatus:
    with read_conn() as conn:
        counts = {r["status"]: r["n"] for r in conn.execute(
            "SELECT status, COUNT(*) AS n FROM partitions GROUP BY status"
        )}
        for key in ("active", "deleting", "delete_failed", "cleaned"):
            counts.setdefault(key, 0)

        now = now_ms()
        pending = conn.execute(
            """
            SELECT COUNT(*) AS n FROM partitions p
            WHERE p.status = 'active' AND p.expires_ms <= ?
              AND NOT EXISTS (
                  SELECT 1 FROM preservation_partitions pp
                  JOIN preservations pr ON pr.preservation_id = pp.preservation_id
                  WHERE pp.partition_id = p.partition_id AND pr.status = 'active'
              )
            """,
            (now,),
        ).fetchone()["n"]

        in_flight = [
            InFlightDelete(
                partition_id=r["partition_id"],
                owner=r["delete_owner"],
                lease_expires_at=ms_to_iso(r["delete_lease_ms"]),
                attempts=r["delete_attempts"],
            )
            for r in conn.execute(
                "SELECT partition_id, delete_owner, delete_lease_ms, delete_attempts "
                "FROM partitions WHERE status = 'deleting' ORDER BY partition_id"
            )
        ]
        failures = [
            DeleteFailure(
                partition_id=r["partition_id"],
                attempts=r["delete_attempts"],
                last_error=r["last_error"],
            )
            for r in conn.execute(
                "SELECT partition_id, delete_attempts, last_error "
                "FROM partitions "
                "WHERE last_error IS NOT NULL AND status != 'cleaned' "
                "ORDER BY partition_id"
            )
        ]
        active_pres = conn.execute(
            "SELECT COUNT(*) AS n FROM preservations WHERE status = 'active'"
        ).fetchone()["n"]

    last_run = getattr(request.app.state, "last_cleanup_run", None)
    return CleanupStatus(
        process=config.OWNER,
        now=ms_to_iso(now),
        counts=counts,
        pending_expired_unpreserved=pending,
        in_flight=in_flight,
        failures=failures,
        active_preservations=active_pres,
        background_cleanup_enabled=config.BACKGROUND_CLEANUP_ENABLED,
        cleanup_interval_seconds=config.CLEANUP_INTERVAL_SECONDS,
        last_run=CleanupRunResult(**last_run) if last_run else None,
    )
