"""保全接口：按车号 + 时间闭区间保全所有重叠分区（整次原子，任一不可保全则整次拒绝）。"""
from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Query, status

from ..db import immediate_txn, read_conn
from ..models import PreservationCreate, PreservationCreateResult, PreservationOut
from ..timeutil import dt_to_ms, ms_to_iso, now_ms

router = APIRouter(prefix="/preservations", tags=["保全"])

# 闭区间重叠判定：分区 [start_ms, end_ms] 与查询区间 [q_start, q_end] 重叠
# <=> partition.start <= q.end AND partition.end >= q.start
_OVERLAPPING_SQL = """
SELECT partition_id, status
FROM partitions
WHERE vehicle_id = ? AND start_ms <= ? AND end_ms >= ?
ORDER BY start_ms, partition_id
"""


def _row_to_out(conn, row) -> PreservationOut:
    parts = [
        r["partition_id"]
        for r in conn.execute(
            "SELECT partition_id FROM preservation_partitions "
            "WHERE preservation_id = ? ORDER BY partition_id",
            (row["preservation_id"],),
        ).fetchall()
    ]
    return PreservationOut(
        preservation_id=row["preservation_id"],
        vehicle_id=row["vehicle_id"],
        start=ms_to_iso(row["start_ms"]),
        end=ms_to_iso(row["end_ms"]),
        investigator=row["investigator"],
        reason=row["reason"],
        status=row["status"],
        created_at=ms_to_iso(row["created_ms"]),
        released_at=ms_to_iso(row["released_ms"]),
        partitions=parts,
    )


@router.post(
    "",
    response_model=PreservationCreateResult,
    status_code=status.HTTP_201_CREATED,
    summary="创建保全（覆盖所有重叠分区，原子生效）",
)
def create_preservation(body: PreservationCreate) -> PreservationCreateResult:
    """保全与查询区间重叠的全部分区。

    - 任一重叠分区**已开始删除（deleting / delete_failed）或已清理（cleaned）**，
      则整次拒绝（409），不产生任何保全；
    - 无重叠分区时返回 404；
    - 与后台清理的删除资格认领在同一 SQLite 写锁上串行，先后确定；
    - 多个保全可重叠覆盖同一分区，分别生效、分别解除。
    """
    q_start, q_end = dt_to_ms(body.start), dt_to_ms(body.end)
    with immediate_txn() as conn:
        rows = conn.execute(
            _OVERLAPPING_SQL, (body.vehicle_id, q_end, q_start)
        ).fetchall()
        if not rows:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"车号 {body.vehicle_id} 在区间 "
                f"[{ms_to_iso(q_start)}, {ms_to_iso(q_end)}] 内没有重叠分区",
            )
        blocked = [r for r in rows if r["status"] != "active"]
        if blocked:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="以下重叠分区已开始删除或已清理，整次保全被拒绝："
                + ", ".join(f"{r['partition_id']}({r['status']})" for r in blocked),
            )
        pid = f"PRS-{uuid.uuid4().hex[:12]}"
        conn.execute(
            "INSERT INTO preservations "
            "(preservation_id, vehicle_id, start_ms, end_ms, investigator, reason, status, created_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, 'active', ?)",
            (pid, body.vehicle_id, q_start, q_end, body.investigator, body.reason, now_ms()),
        )
        conn.executemany(
            "INSERT INTO preservation_partitions (preservation_id, partition_id) VALUES (?, ?)",
            [(pid, r["partition_id"]) for r in rows],
        )
        out = PreservationOut(
            preservation_id=pid,
            vehicle_id=body.vehicle_id,
            start=ms_to_iso(q_start),
            end=ms_to_iso(q_end),
            investigator=body.investigator,
            reason=body.reason,
            status="active",
            created_at=ms_to_iso(now_ms()),
            released_at=None,
            partitions=[r["partition_id"] for r in rows],
        )
    return PreservationCreateResult(
        preservation=out,
        message=f"已保全 {len(out.partitions)} 个重叠分区；解除前这些分区不会被清理",
    )


@router.get("", response_model=list[PreservationOut], summary="列出保全")
def list_preservations(
    vehicle_id: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status", pattern="^(active|released)$"),
) -> list[PreservationOut]:
    sql = "SELECT * FROM preservations WHERE (? IS NULL OR vehicle_id = ?) AND (? IS NULL OR status = ?) ORDER BY created_ms DESC"
    with read_conn() as conn:
        rows = conn.execute(
            sql, (vehicle_id, vehicle_id, status_filter, status_filter)
        ).fetchall()
        return [_row_to_out(conn, r) for r in rows]


@router.get("/{preservation_id}", response_model=PreservationOut, summary="保全详情")
def get_preservation(preservation_id: str) -> PreservationOut:
    with read_conn() as conn:
        row = conn.execute(
            "SELECT * FROM preservations WHERE preservation_id = ?", (preservation_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"保全 {preservation_id} 不存在")
        return _row_to_out(conn, row)


@router.post("/{preservation_id}/release", response_model=PreservationOut, summary="解除保全")
def release_preservation(preservation_id: str) -> PreservationOut:
    """解除后，其覆盖的分区若已到期且没有其他生效保全覆盖，即可被后台清理。

    重复解除是幂等的（返回当前状态）。
    """
    with immediate_txn() as conn:
        row = conn.execute(
            "SELECT * FROM preservations WHERE preservation_id = ?", (preservation_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"保全 {preservation_id} 不存在")
        if row["status"] == "active":
            conn.execute(
                "UPDATE preservations SET status = 'released', released_ms = ? "
                "WHERE preservation_id = ?",
                (now_ms(), preservation_id),
            )
            row = conn.execute(
                "SELECT * FROM preservations WHERE preservation_id = ?", (preservation_id,)
            ).fetchone()
        return _row_to_out(conn, row)
