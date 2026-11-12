"""分区清单与波形查询接口。"""
from __future__ import annotations

import json
import os
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, status

from .. import config
from ..db import read_conn
from ..models import PartitionOut, Sample, SamplesResponse
from ..timeutil import dt_to_ms, iso_to_ms, ms_to_iso, now_ms

router = APIRouter(tags=["分区与波形"])


def _partition_out(row) -> PartitionOut:
    file_exists = os.path.exists(config.DATA_DIR / row["file_path"])
    return PartitionOut(
        partition_id=row["partition_id"],
        vehicle_id=row["vehicle_id"],
        start=ms_to_iso(row["start_ms"]),
        end=ms_to_iso(row["end_ms"]),
        expires_at=ms_to_iso(row["expires_ms"]),
        expired=row["expires_ms"] <= now_ms(),
        status=row["status"],
        file_exists=file_exists,
        active_preservations=row["active_preservations"],
        delete_attempts=row["delete_attempts"],
        delete_owner=row["delete_owner"],
        delete_lease_expires_at=ms_to_iso(row["delete_lease_ms"]),
        last_error=row["last_error"],
        cleaned_at=ms_to_iso(row["cleaned_ms"]),
    )


_LIST_SQL = """
SELECT p.*,
       (SELECT COUNT(*)
        FROM preservation_partitions pp
        JOIN preservations pr ON pr.preservation_id = pp.preservation_id
        WHERE pp.partition_id = p.partition_id AND pr.status = 'active'
       ) AS active_preservations
FROM partitions p
WHERE (? IS NULL OR p.vehicle_id = ?)
  AND (? IS NULL OR p.status = ?)
ORDER BY p.vehicle_id, p.start_ms, p.partition_id
"""


@router.get(
    "/partitions",
    response_model=list[PartitionOut],
    summary="列出分区（含状态、到期、保全数、文件是否在盘）",
    tags=["分区与波形"],
)
def list_partitions(
    vehicle_id: str | None = Query(default=None),
    status_filter: str | None = Query(
        default=None, alias="status", pattern="^(active|deleting|delete_failed|cleaned)$"
    ),
) -> list[PartitionOut]:
    with read_conn() as conn:
        rows = conn.execute(
            _LIST_SQL, (vehicle_id, vehicle_id, status_filter, status_filter)
        ).fetchall()
    return [_partition_out(r) for r in rows]


@router.get(
    "/samples",
    response_model=SamplesResponse,
    summary="按车号 + 时间闭区间查询尚存的采样时间和值",
    tags=["分区与波形"],
)
def get_samples(
    vehicle_id: str = Query(examples=["T203"]),
    start: datetime = Query(examples=["2026-10-03T00:30:00Z"], description="区间起点（含）"),
    end: datetime = Query(examples=["2026-10-03T00:35:00Z"], description="区间终点（含）"),
    limit: int = Query(default=1000, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> SamplesResponse:
    """只读取 status=active 的分区；deleting/delete_failed/cleaned 分区不对外提供波形。"""
    q_start, q_end = dt_to_ms(start), dt_to_ms(end)
    if q_end < q_start:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "end 必须不早于 start（闭区间）")

    with read_conn() as conn:
        rows = conn.execute(
            "SELECT partition_id, file_path FROM partitions "
            "WHERE vehicle_id = ? AND status = 'active' AND start_ms <= ? AND end_ms >= ? "
            "ORDER BY start_ms, partition_id",
            (vehicle_id, q_end, q_start),
        ).fetchall()

    warnings: list[str] = []
    hits: list[tuple[int, dict]] = []
    used_partitions: list[str] = []
    for row in rows:
        path = config.DATA_DIR / row["file_path"]
        if not path.exists():
            warnings.append(f"分区 {row['partition_id']} 索引为 active 但文件缺失，已跳过")
            continue
        try:
            with open(path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    ts_ms = iso_to_ms(rec["ts"])
                    if q_start <= ts_ms <= q_end:
                        hits.append((ts_ms, rec))
        except OSError:
            # 读取期间该分区恰好被清理认领并删除（最终一致）：跳过并提示
            warnings.append(f"分区 {row['partition_id']} 读取期间文件被清理，已跳过")
            continue
        used_partitions.append(row["partition_id"])

    hits.sort(key=lambda item: item[0])
    page = hits[offset : offset + limit]
    return SamplesResponse(
        vehicle_id=vehicle_id,
        start=ms_to_iso(q_start),
        end=ms_to_iso(q_end),
        partitions=used_partitions,
        matched=len(hits),
        returned=len(page),
        samples=[
            Sample(ts=ms_to_iso(ts), vehicle_id=rec["vehicle_id"],
                   ax=rec["ax"], ay=rec["ay"], az=rec["az"])
            for ts, rec in page
        ],
        warnings=warnings,
    )
