"""种子清单载入与启动时文件 reconcile。

幂等性保证：
- 分区行以 partition_id 为主键，INSERT OR IGNORE；已清理（cleaned）的分区行
  作为墓碑永久保留，重启后不会被重新插入，种子文件也不会被重新拷回。
- 文件拷贝与行插入/标记在同一个 BEGIN IMMEDIATE 事务内完成，其他进程的
  清理认领要么在种子载入之前、要么在其之后，不会穿插。
- reconcile 删除“无对应未清理分区行”的孤儿文件，保证索引与磁盘最终一致。
"""
from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path

from . import config
from .db import immediate_txn
from .timeutil import iso_to_ms, ms_to_iso, now_ms

log = logging.getLogger("datalake.seed")


def _manifest_path() -> Path:
    return config.SEED_DIR / "manifest.json"


def seed_from_manifest() -> dict:
    """首次（及每次）启动时载入清单：只新增从未见过的分区，绝不复活已清理分区。"""
    mpath = _manifest_path()
    if not mpath.exists():
        log.warning("种子清单不存在：%s，跳过载入", mpath)
        return {"manifest": None, "inserted": [], "already_known": []}

    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    inserted: list[str] = []
    already_known: list[str] = []
    copied_files: list[str] = []

    with immediate_txn() as conn:
        for part in manifest.get("partitions", []):
            pid = part["partition_id"]
            rel_path = part.get("file") or f"{config.PARTITIONS_SUBDIR}/{pid}.jsonl"
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO partitions
                    (partition_id, vehicle_id, start_ms, end_ms, expires_ms,
                     file_path, sample_count, status, created_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?)
                """,
                (
                    pid,
                    part["vehicle_id"],
                    iso_to_ms(part["start_ts"]),
                    iso_to_ms(part["end_ts"]),
                    iso_to_ms(part["expires_at"]),
                    rel_path,
                    int(part.get("sample_count", 0)),
                    now_ms(),
                ),
            )
            if cur.rowcount == 1:
                inserted.append(pid)
            else:
                already_known.append(pid)

            # 物化种子文件：仅对未清理且尚未落盘的分区；cleaned 分区绝不回拷
            row = conn.execute(
                "SELECT status, file_materialized FROM partitions WHERE partition_id = ?",
                (pid,),
            ).fetchone()
            if row["status"] != "cleaned" and not row["file_materialized"]:
                src = config.SEED_DIR / rel_path
                dst = config.DATA_DIR / rel_path
                dst.parent.mkdir(parents=True, exist_ok=True)
                if not dst.exists():
                    shutil.copyfile(src, dst)
                    copied_files.append(rel_path)
                conn.execute(
                    "UPDATE partitions SET file_materialized = 1 WHERE partition_id = ?",
                    (pid,),
                )

        conn.execute(
            "INSERT INTO meta(key, value) VALUES('seeded_at', ?) "
            "ON CONFLICT(key) DO NOTHING",
            (ms_to_iso(now_ms()),),
        )
        conn.execute(
            "INSERT INTO meta(key, value) VALUES('seed_manifest', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(mpath),),
        )

    log.info(
        "种子载入完成：新增 %d 个分区（%s），已存在 %d 个，拷贝文件 %d 个",
        len(inserted), ",".join(inserted) or "-", len(already_known), len(copied_files),
    )
    return {
        "manifest": str(mpath),
        "inserted": inserted,
        "already_known": already_known,
        "copied_files": copied_files,
    }


def reconcile_files() -> dict:
    """启动时对齐索引与磁盘：删除孤儿文件（无分区行或行已 cleaned）。"""
    removed: list[str] = []
    missing_active: list[str] = []
    with immediate_txn() as conn:
        rows = conn.execute(
            "SELECT partition_id, file_path, status FROM partitions"
        ).fetchall()
        keep = {r["file_path"] for r in rows if r["status"] != "cleaned"}

        pdir = config.DATA_DIR / config.PARTITIONS_SUBDIR
        if pdir.is_dir():
            for f in sorted(pdir.glob("*.jsonl")):
                rel = f"{config.PARTITIONS_SUBDIR}/{f.name}"
                if rel not in keep:
                    try:
                        f.unlink()
                        removed.append(rel)
                    except FileNotFoundError:
                        pass

        for r in rows:
            if r["status"] == "active" and not (config.DATA_DIR / r["file_path"]).exists():
                missing_active.append(r["partition_id"])

    if removed:
        log.info("reconcile 删除孤儿文件 %d 个：%s", len(removed), removed)
    if missing_active:
        log.warning("以下 active 分区缺少数据文件：%s", missing_active)
    return {"removed_orphans": removed, "missing_active_files": missing_active}
