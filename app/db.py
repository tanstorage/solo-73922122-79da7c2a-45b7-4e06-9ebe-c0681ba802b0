"""SQLite 存储层。

并发模型：
- 所有“检查 + 变更”的关键段（保全创建、删除资格认领、种子载入、文件 reconcile）
  都包在 BEGIN IMMEDIATE 事务里。SQLite 写锁把这些关键段在多个 API 进程之间
  全序化：谁先拿到写锁谁先生效，后进入者读到前者已提交的状态，从而保证
  “保全 vs 删除资格”有确定的先后。
- WAL 模式 + busy_timeout，读写不互相阻塞；清理某个分区期间，其他分区
  的查询/保全不受影响。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

from . import config

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS partitions (
        partition_id      TEXT PRIMARY KEY,
        vehicle_id        TEXT NOT NULL,
        start_ms          INTEGER NOT NULL,   -- 采样时段闭区间左端点（epoch ms）
        end_ms            INTEGER NOT NULL,   -- 采样时段闭区间右端点（epoch ms）
        expires_ms        INTEGER NOT NULL,   -- 到期时间（epoch ms）
        file_path         TEXT NOT NULL,      -- 相对 DATA_DIR 的 JSONL 路径
        sample_count      INTEGER NOT NULL DEFAULT 0,
        status            TEXT NOT NULL DEFAULT 'active'
                          CHECK (status IN ('active','deleting','delete_failed','cleaned')),
        file_materialized INTEGER NOT NULL DEFAULT 0,  -- 种子文件是否已落盘
        delete_owner      TEXT,               -- 持有删除资格的进程
        delete_lease_ms   INTEGER,            -- 删除资格租约到期时刻（epoch ms）
        next_retry_ms     INTEGER,            -- 删除失败后的下次可重试时刻（epoch ms）
        delete_attempts   INTEGER NOT NULL DEFAULT 0,
        last_error        TEXT,               -- 最近一次删除失败原因
        created_ms        INTEGER NOT NULL,
        cleaned_ms        INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_partitions_vehicle ON partitions(vehicle_id, start_ms, end_ms)",
    "CREATE INDEX IF NOT EXISTS idx_partitions_status ON partitions(status, expires_ms)",
    """
    CREATE TABLE IF NOT EXISTS preservations (
        preservation_id TEXT PRIMARY KEY,
        vehicle_id      TEXT NOT NULL,
        start_ms        INTEGER NOT NULL,     -- 保全闭区间（epoch ms）
        end_ms          INTEGER NOT NULL,
        investigator    TEXT NOT NULL,
        reason          TEXT NOT NULL DEFAULT '',
        status          TEXT NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active','released')),
        created_ms      INTEGER NOT NULL,
        released_ms     INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_preservations_status ON preservations(status)",
    """
    CREATE TABLE IF NOT EXISTS preservation_partitions (
        preservation_id TEXT NOT NULL REFERENCES preservations(preservation_id),
        partition_id    TEXT NOT NULL REFERENCES partitions(partition_id),
        PRIMARY KEY (preservation_id, partition_id)
    )
    """,
]


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(config.DB_PATH), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def immediate_txn() -> Iterator[sqlite3.Connection]:
    """BEGIN IMMEDIATE 事务：进入即拿写锁，跨进程串行化。"""
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass  # BEGIN 失败时没有活动事务，ROLLBACK 会报错，忽略
        finally:
            conn.close()
        raise
    else:
        conn.close()


@contextmanager
def read_conn() -> Iterator[sqlite3.Connection]:
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    (config.DATA_DIR / config.PARTITIONS_SUBDIR).mkdir(parents=True, exist_ok=True)
    config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    # CREATE ... IF NOT EXISTS 幂等，多进程并发建表安全
    with read_conn() as conn:
        for stmt in SCHEMA_STATEMENTS:
            conn.execute(stmt)
