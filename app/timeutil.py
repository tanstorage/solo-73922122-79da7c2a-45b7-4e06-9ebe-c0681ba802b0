"""时间工具：API 层使用 ISO 8601，存储层统一使用 UTC  epoch 毫秒（整数）。"""
from __future__ import annotations

import time
from datetime import datetime, timezone


def now_ms() -> int:
    return int(time.time() * 1000)


def to_utc(dt: datetime) -> datetime:
    """归一化为带时区的 UTC 时间；naive 输入按 UTC 解释。"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def dt_to_ms(dt: datetime) -> int:
    return int(to_utc(dt).timestamp() * 1000)


def ms_to_iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def iso_to_ms(text: str) -> int:
    """解析 JSONL 中的时间戳（兼容 'Z' 后缀与显式时区偏移）。"""
    return dt_to_ms(datetime.fromisoformat(text))
