#!/usr/bin/env python3
"""生成种子数据集：seed/manifest.json + seed/partitions/*.jsonl。

输出是确定性的（按分区号作随机种子），重复运行结果一致。
每个分区覆盖一个闭合采样时段 [start, end]，采样点落在 [start, end) 内，
相邻分区的采样时间戳互不重叠。
"""
from __future__ import annotations

import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent.parent / "seed"
SAMPLE_INTERVAL_SECONDS = 5  # 1 小时分区 -> 720 个采样点

# (partition_id, vehicle_id, 采样开始, 时长小时, 到期时间)
PARTITIONS = [
    ("P0001", "T101", "2026-10-01T00:00:00Z", 1, "2026-11-01T00:00:00Z"),  # 已到期
    ("P0002", "T101", "2026-10-01T01:00:00Z", 1, "2026-11-01T00:00:00Z"),  # 已到期
    ("P0003", "T101", "2026-10-01T02:00:00Z", 1, "2026-12-01T00:00:00Z"),  # 未到期
    ("P0004", "T102", "2026-10-02T00:00:00Z", 1, "2026-11-05T00:00:00Z"),  # 已到期
    ("P0005", "T102", "2026-10-02T01:00:00Z", 1, "2026-12-20T00:00:00Z"),  # 未到期
    ("P0006", "T203", "2026-10-03T00:00:00Z", 1, "2026-11-10T00:00:00Z"),  # 已到期，含故障冲击段
    ("P0007", "T203", "2026-10-03T01:00:00Z", 1, "2027-01-15T00:00:00Z"),  # 未到期
    ("P0008", "T101", "2026-10-01T03:00:00Z", 1, "2026-11-02T00:00:00Z"),  # 已到期
]

# P0006 在 00:31:00 - 00:32:00 之间有一段转向架故障冲击，供调查保全演示
FAULT_WINDOWS = {"P0006": (31 * 60, 32 * 60)}


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def waveform(pid: str, t: float, rng: random.Random) -> tuple[float, float, float]:
    """合成三轴振动信号；故障窗口内幅值放大。"""
    burst = 1.0
    win = FAULT_WINDOWS.get(pid)
    if win and win[0] <= t < win[1]:
        burst = 6.0
    ax = burst * 0.6 * math.sin(2 * math.pi * 7.0 * t) + 0.15 * math.sin(2 * math.pi * 53.0 * t)
    ay = burst * 0.5 * math.sin(2 * math.pi * 6.3 * t + 1.1) + 0.12 * math.sin(2 * math.pi * 41.0 * t)
    az = 9.81 + burst * 0.4 * math.sin(2 * math.pi * 4.7 * t + 0.5) + 0.10 * math.sin(2 * math.pi * 60.0 * t)
    noise = 0.05 if burst == 1.0 else 0.3
    ax += rng.uniform(-noise, noise)
    ay += rng.uniform(-noise, noise)
    az += rng.uniform(-noise, noise)
    return round(ax, 4), round(ay, 4), round(az, 4)


def main() -> None:
    (OUT_DIR / "partitions").mkdir(parents=True, exist_ok=True)
    manifest_parts = []
    for pid, vehicle, start_s, hours, expires_s in PARTITIONS:
        start = parse(start_s)
        end = start + timedelta(hours=hours)
        rng = random.Random(f"metro-vibration:{pid}")
        n_samples = int(hours * 3600 // SAMPLE_INTERVAL_SECONDS)
        rel_path = f"partitions/{pid}.jsonl"
        lines = []
        for k in range(n_samples):
            t = k * SAMPLE_INTERVAL_SECONDS
            ts = start + timedelta(seconds=t)
            ax, ay, az = waveform(pid, t, rng)
            lines.append(json.dumps(
                {"ts": iso(ts), "vehicle_id": vehicle, "ax": ax, "ay": ay, "az": az},
                separators=(",", ":"),
            ))
        (OUT_DIR / rel_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
        manifest_parts.append({
            "partition_id": pid,
            "vehicle_id": vehicle,
            "start_ts": iso(start),
            "end_ts": iso(end),          # 闭合区间右端点（含）
            "expires_at": iso(parse(expires_s)),
            "file": rel_path,
            "sample_count": n_samples,
        })
    manifest = {
        "dataset": "metro-vibration",
        "description": "轨道交通车辆转向架振动采样（三轴加速度，5s 间隔）",
        "generated_at": "2026-11-11T00:00:00Z",
        "partitions": manifest_parts,
    }
    (OUT_DIR / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"manifest: {len(manifest_parts)} partitions -> {OUT_DIR}")


if __name__ == "__main__":
    main()
