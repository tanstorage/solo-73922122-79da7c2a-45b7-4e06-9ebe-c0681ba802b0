"""运行配置：全部可通过环境变量覆盖。"""
from __future__ import annotations

import os
import socket
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


def _flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


# 数据目录：SQLite 数据库与 JSONL 分区文件都放这里（Docker 中为共享卷 /data）
DATA_DIR = Path(os.environ.get("DATA_DIR", str(BASE_DIR / "var"))).resolve()
# 首次载入用的种子清单与数据目录
SEED_DIR = Path(os.environ.get("SEED_DIR", str(BASE_DIR / "seed"))).resolve()
DB_PATH = Path(os.environ.get("DB_PATH", str(DATA_DIR / "datalake.db"))).resolve()
PARTITIONS_SUBDIR = "partitions"

# 后台清理循环间隔（秒）
CLEANUP_INTERVAL_SECONDS = float(os.environ.get("CLEANUP_INTERVAL_SECONDS", "15"))
# 删除资格租约时长（秒）：持有者崩溃后，租约到期才允许其他进程接管该分区的删除
DELETE_LEASE_SECONDS = float(os.environ.get("DELETE_LEASE_SECONDS", "30"))
# 删除失败后的重试间隔（秒）：避免持续失败的分区霸占清理循环、饿死其他分区
DELETE_RETRY_SECONDS = float(os.environ.get("DELETE_RETRY_SECONDS", "30"))
# 单次清理 pass 最多处理的分区数
CLEANUP_MAX_PER_PASS = int(os.environ.get("CLEANUP_MAX_PER_PASS", "100"))
# 是否启用后台清理循环（手动触发接口不受此开关影响）
BACKGROUND_CLEANUP_ENABLED = _flag("BACKGROUND_CLEANUP_ENABLED", True)
# 故障注入（仅用于演示/测试删除失败路径）：逗号分隔的分区号，删除这些分区时抛出 OSError
DELETE_FAIL_PARTITIONS = {
    x.strip() for x in os.environ.get("DELETE_FAIL_PARTITIONS", "").split(",") if x.strip()
}

PROCESS_NAME = os.environ.get("PROCESS_NAME") or socket.gethostname()
OWNER = f"{PROCESS_NAME}:{os.getpid()}"
