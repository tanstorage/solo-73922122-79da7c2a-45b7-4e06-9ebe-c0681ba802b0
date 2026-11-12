#!/usr/bin/env python3
"""测试专用服务器启动器。

本测试环境无法可靠地向进程发送信号（kill 被沙箱拒绝），因此让服务器
通过 threading.Timer + os._exit 在 SERVER_LIFETIME 秒后自行退出；
os._exit 不做任何清理，等价于崩溃，正好覆盖“中断后重启”场景。
"""
from __future__ import annotations

import os
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import uvicorn  # noqa: E402


def main() -> None:
    lifetime = float(os.environ.get("SERVER_LIFETIME", "60"))
    port = int(os.environ["PORT"])
    threading.Timer(lifetime, os._exit, args=(1,)).start()
    # 测试沙箱的 ptrace 监督与 uvloop/httptools 的 C 扩展相处不佳（偶发 SIGSEGV），
    # 测试改用纯 Python 的 asyncio 循环与 h11 解析器；应用逻辑不受影响。
    uvicorn.run("app.main:app", host="127.0.0.1", port=port,
                log_level="warning", loop="asyncio", http="h11")


if __name__ == "__main__":
    main()
