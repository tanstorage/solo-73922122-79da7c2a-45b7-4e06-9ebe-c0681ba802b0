#!/usr/bin/env python3
"""按 docker compose handover profile 的环境变量，本地干跑 demo_handover 全流程。

复现原始缺陷场景：handover-a 的慢删除（10s）跨越 3s 租约且注入删除失败，
handover-b 到期接管；handover-a 迟到的失败必须被围栏挡回（superseded），
P0001 保持 cleaned，不计本轮 failed。
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PORT_A, PORT_B = 9811, 9812


def http(method, port, path, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def start(d, name, port, extra):
    env = dict(os.environ)
    env.update({
        "DATA_DIR": str(d), "PROCESS_NAME": name,
        "BACKGROUND_CLEANUP_ENABLED": "false",
        "DELETE_LEASE_SECONDS": "3", "DELETE_RETRY_SECONDS": "2",
        "SERVER_LIFETIME": "70", "PORT": str(port), **extra,
    })
    log = open(d / f"{name}.log", "ab")
    p = subprocess.Popen(
        [str(ROOT / ".venv" / "bin" / "python"), str(ROOT / "tests" / "server_runner.py")],
        cwd=ROOT, env=env, stdout=log, stderr=log)
    deadline = time.time() + 40
    while time.time() < deadline:
        try:
            if http("GET", port, "/health")[0] == 200:
                return p
        except (urllib.error.URLError, OSError):
            time.sleep(0.3)
    raise RuntimeError(f"{name} not healthy")


def main() -> int:
    d = Path("/tmp") / f"lk-demo-dry-{os.getpid()}"
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    start(d, "handover-a", PORT_A, {
        "SLOW_DELETE_PARTITIONS": "P0001", "SLOW_DELETE_SECONDS": "10",
        "DELETE_FAIL_PARTITIONS": "P0001"})
    start(d, "handover-b", PORT_B, {"CLEANUP_MAX_PER_PASS": "1"})

    failures: list[str] = []

    def check(name, cond, detail=""):
        print(f"  [{'PASS' if cond else 'FAIL'}] {name} {detail}")
        if not cond:
            failures.append(name)

    try:
        # 1) A 发起慢删除，2s 后断开（取消等待）
        req = urllib.request.Request(
            f"http://127.0.0.1:{PORT_A}/cleanup/run", data=b"{}", method="POST",
            headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=2)
        except (urllib.error.URLError, TimeoutError):
            pass

        deadline = time.time() + 8
        while time.time() < deadline:
            p1 = next(p for p in http("GET", PORT_B, "/partitions")[1]
                      if p["partition_id"] == "P0001")
            if p1["status"] == "deleting":
                break
            time.sleep(0.3)
        check("P0001 被 A 认领 deleting（慢删除中）", p1["status"] == "deleting", p1["status"])
        check("取消等待后 A in_progress=true",
              http("GET", PORT_A, "/cleanup/status")[1]["in_progress"]["in_progress"] is True)

        # 2) 租约到期后 B 高频接管
        time.sleep(3.2)
        res = {}
        deadline = time.time() + 25
        while time.time() < deadline:
            code, res = http("POST", PORT_B, "/cleanup/run", timeout=5)
            p1 = next(p for p in http("GET", PORT_B, "/partitions")[1]
                      if p["partition_id"] == "P0001")
            if p1["status"] == "cleaned":
                break
            time.sleep(0.6)
        check("B 接管后 P0001 cleaned", p1["status"] == "cleaned", p1["status"])
        check("B 接管轮次没有 failed（不计失效尝试为失败）",
              all(o["result"] != "failed" for o in res["outcomes"]), res.get("message", ""))

        # 3) 等 A 迟到结果（约 t=10s 落库）
        deadline = time.time() + 20
        last = None
        while time.time() < deadline:
            time.sleep(1)
            last = http("GET", PORT_A, "/cleanup/status")[1].get("last_run")
            outs = {o["partition_id"]: o["result"] for o in (last or {}).get("outcomes", [])}
            if outs.get("P0001") == "superseded":
                break
        check("A 迟到的失败记为 superseded", outs.get("P0001") == "superseded", str(outs))
        check("A 响应中 P0001 不计 failed/cleaned",
              all(not (o["partition_id"] == "P0001" and o["result"] in ("failed", "cleaned"))
                  for o in (last or {}).get("outcomes", [])),
              (last or {}).get("message", ""))
        check("A 工作结束后闸门释放",
              http("GET", PORT_A, "/cleanup/status")[1]["in_progress"]["in_progress"] is False)

        # 4) 终态与无关分区
        rows = {p["partition_id"]: p for p in http("GET", PORT_B, "/partitions")[1]}
        check("P0001 终态 cleaned、文件消失、无错误残留",
              rows["P0001"]["status"] == "cleaned" and not rows["P0001"]["file_exists"]
              and rows["P0001"]["last_error"] is None)
        check("无关分区 P0003 仍 active 且在盘（删除期间不持写锁）",
              rows["P0003"]["status"] == "active" and rows["P0003"]["file_exists"])
    finally:
        print("（服务器到寿命自退，无需信号）")

    print("\n干跑结果：", "全部通过" if not failures else f"失败 {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
