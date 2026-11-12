#!/usr/bin/env python3
"""任务交接与取消等待语义测试（不依赖信号、不依赖 docker）。

覆盖三类修复点：
A. 围栏（fence）：租约到期被接管后，旧任务的成功/失败落库 rowcount=0，
   绝不覆盖新任务的状态、租约、错误与完成结果；失效尝试在响应中为
   superseded，不计本轮 cleaned/failed。
B. 进程内清理闸门 CleanupRunGate：取消等待只放弃响应，工作线程继续；
   实际工作结束前下一轮必须等待；重复取消不能提前放行；迟到结果仍记录。
C. 慢删除跨租约接管的端到端双进程场景（真实两个 uvicorn 进程 + SQLite 共享卷）。

运行：python3 tests/handover_test.py
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

FAILURES: list[str] = []
PASSES: list[str] = []


def ok(name: str, cond: bool, detail: str = "") -> None:
    (PASSES if cond else FAILURES).append(f"{name} {detail}")
    print(f"  [{'PASS' if cond else 'FAIL'}] {name} {detail}")


# ================================================================ A. 围栏落库
def test_fence_blocks_stale_writes() -> None:
    print("== A1 围栏 UPDATE：旧任务迟到成功/失败均无法覆盖接管者 ==")
    from app import config
    from app.cleanup import (
        ClaimTicket,
        FINISH_CLEANED_SQL,
        FINISH_FAILED_SQL,
        execute_delete,
    )
    from app.db import immediate_txn, init_db
    from app.timeutil import now_ms

    d = Path("/tmp") / f"lk-handover-A1-{os.getpid()}"
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    (d / "partitions").mkdir(parents=True)
    # 直接 monkeypatch 路径常量，避免依赖环境变量重载模块
    config.DATA_DIR = d.resolve()
    config.DB_PATH = (d / "datalake.db").resolve()
    init_db()
    with immediate_txn() as conn:
        for pid in ("PA", "PB"):
            conn.execute(
                "INSERT INTO partitions (partition_id, vehicle_id, start_ms, end_ms, "
                "expires_ms, file_path, status, created_ms) "
                "VALUES (?, 'T', 0, 1, 0, ?, 'active', ?)",
                (pid, f"partitions/{pid}.jsonl", now_ms()),
            )
    (d / "partitions" / "PA.jsonl").write_text("{}\n")
    (d / "partitions" / "PB.jsonl").write_text("{}\n")

    # —— 旧任务 O1 的围栏（attempts=1, lease=已过期时刻）
    old_ticket = ClaimTicket(
        partition_id="PA", file_path="partitions/PA.jsonl",
        owner="old:1", attempts=1, lease_ms=now_ms() - 5000,
    )
    # 新任务 O2 已接管：attempts=2、新租约、cleaned 终态
    with immediate_txn() as conn:
        conn.execute(
            "UPDATE partitions SET status='cleaned', delete_owner=NULL, "
            "delete_lease_ms=NULL, delete_attempts=2, cleaned_ms=? WHERE partition_id='PA'",
            (now_ms(),),
        )

    # 旧任务迟到“成功” -> rowcount 0，cleaned 不被改动
    with immediate_txn() as conn:
        cur = conn.execute(
            FINISH_CLEANED_SQL,
            {"cleaned_ms": now_ms(), "pid": "PA", "owner": "old:1",
             "attempts": 1, "lease": old_ticket.lease_ms},
        )
        ok("A1 迟到成功 UPDATE 零行命中", cur.rowcount == 0, f"rowcount={cur.rowcount}")
    with immediate_txn() as conn:
        row = conn.execute(
            "SELECT status, delete_attempts FROM partitions WHERE partition_id='PA'"
        ).fetchone()
    ok("A1 接管者的 cleaned 终态未被覆盖", row["status"] == "cleaned" and row["delete_attempts"] == 2,
       f"{row['status']}/attempts={row['delete_attempts']}")

    # 旧任务迟到“失败” -> rowcount 0，绝不把 cleaned 改回 delete_failed
    with immediate_txn() as conn:
        cur = conn.execute(
            FINISH_FAILED_SQL,
            {"err": "late boom", "retry": now_ms() + 9999, "pid": "PA",
             "owner": "old:1", "attempts": 1, "lease": old_ticket.lease_ms},
        )
        ok("A1 迟到失败 UPDATE 零行命中", cur.rowcount == 0, f"rowcount={cur.rowcount}")
    with immediate_txn() as conn:
        row = conn.execute(
            "SELECT status, last_error FROM partitions WHERE partition_id='PA'"
        ).fetchone()
    ok("A1 cleaned 未被迟到报错改回 delete_failed",
       row["status"] == "cleaned" and row["last_error"] is None, str(dict(row)))

    # —— execute_delete 端到端：接管后旧任务完成删除 -> superseded，且不动新任务状态
    # 新任务 O2 对 PB 接管后仍处于 deleting（持有新围栏 attempts=2）
    new_lease = now_ms() + 60000
    with immediate_txn() as conn:
        conn.execute(
            "UPDATE partitions SET status='deleting', delete_owner='new:2', "
            "delete_lease_ms=?, delete_attempts=2, last_error='new-task-context' "
            "WHERE partition_id='PB'",
            (new_lease,),
        )
    stale = ClaimTicket(
        partition_id="PB", file_path="partitions/PB.jsonl",
        owner="old:1", attempts=1, lease_ms=now_ms() - 5000,
    )
    outcome = execute_delete(stale)  # 文件存在，物理删除成功但围栏失效
    ok("A1 execute_delete 失效尝试返回 superseded", outcome["result"] == "superseded",
       outcome["result"])
    with immediate_txn() as conn:
        row = conn.execute(
            "SELECT status, delete_owner, delete_lease_ms, delete_attempts, last_error "
            "FROM partitions WHERE partition_id='PB'"
        ).fetchone()
    ok("A1 接管者的 deleting/租约/错误原样保留",
       row["status"] == "deleting" and row["delete_owner"] == "new:2"
       and row["delete_lease_ms"] == new_lease and row["delete_attempts"] == 2
       and row["last_error"] == "new-task-context",
       str(dict(row)))


# ================================================================ B. 闸门取消语义
class _FakeState:
    def __init__(self) -> None:
        self.last_cleanup_run = None
        self.recorded_reasons: list[str] = []

    def __setattr__(self, name: str, value) -> None:
        if name == "last_cleanup_run" and value is not None:
            self.recorded_reasons.append(value["reason"])
        super().__setattr__(name, value)


class _FakeApp:
    def __init__(self) -> None:
        self.state = _FakeState()


async def _gate_scenario() -> None:
    from app.cleanup import CleanupRunGate
    import app.cleanup as cleanup_mod

    app = _FakeApp()
    gate = CleanupRunGate(app)

    block = threading.Event()      # 卡住工作线程里的 cleanup_pass
    entered = threading.Event()    # cleanup_pass 已进入
    call_count = {"n": 0}

    def fake_pass(reason: str) -> dict:
        call_count["n"] += 1
        entered.set()
        block.wait(timeout=30)
        return {"process": "p", "reason": reason, "started_at": "s",
                "finished_at": "f", "outcomes": [], "message": "done"}

    orig_pass = cleanup_mod.cleanup_pass
    cleanup_mod.cleanup_pass = fake_pass
    try:
        # 第一轮启动，工作线程卡在 block 上
        first = asyncio.create_task(gate.run("manual-1"))
        await asyncio.wait_for(asyncio.to_thread(entered.wait), 10)
        entered.clear()
        ok("B1 第一轮工作已进入执行", gate.in_progress and call_count["n"] == 1)

        # 第二个调用在闸门处排队
        second = asyncio.create_task(gate.run("manual-2"))
        await asyncio.sleep(0.3)
        ok("B2 工作未结束时第二轮在闸门等待", gate.in_progress and not second.done()
           and call_count["n"] == 1)

        # 取消第二轮（重复取消两次）：不得提前放行，工作线程仍只有 1 个
        second.cancel()
        second.cancel()
        for _ in range(5):
            try:
                await second
            except asyncio.CancelledError:
                break
            await asyncio.sleep(0.05)
        ok("B3 重复取消第二轮只取消等待", second.cancelled())
        await asyncio.sleep(0.2)
        ok("B4 取消后闸门仍未放行（实际工作未结束）",
           gate.in_progress and call_count["n"] == 1, gate.snapshot()["reason"])

        # 再排队第三轮，确认仍被闸门挡住
        third = asyncio.create_task(gate.run("manual-3"))
        await asyncio.sleep(0.3)
        ok("B5 取消后新的一轮依旧排队等待", not third.done() and call_count["n"] == 1)

        # 第一轮的 HTTP 等待者也取消：工作线程仍不可中断
        first.cancel()
        try:
            await first
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(0.3)
        ok("B6 第一轮等待取消后实际工作仍在跑（in_progress 保持真实）",
           gate.in_progress and call_count["n"] == 1, gate.snapshot()["reason"])
        ok("B6b 旧工作未结束前第三轮的工作不会启动",
           not entered.is_set() and call_count["n"] == 1)

        # 放行实际工作：第一轮工作线程结束，守护任务收尾，第三轮才真正启动
        block.set()
        res3 = await asyncio.wait_for(third, 10)
        ok("B7 实际工作结束后才放行下一轮", call_count["n"] == 2 and res3["reason"] == "manual-3",
           f"calls={call_count['n']}")
        # 守护任务在第一轮真实结束时即记录 manual-1（早于第三轮 manual-3）
        ok("B8 被取消等待的第一轮结果由守护任务落库",
           app.state.recorded_reasons[:1] == ["manual-1"],
           str(app.state.recorded_reasons))
        ok("B9 闸门已释放", not gate.in_progress)

        # 无竞争时新一轮可直接执行
        block.clear()

        async def quick() -> None:
            def quick_pass(reason: str) -> dict:
                return {"process": "p", "reason": reason, "started_at": "s",
                        "finished_at": "f", "outcomes": [], "message": "q"}

            cleanup_mod.cleanup_pass = quick_pass
            r = await gate.run("manual-4")
            cleanup_mod.cleanup_pass = fake_pass
            ok("B10 闸门释放后新一轮直接执行", r["reason"] == "manual-4" and not gate.in_progress)

        await quick()
    finally:
        cleanup_mod.cleanup_pass = orig_pass


def test_gate_cancellation() -> None:
    print("== B 清理闸门：取消等待不取消工作，实际结束才放行，重复取消不提前放行 ==")
    asyncio.run(_gate_scenario())


# ================================================================ C. 双进程端到端
def http(method: str, port: int, path: str, body: dict | None = None, timeout: float = 10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


class Server:
    _next_port = 9700

    def __init__(self, data_dir: Path, name: str, lifetime: int = 55, **env):
        Server._next_port += 1
        self.port = Server._next_port
        self.data_dir = data_dir
        self.name = name
        self.lifetime = lifetime
        self.extra_env = env
        self.proc: subprocess.Popen | None = None

    def start(self) -> "Server":
        env = dict(os.environ)
        env.update({
            "DATA_DIR": str(self.data_dir),
            "PROCESS_NAME": self.name,
            "CLEANUP_INTERVAL_SECONDS": "60",  # 端到端场景全用手动触发，避免干扰
            "DELETE_LEASE_SECONDS": "3",
            "DELETE_RETRY_SECONDS": "2",
            "SERVER_LIFETIME": str(self.lifetime),
            "PORT": str(self.port),
        })
        env.update({k: str(v) for k, v in self.extra_env.items()})
        log = open(self.data_dir / f"server-{self.name}-{self.port}.log", "ab")
        self.proc = subprocess.Popen(
            [str(ROOT / ".venv" / "bin" / "python"), str(ROOT / "tests" / "server_runner.py")],
            cwd=ROOT, env=env, stdout=log, stderr=log,
        )
        deadline = time.time() + 40
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server {self.name} exited early; see {log.name}")
            try:
                code, _ = http("GET", self.port, "/health")
                if code == 200:
                    return self
            except (urllib.error.URLError, ConnectionError, OSError):
                pass
            time.sleep(0.3)
        raise RuntimeError(f"server {self.name} not healthy; see {log.name}")

    def wait_dead(self) -> None:
        """本沙箱禁止 os.kill，服务器由 SERVER_LIFETIME 定时器 os._exit 自退。"""
        if self.proc is None:
            return
        deadline = time.time() + self.lifetime + 20
        while time.time() < deadline:
            if self.proc.poll() is not None:
                return
            try:
                http("GET", self.port, "/health", timeout=2)
            except (urllib.error.URLError, ConnectionError, OSError):
                if self.proc.poll() is not None:
                    return
            time.sleep(0.5)
        raise RuntimeError(f"server {self.name} still alive after lifetime")


def _partition(port: int, pid: str) -> dict:
    _, parts = http("GET", port, "/partitions")
    return next(p for p in parts if p["partition_id"] == pid)


def test_cross_process_takeover_and_late_failure() -> None:
    print("== C 双进程：慢删除跨租约接管 + 迟到报错不回退 + 失效尝试不计成败 ==")
    d = Path("/tmp") / f"lk-handover-C-{os.getpid()}"
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)

    # A：慢删除（10s > 3s 租约）且对 P0001 注入删除失败 —— 制造“旧任务迟到报错”
    a = Server(d, "hand-a", SLOW_DELETE_PARTITIONS="P0001",
               DELETE_FAIL_PARTITIONS="P0001", SLOW_DELETE_SECONDS="10").start()
    # B：干净进程，负责接管；每轮只认领 1 个，避免 B 的 pass 先处理其他分区
    # 而让接管动作排到 A 的慢删除落库之后
    b = Server(d, "hand-b", CLEANUP_MAX_PER_PASS="1").start()

    try:
        # 触发 A 的慢删除（后台线程跑，HTTP 请求 2s 后主动断开模拟取消等待）
        req = urllib.request.Request(
            f"http://127.0.0.1:{a.port}/cleanup/run", data=b"{}", method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            urllib.request.urlopen(req, timeout=2)
        except (urllib.error.URLError, TimeoutError):
            pass  # 预期超时断开：A 的工作线程仍在删

        # 等待 A 完成认领并进入 deleting
        deadline = time.time() + 8
        while time.time() < deadline:
            p = _partition(b.port, "P0001")
            if p["status"] == "deleting":
                break
            time.sleep(0.3)
        ok("C1 A 已认领 P0001 进入 deleting", p["status"] == "deleting", p["status"])
        ok("C2 取消等待后 A 的工作仍在进行（闸门 in_progress）",
           http("GET", a.port, "/cleanup/status")[1]["in_progress"]["in_progress"] is True)

        # 等待租约到期（3s），由 B 接管并成功删除
        deadline = time.time() + 25
        final = {}
        while time.time() < deadline:
            code, res = http("POST", b.port, "/cleanup/run")
            final = {o["partition_id"]: o["result"] for o in res["outcomes"]}
            p = _partition(b.port, "P0001")
            if p["status"] == "cleaned":
                break
            time.sleep(1)
        p = _partition(b.port, "P0001")
        ok("C3 租约到期后 B 接管并回收 P0001", p["status"] == "cleaned", p["status"])
        ok("C4 B 响应未把接管来的分区计为失败",
           final.get("P0001") in (None, "cleaned", "superseded"), str(final))

        # 等 A 的慢删除（10s 睡眠 + 注入失败）迟到落库
        deadline = time.time() + 18
        a_last = None
        while time.time() < deadline:
            time.sleep(1)
            _, st = http("GET", a.port, "/cleanup/status")
            a_last = st.get("last_run")
            if a_last and any(o["partition_id"] == "P0001" for o in a_last["outcomes"]):
                break
        p = _partition(b.port, "P0001")
        ok("C5 旧任务 A 迟到报错后 P0001 仍为 cleaned（未回退 delete_failed）",
           p["status"] == "cleaned", p["status"])
        ok("C6 A 的迟到失败在其响应中标记 superseded，不计本轮 failed",
           a_last is not None and any(
               o["partition_id"] == "P0001" and o["result"] == "superseded"
               for o in a_last["outcomes"]
           )
           and all(o["result"] != "failed" for o in a_last["outcomes"]
                   if o["partition_id"] == "P0001"),
           json.dumps(a_last, ensure_ascii=False) if a_last else "no last_run")
        ok("C7 A 闸门在实际工作结束后已释放",
           http("GET", a.port, "/cleanup/status")[1]["in_progress"]["in_progress"] is False)
        # 文件与索引一致
        ok("C8 文件已不在盘上", not (d / "partitions" / "P0001.jsonl").exists())

        # 同进程取消等待后下一轮必须等真实结束：再发一次慢删除验证无并发工作
        # （P0002 也可慢删除；先确认 A 现在可以正常开启新一轮）
        code, res = http("POST", a.port, "/cleanup/run")
        ok("C9 闸门释放后 A 可正常执行下一轮", code == 200 and "outcomes" in res)
    finally:
        # 不能发信号：等两个服务器到寿命自退（os._exit，等价崩溃）
        a.wait_dead()
        b.wait_dead()


def main() -> int:
    from app import config

    saved_data_dir, saved_db_path = config.DATA_DIR, config.DB_PATH
    try:
        test_fence_blocks_stale_writes()
        test_gate_cancellation()
        test_cross_process_takeover_and_late_failure()
    finally:
        config.DATA_DIR, config.DB_PATH = saved_data_dir, saved_db_path
    print("\n========== 汇总 ==========")
    print(f"PASS: {len(PASSES)}  FAIL: {len(FAILURES)}")
    for f in FAILURES:
        print("FAIL:", f)
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
