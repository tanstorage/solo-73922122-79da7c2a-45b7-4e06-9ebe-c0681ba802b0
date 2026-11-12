"""后台清理：仅处理“已到期且无生效保全”的分区，逐分区认领删除资格。

关键语义：
- 认领（claim）在 BEGIN IMMEDIATE 事务内完成：与保全创建互斥串行，
  谁先提交谁生效；条件 UPDATE + rowcount 校验保证同一分区只有一个赢家。
- 拿到资格后释放数据库锁再删文件：清理某分区期间，其他分区照常可操作。
- 每次认领带不可复用的**围栏（fence）**：(delete_owner, delete_attempts,
  delete_lease_ms)。删除结束后的所有状态回写都带围栏条件 UPDATE：
  租约到期被新任务接管后，旧任务无论成功还是失败都无法覆盖新任务的
  状态、租约、错误与完成结果，只会得到 superseded（资格已被接管）结果。
- 文件删除失败 -> 状态置 delete_failed 并记录错误，绝不标记为已回收；
  后续 pass 会重试。失效（被接管）的尝试既不计成功也不计失败。
- 进程中断 -> deleting 行的租约到期后，任一存活进程可重新认领并继续删除；
  文件已删但状态未更新的情况通过“文件不存在即视为删除成功”收敛到 cleaned。
- 进程内串行由 CleanupRunGate 保证：取消等待只放弃 HTTP 响应，工作线程
  仍在运行；在实际工作结束前不会放行同进程的下一轮，重复取消同样不放行。
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

from . import config
from .db import immediate_txn
from .timeutil import ms_to_iso, now_ms

log = logging.getLogger("datalake.cleanup")

# 可认领条件：活跃 / 删除失败且已到重试时刻 / 删除中但租约已过期（持有进程已崩溃/过慢）
CLAIM_SQL = """
SELECT p.partition_id, p.file_path
FROM partitions p
WHERE p.expires_ms <= :now
  AND (
      p.status = 'active'
      OR (p.status = 'delete_failed' AND p.next_retry_ms <= :now)
      OR (p.status = 'deleting' AND p.delete_lease_ms <= :now)
  )
  AND NOT EXISTS (
      SELECT 1
      FROM preservation_partitions pp
      JOIN preservations pr ON pr.preservation_id = pp.preservation_id
      WHERE pp.partition_id = p.partition_id
        AND pr.status = 'active'
  )
ORDER BY p.expires_ms ASC, p.partition_id ASC
LIMIT :lim
"""

MARK_CLAIMED_SQL = """
UPDATE partitions
SET status = 'deleting',
    delete_owner = :owner,
    delete_lease_ms = :lease,
    delete_attempts = delete_attempts + 1
WHERE partition_id = :pid
  AND (
      status = 'active'
      OR (status = 'delete_failed' AND next_retry_ms <= :now)
      OR (status = 'deleting' AND delete_lease_ms <= :now)
  )
"""

# 成功回写：仅当行仍属于本次认领的围栏时生效。
# 旧任务在租约到期、新任务接管（attempts/lease/owner 变化）后执行此语句，
# rowcount=0 —— 新任务的状态、租约、错误与完成结果绝不会被迟到结果覆盖。
FINISH_CLEANED_SQL = """
UPDATE partitions
SET status = 'cleaned', cleaned_ms = :cleaned_ms,
    delete_owner = NULL, delete_lease_ms = NULL, next_retry_ms = NULL,
    last_error = NULL
WHERE partition_id = :pid
  AND delete_owner = :owner
  AND delete_attempts = :attempts
  AND delete_lease_ms = :lease
"""

# 失败回写：同样带围栏；被接管的失败不得把新任务（可能已 cleaned）改回 delete_failed。
FINISH_FAILED_SQL = """
UPDATE partitions
SET status = 'delete_failed', last_error = :err,
    delete_lease_ms = NULL,
    next_retry_ms = :retry
WHERE partition_id = :pid
  AND delete_owner = :owner
  AND delete_attempts = :attempts
  AND delete_lease_ms = :lease
"""

# 结果取值：cleaned/failed 是本资格的真实结局；superseded 表示本资格在
# 落库前已因租约到期被接管，本次尝试的结果被丢弃，不计入本轮成败。
RESULT_CLEANED = "cleaned"
RESULT_FAILED = "failed"
RESULT_SUPERSEDED = "superseded"


@dataclass(frozen=True)
class ClaimTicket:
    """一次删除资格认领的围栏凭证（与写入行的值严格一致）。"""

    partition_id: str
    file_path: str
    owner: str
    attempts: int
    lease_ms: int


def claim_one(exclude: set[str] | None = None) -> ClaimTicket | None:
    """在一个 IMMEDIATE 事务内挑选并认领一个可删除分区。

    exclude：本轮 pass 已尝试过的分区，避免持续失败的分区在同一轮里被
    反复认领而饿死其他分区（与 next_retry_ms 退避共同保证公平）。
    """
    now = now_ms()
    exclude = exclude or set()
    with immediate_txn() as conn:
        rows = conn.execute(CLAIM_SQL, {"now": now, "lim": len(exclude) + 1}).fetchall()
        row = next((r for r in rows if r["partition_id"] not in exclude), None)
        if row is None:
            return None
        lease = now + int(config.DELETE_LEASE_SECONDS * 1000)
        cur = conn.execute(
            MARK_CLAIMED_SQL,
            {
                "owner": config.OWNER,
                "lease": lease,
                "pid": row["partition_id"],
                "now": now,
            },
        )
        if cur.rowcount != 1:
            # 理论上被 IMMEDIATE 串行化后不会发生；兜底视为认领失败
            return None
        claimed = conn.execute(
            "SELECT delete_attempts AS attempts FROM partitions WHERE partition_id = ?",
            (row["partition_id"],),
        ).fetchone()
        return ClaimTicket(
            partition_id=row["partition_id"],
            file_path=row["file_path"],
            owner=config.OWNER,
            attempts=int(claimed["attempts"]),
            lease_ms=lease,
        )


def _delete_file(partition_id: str, file_path: str) -> tuple[bool, str]:
    """执行物理删除，返回 (是否成功/幂等, 细节)。

    演示故障注入：
    - DELETE_FAIL_PARTITIONS：删除必失败（OSError）；
    - SLOW_DELETE_PARTITIONS：删除前睡眠 SLOW_DELETE_SECONDS，
      制造“慢删除跨越租约”的接管窗口。
    本函数不触碰数据库，保证删除期间不持有数据库写锁。
    """
    path = config.DATA_DIR / file_path
    if partition_id in config.SLOW_DELETE_PARTITIONS:
        log.info(
            "分区 %s 注入慢删除：睡眠 %.1fs（租约 %.0fs，用于跨租约接管演示）",
            partition_id, config.SLOW_DELETE_SECONDS, config.DELETE_LEASE_SECONDS,
        )
        time.sleep(config.SLOW_DELETE_SECONDS)
    try:
        if partition_id in config.DELETE_FAIL_PARTITIONS:
            raise OSError("injected delete failure (DELETE_FAIL_PARTITIONS)")
        os.remove(path)
        return True, "file deleted"
    except FileNotFoundError:
        return True, "file already absent (crash-resume converge)"
    except OSError as exc:
        return False, f"{type(exc).__name__}: {exc}"


def execute_delete(ticket: ClaimTicket) -> dict[str, str]:
    """删文件并按围栏条件更新索引；失败绝不报已回收，失效尝试绝不覆盖接管者。"""
    ok_delete, detail = _delete_file(ticket.partition_id, ticket.file_path)
    now = now_ms()
    with immediate_txn() as conn:
        if ok_delete:
            cur = conn.execute(
                FINISH_CLEANED_SQL,
                {
                    "cleaned_ms": now,
                    "pid": ticket.partition_id,
                    "owner": ticket.owner,
                    "attempts": ticket.attempts,
                    "lease": ticket.lease_ms,
                },
            )
        else:
            log.warning("分区 %s 删除失败：%s", ticket.partition_id, detail)
            cur = conn.execute(
                FINISH_FAILED_SQL,
                {
                    "err": detail,
                    "retry": now + int(config.DELETE_RETRY_SECONDS * 1000),
                    "pid": ticket.partition_id,
                    "owner": ticket.owner,
                    "attempts": ticket.attempts,
                    "lease": ticket.lease_ms,
                },
            )
        if cur.rowcount != 1:
            # 租约已到期、资格被其他任务接管：本结果作废，绝不覆盖接管者的状态。
            row = conn.execute(
                "SELECT status, delete_owner AS owner FROM partitions "
                "WHERE partition_id = ?",
                (ticket.partition_id,),
            ).fetchone()
            current = f"{row['status']}/{row['owner']}" if row is not None else "row-missing"
            log.warning(
                "分区 %s 的删除资格已被接管（旧围栏 owner=%s attempts=%s lease=%s），"
                "迟到结果 %s 已忽略；当前状态=%s",
                ticket.partition_id, ticket.owner, ticket.attempts, ticket.lease_ms,
                "cleaned" if ok_delete else "failed", current,
            )
            return {
                "partition_id": ticket.partition_id,
                "result": RESULT_SUPERSEDED,
                "detail": (
                    f"delete attempt superseded after lease expiry; "
                    f"late {'success' if ok_delete else 'failure'} ignored, "
                    f"partition now held by another attempt ({current})"
                ),
            }

    if ok_delete:
        log.info("分区 %s 已回收（%s）", ticket.partition_id, detail)
        return {"partition_id": ticket.partition_id, "result": RESULT_CLEANED, "detail": detail}
    return {"partition_id": ticket.partition_id, "result": RESULT_FAILED, "detail": detail}


def cleanup_pass(reason: str) -> dict[str, Any]:
    """同步执行一轮清理：逐个认领并删除，直到没有合格分区或达到上限。"""
    started = now_ms()
    outcomes: list[dict[str, str]] = []
    attempted: set[str] = set()
    for _ in range(config.CLEANUP_MAX_PER_PASS):
        claimed = claim_one(exclude=attempted)
        if claimed is None:
            break
        attempted.add(claimed.partition_id)
        outcomes.append(execute_delete(claimed))
    finished = now_ms()
    n_cleaned = sum(1 for o in outcomes if o["result"] == RESULT_CLEANED)
    n_failed = sum(1 for o in outcomes if o["result"] == RESULT_FAILED)
    n_superseded = sum(1 for o in outcomes if o["result"] == RESULT_SUPERSEDED)
    message = (
        f"claimed {len(outcomes)} partition(s): {n_cleaned} cleaned, {n_failed} failed"
    )
    if n_superseded:
        # 失效尝试单列，不混入本轮成败
        message += f", {n_superseded} superseded (ignored after takeover)"
    result = {
        "process": config.OWNER,
        "reason": reason,
        "started_at": ms_to_iso(started),
        "finished_at": ms_to_iso(finished),
        "outcomes": outcomes,
        "message": message,
    }
    log.info("清理 pass（%s）完成：%s", reason, message)
    return result


class CleanupRunGate:
    """进程内清理串行闸门：替代裸 asyncio.Lock。

    语义：
    - 同一时刻最多一个清理工作线程在跑（跨进程仍由 SQLite 写锁串行）；
    - 调用方取消等待（HTTP 断开、任务取消）只放弃等待本身，工作线程
      不可中断地继续执行；闸门在**实际工作结束**后才释放，因此下一轮
      清理必须等上一轮的物理删除全部完成；
    - 重复取消同样不能提前放行；
    - 迟到结束的 pass 仍会写入 last_cleanup_run，供 /cleanup/status 观察。
    """

    def __init__(self, app: Any) -> None:
        self._app = app
        # 一把锁覆盖“等待 + 实际工作”的完整生命周期：只要上一轮的物理删除
        # 没有真正结束，锁就不释放，同进程下一轮只能在 acquire() 处排队。
        self._lock = asyncio.Lock()
        # _current 仅用于可观测性（in_progress/snapshot），不参与互斥判定。
        self._current: asyncio.Future | None = None
        self._current_reason: str | None = None
        self._current_started_ms: int | None = None

    @property
    def in_progress(self) -> bool:
        return self._current is not None and not self._current.done()

    def snapshot(self) -> dict[str, Any]:
        return {
            "in_progress": self.in_progress,
            "reason": self._current_reason if self.in_progress else None,
            "started_at": ms_to_iso(self._current_started_ms) if self.in_progress else None,
        }

    async def run(self, reason: str) -> dict[str, Any]:
        # 等待闸门期间可被取消；闸门未到手就取消，没有任何工作需要等待。
        await self._lock.acquire()
        started = now_ms()
        future: asyncio.Future = asyncio.ensure_future(
            asyncio.get_running_loop().run_in_executor(None, cleanup_pass, reason)
        )
        self._current = future
        self._current_reason = reason
        self._current_started_ms = started
        try:
            # shield：调用方取消等待时 CancelledError 只作用于外层 await，
            # 工作 Future（线程）继续执行；锁仍由本协程持有，守护任务接手后
            # 直到工作真正结束才释放，下一轮绝不可能提前开跑。
            result = await asyncio.shield(future)
        except asyncio.CancelledError:
            self._arm_guardian(future, reason)
            raise
        except BaseException:
            # 工作线程本身异常（极少）：先释放闸门再抛出，避免死锁。
            self._clear_current(future)
            self._release_lock()
            raise
        else:
            self._app.state.last_cleanup_run = result
            self._clear_current(future)
            self._release_lock()
            return result

    def _arm_guardian(self, future: asyncio.Future, reason: str) -> None:
        async def guardian() -> None:
            try:
                # 锁仍被取消的 run() 协程持有；这里等实际工作结束。
                # run_in_executor 的 Future 无法真正取消工作线程。
                result = await asyncio.shield(future)
                self._app.state.last_cleanup_run = result
            except asyncio.CancelledError:
                # 停机阶段守护被取消：工作若已结束仍补记；锁的最终释放
                # 由 lifespan 的 gate.aclose()（等待锁可用）兜底
                if future.done() and not future.cancelled():
                    self._app.state.last_cleanup_run = future.result()
                raise
            except Exception:  # noqa: BLE001 - 守护不能炸掉事件循环
                log.exception("清理 pass（%s）工作线程异常", reason)
            finally:
                self._clear_current(future)
                self._release_lock()
                log.info("被取消等待的清理 pass（%s）实际工作已结束，闸门放行", reason)

        asyncio.create_task(guardian())

    def _clear_current(self, future: asyncio.Future) -> None:
        if self._current is future:
            self._current = None
            self._current_reason = None
            self._current_started_ms = None

    def _release_lock(self) -> None:
        if self._lock.locked():
            self._lock.release()

    async def aclose(self) -> None:
        """关闭进程前等待仍在运行的清理工作真正结束（优雅停机）。

        被取消等待的 run() 把锁的释放责任交给守护任务；这里以“能拿到锁”
        作为工作已收尾的权威信号，避免在守护任务调度之前提前停机。
        """
        if not self._lock.locked():
            return
        log.info("停机等待在途清理工作结束 ...")
        await self._lock.acquire()
        self._release_lock()


async def run_cleanup_once(app: Any, reason: str) -> dict[str, Any]:
    """通过进程内闸门执行一轮清理；取消等待不取消实际删除工作。"""
    return await app.state.cleanup_gate.run(reason)


async def cleanup_loop(app: Any) -> None:
    """后台循环：到期的删除资格认领（含崩溃遗留的租约过期分区）在这里被持续拾起。

    先等待一个周期间隔再执行首轮，给演示/保全操作留出时间窗；
    手动 POST /cleanup/run 不受此延迟影响。
    """
    log.info(
        "后台清理循环已启动：interval=%ss lease=%ss owner=%s",
        config.CLEANUP_INTERVAL_SECONDS, config.DELETE_LEASE_SECONDS, config.OWNER,
    )
    while True:
        await asyncio.sleep(config.CLEANUP_INTERVAL_SECONDS)
        try:
            await run_cleanup_once(app, "timer")
        except asyncio.CancelledError:
            # 关闭流程：若有在途工作，lifespan 会通过 gate.aclose 等它结束
            raise
        except Exception:  # noqa: BLE001 - 循环不允许因单次失败退出
            log.exception("清理 pass 异常，下个周期重试")
