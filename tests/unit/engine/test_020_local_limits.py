"""0.2.0 local executor state (review L-2, L-4): fixed-window debt, one cap and one probe per
process shared by every engine, whatever loop each runs on.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from llm_queue import LLMQueue
from llm_queue.engine.local import LocalLimits, _Breaker, _Bucket, _Budget

DEV = {
    "LLMQ_STORE_URL": "redis://localhost:6379/0",
    "LLMQ_ENVIRONMENT": "dev",
    "LLMQ_REGISTRY": '{"registry_version": 1, "models": {}}',
}
MINUTE = 60.0 * 1_000_000  # a whole minute far from 0, so `win == -1` is never it


def test_fixed_window_below_one_a_minute_carries_the_debt() -> None:
    """rpm 4 over 10 processes: 0.4 a minute here, so at most 1 call every 3 minutes (L-2)."""
    bucket = _Bucket(0.4, 1.0, "fixed")
    taken = 0
    for minute in range(30):
        now = MINUTE + minute * 60 + 1
        if bucket.wait(1, now) == 0:
            bucket.take(1, now)
            taken += 1
    assert taken <= 0.4 * 30 + 1  # 12 + the first; it was 30 (one per minute)
    assert taken >= 10


def test_fixed_window_wait_points_at_the_minute_the_debt_is_paid() -> None:
    bucket = _Bucket(0.4, 1.0, "fixed")
    bucket.take(1, MINUTE + 1)
    assert bucket.wait(1, MINUTE + 61) == pytest.approx(MINUTE + 180 - (MINUTE + 61))
    assert bucket.wait(1, MINUTE + 181) == 0
    assert bucket.left(MINUTE + 61) == 0.0  # 0.6 still owed


def test_one_estimate_above_the_share_is_paid_off_over_the_next_minutes() -> None:
    """tpm 10,000 over 10 processes, one call estimated at 5,000: 5 minutes of budget (L-2)."""
    bucket = _Bucket(1000.0, 1.0, "fixed")
    assert bucket.wait(5000, MINUTE + 1) == 0
    bucket.take(5000, MINUTE + 1)
    for minute in range(1, 5):  # 4000, 3000, 2000, 1000 still owed
        assert bucket.wait(500, MINUTE + minute * 60 + 1) > 0
    assert bucket.wait(1000, MINUTE + 5 * 60 + 1) == 0


def test_fixed_window_at_or_above_one_is_unchanged() -> None:
    bucket = _Bucket(2.0, 1.0, "fixed")
    for _ in range(2):
        assert bucket.wait(1, MINUTE + 1) == 0
        bucket.take(1, MINUTE + 1)
    assert bucket.wait(1, MINUTE + 2) == pytest.approx(58)
    assert bucket.wait(1, MINUTE + 61) == 0  # a full minute is not carried


def test_the_half_open_probe_is_claimed_once_per_process() -> None:
    """Two engines of one process asked together: only one may probe (L-4)."""
    limits = LocalLimits()
    limits.breakers[("m", "a/r1")] = _Breaker(state="open", until=0.0)
    budgets = [_Budget(("d", "m", "a/r1", "rpm"), 60.0, 1.0, "smooth", "r")]
    first = limits.claim("m", "a/r1", budgets, 1, 0)
    second = limits.claim("m", "a/r1", budgets, 1, 0)
    assert first == (True, True, 0.0, True)
    assert second[0] is False
    assert second[2] > 0


async def test_slots_are_shared_by_engines_on_other_loops() -> None:
    """A slot held on another thread's loop counts here, and is handed over on release."""
    limits = LocalLimits(concurrency=1)
    slots = limits.slots("m")
    assert limits.slots("m") is slots
    held, release, done = threading.Event(), threading.Event(), threading.Event()

    async def other() -> None:
        async with slots.held(False):
            held.set()
            await asyncio.to_thread(release.wait)
        done.set()

    thread = threading.Thread(target=lambda: asyncio.run(other()))
    thread.start()
    assert await asyncio.to_thread(held.wait, 5)
    waiter = asyncio.create_task(_hold(slots))
    await asyncio.sleep(0.05)
    assert slots.in_use == 1
    assert slots.waiting(False) == 1
    assert not waiter.done()
    release.set()
    await asyncio.wait_for(waiter, 5)
    await asyncio.to_thread(thread.join, 5)
    assert done.is_set()
    assert slots.in_use == 0


async def _hold(slots: object) -> None:
    async with slots.held(False):  # type: ignore[attr-defined]
        await asyncio.sleep(0)


async def test_a_cancelled_waiter_passes_the_slot_on() -> None:
    limits = LocalLimits(concurrency=1)
    slots = limits.slots("m")
    gate = asyncio.Event()

    async def holder() -> None:
        async with slots.held(False):
            await gate.wait()

    first = asyncio.create_task(holder())
    await asyncio.sleep(0)
    cancelled = asyncio.create_task(_hold(slots))
    last = asyncio.create_task(_hold(slots))
    await asyncio.sleep(0.01)
    cancelled.cancel()
    gate.set()
    await asyncio.wait_for(asyncio.gather(first, last), 2)
    assert cancelled.cancelled()
    assert slots.in_use == 0
    assert slots.waiting(False) == 0


def test_queue_mode_engines_share_one_static_cap_state() -> None:
    """The started engine and the sync calls' private engine: one cap, budget, probe (L-4)."""
    llmq = LLMQueue.from_env({**DEV, "LLMQ_LOCAL_CONCURRENCY": "3"})
    builder = llmq._builder
    assert builder is not None
    private = builder()
    assert private.local.limits is llmq._engine.local.limits  # type: ignore[union-attr]
    assert llmq._engine.local.limits.concurrency == 3  # type: ignore[union-attr]
