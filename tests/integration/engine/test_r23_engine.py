"""Round 2 / round 3 engine findings (FE-*, RV-*, R3RC-*, R3FM-*), each pinned by a test.

Each test names its finding and what used to happen. Store faults are injected by patching the
engine's store helpers; no real LLM is called.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from engine_testkit import CHAT, Make, away_from_minute_end

from llm_queue.engine.admission import Decision

pytestmark = pytest.mark.integration

SLOW = {"defaults": {"latency_ms": {"p50": 8000, "p95": 8000}}}


async def _until(check: Any, secs: float = 3.0, what: str = "condition") -> None:
    """Poll `check()` until it is true; fail after `secs`."""
    loop = asyncio.get_running_loop()
    end = loop.time() + secs
    while not check():
        if loop.time() > end:
            raise AssertionError(f"{what} not reached in {secs:g} s")
        await asyncio.sleep(0.02)


async def _until_async(check: Any, secs: float = 3.0, what: str = "condition") -> None:
    """Poll the coroutine `check()` until it returns true; fail after `secs`."""
    loop = asyncio.get_running_loop()
    end = loop.time() + secs
    while not await check():
        if loop.time() > end:
            raise AssertionError(f"{what} not reached in {secs:g} s")
        await asyncio.sleep(0.02)


async def _confirmed(engine: Any, rid: str) -> bool:
    """True once the call's record says its send landed."""
    return (await engine.requests.job(rid)).get("confirmed") == "1"


async def _cancel(*tasks: asyncio.Task[Any]) -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


# ---------------------------------------------------------------- FE-1, FE-2, FE-6, RV-N2


async def test_fe1_a_given_back_call_releases_its_claim(make: Make) -> None:
    """FE-1: a call given back at the drain's end kept its claim's heartbeat fresh.

    Every other pod's PICK_TAKE answered `duplicate` until the claim was a lease old (30-60 s
    with production timers). The give-back now sets the claim's heartbeat to 0.
    """
    engine, _ = await make(None, SLOW)
    caller = asyncio.create_task(engine.call("small", CHAT, timeout_secs=8, request_id="fe1"))
    await _until(lambda: any(h.run for h in engine._held.values()), what="a running call")
    job = await engine.requests.job("fe1")
    assert job["state"] == "running"
    assert job["hb"] != "0"
    await engine.stop(drain_secs=0)
    job = await engine.requests.job("fe1")
    assert job["hb"] == "0"  # a fresh heartbeat before: a duplicate elsewhere for a lease
    await _cancel(caller)


async def test_fe2_a_call_received_during_stop_is_returned(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FE-2: a call received while `stop()` ran, after the held calls were handed back, stayed.

    Its hold ended at once (`_stopping`) without returning the message: hidden for the drain
    plus the visibility time and charged a failure. It is now returned.
    """
    engine, _ = await make()
    gate = asyncio.Event()
    real = engine.queues.receive_first
    returned: list[str] = []
    real_return = engine.queues.return_

    async def receive_first(*args: Any, **kwargs: Any) -> Any:
        await gate.wait()
        return await real(*args, **kwargs)

    async def return_(message: Any, delay_secs: float = 0.0) -> bool:
        returned.append(message.id)
        return await real_return(message, delay_secs)

    async def decide(*args: Any, **kwargs: Any) -> Decision:
        return Decision("wait", wait_secs=0.05)

    monkeypatch.setattr(engine.queues, "receive_first", receive_first)
    monkeypatch.setattr(engine.queues, "return_", return_)
    monkeypatch.setattr(engine, "_decide", decide)
    await asyncio.sleep(0.3)  # the loops are waiting inside the blocked receive
    caller = asyncio.create_task(engine.call("small", CHAT, timeout_secs=8, request_id="fe2"))
    await _until_async(lambda: _confirmed(engine, "fe2"), what="the call sent")
    stopping = asyncio.create_task(engine.stop(drain_secs=1.5))
    await _until(lambda: engine._stopping, what="stop started")
    gate.set()
    await _until(lambda: bool(returned), what="the late call returned")
    assert not engine._held
    await stopping
    await _cancel(caller)


async def test_fe6_stop_with_no_drain_still_returns_waiting_calls(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FE-6: `stop(drain_secs=0)` returned no waiting call: the deadline had already passed.

    The gather was cancelled at its first await; every waiting call stayed hidden for its
    visibility time and was charged a failure. The returns now get a short floor.
    """
    await away_from_minute_end()
    engine, _ = await make({"small": {"rpm": 1, "max_wait_secs": 20, "timeout_secs": 20}})
    assert (await engine.call("small", CHAT)).ok
    returned: list[bool] = []
    real_return = engine.queues.return_

    async def return_(message: Any, delay_secs: float = 0.0) -> bool:
        ok = await real_return(message, delay_secs)
        returned.append(ok)
        return ok

    monkeypatch.setattr(engine.queues, "return_", return_)
    second = asyncio.create_task(engine.call("small", CHAT, timeout_secs=20))
    await _until(lambda: any(h.hold for h in engine._held.values()), what="a held call")
    await engine.stop(drain_secs=0)
    assert returned == [True]  # nothing returned before
    await _cancel(second)


async def test_rvn2_a_hold_cancelled_before_its_first_step_frees_its_class(make: Make) -> None:
    """RV-N2: a hold task cancelled before its first step never ran its `finally`.

    Its class of the model stayed in `_held_class`, so this process never received that
    class again. A done-callback now frees it.
    """
    engine, _ = await make()
    from test_audit_runtime import _a_held

    held = _a_held(engine, "rvn2")
    engine._held["rvn2"] = held
    key = ("small", "n")
    engine._held_class.add(key)
    task = held.hold = asyncio.create_task(engine._hold(held, 5.0))
    task.add_done_callback(lambda done: engine._hold_cancelled(done, key))
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await asyncio.sleep(0)
    assert key not in engine._held_class
