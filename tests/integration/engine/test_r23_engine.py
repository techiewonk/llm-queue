"""Round 2 / round 3 engine findings (FE-*, RV-*, R3RC-*, R3FM-*), each pinned by a test.

Each test names its finding and what used to happen. Store faults are injected by patching the
engine's store helpers; no real LLM is called.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from engine_testkit import CHAT, Make, away_from_minute_end
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ReadOnlyError

from llm_queue.engine.admission import Decision
from llm_queue.engine.queue import Message

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


# ---------------------------------------------------------------- FE-7 to FE-20, RV-N3, FL-3


async def test_fe7_a_read_only_store_is_not_back(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FE-7: paused calls were released on a PING, which a read-only replica answers.

    During a failover they were released on a store that still refused writes, failed, paused
    again and repeated each second. The probe is now the bounded write probe.
    """
    engine, _ = await make()

    async def read_only(*args: Any, **kwargs: Any) -> Any:
        raise ReadOnlyError("You can't write against a read only replica.")

    monkeypatch.setattr(engine.state, "set", read_only)
    engine._store_waiting = 1
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(engine._ping_until_back(), 2.5)  # True after 1 s before


async def test_fe9_a_bad_message_whose_fail_errs_is_returned(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FE-9: an unreadable message whose `fail` met a store blip was in no list.

    It was hidden for its visibility time and charged a failure. It is now returned.
    """
    engine, _ = await make()
    returned: list[str] = []

    async def fail(*args: Any, **kwargs: Any) -> None:
        raise StoreConnectionError("connection reset")

    async def return_(message: Any, delay_secs: float = 0.0) -> bool:
        returned.append(message.id)
        return True

    monkeypatch.setattr(engine.queues, "fail", fail)
    monkeypatch.setattr(engine.queues, "return_", return_)
    view = engine.registry.current
    name = engine._queue_name(view, "small", False)
    await engine.queues.send(name, b"not an envelope", engine._queue_settings(view, "small"))
    await _until(lambda: bool(returned), what="the bad message returned")


async def test_fe14_a_stale_ack_is_logged(make: Make, caplog: pytest.LogCaptureFixture) -> None:
    """FE-14: `_ack` dropped the answer of `queues.ack`; a run that lost its message was silent."""
    engine, _ = await make()
    from test_audit_runtime import _a_held

    held = _a_held(engine, "fe14")
    held.message = Message(queue="q", id="1-0", generation=1, body=b"")

    async def ack(message: Any) -> bool:
        return False

    engine.queues.ack = ack  # type: ignore[method-assign]  # a stale receipt
    await engine._ack(held)
    assert "ack_stale_receipt" in caplog.text


async def test_fe15_one_failing_model_does_not_stop_the_sweep(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FE-15: one model whose sweep raised stopped the sweep for every model after it."""
    engine, _ = await make()
    swept: list[str] = []

    async def sweep(alias: str, deployments: Any = ()) -> int:
        if alias == "big":
            raise RuntimeError("bad model")
        swept.append(alias)
        return 0

    monkeypatch.setattr(engine.admission, "sweep", sweep)
    await _until(lambda: "small" in swept, secs=4, what="the models after it swept")


async def test_fe18_a_failing_free_stuck_does_not_end_the_heartbeat(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FE-18: `_free_stuck` was the only heartbeat step outside a `try`; it ended the beat."""
    engine, _ = await make()

    def broken() -> None:
        raise RuntimeError("bug")

    monkeypatch.setattr(engine, "_free_stuck", broken)
    seen = engine._beat_ok_mono
    await _until(lambda: engine._beat_ok_mono > seen + 0.5, secs=4, what="two more beats")
    assert engine._live()


async def test_fe19_background_loops_are_named(make: Make) -> None:
    """FE-19: `stop_task_failed` logged `Task-123`: no background task had a name."""
    engine, _ = await make()
    names = [t.get_name() for t in [*engine._tasks, *engine._model_tasks.values()]]
    assert names
    assert all(name.startswith("llmq:") for name in names)


async def test_rvn3_a_cancel_after_stop_starts_no_canceller(make: Make) -> None:
    """RV-N3: a cancel after `stop()` started a new canceller writing to closing clients."""
    engine, _ = await make()
    await engine.stop(drain_secs=0)
    done = engine._cancel_later("rvn3")
    assert done.done()
    assert engine._canceller is None or engine._canceller.done()


async def test_fl3_a_receive_cancelled_in_flight_is_reclaimed(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FL-3 / FE-8: a receive cancelled in flight left its batch hidden for the visibility time.

    Only store errors recorded the claim; a cancel (loop restart, shutdown) did not, and a
    full worker never reclaimed. Both now reclaim.
    """
    engine, _ = await make()
    blocked = asyncio.Event()

    async def receive_first(*args: Any, **kwargs: Any) -> Any:
        blocked.set()
        await asyncio.sleep(30)

    monkeypatch.setattr(engine.queues, "receive_first", receive_first)
    task = asyncio.create_task(engine._receive_once("titan"))
    await asyncio.wait_for(blocked.wait(), 2)
    await _cancel(task)
    assert "titan" in engine._unclaimed
    reclaimed: list[Any] = []

    async def reclaim(key: str, nonce: str) -> int:
        reclaimed.append((key, nonce))
        return 0

    monkeypatch.setattr(engine.queues, "reclaim", reclaim)
    engine._slots["titan"].free = 0  # a full worker reclaims too
    assert await engine._receive_once("titan") is None
    assert reclaimed
