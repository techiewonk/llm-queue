"""Findings of the runtime audit (docs/internal/audit, RT-1 to RT-33), each pinned by a test.

Each test names its finding and what used to happen. Store faults are injected by patching the
engine's store helpers; no real LLM is called.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import time
from typing import Any

import pytest
from engine_testkit import CHAT, Make, away_from_minute_end
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ResponseError

from llm_queue.engine.admission import Decision
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.runtime import Engine

pytestmark = pytest.mark.integration


async def _until(check: Any, secs: float = 3.0, what: str = "condition") -> None:
    """Poll `check()` until it is true; fail after `secs`."""
    loop = asyncio.get_running_loop()
    end = loop.time() + secs
    while not check():
        if loop.time() > end:
            raise AssertionError(f"{what} not reached in {secs:g} s")
        await asyncio.sleep(0.02)


# ---------------------------------------------------------------- RT-1


async def test_rt1_direct_run_refused_by_the_store_fails_fast(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-1: a direct run's store refusal was lost in an unawaited task.

    The caller waited until its deadline and got `deadline_exceeded` (and asyncio logged "Task
    exception was never retrieved"). It now gets the real error at once.
    """
    engine, _ = await make()

    async def refused(*args: Any, **kwargs: Any) -> Any:
        raise ResponseError("ERR script failed")

    monkeypatch.setattr(engine.admission, "pick_take", refused)
    started = time.monotonic()
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, force_direct=True, timeout_secs=8)
    assert info.value.kind == "queue_unavailable"
    assert info.value.request_id
    assert time.monotonic() - started < 2.0


async def test_rt1_direct_run_without_admission_numbers_fails_fast(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-1: `_decide` raises `CallFailed` for missing admission numbers on a direct run.

    The error landed in the run's task and the caller waited for its deadline.
    """
    engine, _ = await make()

    async def no_config(*args: Any, **kwargs: Any) -> Decision:
        return Decision("no_config")

    async def refresh() -> None:
        return None

    monkeypatch.setattr(engine.admission, "pick_take", no_config)
    monkeypatch.setattr(engine.registry, "refresh", refresh)
    started = time.monotonic()
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, force_direct=True, timeout_secs=8)
    assert info.value.kind == "queue_unavailable"
    assert "admission numbers" in str(info.value)
    assert time.monotonic() - started < 2.0
    await _until(lambda: not engine._held, what="nothing held")


# ---------------------------------------------------------------- RT-15


async def test_rt15_store_retries_are_jittered(make: Make) -> None:
    """Audit RT-15: store retries waited a fixed 0.25 s / 0.5 s, so every call retried in step.

    The pause is now random up to an exponential ceiling (0.1 s doubling, capped at 1 s).
    """
    engine, _ = await make()
    first = {round(engine._store_retry_secs(1), 4) for _ in range(50)}
    assert len(first) > 10
    assert all(0.01 <= p <= 0.1 for p in first)
    assert all(0.01 <= engine._store_retry_secs(9) <= 1.0 for _ in range(50))
    assert max(engine._store_retry_secs(9) for _ in range(200)) > 0.5


async def test_rt15_result_read_retries_through_a_blip(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-15: a caller's result read that meets a store blip retries (now jittered)."""
    engine, _ = await make()
    real = engine.requests.take_result
    fails = {"left": 3}

    async def take(*args: Any, **kwargs: Any) -> Any:
        if fails["left"]:
            fails["left"] -= 1
            raise StoreConnectionError("blip")
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine.requests, "take_result", take)
    reply = await engine.call("small", CHAT)
    assert reply.ok
    assert fails["left"] == 0


# ---------------------------------------------------------------- RT-3

SLOW = {"defaults": {"latency_ms": {"p50": 8000, "p95": 8000}}}


async def test_rt3_cancelled_calls_share_one_canceller(make: Make) -> None:
    """Audit RT-3: each cancelled caller started its own cleanup task (3 store calls each).

    Many cancels at once (clients leaving) made one store connection storm per call. One
    canceller now cancels them in batches, and every call is still cancelled.
    """
    engine, _ = await make(None, SLOW)
    tasks = [asyncio.create_task(engine.call("small", CHAT, timeout_secs=8)) for _ in range(20)]
    await _until(lambda: len(engine._local_callers) == 20, what="20 calls waiting")
    await asyncio.sleep(0.3)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    assert engine._canceller is not None
    await _until(lambda: engine._canceller is None or engine._canceller.done(), what="cancels")
    cancels = [k async for k in engine.state.scan_iter(match=engine.keys.request("*", "cancel"))]
    assert len(cancels) == 20


async def test_rt3_cancel_cleanup_is_time_bound_and_logged(
    make: Make, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Audit RT-3: a cleanup against a hung store ran without a timeout and hid its failure."""
    engine, _ = await make(None, SLOW)
    engine.settings = dataclasses.replace(
        engine.settings,
        timings=dataclasses.replace(engine.settings.timings, health_probe_secs=0.3),
    )
    batches: list[int] = []

    async def hung(rids: Any) -> None:
        batches.append(len(rids))
        await asyncio.sleep(30)

    monkeypatch.setattr(engine, "_cancel_batch", hung)
    tasks = [asyncio.create_task(engine.call("small", CHAT, timeout_secs=8)) for _ in range(10)]
    await _until(lambda: len(engine._local_callers) == 10, what="10 calls waiting")
    started = time.monotonic()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    assert time.monotonic() - started < 1.5  # each waited for the bounded batch, not 30 s
    await _until(lambda: engine._canceller is None or engine._canceller.done(), what="canceller")
    assert sum(batches) == 10
    assert len(batches) <= 2  # batched: not one cleanup per call
    assert "cancel_cleanup_failed" in caplog.text


# ---------------------------------------------------------------- RT-4, RT-25


async def _consume(engine: Engine) -> None:
    async for _ in engine.stream("small", CHAT, timeout_secs=8):
        pass


async def test_rt4_stream_close_lets_the_consumers_cancel_through(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-4: a stream's cleanup awaited its call task under `suppress(BaseException)`.

    A call task slow to stop (its cancel cleanup waits on the store) held the consumer: a
    second cancel was swallowed and closing the stream could not be cut short.
    """
    engine, _ = await make()
    stopped = asyncio.Event()

    async def stubborn(*args: Any, **kwargs: Any) -> Any:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(3)  # a cleanup that a second cancel does not stop
            stopped.set()
            raise

    monkeypatch.setattr(engine, "call", stubborn)
    consumer = asyncio.create_task(_consume(engine))
    await asyncio.sleep(0.2)
    consumer.cancel()  # the stream's `finally` now waits for the call task
    await asyncio.sleep(0.1)
    started = time.monotonic()
    consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer
    assert time.monotonic() - started < 0.5  # ~2.9 s before the fix
    await asyncio.wait_for(stopped.wait(), 5)


async def test_rt25_cancelled_stream_leaves_no_chunk_getter(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-25: a stream cancelled mid-wait left its chunk-queue getter task pending."""
    engine, _ = await make()

    async def waits(*args: Any, **kwargs: Any) -> Any:
        await asyncio.sleep(30)

    monkeypatch.setattr(engine, "call", waits)

    def getters() -> int:
        return sum(
            1
            for t in asyncio.all_tasks()
            if not t.done() and getattr(t.get_coro(), "__qualname__", "") == "Queue.get"
        )

    before = getters()
    consumers = [asyncio.create_task(_consume(engine)) for _ in range(50)]
    await asyncio.sleep(0.2)
    for consumer in consumers:
        consumer.cancel()
    await asyncio.gather(*consumers, return_exceptions=True)
    await asyncio.sleep(0)
    assert getters() == before  # 50 left pending before the fix


# ---------------------------------------------------------------- RT-5, L4, RT-6, RT-7, RT-33


def _a_held(engine: Engine, rid: str, *, deadline_in_secs: float = 30.0) -> Any:
    """A direct-path `_Held` for `small` (not registered in `engine._held`)."""
    from llm_queue.engine.envelope import Envelope
    from llm_queue.engine.runtime import _Held

    env = Envelope(
        request_id=rid, op="converse", model="small", params=CHAT, caller="c", timeout_secs=30
    )
    position_ms = int((engine.clock.now() + deadline_in_secs - 30) * 1000)
    return _Held(None, env, first_seen=engine.clock.now(), position_ms=position_ms, tag="dtest")


async def test_l4_a_grant_for_a_released_call_goes_back(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scenario L4: a call let go while its admission was in flight still started a run.

    `_free_stuck` (or `stop`) dropped it, its in-flight admission then granted it and
    `_start_run` ran it with a grant the heartbeat never renewed: its lease was left to the
    sweep, and leases stayed behind. The grant now goes straight back and nothing runs.
    """
    engine, _ = await make()
    undone: list[tuple[str, str, str]] = []

    async def undo(alias: str, owner: str, rid: str) -> None:
        undone.append((alias, owner, rid))

    monkeypatch.setattr(engine.admission, "undo", undo)
    held = _a_held(engine, "r-released")  # not in engine._held: let go already
    engine._start_run(held, Decision("granted", deployment="aws-a/r1"), 1)
    await _until(lambda: bool(undone), what="the grant given back")
    assert undone[0][0] == "small"
    assert held.run is None
    assert not held.grants


async def test_rt5_hold_ends_once_the_call_is_let_go(make: Make) -> None:
    """Audit RT-5: `_hold` checked `_stopping` only before its sleep, so it ran one more round.

    A call handed back by `stop()` (or freed) could be re-admitted and run after its message
    was already back on the queue. The hold now ends as soon as the call is no longer held.
    """
    engine, _ = await make()
    held = _a_held(engine, "r-hold")
    engine._held["r-hold"] = held
    decided: list[int] = []

    async def decide(*args: Any, **kwargs: Any) -> Decision:
        decided.append(1)
        return Decision("wait", wait_secs=0.05)

    engine._decide = decide  # never granted here
    task = held.hold = asyncio.create_task(engine._hold(held, 0.05))
    await _until(lambda: len(decided) >= 2, what="the hold polling")
    engine._drop(held)
    await asyncio.wait_for(task, 2)
    rounds = len(decided)
    await asyncio.sleep(0.2)
    assert len(decided) == rounds


async def test_rt5_stop_stops_the_holds_it_hands_back(make: Make) -> None:
    """Audit RT-5: `stop()` returned a held call's message but left its hold task running."""
    await away_from_minute_end()
    engine, _ = await make({"small": {"rpm": 1, "max_wait_secs": 20, "timeout_secs": 20}})
    first = await engine.call("small", CHAT)
    assert first.ok
    second = asyncio.create_task(engine.call("small", CHAT, timeout_secs=20))
    await _until(lambda: any(h.hold for h in engine._held.values()), what="a held call")
    holds = [h.hold for h in engine._held.values() if h.hold is not None]
    await engine.stop(drain_secs=0.5)
    assert all(h.done() for h in holds)
    assert not engine._held
    second.cancel()
    await asyncio.gather(second, return_exceptions=True)


async def test_rt6_shutdown_give_back_is_awaited_by_stop(make: Make) -> None:
    """Audit RT-6: the drain's give-back ran shielded as an orphan coroutine.

    `stop()` did not wait for it, so it kept writing while the clients were closed. It is now
    a kept task `stop()` waits for: none is left when `stop()` returns.
    """
    engine, _ = await make(None, SLOW)
    caller = asyncio.create_task(engine.call("small", CHAT, timeout_secs=8))
    await _until(lambda: any(h.run for h in engine._held.values()), what="a running call")
    await engine.stop(drain_secs=0)
    assert not engine._settling
    assert not engine._held
    caller.cancel()
    await asyncio.gather(caller, return_exceptions=True)


async def test_rt7_stop_can_be_cancelled(make: Make) -> None:
    """Audit RT-7: `stop()` awaited its tasks under `suppress(CancelledError, Exception)`.

    Its own cancel (a k8s hook timing out) was swallowed while a slow task finished.
    """
    engine, _ = await make()

    async def slow_to_stop() -> None:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(3)
            raise

    engine._tasks.append(asyncio.create_task(slow_to_stop()))
    stopping = asyncio.create_task(engine.stop(drain_secs=0))
    await asyncio.sleep(0.3)
    started = time.monotonic()
    stopping.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stopping
    assert time.monotonic() - started < 0.5


async def test_rt7_stop_logs_a_loop_that_died(make: Make, caplog: pytest.LogCaptureFixture) -> None:
    """Audit RT-7: a background loop that died with an error was swallowed silently at stop."""
    engine, _ = await make()

    async def broken() -> None:
        raise RuntimeError("loop bug")

    engine._tasks.append(asyncio.create_task(broken()))
    await asyncio.sleep(0)
    await engine.stop(drain_secs=0)
    assert "stop_task_failed" in caplog.text
    assert "loop bug" in caplog.text


async def test_rt33_heartbeat_steps_fail_on_their_own(
    make: Make, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Audit RT-33: a failing cancel check was logged as `renew` and counted as store trouble."""
    engine, _ = await make()

    async def broken() -> None:
        raise ResponseError("ERR mget refused")

    monkeypatch.setattr(engine, "_check_cancels", broken)
    trouble = engine._store_trouble_at
    await _until(lambda: "step=cancels" in caplog.text or "'cancels'" in caplog.text, secs=4)
    assert "step=renew" not in caplog.text
    assert engine._store_trouble_at == trouble


# ---------------------------------------------------------------- S11, RT-8


async def test_s11_worker_reclaims_a_receive_whose_reply_was_lost(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scenario S11: a cut reply to the worker's receive hid the call for the visibility time.

    The receive ran on the store (the message was taken), its reply was lost; the call waited
    60 s (the default `visibility_secs`). The worker now returns it at once and serves it.
    """
    engine, _ = await make()
    real = engine.queues.receive_first
    cut: list[int] = []

    async def receive_first(*args: Any, **kwargs: Any) -> Any:
        got = await real(*args, **kwargs)
        if got[0] and not cut:
            cut.append(len(got[0]))
            raise StoreConnectionError("Connection closed by server.")
        return got

    monkeypatch.setattr(engine.queues, "receive_first", receive_first)
    started = time.monotonic()
    reply = await engine.call("small", CHAT, timeout_secs=8)
    assert reply.ok
    assert cut
    assert time.monotonic() - started < 5  # 60 s before


async def test_rt8_a_failed_dead_letter_reply_skips_only_itself(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-8: a store error replying to a dead letter skipped the whole received batch.

    The messages received with it were neither admitted nor returned (hidden 60 s).
    """
    from llm_queue.engine.queue import DeadLetter

    engine, _ = await make()
    real = engine.queues.receive_first
    sent: list[int] = []

    async def receive_first(names: Any, *args: Any, **kwargs: Any) -> Any:
        messages, dead = await real(names, *args, **kwargs)
        if messages and not sent:
            sent.append(1)
            dead = [DeadLetter(names[0], "0-1", b"{}")]
        return messages, dead

    async def reply_fails(*args: Any, **kwargs: Any) -> None:
        raise StoreConnectionError("blip")

    monkeypatch.setattr(engine.queues, "receive_first", receive_first)
    monkeypatch.setattr(engine, "_dead_lettered", reply_fails)
    started = time.monotonic()
    reply = await engine.call("small", CHAT, timeout_secs=8)
    assert reply.ok
    assert sent
    assert time.monotonic() - started < 3
