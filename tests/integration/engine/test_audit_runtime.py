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
from engine_testkit import CHAT, GPT, SMALL, Make, away_from_minute_end, stats
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ResponseError

from llm_queue.engine import runtime
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
    now = engine.clock.now()
    return _Held(None, env, first_seen_secs=now, position_ms=position_ms, tag="dtest")


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
    caller = asyncio.create_task(engine.call("small", CHAT, timeout_secs=8, request_id="rt6"))
    await _until(lambda: any(h.run for h in engine._held.values()), what="a running call")
    returned: list[str] = []
    real_return = engine.queues.return_

    async def return_(message: Any, delay_secs: float = 0.0) -> bool:
        returned.append(message.id)
        return await real_return(message, delay_secs)

    engine.queues.return_ = return_  # type: ignore[method-assign]  # record the give-back
    await engine.stop(drain_secs=0)
    assert not engine._settling
    assert not engine._held
    # The give-back really ran before `stop()` returned (RV-T1: the test checked only the set).
    assert returned
    job = await engine.requests.job("rt6")
    assert job.get("hb") == "0"  # its claim released too (FE-1)
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
    trouble = engine._store_trouble_mono
    await _until(lambda: "step=cancels" in caplog.text or "'cancels'" in caplog.text, secs=4)
    assert "step=renew" not in caplog.text
    assert engine._store_trouble_mono == trouble


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


# ---------------------------------------------------------------- RT-9 to RT-12


@pytest.mark.parametrize("failure", [asyncio.CancelledError(), StoreConnectionError("cut")])
async def test_rt9_a_failed_abandon_never_replaces_the_send_error(
    make: Make, monkeypatch: pytest.MonkeyPatch, failure: BaseException
) -> None:
    """Audit RT-9: `_send` awaited `abandon` bare in its handlers.

    Its store error replaced the original (a cancel turned into a store error).
    """
    engine, _ = await make()
    view = engine.registry.current

    async def send(*args: Any, **kwargs: Any) -> Any:
        raise failure

    async def abandon(*args: Any, **kwargs: Any) -> None:
        raise StoreConnectionError("abandon failed too")

    monkeypatch.setattr(engine.queues, "send", send)
    monkeypatch.setattr(engine.requests, "abandon", abandon)
    env = _a_held(engine, "r-send").envelope
    with pytest.raises(type(failure)) as info:
        await engine._send("r-send", env, view, "small", False, 10.0)
    assert info.value is failure


async def test_rt10_model_fallback_status_failure_keeps_the_grant_running(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-10: a failed status write after a fallback grant left the grant unrun.

    It was neither run nor given back and held its slot until the lease sweep.
    """
    engine, _ = await make(
        {
            "big": {
                "rpm": 1,
                "rate_window": "smooth",
                "deployments": {"aws-a": {"confirmed": ["r1"]}},
                "model_fallback": {"on": ["exhausted"], "after_secs": 0, "models": ["small"]},
            }
        }
    )
    assert (await engine.call("big", CHAT)).ok

    async def status(*args: Any, **kwargs: Any) -> bool:
        raise StoreConnectionError("status write failed")

    monkeypatch.setattr(engine.requests, "set_status", status)
    second = await engine.call("big", CHAT, timeout_secs=8)
    assert second.meta.served_model == "small"


async def test_rt11_a_refused_finish_keeps_a_successful_call_done(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-11: a FINISH refused (out of memory) after success refunded and re-ran it.

    It raised out of the gather while the ack ran as an orphan; the grant was still listed, so
    `_release` gave back the whole estimate and returned the message.
    """
    from redis.exceptions import OutOfMemoryError

    engine, fake = await make()
    undone: list[str] = []
    real_undo = engine.admission.undo

    async def finish(*args: Any, **kwargs: Any) -> bool:
        raise OutOfMemoryError("OOM command not allowed")

    async def undo(alias: str, owner: str, rid: str) -> bool:
        undone.append(rid)
        return bool(await real_undo(alias, owner, rid))

    monkeypatch.setattr(engine.admission, "finish", finish)
    monkeypatch.setattr(engine.admission, "undo", undo)
    reply = await engine.call("small", CHAT)
    assert reply.ok
    await asyncio.sleep(0.5)  # a second run, if any, would have reached the provider by now
    assert not undone
    assert stats(fake, f"{SMALL}@r1")["calls"] == 1
    await _until(lambda: not engine._held, what="nothing held")


async def test_rt12_debug_capture_failure_never_reruns_the_call(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-12: a store error writing the debug capture released a call that succeeded.

    It ran again at the provider. The capture still comes before the result (the docstring
    said after: RV-T1) and never fails it.
    """
    engine, fake = await make({"small": {"debug_capture": True}})
    engine.settings = dataclasses.replace(engine.settings, debug_capture_allowed=True)

    async def debug(*args: Any, **kwargs: Any) -> None:
        raise StoreConnectionError("debug write failed")

    monkeypatch.setattr(engine, "_debug", debug)
    reply = await engine.call("small", CHAT)
    assert reply.ok
    await asyncio.sleep(0.5)
    assert stats(fake, f"{SMALL}@r1").get("duplicates", 0) == 0
    assert stats(fake, f"{SMALL}@r1")["calls"] == 1


# ---------------------------------------------------------------- RT-13 to RT-19, EN-2, CO-9


async def _hang(*args: Any, **kwargs: Any) -> Any:
    await asyncio.sleep(30)


@pytest.mark.parametrize("fault", ["hang", "refused"])
async def test_rt13_start_is_bounded_on_a_dead_store(
    make: Make, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """Audit RT-13: `start()` had no timeout and let a store refusal out raw.

    A blackholed store held start for connect timeouts times retries (41 s per command); an
    out-of-memory refusal raised redis-py's own error. Start now goes on, bounded.
    """
    engine, _ = await make()
    fresh = Engine(engine.state, engine.registry, engine.providers, settings=engine.settings)
    monkeypatch.setattr(runtime, "START_PROBE_SECS", 0.3)

    async def refused(*args: Any, **kwargs: Any) -> Any:
        raise ResponseError("OOM command not allowed when used memory > 'maxmemory'")

    if fault == "hang":
        monkeypatch.setattr(fresh.registry, "boot", _hang)
    monkeypatch.setattr(fresh.clock, "sync", _hang if fault == "hang" else refused)
    started = time.monotonic()
    await fresh.start()
    try:
        assert time.monotonic() - started < 1.5
        assert fresh.ready
    finally:
        await fresh.stop(drain_secs=0)


async def test_rt14_a_store_answering_with_errors_fails_paused_calls_cleanly(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-14: a probe `ping` refused (MISCONF, NOAUTH) raised raw out of `_store_back`."""
    engine, _ = await make({"small": {"store_down": "pause"}})
    engine.settings = dataclasses.replace(engine.settings, caller_margin_secs=0.5)

    async def cut(*args: Any, **kwargs: Any) -> Any:
        raise StoreConnectionError("Connection closed by server.")

    async def misconf(**kwargs: Any) -> Any:
        raise ResponseError("MISCONF Redis is configured to save RDB snapshots")

    monkeypatch.setattr(engine.requests, "register_send", cut)
    monkeypatch.setattr(engine.state, "ping", misconf)
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, timeout_secs=2)
    assert info.value.kind in ("queue_unavailable", "deadline_exceeded")
    assert info.value.request_id


async def test_rt16_a_deployment_removed_since_the_grant_is_retried(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-16: a reload removing the granted deployment raised StopIteration in `_attempt`.

    It surfaced as a bare `run_failed` and the call ran again. The grant now goes back in full
    and the attempt asks for another deployment.
    """
    engine, _ = await make()
    finished: list[dict[str, Any]] = []

    async def finish(*args: Any, **kwargs: Any) -> bool:
        finished.append(kwargs)
        return True

    monkeypatch.setattr(engine.admission, "finish", finish)
    held = _a_held(engine, "r-removed")
    result = await engine._attempt(
        engine.registry.current, "small", held, "aws-a/r9", "owner|w@t|1", 1
    )
    assert (result.outcome, result.kind) == ("retry", "target")
    assert finished
    assert finished[0]["outcome"] == "none"
    assert finished[0]["refund_r"] == 1


async def test_rt17_an_expired_id_found_on_re_registration_fails_at_once(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-17: after a re-registration only `conflict` was checked.

    An `expired` outcome went back to waiting for a result that was gone, until the deadline.
    """
    from llm_queue.engine.requests import Registration

    engine, _ = await make()
    outcomes = iter(["attached", "expired"])

    async def register_send(*args: Any, **kwargs: Any) -> Registration:
        return Registration(next(outcomes))

    async def nothing(*args: Any, **kwargs: Any) -> Any:
        return None

    async def no_job(*args: Any, **kwargs: Any) -> dict[str, str]:
        return {}

    monkeypatch.setattr(engine.requests, "register_send", register_send)
    monkeypatch.setattr(engine.requests, "take_result", nothing)
    monkeypatch.setattr(engine.requests, "job", no_job)
    started = time.monotonic()
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, request_id="r-expired", timeout_secs=8)
    assert info.value.kind == "result_expired"
    assert info.value.request_id == "r-expired"  # RT-18: the id is on every caller's error
    assert time.monotonic() - started < 2


async def test_rt19_static_cap_stops_on_a_request_error(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-19: `static_cap` retried every error on each deployment, unlogged.

    A validation error went to every deployment and came back as `provider_error`.
    """
    engine, fake = await make(
        {
            "big": {
                "store_down": "static_cap",
                "deployments": {"aws-a": {"confirmed": ["r1", "r2"]}},
            }
        },
        {
            "models": {
                f"{GPT}@r1": {"force_error": "ValidationException"},
                f"{GPT}@r2": {"force_error": "ValidationException"},
            }
        },
    )

    async def cut(*args: Any, **kwargs: Any) -> Any:
        raise StoreConnectionError("Connection closed by server.")

    monkeypatch.setattr(engine.requests, "register_send", cut)
    with pytest.raises(CallFailed) as info:
        await engine.call("big", CHAT)
    assert info.value.kind == "invalid_request"
    calls = stats(fake, f"{GPT}@r1").get("calls", 0) + stats(fake, f"{GPT}@r2").get("calls", 0)
    assert calls == 1


async def test_en2_an_unusable_request_fails_once(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """EN-2: any error building the admission ask put the call straight back (`_release`).

    It ran again and again until dead-lettered. It now fails at once with `invalid_request`.
    """
    engine, _ = await make()

    def broken(*args: Any, **kwargs: Any) -> Any:
        raise TypeError("an estimator that cannot read this request")

    monkeypatch.setattr(engine, "_ask", broken)
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, timeout_secs=8)
    assert info.value.kind == "invalid_request"
    await _until(lambda: not engine._held, what="nothing held")


async def test_co9_a_corrupt_registry_counter_never_fails_a_call(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CO-9: a corrupt stored change counter (ValueError) raised out of a registry refresh.

    It reached the live call that met `no_config`. It is now logged and the call goes on.
    """
    engine, _ = await make()
    real = engine.admission.pick_take
    first = {"done": False}

    async def pick_take(*args: Any, **kwargs: Any) -> Decision:
        if not first["done"]:
            first["done"] = True
            return Decision("no_config")
        got: Decision = await real(*args, **kwargs)
        return got

    async def corrupt() -> None:
        raise ValueError("invalid literal for int() with base 10: b'x'")

    monkeypatch.setattr(engine.admission, "pick_take", pick_take)
    monkeypatch.setattr(engine.registry, "refresh", corrupt)
    reply = await engine.call("small", CHAT, timeout_secs=8)
    assert reply.ok


# ---------------------------------------------------------------- RT-22 to RT-32, CO-*, CL-6


async def test_rt22_free_slots_are_read_without_private_state(make: Make) -> None:
    """Audit RT-22: the receive loop read `asyncio.Semaphore._value` (private, unpromised)."""
    engine, _ = await make({"small": {"worker_slots": 2}})
    slots = engine._slots["small"]
    assert (slots.size, slots.free) == (2, 2)
    async with slots:
        assert slots.free == 1
    assert slots.free == 2


async def test_rt23_a_live_worker_slots_edit_is_warned_once(
    make: Make, caplog: pytest.LogCaptureFixture
) -> None:
    """Audit RT-23: a live `worker_slots` edit did nothing until restart, and nothing said so."""
    engine, _ = await make({"small": {"worker_slots": 2}})
    engine._slots["small"].size = 5  # as if started with 5; the registry says 2
    engine._slots["small"].warned = 5
    engine._sync_model_loops()
    engine._sync_model_loops()
    assert caplog.text.count("worker_slots_changed_needs_restart") == 1


async def test_rt24_a_release_that_cannot_return_is_logged(
    make: Make, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Audit RT-24: `_release` gave up silently; the message stayed hidden and nobody saw why."""
    from llm_queue.engine.queue import Message

    engine, _ = await make()
    engine.settings = dataclasses.replace(engine.settings, lease_secs=0.2)

    async def down(*args: Any, **kwargs: Any) -> Any:
        raise StoreConnectionError("down")

    monkeypatch.setattr(engine.queues, "return_", down)
    held = _a_held(engine, "r-release")
    held.message = Message("q", "1-0", 1, b"{}")
    await engine._release(held)
    assert "release_return_failed" in caplog.text


async def test_rt27_the_probe_lock_is_held_for_each_probe(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Audit RT-27: the probe lock lived 5 s while one pass probed for up to 30 s per deployment.

    Another pod took the lock mid-pass and probed the same deployments at the same time.
    """
    engine, _ = await make()
    key = f"{engine.keys.prefix}:lock:probe"
    await engine.state.set(key, engine.worker_id, ex=5)

    async def disabled(alias: str) -> dict[str, float]:
        return {"aws-a/r1": 0.0}

    async def probe(*args: Any) -> bool:
        return True

    async def probed(*args: Any, **kwargs: Any) -> str:
        return "recovered"

    monkeypatch.setattr(engine.health, "disabled", disabled)
    monkeypatch.setattr(engine, "_probe", probe)
    monkeypatch.setattr(engine.health, "probed", probed)
    await engine.probe_due("small", lock_key=key)
    assert await engine.state.ttl(key) > 30  # the probe's whole budget


async def test_rt31_model_unavailable_reads_breakers_in_one_round_trip(
    make: Make, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Audit RT-31: the `model_unavailable` diagnosis read each breaker in its own round trip."""
    engine, _ = await make()
    await engine.state.hset(engine.keys.breaker("big", "aws-a/r1"), "state", "open")

    async def none(*args: Any, **kwargs: Any) -> Decision:
        return Decision("none")

    reads: list[str] = []
    real = engine.state.hgetall

    async def hgetall(*args: Any, **kwargs: Any) -> Any:
        reads.append(str(args[0]))
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine.admission, "pick_take", none)
    monkeypatch.setattr(engine.state, "hgetall", hgetall)
    with pytest.raises(CallFailed) as info:
        await engine.call("big", CHAT, timeout_secs=8)
    assert info.value.kind == "model_unavailable"
    assert "breaker open" in caplog.text
    assert not [r for r in reads if ":cb:" in r]  # 3 HGETALLs before (one per deployment)


async def test_rt32_two_waiters_on_one_id_both_wake(make: Make) -> None:
    """Audit RT-32: the shared wake-up was dropped by whichever waiter left first.

    The bookkeeping is one context now; two coroutines on one id still both get the reply.
    """
    engine, _ = await make()
    first, second = await asyncio.gather(
        engine.call("small", CHAT, request_id="r-shared"),
        engine.call("small", CHAT, request_id="r-shared"),
    )
    assert first.ok
    assert second.ok
    assert "r-shared" not in engine._waiters
    assert "r-shared" not in engine._local_callers


async def test_co1_aclose_closes_every_client_and_raises_the_first_failure(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CO-1: a provider close that raised left both store clients open."""
    engine, _ = await make()
    closed: list[str] = []

    async def providers_fail() -> None:
        raise RuntimeError("provider close failed")

    async def state_close() -> None:
        closed.append("state")

    monkeypatch.setattr(engine.providers, "aclose", providers_fail)
    monkeypatch.setattr(engine.state, "aclose", state_close)
    with pytest.raises(RuntimeError, match="provider close failed"):
        await engine.aclose()
    assert closed == ["state"]


async def test_co4_registry_reload_errors_are_counted(make: Make) -> None:
    """CO-4: the registry store had no metrics sink, so reload failures were only logged."""
    engine, _ = await make()
    assert engine.registry.metrics is engine.metrics
    counted: list[str] = []

    class Sink:
        def count(self, name: str, value: float = 1.0, **labels: str) -> None:
            counted.append(name)

    engine.registry.metrics = Sink()  # a counting fake
    engine.registry._count_reload_error(ValueError("corrupt counter"))
    assert counted == ["llmq_registry_reload_errors_total"]


async def test_cl6_openai_routes_are_checked_at_start(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CL-6: only Bedrock routes went to `warm`, so a missing OpenAI key showed on a call."""
    from engine_testkit import registry as test_registry

    from llm_queue.registry import ResolvedRegistry
    from llm_queue.registry.validate import check

    engine, _ = await make()
    doc = test_registry()
    doc["accounts"]["vllm"] = {
        "kind": "openai",
        "base_url": "http://127.0.0.1:9/v1",
        "api_key_ref": "LLMQ_ACCOUNT_VLLM_KEY",
    }
    doc["models"]["local"] = {
        "type": "chat",
        "provider": "openai",
        "model_id": "gpt-oss-120b",
        "deployments": {"vllm": {"confirmed": ["dc1"]}},
    }
    resolved, errors, _ = check(doc, allowed_hosts=("127.0.0.1",))
    assert resolved is not None, errors
    monkeypatch.setattr(engine.registry, "_current", ResolvedRegistry(resolved))
    seen: list[str] = []

    async def warm(routes: Any, slots: Any = None) -> None:
        seen.extend(r.account.kind for r in routes)

    monkeypatch.setattr(engine.providers.pool, "warm", warm)
    await engine._warm_clients()
    assert "openai" in seen
