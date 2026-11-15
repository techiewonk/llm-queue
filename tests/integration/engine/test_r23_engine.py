"""Round 2 / round 3 engine findings (FE-*, RV-*, R3RC-*, R3FM-*), each pinned by a test.

Each test names its finding and what used to happen. Store faults are injected by patching the
engine's store helpers; no real LLM is called.
"""

from __future__ import annotations

import asyncio
import dataclasses
import math
from typing import Any

import pytest
from engine_testkit import CHAT, Make, away_from_minute_end
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import OutOfMemoryError, ReadOnlyError, ResponseError
from redis.exceptions import TimeoutError as StoreTimeoutError

from llm_queue.engine.admission import Decision
from llm_queue.engine.envelope import Reply, params_hash
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.queue import Message
from llm_queue.engine.runtime import Engine, _store_refused

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


# ---------------------------------------------------------------- owner Q2-Q6, R3RC-1, FE-17


async def test_q6_an_app_worker_id_gets_a_random_part(make: Make) -> None:
    """R3RC-9 / owner Q6: an app's `worker_id` was used as is; two processes shared keys."""
    engine, _ = await make()
    one = Engine(engine.state, engine.registry, engine.providers, worker_id="pod-a")
    two = Engine(engine.state, engine.registry, engine.providers, worker_id="pod-a")
    assert one.worker_id.startswith("pod-a-")
    assert one.worker_id != two.worker_id


async def test_q4_send_refreshes_the_caller_key_for_two_beats(make: Make) -> None:
    """R3FM-1 / owner Q4: the caller key lived 30 s, renewed every 20 s, never by a send.

    One late renewal dropped a live caller's queued calls as `caller_gone`. It now lives two
    beats at least and every send refreshes it.
    """
    engine, _ = await make(worker=False)
    key = engine.keys.caller(engine.worker_id)
    await engine.state.delete(key)
    task = asyncio.create_task(engine.call("small", CHAT, timeout_secs=3))
    await _until_async(lambda: engine.state.exists(key), what="the key set by the send")
    ttl = await engine.state.ttl(key)
    assert ttl >= math.ceil(2 * engine.settings.lease_secs / 3)
    await _cancel(task)


async def test_q2_a_failed_id_runs_again(make: Make) -> None:
    """Owner Q2: a retry of an id that ended failed got the stored failure for 5 min.

    Then `result_expired` for 30 min. Only a success is replayed now; a failure runs again.
    """
    engine, _ = await make()
    rid = "q2-fail"
    await engine.requests.register(
        rid,
        caller="old",
        user_hash=engine.hash_id(""),
        params_hash=params_hash({"m": "small", "p": CHAT}),
        model="small",
        priority=False,
        app_id=True,
    )
    reply = Reply(request_id=rid, ok=False, error="provider_error", message="boom")
    await engine.requests.put_result(reply, final_state="failed")
    again = await engine.call("small", CHAT, request_id=rid)
    assert again.ok  # the stored failure before


async def test_q3_an_attach_after_a_cancel_is_cancelled(make: Make) -> None:
    """Owner Q3: a pending cancel wins; an attach to that id gets `cancelled` too.

    (After the call ended `cancelled`, a retry runs again: owner Q2.)
    """
    engine, _ = await make(worker=False)  # nobody runs it: the cancel stays pending
    first = asyncio.create_task(engine.call("small", CHAT, request_id="q3", timeout_secs=2))
    await _until_async(lambda: _confirmed(engine, "q3"), what="the call sent")
    await engine.cancel("q3")
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, request_id="q3", timeout_secs=2)
    assert info.value.kind == "cancelled"  # attached and waited before
    with pytest.raises(CallFailed):
        await first


async def test_r3rc1_two_pods_cancelling_a_shared_id_cancel_it(make: Make) -> None:
    """R3RC-1: two pods waiting on one id never cancelled it, each seeing the other's entry.

    The call ran to its end at the provider for nobody. Any caller's cancel wins now.
    """
    worker, _ = await make(None, SLOW)
    a, _ = await make(worker=False)
    b, _ = await make(worker=False)
    one = asyncio.create_task(a.call("small", CHAT, request_id="rc1", timeout_secs=8))
    await _until(lambda: "rc1" in worker._held, what="the call received")
    two = asyncio.create_task(b.call("small", CHAT, request_id="rc1", timeout_secs=8))
    await asyncio.sleep(0.3)
    await _cancel(one)
    await _until_async(
        lambda: worker.state.exists(worker.keys.request("rc1", "cancel")), what="the cancel"
    )
    with pytest.raises(CallFailed):
        await two


async def test_fe17_a_caller_only_direct_run_stops_on_cancel(make: Make) -> None:
    """FE-17: a caller-only process ran a cancelled direct call on to its own deadline."""
    engine, _ = await make(None, SLOW, worker=False)
    task = asyncio.create_task(
        engine.call("small", CHAT, request_id="fe17", timeout_secs=8, force_direct=True)
    )
    await _until(lambda: "fe17" in engine._held, what="the direct run")
    held = engine._held["fe17"]
    await _until(lambda: held.run is not None, what="the attempt running")
    await _cancel(task)
    run = held.run
    assert run is not None
    await _until(run.done, secs=2, what="the direct run stopped")


# ---------------------------------------------------------------- Q7, Q11, Q12, R3FM-2, FE-5


def test_q12_refusals_count_as_the_store_down() -> None:
    """R3FM-7 / owner Q12: NOREPLICAS, OOM, MISCONF and BUSY reached callers as refusals."""
    assert _store_refused(ResponseError("NOREPLICAS Not enough good replicas to write."))
    assert _store_refused(ResponseError("MISCONF Redis is configured to save RDB snapshots"))
    assert _store_refused(ResponseError("BUSY Redis is busy running a script."))
    assert _store_refused(OutOfMemoryError("command not allowed when used memory > maxmemory"))
    assert not _store_refused(ResponseError("ERR wrong number of arguments"))


async def test_q12_an_answer_in_hand_rides_out_a_refusal(make: Make) -> None:
    """R3FM-7: a NOREPLICAS refusal of the result write dropped the answer; the call re-ran."""
    engine, _ = await make()
    tries: list[int] = []

    async def write() -> None:
        tries.append(1)
        if len(tries) < 3:
            raise ResponseError("NOREPLICAS Not enough good replicas to write.")

    await engine._through_outage(write)
    assert len(tries) == 3  # raised at the first refusal before


async def test_q7_an_answer_in_hand_is_kept_to_the_calls_deadline(make: Make) -> None:
    """Owner Q7: the answer in hand was dropped after one lease (3 s here) of store outage.

    The call then ran again. It is now kept while its caller still waits (its deadline).
    """
    engine, _ = await make()
    from test_audit_runtime import _a_held

    held = _a_held(engine, "q7", deadline_in_secs=8.0)
    loop = asyncio.get_running_loop()
    started = loop.time()

    async def write() -> None:
        if loop.time() - started < engine.settings.lease_secs + 1.0:
            raise StoreConnectionError("connection refused")

    await engine._through_outage(write, held)  # raised after one lease before


async def test_q11_fail_fast_remembers_an_outage(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R3FM-3 / owner Q11: every `fail_fast` call re-discovered the outage at full cost.

    Each paid the client's retries (1.5-21.5 s). The outage is remembered for a few seconds.
    """
    engine, _ = await make({"small": {"store_down": "fail_fast"}})
    tried: list[int] = []

    async def down(*args: Any, **kwargs: Any) -> Any:
        tried.append(1)
        raise StoreConnectionError("connection refused")

    monkeypatch.setattr(engine, "_call_routed", down)
    monkeypatch.setattr(engine, "_spawn", lambda work, what="": _close(work))
    for _ in range(3):
        with pytest.raises(CallFailed) as info:
            await engine.call("small", CHAT, timeout_secs=5)
        assert info.value.kind == "queue_unavailable"
    assert len(tried) == 1  # three tries of the store before


def _close(work: Any) -> None:
    """Drop a background write in a test (its coroutine closed unrun)."""
    work.close()


async def test_r3fm2_static_cap_answer_is_stored_for_the_queued_copy(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R3FM-2: a send whose reply timed out ran at once under `static_cap`, and its queued
    copy ran a second time. The static answer is now stored, so the copy meets `done`.
    """
    engine, _ = await make({"small": {"store_down": "static_cap"}})

    async def timed_out(*args: Any, **kwargs: Any) -> Any:
        raise StoreTimeoutError("Timeout reading from socket")

    monkeypatch.setattr(engine, "_call_routed", timed_out)
    reply = await engine.call("small", CHAT, request_id="fm2", timeout_secs=5)
    assert reply.ok
    key = engine.keys.request("fm2", "res")
    await _until_async(lambda: engine.state.exists(key), what="the answer stored")


async def test_fe5_a_failed_direct_run_stores_its_failure(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FE-5: a direct run that raised wrote no reply; callers in other processes waited.

    A retry of the id attached to the dead record for 30 min. The failure is stored now.
    """
    engine, _ = await make()

    async def refused(*args: Any, **kwargs: Any) -> Any:
        raise ResponseError("ERR script failed")

    monkeypatch.setattr(engine.admission, "pick_take", refused)
    with pytest.raises(CallFailed):
        await engine.call("small", CHAT, request_id="fe5", force_direct=True, timeout_secs=5)
    job = await engine.requests.job("fe5")
    assert job.get("state") == "failed"  # confirmed and running for 30 min before


# ---------------------------------------------------------------- R3RC-5, R3RC-4, R3FM-4


async def test_r3rc5_a_stored_result_is_read_at_once(make: Make) -> None:
    """R3RC-5: a retry of an id whose result was stored waited for a wake-up first.

    Nobody published again: it waited for the sweep (`poll_secs`, 5 s by default).
    """
    engine, _ = await make()
    assert (await engine.call("small", CHAT, request_id="rc5")).ok
    engine.settings = dataclasses.replace(engine.settings, poll_secs=3.0)
    loop = asyncio.get_running_loop()
    started = loop.time()
    assert (await engine.call("small", CHAT, request_id="rc5")).ok
    assert loop.time() - started < 1.0


async def test_r3rc4_an_attached_caller_takes_over_a_send_that_never_happened(
    make: Make,
) -> None:
    """R3RC-4: a caller attached to an id whose sender died before sending waited 60 s.

    Its recheck came only every `result_recheck_secs`; a 10 s call got `deadline_exceeded`.
    It now checks after the takeover grace and takes the call over.
    """
    engine, _ = await make()
    rid = "rc4"
    await engine.requests.register(
        rid,
        caller="ghost",
        user_hash=engine.hash_id(""),
        params_hash=params_hash({"m": "small", "p": CHAT}),
        model="small",
        priority=False,
        app_id=True,
    )
    reply = await engine.call("small", CHAT, request_id=rid, timeout_secs=10)
    assert reply.ok


async def test_r3fm4_a_call_whose_record_the_store_lost_is_sent_again(make: Make) -> None:
    """R3FM-4 / R3RC-2: after a store lost its data, waiting callers sat to their deadline.

    Only running grants were put back. At least once (owner D1): the result sweep finds the
    record gone and the caller sends the call again.
    """
    caller, _ = await make(worker=False)
    task = asyncio.create_task(caller.call("small", CHAT, request_id="fm4", timeout_secs=10))
    await _until_async(lambda: _confirmed(caller, "fm4"), what="the call sent")
    view = caller.registry.current
    name = caller._queue_name(view, "small", False)
    await caller.state.delete(caller.keys.request("fm4", "job"), *caller.queues.queue_keys(name))
    await make()  # a worker
    reply = await asyncio.wait_for(task, 8)
    assert reply.ok  # deadline_exceeded before
