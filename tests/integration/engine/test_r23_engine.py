"""Round 2 / round 3 engine findings (FE-*, RV-*, R3RC-*, R3FM-*), each pinned by a test.

Each test names its finding and what used to happen. Store faults are injected by patching the
engine's store helpers; no real LLM is called.
"""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import math
from typing import Any

import pytest
from engine_testkit import CHAT, Make, away_from_minute_end, ttl_of
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import OutOfMemoryError, ReadOnlyError, ResponseError
from redis.exceptions import TimeoutError as StoreTimeoutError

from llm_queue.engine import runtime
from llm_queue.engine.admission import Decision
from llm_queue.engine.envelope import Reply, params_hash
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.inference import InferenceRejected
from llm_queue.engine.queue import Message, NotReplicated, StreamQueues
from llm_queue.engine.runtime import Engine, _store_refused
from llm_queue.engine.store_client import StorePoolBusy, store_client

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
    return bool((await engine.requests.job(rid)).get("confirmed") == "1")


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
        return bool(await real_return(message, delay_secs))

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
        ok = bool(await real_return(message, delay_secs))
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

    engine.queues.ack = ack  # a stale receipt
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
    ttl = await ttl_of(engine, key)
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


# ---------------------------------------------------------------- FE-13, FE-20, RV-N6


async def test_fe13_a_retry_admits_against_the_current_view(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FE-13: a retry asked admission with the view read before the failed attempt.

    A deployment a reload removed meanwhile was still offered.
    """
    engine, _ = await make()
    from test_audit_runtime import _a_held

    held = _a_held(engine, "fe13")
    engine._held["fe13"] = held
    newer = copy.copy(engine.registry.current)
    seen: list[Any] = []

    async def attempt(*args: Any, **kwargs: Any) -> Any:
        engine.registry._current = newer  # a reload during the attempt
        return runtime._AttemptResult("retry", "provider_error", None, RuntimeError("5xx"))

    async def decide(view: Any, *args: Any, **kwargs: Any) -> None:
        seen.append(view)

    async def allowed(*args: Any) -> bool:
        return True

    monkeypatch.setattr(engine, "_attempt", attempt)
    monkeypatch.setattr(engine, "_decide", decide)
    monkeypatch.setattr(engine, "_retry_allowed", allowed)
    monkeypatch.setattr(engine, "_backoff", lambda *args: 0.0)
    await engine._attempts(held, Decision("granted", deployment="aws-a/r1"), 1, "small")
    assert seen
    assert seen[0] is newer  # the view from before the attempt


async def test_fe20_ring_again_error_does_not_end_the_model_loop(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FE-20: `ring_again` meeting an error reply (UNBLOCKED in a failover) ended the loop.

    The model took no calls until the next heartbeat restarted it.
    """
    engine, _ = await make()
    view = engine.registry.current
    name = engine._queue_name(view, "small", False)

    async def receive_once(alias: str) -> None:
        return None  # no free slot: a doorbell token taken goes back

    async def wait_bell(*args: Any, **kwargs: Any) -> str:
        return str(name)

    async def ring_again(queue: str) -> None:
        raise ResponseError("UNBLOCKED force unblock from blocking operation")

    monkeypatch.setattr(engine, "_receive_once", receive_once)
    monkeypatch.setattr(engine.queues, "wait_bell", wait_bell)
    monkeypatch.setattr(engine.queues, "ring_again", ring_again)
    engine._held_class.add(("small", "n"))  # so a held class sends it to the doorbell path
    loop = asyncio.create_task(engine._model_loop("small"))
    await asyncio.sleep(0.6)
    assert not loop.done()  # ended with the ResponseError before
    await _cancel(loop)


async def test_rvn6_the_probe_lock_goes_back_to_its_spacing(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RV-N6: a probe stretched the lock to 35 s and nobody probed again for that long."""
    engine, _ = await make()
    monkeypatch.setattr(runtime, "PROBE_EVERY_SECS", 1.0)
    key = f"{engine.keys.prefix}:lock:probe"
    probed: list[str] = []

    async def probe_due(alias: str, *, lock_key: str | None = None) -> dict[str, str]:
        await engine.state.expire(key, 35)  # as a real probe does
        probed.append(alias)
        return {}

    monkeypatch.setattr(engine, "probe_due", probe_due)
    task = asyncio.create_task(engine._prober())
    await _until(lambda: len(probed) >= 4, secs=4, what="one probe pass")
    await asyncio.sleep(0.2)
    assert await ttl_of(engine, key) <= 1  # 35 s before
    await _cancel(task)


# ---------------------------------------------------------------- WIP commit: Q9, Q10, R3FM-8...


async def test_q9_a_short_send_stays_on_the_queue(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Owner Q9: a send short of replicas ran the call direct in the caller (its copy bounced).

    It is now sent anyway (warned, counted) and run by a worker.
    """
    engine, _ = await make()
    engine.replica_wait.replicas = 1
    real = engine.queues.send
    direct: list[Any] = []

    async def send(*args: Any, **kwargs: Any) -> str:
        await real(*args, **{**kwargs, "wait_replicas": 0})
        raise NotReplicated("0 of 1 replicas")

    monkeypatch.setattr(engine.queues, "send", send)
    monkeypatch.setattr(engine, "_start_direct", lambda env: direct.append(env))
    assert (await engine.call("small", CHAT, timeout_secs=5)).ok
    assert not direct


async def test_q10_strict_mode_with_an_evicting_store_is_not_ready(make: Make) -> None:
    """Owner Q10: an evicting store policy in strict mode was only a warning at start."""
    engine, _ = await make()
    engine.settings = dataclasses.replace(engine.settings, strict=True)
    engine._eviction["state"] = "allkeys-lru"
    health = await engine.health_check()
    assert health["ready"] is False
    assert "allkeys-lru" in health["not_ready_reason"]


async def test_r3fm8_a_promotion_reregisters_and_rechecks(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R3FM-8: a promotion that lost writes kept the old epoch: nothing was re-checked."""
    engine, _ = await make()
    ids = iter(["a" * 40, "b" * 40])
    reregistered: list[int] = []

    async def info(section: str) -> dict[str, Any]:
        return {"connected_slaves": 0, "master_replid": next(ids)}

    async def reregister(owners: Any = None) -> None:
        reregistered.append(1)

    monkeypatch.setattr(engine.state, "info", info)
    monkeypatch.setattr(engine, "_reregister", reregister)
    await engine._check_replication()
    await engine._check_replication()
    assert reregistered == [1]
    assert engine._sweep_now.is_set()
    assert engine.replica_wait.connected == 0


async def test_r3rc6_a_silent_subscriber_is_reset(make: Make) -> None:
    """R3RC-6 / R3FM-12: a half-open or cut-off subscriber was never noticed (about 15 min)."""
    engine, _ = await make()
    alive = runtime._Alive("result_reader")
    alive.seen_mono -= 100
    with pytest.raises(runtime._StaleSubscription):
        await engine._keep_alive(engine._pubsub, alive)
    alive.reset()
    engine._reader_stale("result_reader")  # the sweep found unannounced results
    with pytest.raises(runtime._StaleSubscription):
        await engine._keep_alive(engine._pubsub, alive)


async def test_r3rc8_a_duplicate_goes_back_when_the_claim_lapses(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R3RC-8: a duplicate always went back for half a lease, whatever the claim's age."""
    engine, _ = await make()
    from test_audit_runtime import _a_held

    held = _a_held(engine, "rc8")
    held.message = Message(queue="q", id="1-0", generation=1, body=b"")
    delays: list[float] = []

    async def pick_take(*args: Any, **kwargs: Any) -> Decision:
        return Decision("duplicate", wait_secs=0.7)

    async def return_(message: Any, delay_secs: float = 0.0) -> bool:
        delays.append(delay_secs)
        return True

    monkeypatch.setattr(engine.admission, "pick_take", pick_take)
    monkeypatch.setattr(engine.queues, "return_", return_)
    await engine._decide(engine.registry.current, "small", held, 1, frozenset())
    assert delays == [0.7]


async def test_rven14_a_rejected_request_on_static_cap_is_a_call_failure(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RV-EN14: `InferenceRejected` reached the app raw on the `static_cap` path."""
    engine, _ = await make({"small": {"store_down": "static_cap"}})

    async def down(*args: Any, **kwargs: Any) -> Any:
        raise StoreConnectionError("connection refused")

    def rejected(*args: Any, **kwargs: Any) -> Any:
        raise InferenceRejected("max_tokens above the cap")

    monkeypatch.setattr(engine, "_call_routed", down)
    monkeypatch.setattr(engine, "_params", rejected)
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, timeout_secs=5)
    assert info.value.kind == "invalid_request"


# ---------------------------------------------------------------- FL-21, Q27


async def test_fl21_the_heartbeat_keeps_the_caller_key_in_its_renew(make: Make) -> None:
    """FL-21: the caller key was a SET of its own each beat; it rides on RENEW now."""
    engine, _ = await make()
    key = engine.keys.caller(engine.worker_id)
    await engine.state.delete(key)
    await _until_async(lambda: engine.state.exists(key), secs=3, what="the key renewed")
    assert await ttl_of(engine, key) >= math.ceil(2 * engine.settings.lease_secs / 3)


async def test_q27_a_send_uses_its_queues_retention(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Owner Q27: a call's send used the library's retention, not its queue's own."""
    engine, _ = await make({"small": {"queue": {"retention_secs": 7200}}})
    seen: list[int] = []
    real = engine.requests.register_send

    async def register_send(*args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs["retention_secs"])
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine.requests, "register_send", register_send)
    assert (await engine.call("small", CHAT, timeout_secs=5)).ok
    assert seen == [7200]


# ---------------------------------------------------------------- R3PF-6, Q43


async def test_r3pf6_a_busy_store_pool_is_not_a_store_outage(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R3PF-6: a saturated connection pool was read as the store down.

    Calls paused or ran outside their budgets (`store_down`), and the outage memo told every
    other call the store was down. It is now `rate_limited`, retry in a second.
    """
    engine, _ = await make({"small": {"store_down": "fail_fast"}})

    async def busy(*args: Any, **kwargs: Any) -> Any:
        raise StorePoolBusy("all 10 store connections stayed in use")

    monkeypatch.setattr(engine, "_call_routed", busy)
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, timeout_secs=5)
    assert info.value.kind == "rate_limited"
    assert not engine._outage_remembered()


async def test_q43_a_live_edit_replans_the_bedrock_pools(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Owner Q43: a live `max_concurrency` / `worker_slots` edit resized nothing until restart."""
    engine, _ = await make({"small": {"max_concurrency": 2}})
    await _until(lambda: engine._planned is not None, what="the first plan")
    planned: list[Any] = []
    monkeypatch.setattr(engine.providers.pool, "plan", planned.append)
    engine._replan_pools()
    assert not planned  # nothing changed
    engine._planned = {}  # as after an edit
    engine._replan_pools()
    assert planned == [engine._pool_slots(engine.registry.current)]
    assert planned[0][("aws-a", "r1")] >= 2


# ---------------------------------------------------------------- CG-1, CG-3, CG-5


async def test_cg1_callers_recover_soon_after_a_store_block_under_load(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CG-1 (pins FT-9): callers after a store block at 300 % load got `queue_unavailable`.

    Seen 11 s after the block in drills; no test pinned the recovery time. 60 calls (three
    times the model's slots) meet a 2 s block: every one is answered, within 6 s of its end.
    """
    engine, _ = await make({"small": {"store_down": "pause", "worker_slots": 20}})
    real = engine.requests.register_send
    loop = asyncio.get_running_loop()
    block_end = loop.time() + 2.0

    async def blocked(*args: Any, **kwargs: Any) -> Any:
        if loop.time() < block_end:
            raise StoreConnectionError("Connection refused")
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine.requests, "register_send", blocked)
    replies = await asyncio.gather(
        *(engine.call("small", CHAT, timeout_secs=15) for _ in range(60))
    )
    assert all(r.ok for r in replies)
    assert loop.time() - block_end < 6.0


async def test_cg3_a_failing_queue_store_sends_calls_direct(make: Make) -> None:
    """CG-3: `LLMQ_QUEUE_FAILOVER_SECS` (a separate queue store that keeps failing sends the
    call down the direct path) had no test. A queue store on a refused port, failover 0.5 s.

    The refused case; the blackholed one is R3FM-3's test below.
    """
    base, _ = await make(worker=False)
    queue_store = store_client("redis://:x@127.0.0.1:1/0")
    engine = Engine(
        base.state,
        base.registry,
        base.providers,
        queue_store=queue_store,
        settings=dataclasses.replace(base.settings, queue_failover_secs=0.5),
    )
    await engine.start(worker=False)
    try:
        reply = await engine.call("small", CHAT, timeout_secs=20)
        assert reply.ok
    finally:
        await engine.stop(drain_secs=0)
        await queue_store.aclose()


async def test_fl22_a_call_ends_in_one_script_on_a_shared_store(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FL-22: the end of a queued call was RESULT_PUT, then FINISH and ACK (two round trips).

    With the queue in the state store it is one script: the three separate writes are not
    used, and the grant, the message and the result are all settled.
    """
    engine, _ = await make()

    async def unused(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("settled by its own script")

    monkeypatch.setattr(engine.requests, "put_result", unused)
    monkeypatch.setattr(engine.admission, "finish", unused)
    monkeypatch.setattr(engine.queues, "ack", unused)
    reply = await engine.call("small", CHAT, request_id="fl22", timeout_secs=10)
    assert reply.ok
    await _until(lambda: not engine._held, what="the call settled")  # woken inside the script
    name = engine._queue_name(engine.registry.current, "small", False)
    assert await engine.state.xlen(engine.queues.queue_keys(name)[0]) == 0  # acked
    entry = f"{engine.keys.prefix}:{{m:small}}:inprog:fl22"
    assert not await engine.state.exists(entry)  # the grant is finished


async def test_fe5_amend_callers_take_over_a_dead_direct_run(make: Make) -> None:
    """AMEND FE-5: a direct run whose process died left the record confirmed with no result;
    attached callers and every retry of the id waited to their deadline (`deadline_exceeded`).

    The dead runner is a record made as a direct run's caller makes it, its liveness key gone:
    a retry takes it over at once, and a caller attached while the runner lived re-registers
    at its first recheck (after the takeover grace) and takes it over too.
    """
    engine, _ = await make()
    alias = "small"
    record: dict[str, Any] = {
        "user_hash": engine.hash_id(""),
        "params_hash": params_hash({"m": alias, "p": CHAT}),
        "model": alias,
        "priority": False,
        "app_id": True,
    }

    async def dead_direct_run(rid: str) -> None:
        await engine.requests.register(rid, caller="dead-pod", **record)
        await engine.requests.confirm(rid, keep_secs=30, direct=True)

    await dead_direct_run("fe5-retry")
    await engine.state.delete(engine.keys.caller("dead-pod"))
    reply = await engine.call(alias, CHAT, request_id="fe5-retry", timeout_secs=8)
    assert reply.ok  # `deadline_exceeded` after 8 s before

    await dead_direct_run("fe5-attached")
    attached = asyncio.create_task(
        engine.call(alias, CHAT, request_id="fe5-attached", timeout_secs=15)
    )
    await _until(lambda: "fe5-attached" in engine._local_callers, what="the caller attached")
    await asyncio.sleep(0.2)  # registered while the runner was alive
    await engine.state.delete(engine.keys.caller("dead-pod"))  # the runner's process dies
    assert (await attached).ok


async def test_r3fm3_a_blackholed_queue_store_is_skipped_after_the_first_detection(
    make: Make,
) -> None:
    """R3FM-3 (send timeout, AMEND CG-3): a blackholed queue store cost every call the
    client's full retry budget (about 21 s) before the direct path, for the whole outage.

    Each send try is now bounded by `queue_failover_secs`, and the outage is remembered: the
    first call fails over within the failover time plus 1 s, the next ones skip the queue
    store at once. The store accepts connections and never answers (not a refused port).
    """
    held: list[asyncio.StreamWriter] = []

    async def blackhole(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        held.append(writer)
        await reader.read()  # never answers

    server = await asyncio.start_server(blackhole, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    base, _ = await make(worker=False)
    settings = dataclasses.replace(base.settings, queue_failover_secs=0.5)
    engine = Engine(base.state, base.registry, base.providers, queue_store=base.state)
    engine.settings = settings
    await engine.start(worker=False)
    dead = store_client(f"redis://:x@127.0.0.1:{port}/0")
    engine._queue_store = dead  # the queue store goes dark after start
    engine.queues = StreamQueues(dead, engine.keys)
    sends = 0
    real_send = engine.queues.send

    async def send(*args: Any, **kwargs: Any) -> Any:
        nonlocal sends
        sends += 1
        return await real_send(*args, **kwargs)

    engine.queues.send = send  # type: ignore[method-assign]  # count the tries
    loop = asyncio.get_running_loop()
    try:
        started = loop.time()
        reply = await engine.call("small", CHAT, timeout_secs=20)
        assert reply.ok
        first_secs = loop.time() - started - reply.meta.provider_ms / 1000
        assert first_secs < settings.queue_failover_secs + 1.0, first_secs
        tried = sends
        for _ in range(3):
            started = loop.time()
            reply = await engine.call("small", CHAT, timeout_secs=20)
            assert reply.ok
            overhead = loop.time() - started - reply.meta.provider_ms / 1000
            assert overhead < 0.3, overhead  # the provider's time, nothing spent on the store
        assert sends == tried  # the dark store was not tried again
    finally:
        await engine.stop(drain_secs=0)
        await dead.aclose()
        server.close()
        for writer in held:
            writer.close()


async def test_cg5_a_caller_whose_key_expired_is_gone(make: Make, store: Any) -> None:
    """CG-5: `caller_gone` (a queued call whose caller's liveness key lapsed is dropped
    unrun) was exercised only by timing in scenarios B95 and C2.
    """
    worker, _ = await make()
    caller, _ = await make(worker=False)
    worker.pause_receiving()
    secs, _ = await store.time()
    await store.set(worker.keys.epoch, str(secs - 100))  # the store has been up a while
    heartbeat = next(t for t in caller._tasks if t.get_name() == "llmq:heartbeat")
    await _cancel(heartbeat)  # the caller stops renewing
    task = asyncio.create_task(caller.call("small", CHAT, request_id="cg5", timeout_secs=8))
    await _until_async(lambda: _confirmed(caller, "cg5"), what="the call sent")
    await store.delete(caller.keys.caller(caller.worker_id))  # its key lapsed
    worker._paused = False
    worker.ready = True
    with pytest.raises(CallFailed) as info:
        await task
    assert info.value.kind == "cancelled"
    status = await worker.requests.status("cg5")
    assert status["state"] == "cancelled"
