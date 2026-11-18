"""0.2.0 drain API on a real store (plan A1-A4): hand-back, the pause race, resume, health,
jobs and the drain file. Two engines share one prefix, so one's calls can move to the other.
No real LLM is called.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
import redis.asyncio
from aiohttp.test_utils import TestServer
from engine_testkit import (
    CHAT,
    SMALL,
    FakeEngine,
    Make,
    Scenario,
    away_from_minute_end,
    create_app,
    stats,
)
from engine_testkit import registry as registry_doc

from llm_queue import LLMQueue, LLMQueueError
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.runtime import Engine

pytestmark = pytest.mark.integration

# A budget of one call a minute: the second call waits (held) for budget.
ONE_RPM = {"small": {"rpm": 1, "max_wait_secs": 30, "timeout_secs": 30}}


async def _until(check: Callable[[], Any], secs: float = 5.0, what: str = "condition") -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + secs
    while not check():
        if loop.time() > end:
            raise AssertionError(f"{what} not reached in {secs:g} s")
        await asyncio.sleep(0.02)


def _holding(engine: Engine) -> bool:
    return any(h.hold is not None and h.run is None for h in engine._held.values())


async def _held_on(make: Make) -> tuple[Engine, asyncio.Task[Any]]:
    """An engine holding one call waiting for budget (rpm 1, already used this minute)."""
    await away_from_minute_end(margin_secs=25)
    engine, _ = await make(ONE_RPM)
    assert (await engine.call("small", CHAT)).ok
    task = asyncio.create_task(engine.call("small", CHAT, request_id="r-held", timeout_secs=30))
    await _until(lambda: _holding(engine), what="a held call")
    return engine, task


async def _cancel(task: asyncio.Task[Any]) -> None:
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


# ---------------------------------------------------------------- A1: hand-back


async def test_pause_hands_held_calls_back_and_another_engine_serves_them(make: Make) -> None:
    first, task = await _held_on(make)
    second, fake = await make(ONE_RPM)  # its own fakellm: its calls are counted apart
    first.pause_receiving()
    await _until(lambda: not first._held, what="the held call handed back")
    # Budget for the second engine; the first, paused, takes nothing.
    await second.registry.set({"rpm": 100}, model="small", actor="t")
    reply = await asyncio.wait_for(task, 20)
    assert reply.ok
    assert stats(fake, f"{SMALL}@r1")["calls"] == 1  # run by the second engine
    assert not first._held


async def test_hand_back_can_be_turned_off(make: Make) -> None:
    engine, task = await _held_on(make)
    engine.settings = dataclasses.replace(engine.settings, drain_handback_enabled=False)
    engine.pause_receiving()
    await asyncio.sleep(0.5)
    assert _holding(engine)  # 0.1.1 behaviour: held calls stay until stop()
    await _cancel(task)


async def test_pause_from_another_thread_hands_back(make: Make) -> None:
    """`pause_receiving()` stays sync and callable from any thread (a signal handler)."""
    engine, task = await _held_on(make)
    await asyncio.to_thread(engine.pause_receiving)
    await _until(lambda: not engine._held, what="the held call handed back")
    status = await engine.request_status("r-held")
    assert status.get("state") != "in_progress"
    await _cancel(task)


async def test_stop_right_after_pause_awaits_the_hand_back(make: Make) -> None:
    engine, task = await _held_on(make)
    engine.pause_receiving()
    await engine.stop(drain_secs=0)
    assert engine._handing_back is None or engine._handing_back.done()
    assert not engine._held
    await _cancel(task)


async def test_a_receive_in_flight_at_pause_is_released(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The race: a receive that took a message before the pause held it after the hand-back."""
    await away_from_minute_end(margin_secs=25)
    worker, _ = await make(ONE_RPM)
    assert (await worker.call("small", CHAT)).ok
    real_decide = worker._decide
    decided: list[str] = []

    async def decide(*args: Any, **kwargs: Any) -> Any:
        worker.pause_receiving()  # the pause lands while the admission runs
        decision = await real_decide(*args, **kwargs)
        decided.append(decision.kind if decision is not None else "none")
        return decision

    monkeypatch.setattr(worker, "_decide", decide)
    task = asyncio.create_task(worker.call("small", CHAT, request_id="r-race", timeout_secs=30))
    await _until(lambda: decided, what="an admission")
    assert decided[0] == "wait"
    await asyncio.sleep(0.3)
    assert not worker._held  # released, not held here
    monkeypatch.undo()
    other, _ = await make(ONE_RPM)
    await other.registry.set({"rpm": 100}, model="small", actor="t")
    assert (await asyncio.wait_for(task, 20)).ok  # served by the other engine


async def test_a_batch_received_at_pause_goes_back_unadmitted(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker, _ = await make()
    caller, _ = await make(worker=False)
    real = worker.queues.receive_first
    admitted: list[str] = []
    real_admit = worker._admit_new

    async def receive_first(*args: Any, **kwargs: Any) -> Any:
        got = await real(*args, **kwargs)
        if got[0]:
            worker.pause_receiving()
        return got

    async def admit(alias: str, message: Any) -> bool:
        admitted.append(message.id)
        return await real_admit(alias, message)

    monkeypatch.setattr(worker.queues, "receive_first", receive_first)
    monkeypatch.setattr(worker, "_admit_new", admit)
    task = asyncio.create_task(caller.call("small", CHAT, request_id="r-batch", timeout_secs=20))
    await _until(lambda: worker.draining, what="the pause")
    await asyncio.sleep(0.3)
    assert admitted == []
    monkeypatch.undo()
    worker.resume_receiving()
    assert (await asyncio.wait_for(task, 15)).ok


# ---------------------------------------------------------------- A2 / A3: resume and health


async def test_resume_makes_the_engine_ready_and_receiving_again(make: Make) -> None:
    worker, _ = await make()
    caller, _ = await make(worker=False)
    worker.pause_receiving()
    task = asyncio.create_task(caller.call("small", CHAT, request_id="r-resume", timeout_secs=20))
    await asyncio.sleep(0.8)
    assert not task.done()
    worker.resume_receiving()
    assert (await worker.health_check())["ready"] is True
    assert (await asyncio.wait_for(task, 15)).ok


async def test_a8_resume_from_another_thread_wakes_the_loops(make: Make) -> None:
    """A-8: `resume_receiving()` set the loop's events from the calling thread.

    The pause is thread-safe; the resume was not: with asyncio debug on it raised, and the
    wake-up was not delivered. It now goes through `call_soon_threadsafe`.
    """
    worker, _ = await make()
    caller, _ = await make(worker=False)
    worker.pause_receiving()
    await asyncio.sleep(0.3)  # the receive loops wait out the pause on their events
    loop = asyncio.get_running_loop()
    loop.set_debug(True)
    try:
        await asyncio.to_thread(worker.resume_receiving)
    finally:
        loop.set_debug(False)
    assert (await asyncio.wait_for(caller.call("small", CHAT, timeout_secs=20), 15)).ok


async def test_g22_a_pause_hands_back_a_call_waiting_between_retries(make: Make) -> None:
    """G22 (DR-15): a call in retry backoff (run task alive, no grant) was not handed back.

    It kept the drain open until its deadline and retried on the paused pod. It now goes back
    at once, not failed, and another pod serves it.
    """
    first, fake = await make(
        {"small": {"retry_base_secs": 8, "retry_max_secs": 8, "max_attempts": 3}},
        {"models": {f"{SMALL}@r1": {"force_error": "InternalServerException"}}},
    )
    caller, _ = await make(worker=False)
    task = asyncio.create_task(caller.call("small", CHAT, request_id="g22-r", timeout_secs=30))
    await _until(
        lambda: (h := first._held.get("g22-r")) is not None and h.retry_wait,
        what="the call in its retry backoff",
    )
    first.pause_receiving()
    await _until(lambda: "g22-r" not in first._held, secs=2, what="the call handed back")
    second, other = await make()
    reply = await asyncio.wait_for(task, 20)
    assert reply.ok
    assert reply.meta.worker == second.worker_id
    assert stats(fake, f"{SMALL}@r1")["calls"] == 1  # never retried on the paused pod
    assert stats(other, f"{SMALL}@r1")["calls"] == 1


async def test_g22_a_stream_cut_by_the_drain_timeout_ends_and_is_not_run_again(
    make: Make,
) -> None:
    """G22 (DR-11): a stream that sent chunks when the drain ran out cannot move elsewhere.

    Its caller gets `worker_shutdown` with the partial text, and no other pod runs it again.
    """
    first, fake = await make(
        {"small": {"stream_flush_ms": 0, "timeout_secs": 30, "attempt_timeout_secs": 25}},
        {"models": {f"{SMALL}@r1": {"output_tokens": 400, "tokens_per_s": 20}}},
    )
    caller, _ = await make(worker=False)
    seen: list[dict[str, Any]] = []

    async def consume() -> None:
        async for event in caller.stream("small", CHAT, request_id="g22-s", timeout_secs=30):
            seen.append(event)

    task = asyncio.create_task(consume())
    await _until(lambda: len(seen) > 2, secs=10, what="chunks reaching the caller")
    await first.stop(drain_secs=0.2)
    with pytest.raises(CallFailed) as info:
        await asyncio.wait_for(task, 10)
    assert info.value.kind == "worker_shutdown"
    assert info.value.partial_text
    _, other = await make()  # another pod: it must not run the cut stream again
    await asyncio.sleep(1.5)
    assert stats(fake, f"{SMALL}@r1")["calls"] == 1
    assert stats(other, f"{SMALL}@r1").get("calls", 0) == 0


async def test_draining_is_in_health_with_its_reason(make: Make) -> None:
    engine, _ = await make()
    health = await engine.health_check()
    assert (health["draining"], health["draining_since"]) == (False, None)
    assert "not_ready_reason" not in health
    before = time.time()
    engine.pause_receiving()
    health = await engine.health_check()
    assert health["ready"] is False
    assert health["draining"] is True
    assert health["draining_since"] >= before
    assert health["not_ready_reason"] == "draining"
    # Draining wins over an evicting store: the operator's action is the first reason.
    engine._evicting = lambda: "store_eviction_policy"  # type: ignore[method-assign]
    assert (await engine.health_check())["not_ready_reason"] == "draining"
    engine.resume_receiving()
    health = await engine.health_check()
    assert health["draining"] is False
    assert health["not_ready_reason"] == "store_eviction_policy"


async def test_resume_is_refused_after_stop(make: Make) -> None:
    engine, _ = await make()
    engine.pause_receiving()
    await engine.stop(drain_secs=0)
    with pytest.raises(LLMQueueError, match="stopped"):
        engine.resume_receiving()
    assert engine.ready is False


# ---------------------------------------------------------------- LLMQueue: jobs, drain, file


@pytest.fixture
async def queues(
    store: redis.asyncio.Redis, prefix: str
) -> AsyncIterator[Callable[..., Awaitable[LLMQueue]]]:
    """Build LLMQueues from the environment on one key prefix and one fakellm."""
    fake = FakeEngine(Scenario.from_mapping({}))
    server = TestServer(create_app(engine=fake), host="127.0.0.1")
    await server.start_server()
    url = str(server.make_url("")).rstrip("/")
    port = store.connection_pool.connection_kwargs["port"]
    key_prefix = prefix.replace(":", "-")
    built: list[LLMQueue] = []

    async def build(
        extra: dict[str, str] | None = None, jobs: dict[str, Any] | None = None
    ) -> LLMQueue:
        env = {
            "LLMQ_STORE_URL": f"redis://:localdev@127.0.0.1:{port}/0",
            "LLMQ_ENVIRONMENT": "test",
            "LLMQ_KEY_PREFIX": key_prefix,
            "LLMQ_REGISTRY": json.dumps(registry_doc()),
            "LLMQ_BEDROCK_ENDPOINT_URL": url,
            "LLMQ_METRICS_SINK_MODE": "none",
            **(extra or {}),
        }
        queue = LLMQueue.from_env(
            env,
            credentials=lambda account: {
                "aws_access_key_id": account.name,
                "aws_secret_access_key": "x",
            },
        )
        for name, fn in (jobs or {}).items():
            queue.register_job(name, fn)
        await queue.start()
        built.append(queue)
        return queue

    yield build
    for queue in built:
        await queue.stop(drain_secs=1)
    await server.close()
    keys = [k async for k in store.scan_iter(match=f"{key_prefix}:*", count=500)]
    if keys:
        await store.delete(*keys)


async def test_jobs_pause_and_resume(queues: Callable[..., Awaitable[LLMQueue]]) -> None:
    ran: list[dict[str, Any]] = []

    async def job(payload: dict[str, Any]) -> None:
        ran.append(payload)

    llmq = await queues(jobs={"j": job})
    llmq.pause_receiving()
    await llmq.send_job("j", {"n": 1})
    await asyncio.sleep(1.0)
    assert ran == []
    llmq.resume_receiving()
    await _until(lambda: ran, secs=10, what="the job run after resume")
    assert (await llmq.health())["ready"] is True


async def test_queue_drain_waits_for_running_calls_and_resume_after_stop_is_refused(
    queues: Callable[..., Awaitable[LLMQueue]],
) -> None:
    llmq = await queues()
    assert await llmq.drain(drain_secs=2) is True  # nothing running: at once
    assert llmq.draining is True
    health = await llmq.health()
    assert health["draining"] is True
    assert health["not_ready_reason"] == "draining"
    await llmq.stop(drain_secs=0)
    with pytest.raises(LLMQueueError, match="stopped"):
        llmq.resume_receiving()


async def test_admin_drain_is_a_dry_run_by_default_and_audits_only_real_runs(
    queues: Callable[..., Awaitable[LLMQueue]],
) -> None:
    """Admin writes are dry runs unless told (D3): drain and resume too (0.2.0)."""
    llmq = await queues()
    admin = llmq.admin

    async def entries(op: str) -> list[dict[str, Any]]:
        return list((await admin.audit.list(50, op=op))["entries"])

    planned = await admin.drain.start(actor="ann")
    assert (planned["draining"], planned["dry_run"], planned["would_change"]) == (False, True, True)
    assert llmq.draining is False
    assert (await llmq.health())["ready"] is True
    assert await entries("drain") == []

    done = await admin.drain.start(actor="ann", dry_run=False)
    assert (done["draining"], done["dry_run"]) == (True, False)
    assert llmq.draining is True
    assert [e["actor"] for e in await entries("drain")] == ["ann"]

    planned = await admin.drain.resume(actor="ann")
    assert (planned["draining"], planned["would_change"]) == (True, True)
    assert llmq.draining is True
    assert await entries("resume") == []

    done = await admin.drain.resume(actor="ann", dry_run=False)
    assert (done["draining"], done["dry_run"]) == (False, False)
    assert (await llmq.health())["ready"] is True
    assert [e["actor"] for e in await entries("resume")] == ["ann"]


async def test_the_drain_file_pauses_every_queue_and_ignores_an_old_file(
    queues: Callable[..., Awaitable[LLMQueue]], tmp_path: Path
) -> None:
    old = tmp_path / "old"
    old.write_text("x", encoding="utf-8")
    os.utime(old, (time.time() - 60, time.time() - 60))
    stale = await queues({"LLMQ_DRAIN_FILE": str(old), "LLMQ_DRAIN_POLL_SECS": "0.1"})
    path = tmp_path / "drain"
    settings = {"LLMQ_DRAIN_FILE": str(path), "LLMQ_DRAIN_POLL_SECS": "0.1"}
    one, two = await queues(settings), await queues(settings)
    await asyncio.sleep(0.4)
    assert [stale.draining, one.draining, two.draining] == [False, False, False]
    path.write_text("drain", encoding="utf-8")
    await _until(lambda: one.draining and two.draining, what="both queues paused")
    assert stale.draining is False
    one.resume_receiving()
    await asyncio.sleep(0.4)
    assert one.draining is False  # the same file does not pause it again
    later = time.time() + 2
    os.utime(path, (later, later))
    await _until(lambda: one.draining, what="paused again by a newer file")
    await one.stop(drain_secs=0)
    assert one._drain_watcher is None or not one._drain_watcher.running


async def test_a2_stop_stops_receiving_before_the_jobs_drain(
    queues: Callable[..., Awaitable[LLMQueue]],
) -> None:
    """A-2 (G4): `stop()` with no pause kept receiving calls while its jobs drained.

    Calls taken then got only the rest of the drain (streams failed `worker_shutdown`, others
    ran again elsewhere). A call sent after `stop()` began now waits for another pod.
    """
    started = asyncio.Event()

    async def job(payload: dict[str, Any]) -> None:
        started.set()
        await asyncio.sleep(2.0)

    first = await queues(jobs={"j": job})
    second = await queues()
    second.pause_receiving()
    await first.send_job("j", {"n": 1})
    await asyncio.wait_for(started.wait(), 10)
    stopping = asyncio.create_task(first.stop(drain_secs=5))
    await _until(lambda: first.draining, what="the stop's pause")
    call = asyncio.create_task(second.call("small", CHAT, request_id="a2-call", timeout_secs=20))
    await asyncio.sleep(1.0)
    assert not call.done()  # the first queue took and ran it while its job drained, before
    await stopping
    second.resume_receiving()
    reply = await asyncio.wait_for(call, 15)
    assert reply.ok
    assert reply.meta.worker == second._engine.worker_id
