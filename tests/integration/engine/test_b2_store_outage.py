"""A short store outage mid-call (failover drills slowdns and restart): no call runs twice.

The provider answered while the store was out of reach: the result write failed, the message
was released (that failed too) and showed again only after its visibility timeout, so the
caller waited 61 s and the provider ran the call twice. A failover also ended the doorbell
wait with an `UNBLOCKED` error reply, which ended the model's receive loop.
"""

from __future__ import annotations

import asyncio
import dataclasses
import time
from typing import Any

import pytest
from engine_testkit import CHAT, SMALL, Make, stats
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ResponseError

from llm_queue.engine.errors import CallFailed
from llm_queue.engine.runtime import Engine
from llm_queue.engine.streaming import ChunkWriter

pytestmark = pytest.mark.integration


async def test_answer_waits_out_a_store_outage(make: Make, monkeypatch: pytest.MonkeyPatch) -> None:
    """Result write and message return fail for 1.5 s (< the 3 s lease): one run, prompt reply."""
    engine, fake = await make()
    real_put, real_return = engine.requests.put_result, engine.queues.return_
    outage = {"until": 0.0}

    def down() -> None:
        if outage["until"] == 0.0:
            outage["until"] = time.monotonic() + 1.5
        if time.monotonic() < outage["until"]:
            raise StoreConnectionError("Connection closed by server.")

    async def put_result(*args: Any, **kwargs: Any) -> Any:
        down()
        return await real_put(*args, **kwargs)

    async def return_(*args: Any, **kwargs: Any) -> Any:
        if outage["until"]:
            down()
        return await real_return(*args, **kwargs)

    monkeypatch.setattr(engine.requests, "put_result", put_result)
    monkeypatch.setattr(engine.queues, "return_", return_)
    started = time.monotonic()
    reply = await engine.call("small", CHAT, request_id="r-outage")
    assert reply.ok
    assert time.monotonic() - started < 5
    await asyncio.sleep(0.5)  # a second run, if any, would have reached the provider by now
    counts = stats(fake, f"{SMALL}@r1")
    assert counts["calls"] == 1
    assert counts.get("duplicates", 0) == 0
    assert not engine._held


async def _slow_store_cleanup(make: Make, monkeypatch: pytest.MonkeyPatch) -> Engine:
    """An engine whose first send meets a dropped connection (the `pause` path follows).

    Caller margin 0.5 s, no reply within 8 s, and its cancel cleanup waits 1.5 s for the
    store (the store client's retry window on dropped connections).
    """
    engine: Engine
    engine, _ = await make(
        {"small": {"store_down": "pause"}},
        {"defaults": {"latency_ms": {"p50": 8000, "p95": 8000}}},
    )
    engine.settings = dataclasses.replace(engine.settings, caller_margin_secs=0.5)
    real_send, real_callers = engine.requests.register_send, engine.requests.callers
    failed = {"once": False}

    async def register_send(*args: Any, **kwargs: Any) -> Any:
        if not failed["once"]:
            failed["once"] = True
            raise StoreConnectionError("Connection closed by server.")
        return await real_send(*args, **kwargs)

    async def slow_callers(*args: Any, **kwargs: Any) -> Any:
        await asyncio.sleep(1.5)
        return await real_callers(*args, **kwargs)

    monkeypatch.setattr(engine.requests, "register_send", register_send)
    monkeypatch.setattr(engine.requests, "callers", slow_callers)
    return engine


async def test_store_down_retry_answers_by_the_deadline(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scenario L4: calls on the `pause` path answered up to 1.9 s after deadline plus margin.

    The retried inner call got at least 1 s (past the outer deadline), and when the deadline
    cancelled it, its cancel cleanup waited for the slow store.
    """
    engine = await _slow_store_cleanup(make, monkeypatch)
    started = time.monotonic()
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, timeout_secs=1.5)  # 0.5 s left after the pause
    assert info.value.kind == "deadline_exceeded"
    assert time.monotonic() - started < 1.5 + 0.5 + 0.5  # 3.5 s before the fix


async def test_cancel_cleanup_never_runs_past_the_deadline(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A task cancelled just before its call's deadline waited for the store's cleanup."""
    engine = await _slow_store_cleanup(make, monkeypatch)
    started = time.monotonic()
    task = asyncio.create_task(engine.call("small", CHAT, timeout_secs=2.5))  # deadline 3 s
    await asyncio.sleep(2.3)  # the retried call (1.5 s from 1 s) is still waiting
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert time.monotonic() - started < 3.0 + 0.5  # 3.8 s before the fix
    for _ in range(40):  # the cleanup goes on behind: the call is still cancelled
        if engine._running:
            await asyncio.sleep(0.1)
    rids = [k async for k in engine.state.scan_iter(match=engine.keys.request("*", "cancel"))]
    assert len(rids) == 2  # the call and its retry on the store_down path


async def test_store_lost_mid_stream_is_queue_unavailable(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scenario L2: the store failing under the chunk writer ended streams as `provider_error`.

    The provider was fine; the worker's chunk write met a dropped store connection after the
    first chunk had reached the caller. The stream must end with the store's kind and the
    partial text, and never run again.
    """
    engine, fake = await make(
        {"small": {"stream_flush_ms": 0}},
        {"models": {f"{SMALL}@r1": {"output_tokens": 80, "tokens_per_s": 200}}},
    )
    real = ChunkWriter._write
    failed: list[int] = []

    async def flaky(self: ChunkWriter, kind: str, data: Any, **kw: Any) -> None:
        if kind == "chunk" and self.sent and not failed:
            failed.append(1)
            raise StoreConnectionError("Connection closed by server.")
        await real(self, kind, data, **kw)

    monkeypatch.setattr(ChunkWriter, "_write", flaky)
    seen: list[dict[str, Any]] = []

    async def drain() -> None:
        async for event in engine.stream("small", CHAT):
            seen.append(event)

    with pytest.raises(CallFailed) as info:
        await drain()
    assert seen
    assert failed
    assert info.value.kind == "queue_unavailable"
    assert info.value.partial_text
    assert stats(fake, f"{SMALL}@r1")["calls"] == 1  # never run again once chunks were sent


async def test_model_loop_survives_an_unblocked_bell(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failover's `UNBLOCKED` reply to the doorbell BLPOP leaves the receive loop running."""
    engine, _ = await make()
    real = engine.queues.wait_bell
    hit = asyncio.Event()

    async def unblocked(*args: Any, **kwargs: Any) -> Any:
        if not hit.is_set():
            hit.set()
            raise ResponseError(
                "UNBLOCKED force unblock from blocking operation, instance state changed"
                " (master -> replica?)"
            )
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine.queues, "wait_bell", unblocked)
    await asyncio.wait_for(hit.wait(), 5)
    await asyncio.sleep(0.2)
    assert not any(task.done() for task in engine._model_tasks.values())
    reply = await asyncio.wait_for(engine.call("small", CHAT), 5)
    assert reply.ok
