"""`LLMQueue` lifecycle and sync fixes (FC-3, FC-4, FC-6, FC-12, FC-13, R3PF-8).

A fake engine stands in for the real one; no store is needed.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import AsyncIterator, Iterator
from types import SimpleNamespace
from typing import Any, cast

import pytest

from llm_queue.core import LLMQueue, _SyncRunner, _wait_on_loop
from llm_queue.engine.envelope import Reply
from llm_queue.langchain import set_default_queue


class _Engine:
    """Records its steps; `start` can wait on `gate`, `stop` can drain slowly."""

    def __init__(self) -> None:
        self.steps: list[str] = []
        self.gate: asyncio.Event | None = None
        self.drain: asyncio.Event | None = None
        self.registry = SimpleNamespace(current=None)
        self.queues = None
        self.metrics = None
        self.settings = SimpleNamespace(queue_prefix="", drain_secs=5)
        self.stream_closed = False
        self.fail_start = False

    async def start(self, **kw: Any) -> None:
        self.steps.append("start")
        if self.gate is not None:
            await self.gate.wait()
        if self.fail_start:
            raise RuntimeError("start failed")

    async def stop(self, **kw: Any) -> None:
        self.steps.append("stop")
        if self.drain is not None:
            await self.drain.wait()

    async def aclose(self) -> None:
        self.steps.append("aclose")

    async def call(self, model: str, params: Any, **kw: Any) -> Reply:
        return Reply(request_id="r", ok=True, response={"ok": 1})

    async def stream(self, model: str, params: Any, **kw: Any) -> AsyncIterator[dict[str, Any]]:
        try:
            for i in range(10):
                yield {"i": i}
        finally:
            self.stream_closed = True


class _Jobs:
    def __init__(self) -> None:
        self.started = 0

    async def start(self) -> None:
        self.started += 1

    async def stop(self, **kw: Any) -> None:
        return None

    def pause(self) -> None:
        return None


def _queue(engine: _Engine, mode: str = "both", **settings: Any) -> LLMQueue:
    queue = LLMQueue(
        engine,  # type: ignore[arg-type]  # a fake
        settings=SimpleNamespace(mode=mode, **settings),  # type: ignore[arg-type]
    )
    queue._jobs = _Jobs()  # type: ignore[assignment]
    return queue


@pytest.fixture(autouse=True)
def no_default() -> Iterator[None]:
    set_default_queue(None)
    yield
    set_default_queue(None)


async def test_stop_during_a_slow_start_waits_for_it() -> None:
    """FC-3: stop() during start() ran every step, then the start resumed and set `started`.

    The queue ended stopped, closed and started at once, with the job runner running on
    closed clients. stop() now waits for the start, then stops what it started.
    """
    engine = _Engine()
    engine.gate = asyncio.Event()
    queue = _queue(engine)
    starting = asyncio.create_task(queue.start())
    await asyncio.sleep(0.01)
    stopping = asyncio.create_task(queue.stop(drain_secs=0))
    await asyncio.sleep(0.01)
    assert engine.steps == ["start"]  # the stop waits
    engine.gate.set()
    await starting
    await stopping
    assert queue.started is False
    assert engine.steps == ["start", "stop", "aclose"]


async def test_a_sync_call_during_the_drain_uses_the_started_loop() -> None:
    """FC-4: `started` was False during the drain, so a sync call built a private engine.

    That second engine opened store connections and clients mid-shutdown, and its calls were
    cancelled by the next step. The call is served on the draining loop now.
    """
    engine = _Engine()
    engine.drain = asyncio.Event()
    queue = _queue(engine)
    queue._builder = lambda: pytest.fail("no private engine during the drain")
    await queue.start()
    stopping = asyncio.create_task(queue.stop())
    await asyncio.sleep(0.01)
    reply = await asyncio.to_thread(queue.converse_sync, "m", {})
    assert reply.response == {"ok": 1}
    assert queue._sync is None
    engine.drain.set()
    await stopping


async def test_a_stream_closed_early_closes_the_engine_stream() -> None:
    """FC-12: a consumer that stopped early left the inner generator to the finalizer."""
    engine = _Engine()
    queue = _queue(engine, mode="caller")
    await queue.start()
    stream = queue.converse_stream("m", {})
    async for _ in stream:
        break
    await stream.aclose()  # type: ignore[attr-defined]
    assert engine.stream_closed
    await queue.stop(drain_secs=0)


def test_a_failed_private_engine_start_is_closed() -> None:
    """FC-6: a private engine whose start raised was dropped with its clients open."""
    engine = _Engine()
    engine.fail_start = True
    with pytest.raises(RuntimeError, match="start failed"):
        _SyncRunner(lambda: engine, None)  # type: ignore[arg-type, return-value]
    assert engine.steps == ["start", "stop", "aclose"]


def test_a_loop_between_runs_is_not_taken_for_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    """FC-13: a loop between two `run_until_complete` calls failed the call `worker_shutdown`.

    One poll with the loop not running is not enough now; the call gets its answer.
    """
    monkeypatch.setattr("llm_queue.core._LOOP_CHECK_SECS", 0.05)
    loop = asyncio.new_event_loop()

    async def answer() -> int:
        return 7

    future = asyncio.run_coroutine_threadsafe(answer(), loop)

    def later() -> None:  # the loop runs again after one poll
        threading.Event().wait(0.07)
        loop.run_until_complete(asyncio.sleep(0.01))

    runner = threading.Thread(target=later)
    runner.start()
    try:
        assert _wait_on_loop(future, loop) == 7
    finally:
        runner.join()
        loop.close()


def test_an_interrupted_wait_cancels_the_call() -> None:
    """FC-13: a KeyboardInterrupt in the waiting thread left the call running, unwatched."""

    class Interrupted(concurrent.futures.Future[int]):
        def result(self, timeout: float | None = None) -> int:
            raise KeyboardInterrupt

    future = Interrupted()
    loop = asyncio.new_event_loop()
    try:
        with pytest.raises(KeyboardInterrupt):
            _wait_on_loop(future, loop)
        assert future.cancelled()
    finally:
        loop.close()


async def test_a_store_pool_smaller_than_the_slots_is_warned(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """R3PF-8: nothing compared the store pool with the worker slots it serves at start."""
    engine = _Engine()
    view = SimpleNamespace(
        registry=SimpleNamespace(models={"a": 1, "b": 2}),
        value=lambda alias, name: 150,
    )
    engine.registry = SimpleNamespace(current=view)
    queue = _queue(engine, store_max_connections=200)
    await queue.start()
    await queue.stop(drain_secs=0)
    assert any("store_pool_small" in r.getMessage() for r in caplog.records)


def test_public_types_and_no_internal_aliases() -> None:
    """Q35: apps imported Provider, Route, JobFn, Secrets and PublishResult from private modules.

    They are public; the deprecated `LLMQueue.engine` / `.jobs` aliases are gone in 0.2.0.
    """
    import llm_queue

    for name in ("Provider", "Route", "JobFn", "Secrets", "PublishResult"):
        assert name in llm_queue.__all__
        assert getattr(llm_queue, name) is not None
    engine = _Engine()
    queue = _queue(engine)
    assert queue._engine is cast("Any", engine)
    assert not hasattr(queue, "engine")
    assert not hasattr(queue, "jobs")
