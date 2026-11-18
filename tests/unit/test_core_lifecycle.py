"""`LLMQueue` start, stop and the sync runner on a fake engine: failure paths and use after stop.

Each test pins an audit finding (CO-n) and says what used to happen. No store is needed: the
engine is a fake that counts its steps and fails on cue.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
from collections.abc import Callable, Coroutine, Iterator
from types import SimpleNamespace
from typing import Any

import pytest

from llm_queue import CallFailed, LLMQueueError
from llm_queue.core import LLMQueue, _SyncRunner, _wait_on_loop
from llm_queue.engine.envelope import Reply
from llm_queue.langchain import set_default_queue


class _Engine:
    """Counts each lifecycle step; `fail` names steps that raise, `slow` makes calls hang."""

    def __init__(self, fail: tuple[str, ...] = ()) -> None:
        self.fail = fail
        self.steps: list[str] = []
        self.registry = SimpleNamespace(current=None)
        self.queues = None
        self.metrics = None
        self.hash_id = str  # the job runner hashes a job's ids with it
        self.settings = SimpleNamespace(
            queue_prefix="", drain_secs=0, replica_wait_count=0, replica_wait_ms=100
        )
        self.release = asyncio.Event()
        self.tasks: list[str] = []

    async def _step(self, name: str) -> None:
        self.steps.append(name)
        if name in self.fail:
            raise RuntimeError(f"{name} failed")

    async def start(self, **kw: Any) -> None:
        await asyncio.sleep(0)  # a real start awaits: two starts would interleave here
        self.tasks.append("reload")
        await self._step("start")

    def pause_receiving(self) -> None:
        return None  # `stop()` pauses first (A-2)

    async def stop(self, **kw: Any) -> None:
        self.tasks.clear()
        await self._step("stop")

    async def aclose(self) -> None:
        await self._step("aclose")

    async def call(self, model: str, params: Any, **kw: Any) -> Reply:
        await self._step("call")
        return Reply(request_id="r", ok=True, response={})


class _Offload:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.closed = 0

    async def aclose(self) -> None:
        self.closed += 1
        if self.fail:
            raise RuntimeError("s3 close failed")


def _queue(engine: _Engine, offload: _Offload | None = None, mode: str = "caller") -> LLMQueue:
    return LLMQueue(
        engine,  # type: ignore[arg-type]  # a fake
        settings=SimpleNamespace(role_mode=mode),  # type: ignore[arg-type]
        offload=offload,  # type: ignore[arg-type]
    )


@pytest.fixture(autouse=True)
def no_default() -> Iterator[None]:
    set_default_queue(None)
    yield
    set_default_queue(None)


async def test_stop_runs_every_step_when_one_fails(caplog: pytest.LogCaptureFixture) -> None:
    """CO-1: `engine.stop` raising skipped `close_sync`, `aclose` and the offload close.

    `_stopped` was already set, so a second stop() returned at once and the store pools,
    provider sessions and S3 client leaked for the life of the process.
    """
    engine, offload = _Engine(fail=("stop",)), _Offload()
    queue = _queue(engine, offload)
    await queue.start()
    closed_sync: list[bool] = []
    queue.close_sync = lambda: closed_sync.append(True)  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING), pytest.raises(RuntimeError, match="stop failed"):
        await queue.stop()
    assert engine.steps == ["start", "stop", "aclose"]
    assert closed_sync == [True]
    assert offload.closed == 1
    assert "stop_step_failed" in caplog.text
    await queue.stop()  # a second stop is still safe and does nothing
    assert engine.steps == ["start", "stop", "aclose"]


async def test_stop_raises_the_first_failure_after_all_steps() -> None:
    """CO-1: with two failing steps both run, and the first failure is the one raised."""
    engine = _Engine(fail=("stop", "aclose"))
    queue = _queue(engine)
    await queue.start()
    with pytest.raises(RuntimeError, match="stop failed"):
        await queue.stop()
    assert engine.steps == ["start", "stop", "aclose"]


async def test_offload_close_failure_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    """CO-8: a failing S3 close was suppressed without a trace."""
    queue = _queue(_Engine(), _Offload(fail=True))
    await queue.start()
    with caplog.at_level(logging.WARNING):
        await queue.stop()
    assert "offload_close_failed" in caplog.text


async def test_concurrent_starts_start_the_engine_once() -> None:
    """CO-5: two concurrent `start()`s both passed `if self.started` and doubled every task."""
    engine = _Engine()
    queue = _queue(engine)
    await asyncio.gather(queue.start(), queue.start())
    assert engine.steps == ["start"]
    assert engine.tasks == ["reload"]
    await queue.stop()


async def test_failed_start_stops_what_it_started() -> None:
    """CO-5: a start failing after `engine.start` left the engine's tasks running.

    A retry then started them a second time. Now the queue is stopped and says so.
    """
    engine = _Engine()
    queue = _queue(engine, mode="both")

    async def jobs_fail() -> None:
        raise RuntimeError("jobs failed")

    queue._jobs.start = jobs_fail  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="jobs failed"):
        await queue.start()
    assert engine.tasks == []
    assert engine.steps == ["start", "stop", "aclose"]
    assert not queue.started
    with pytest.raises(LLMQueueError, match="build a new one"):
        await queue.start()


async def test_retry_after_a_failed_start_names_the_first_error() -> None:
    """NB-4: a retried start() said only "was stopped; build a new one", hiding the cause.

    The retry now names the error that failed the first start and chains it.
    """
    queue = _queue(_Engine(fail=("start",)))
    with pytest.raises(RuntimeError, match="start failed"):
        await queue.start()
    with pytest.raises(LLMQueueError, match=r"failed to start.*start failed") as info:
        await queue.start()
    assert isinstance(info.value.__cause__, RuntimeError)


async def test_start_from_a_second_loop_is_an_llmqueue_error() -> None:
    """NB-4: a start from another event loop, while one ran, got asyncio's RuntimeError.

    Contending for the start lock from a second loop raised "bound to a different event
    loop" (or waited on a future of the wrong loop); it is now `LLMQueueError`.
    """
    engine = _Engine()
    gate = asyncio.Event()
    engine_start = engine.start

    async def slow_start(**kw: Any) -> None:
        await gate.wait()
        await engine_start(**kw)

    engine.start = slow_start  # type: ignore[method-assign]
    queue = _queue(engine)
    first = asyncio.create_task(queue.start())
    await asyncio.sleep(0)  # the first start holds the lock, waiting on the gate

    def other_loop() -> BaseException | None:
        async def start() -> None:
            await asyncio.wait_for(queue.start(), 5)

        try:
            asyncio.run(start())
        except BaseException as exc:
            return exc
        return None

    error = await asyncio.to_thread(other_loop)
    gate.set()
    await first
    assert isinstance(error, LLMQueueError), repr(error)
    assert "another event loop" in str(error)
    await queue.stop()


async def test_calls_after_stop_get_a_clear_error() -> None:
    """CO-13: a call after stop() went into a closed engine and failed on a closed pool."""
    queue = _queue(_Engine())
    await queue.start()
    await queue.stop()
    with pytest.raises(LLMQueueError, match="is stopped"):
        await queue.converse("m", {"messages": []})
    with pytest.raises(LLMQueueError, match="is stopped"):
        await queue.embed("m", ["x"])
    with pytest.raises(LLMQueueError, match="is stopped"):
        await queue.send_job("j", {})
    with pytest.raises(LLMQueueError, match="is stopped"):
        async for _ in queue.converse_stream("m", {"messages": []}):
            pass


async def test_sync_call_after_stop_builds_no_new_engine() -> None:
    """CO-6: `converse_sync` after stop() built a new private engine that nothing closed."""
    built: list[bool] = []

    def build() -> Any:
        built.append(True)
        return _Engine()

    queue = LLMQueue(
        _Engine(),  # type: ignore[arg-type]  # a fake
        settings=SimpleNamespace(role_mode="caller"),  # type: ignore[arg-type]
        builder=build,
    )
    await queue.start()
    await queue.stop()

    def call() -> None:
        queue.converse_sync("m", {"messages": []})

    with pytest.raises(LLMQueueError, match="is stopped"):
        await asyncio.to_thread(call)
    assert queue._sync is None
    assert built == []


def _runner(engine: _Engine) -> _SyncRunner:
    return _SyncRunner(lambda: engine, None)  # type: ignore[arg-type, return-value]


def test_close_sync_with_a_call_in_flight_raises_call_failed() -> None:
    """CO-7: closing the sync runner gave the in-flight caller `concurrent.futures.CancelledError`.

    That is a plain `Exception` outside the one error family; before the earlier fix the
    caller never returned at all.
    """
    engine = _Engine()
    runner = _runner(engine)
    entered = threading.Event()

    async def hang(e: Any) -> None:
        entered.set()
        await asyncio.Event().wait()

    outcome: list[BaseException] = []

    def call() -> None:
        try:
            runner.run(hang)
        except BaseException as exc:
            outcome.append(exc)

    thread = threading.Thread(target=call)
    thread.start()
    assert entered.wait(10), "the sync call never started"
    runner.close()
    thread.join(10)
    assert not thread.is_alive(), "the sync caller never returned"
    assert len(outcome) == 1
    assert isinstance(outcome[0], CallFailed)
    assert outcome[0].kind == "cancelled"
    assert engine.steps == ["start", "stop", "aclose"]


def test_sync_runner_refuses_calls_after_close() -> None:
    """CO-7: a call submitted after close() took its snapshot ran on the stopped engine."""
    runner = _runner(_Engine())
    runner.close()
    made: list[bool] = []

    def make(e: Any) -> Coroutine[Any, Any, None]:
        made.append(True)
        return asyncio.sleep(0)

    with pytest.raises(LLMQueueError, match="closed"):
        runner.run(make)
    assert made == []  # no coroutine was even built


def test_sync_runner_close_failure_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    """CO-8: a failing private-engine stop was suppressed; now logged, and aclose still runs."""
    engine = _Engine(fail=("stop",))
    runner = _runner(engine)
    with caplog.at_level(logging.WARNING):
        runner.close()
    assert "sync_engine_close_failed" in caplog.text
    assert engine.steps == ["start", "stop", "aclose"]


def test_sync_call_on_a_stopped_loop_fails_instead_of_hanging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CO-26: a sync call sent to a loop that was stopped (not closed) blocked forever."""
    monkeypatch.setattr("llm_queue.core._LOOP_CHECK_SECS", 0.05)
    loop = asyncio.new_event_loop()
    try:
        never: Callable[[], Coroutine[Any, Any, None]] = lambda: asyncio.sleep(3600)  # noqa: E731  # a one-line fake
        future = asyncio.run_coroutine_threadsafe(never(), loop)  # the loop never runs
        with pytest.raises(CallFailed) as info:
            _wait_on_loop(future, loop)
        assert info.value.kind == "worker_shutdown"
        assert future.cancelled()
    finally:  # let the cancelled call finish, so no pending-task warning is left

        async def drain() -> None:
            others = asyncio.all_tasks() - {asyncio.current_task()}
            await asyncio.gather(*others, return_exceptions=True)

        loop.run_until_complete(drain())
        loop.close()


def test_wait_on_loop_returns_the_result() -> None:
    """The poll returns the call's result as `.result()` did."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:

        async def answer() -> int:
            return 42

        assert _wait_on_loop(asyncio.run_coroutine_threadsafe(answer(), loop), loop) == 42
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(5)
        loop.close()


def test_concurrent_cancelled_error_is_not_an_llmqueue_error() -> None:
    """Why CO-7 maps it: `concurrent.futures.CancelledError` is outside the error family."""
    assert not issubclass(concurrent.futures.CancelledError, LLMQueueError)


def test_wait_on_loop_reraises_a_timeout_raised_by_the_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NB-2: a call that itself raised a TimeoutError (or AttemptTimeout) spun the caller forever.

    `_wait_on_loop` took the call's TimeoutError for its own poll timeout and polled again.
    """
    monkeypatch.setattr("llm_queue.core._LOOP_CHECK_SECS", 0.05)
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:

        async def boom() -> None:
            raise TimeoutError("the call's own timeout")

        future = asyncio.run_coroutine_threadsafe(boom(), loop)
        outcome: list[BaseException] = []

        def wait() -> None:
            try:
                _wait_on_loop(future, loop)
            except BaseException as exc:
                outcome.append(exc)

        waiter = threading.Thread(target=wait, daemon=True)
        waiter.start()
        waiter.join(5)
        assert not waiter.is_alive(), "the sync caller never returned"
        assert len(outcome) == 1
        assert isinstance(outcome[0], TimeoutError)
        assert "the call's own timeout" in str(outcome[0])
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(5)
        loop.close()


async def test_calls_during_the_drain_are_served() -> None:
    """NB-1: stop() refused calls from its first line, before the jobs and calls drained.

    A job still running in the drain got `LLMQueueError("stopping or stopped")` on its next
    `converse`, and so did an app's follow-up call during a graceful shutdown. Calls made in
    the jobs drain and in the engine drain are served; a call after the drain is refused.
    """
    engine = _Engine()
    queue = _queue(engine, mode="both")
    await queue.start()
    outcome: list[str] = []

    async def call(step: str) -> None:
        try:
            await queue.converse("m", {"messages": []})
            outcome.append(f"{step}: served")
        except LLMQueueError as exc:
            outcome.append(f"{step}: {exc}")

    async def jobs_stop(drain_secs: float = 0) -> None:
        await call("jobs")

    engine_stop = engine.stop

    async def stop_engine(**kw: Any) -> None:
        await call("engine")
        await engine_stop(**kw)

    queue._jobs.stop = jobs_stop  # type: ignore[method-assign]
    engine.stop = stop_engine  # type: ignore[method-assign]
    await queue.stop()
    assert outcome == ["jobs: served", "engine: served"]
    with pytest.raises(LLMQueueError, match="is stopped"):
        await queue.converse("m", {"messages": []})
