"""Bug-pinning tests for the job runner (EN-7, EN-8, EN-29, EN-32 and round 3, R3JE).

A fake `StreamQueues` stands in for the store, so they run without the Docker stack.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from types import SimpleNamespace
from typing import Any

import pytest

from llm_queue import LLMQueue
from llm_queue.engine.jobs import JobRunner
from llm_queue.engine.queue import Message, NotReplicated, QueueSettings
from llm_queue.errors import ConfigError


def events(caplog: pytest.LogCaptureFixture, name: str) -> list[logging.LogRecord]:
    """The captured records of one structured event."""
    return [r for r in caplog.records if getattr(r, "llmq", {}).get("event") == name]


async def until(check: Any, secs: float = 2.0) -> None:
    """Poll `check()` until true, or fail after `secs`."""
    loop = asyncio.get_running_loop()
    end = loop.time() + secs
    while not check():
        if loop.time() > end:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


class FakeQueues:
    """The parts of `StreamQueues` the job runner uses; `pending` messages are handed out."""

    def __init__(self, *, receive_secs: float = 0.0, return_secs: float = 0.0) -> None:
        self.pending: list[Message] = []
        self.receive_secs = receive_secs
        self.return_secs = return_secs
        self.receives = 0
        self.bells = 0
        self.in_receive = False
        self.returned: list[Message] = []
        self.return_started = False
        self.ack_error: Exception | None = None

    async def receive(
        self, _queue: str, _count: int, _settings: Any
    ) -> tuple[list[Any], list[Any]]:
        self.receives += 1
        self.in_receive = True
        try:
            if self.receive_secs:
                await asyncio.sleep(self.receive_secs)
            taken, self.pending = self.pending[:1], self.pending[1:]
            return taken, []
        finally:
            self.in_receive = False

    async def wait_bell(self, _queues: Any, timeout_secs: float) -> str | None:
        self.bells += 1
        await asyncio.sleep(timeout_secs)
        return None

    async def return_(self, message: Message, delay_secs: float = 0.0) -> bool:
        self.return_started = True
        if self.return_secs:
            await asyncio.sleep(self.return_secs)
        self.returned.append(message)
        return True

    async def ack(self, _message: Message) -> bool:
        if self.ack_error is not None:
            raise self.ack_error
        return True

    async def extend(self, _message: Message, _secs: float) -> bool:
        return True

    async def fail(self, _message: Message, _settings: Any, delay_secs: float = 0.0) -> str:
        return "retry"

    async def mark_consumer(self, _queue: str, _ttl: int) -> None:
        return None

    async def trim_dead(self, _queue: str, _secs: int) -> int:
        return 0


def _message() -> Message:
    return Message("jobs-j", "1-0", 1, b'{"id": "x", "name": "j", "payload": {}}')


async def test_en7_stop_does_not_swallow_its_own_cancellation() -> None:
    """EN-7: `suppress(CancelledError)` around each await ate the cancel of `stop()` itself."""
    queues = FakeQueues(return_secs=1.0)
    queues.pending.append(_message())
    runner = JobRunner(queues)  # type: ignore[arg-type]  # fake
    started = asyncio.Event()

    async def job(_payload: dict[str, Any]) -> None:
        started.set()
        await asyncio.sleep(30)

    runner.register("j", job)
    await runner.start()
    await asyncio.wait_for(started.wait(), 2)
    stopping = asyncio.create_task(runner.stop(drain_secs=0))
    await until(lambda: queues.return_started)  # the job is giving its message back
    stopping.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stopping


async def test_en8_stop_lets_a_receive_in_flight_finish() -> None:
    """EN-8: with no job running, stop cancelled the loop inside RECEIVE: the message it took
    stayed hidden for the visibility time and was charged a failure."""
    queues = FakeQueues(receive_secs=0.3)
    runner = JobRunner(queues)  # type: ignore[arg-type]  # fake

    async def job(_payload: dict[str, Any]) -> None:
        return None

    runner.register("j", job)
    await runner.start()
    await until(lambda: queues.in_receive)
    queues.pending.append(_message())  # the store takes it during this receive
    await runner.stop(drain_secs=0)
    assert [m.id for m in queues.returned] == ["1-0"]


async def test_en29_idle_loop_waits_on_the_doorbell() -> None:
    """EN-29: an idle job loop polled RECEIVE ~7 times a second instead of the doorbell."""
    queues = FakeQueues()
    runner = JobRunner(queues)  # type: ignore[arg-type]  # fake

    async def job(_payload: dict[str, Any]) -> None:
        return None

    runner.register("j", job)
    await runner.start()
    await asyncio.sleep(1.1)  # time itself is under test: the idle rate
    await runner.stop(drain_secs=0)
    assert queues.bells >= 2
    assert queues.receives <= queues.bells + 1


async def test_en32_ack_gave_up_is_logged(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """EN-32: after the last failed ack the loop ended silently (the job ran again later)."""
    queues = FakeQueues()
    queues.ack_error = ConnectionError("down")
    runner = JobRunner(queues, settings=QueueSettings(visibility_secs=0.2))  # type: ignore[arg-type]  # fake
    real_sleep = asyncio.sleep
    monkeypatch.setattr(asyncio, "sleep", lambda _s: real_sleep(0))
    with caplog.at_level(logging.WARNING):
        await runner._ack("j", _message(), "x")
    assert len(events(caplog, "job_ack_gave_up")) == 1


# ---------------------------------------------------------------- round 3 (R3JE)


def _runner() -> JobRunner:
    return JobRunner(FakeQueues())  # type: ignore[arg-type]  # fake


async def _noop(_payload: dict[str, Any]) -> None:
    return None


def _fields(record: logging.LogRecord) -> dict[str, Any]:
    fields: dict[str, Any] = getattr(record, "llmq", {})
    return fields


def test_r3je5_a_sync_job_is_refused_at_register() -> None:
    """R3JE-5: a plain `def` ran to the end, then failed on its result: five runs, then dead.

    A partial of an async function and an object with an async `__call__` stay accepted.
    """
    runner = _runner()

    def sync_job(_payload: dict[str, Any]) -> None:
        return None

    with pytest.raises(ConfigError, match="async"):
        runner.register("j", sync_job)  # type: ignore[arg-type]  # the mistake under test

    class Handler:
        async def __call__(self, _payload: dict[str, Any]) -> None:
            return None

    runner.register("partial", functools.partial(_noop))
    runner.register("callable", Handler())


async def test_r3je6_register_after_start_is_refused() -> None:
    """R3JE-6: a name registered after `start` got no worker loop; its jobs waited unseen."""
    runner = _runner()
    runner.register("j", _noop)
    await runner.start()
    try:
        with pytest.raises(ConfigError, match="before start"):
            runner.register("k", _noop)
    finally:
        await runner.stop(drain_secs=0)


def test_r3je6_register_job_in_caller_mode_is_refused() -> None:
    """R3JE-6: a caller-mode process starts no job loops; its registered jobs never ran."""
    engine = SimpleNamespace(
        queues=None,
        settings=SimpleNamespace(queue_prefix="", queue_wait_replicas=0, queue_wait_ms=100),
        metrics=None,
    )
    queue = LLMQueue(engine, settings=SimpleNamespace(mode="caller"))  # type: ignore[arg-type]
    with pytest.raises(ConfigError, match="caller mode"):
        queue.register_job("j", _noop)


@pytest.mark.parametrize("name", ["", "a b", "a:b", "{x}", "x" * 65])
def test_r3je12_job_names_are_checked(name: str) -> None:
    """R3JE-12: any text went into the store key (spaces, `:`, hash-tag braces, any length)."""
    with pytest.raises(ConfigError, match="job name"):
        _runner().register(name, _noop)


class _SendQueues(FakeQueues):
    """Records the keyword arguments of `send`; raises `error` when set."""

    def __init__(self) -> None:
        super().__init__()
        self.sent: list[dict[str, Any]] = []
        self.error: Exception | None = None

    async def send(self, _queue: str, _body: bytes, _settings: Any, **kwargs: Any) -> str:
        self.sent.append(kwargs)
        if self.error is not None:
            raise self.error
        return "1-0"

    async def has_consumer(self, _queue: str) -> bool:
        return True


async def test_r3je7_send_job_waits_for_replicas() -> None:
    """R3JE-7: a job's send never waited for replicas: a failover right after lost it unseen.

    Too few replicas still sends (owner Q9): the send is warned about, not failed.
    """
    from llm_queue.engine.store_guard import ReplicaWait

    queues = _SendQueues()
    wait = ReplicaWait(replicas=1, wait_ms=50)
    runner = JobRunner(queues, replica_wait=wait)  # type: ignore[arg-type]  # fake
    await runner.send("j", {})
    assert queues.sent == [{"wait_replicas": 1, "wait_ms": 50}]
    queues.error = NotReplicated("0 of 1")
    assert await runner.send("j", {}, job_id="abc") == "abc"


class _AckQueues(FakeQueues):
    """Records the receipt (generation) each ack used."""

    def __init__(self) -> None:
        super().__init__()
        self.acked: list[int] = []

    async def ack(self, message: Message) -> bool:
        self.acked.append(message.generation)
        return True


async def test_r3je9_a_redelivered_running_job_runs_once() -> None:
    """R3JE-9: a job whose hold lapsed was received again by the same process and ran twice
    (and the first run's ack was refused as stale)."""
    queues = _AckQueues()
    runner = JobRunner(queues)  # type: ignore[arg-type]  # fake
    release = asyncio.Event()
    runs = 0

    async def job(_payload: dict[str, Any]) -> None:
        nonlocal runs
        runs += 1
        await release.wait()

    runner.register("j", job)
    queues.pending.append(_message())
    await runner.start()
    await until(lambda: runs == 1)
    body = b'{"id": "x", "name": "j", "payload": {}}'
    queues.pending.append(Message("jobs-j", "1-0", 2, body))  # the same message, new receipt
    await until(lambda: not queues.pending and not queues.in_receive)
    release.set()
    await until(lambda: bool(queues.acked))
    await runner.stop(drain_secs=1)
    assert runs == 1
    assert queues.acked == [2]  # the run acked with the newest receipt


async def test_r3je11_r3je13_failed_job_log(caplog: pytest.LogCaptureFixture) -> None:
    """R3JE-13: any TimeoutError was logged as the job's own limit. R3JE-11: the log had no
    traceback and no job id."""
    runner = _runner()

    async def inner_timeout(_payload: dict[str, Any]) -> None:
        raise TimeoutError("database")

    async def hangs(_payload: dict[str, Any]) -> None:
        await asyncio.sleep(30)

    runner.register("inner", inner_timeout)
    runner.register("slow", hangs, timeout_secs=0.05)
    with caplog.at_level(logging.WARNING):
        await runner._run("inner", _message(), asyncio.Semaphore(0))
        await runner._run("slow", _message(), asyncio.Semaphore(0))
    first, second = events(caplog, "job_failed")
    assert _fields(first)["kind"] == "TimeoutError"
    assert _fields(second)["kind"] == "job_timeout"
    assert first.exc_info is not None
    assert _fields(first)["id"] == "x"
