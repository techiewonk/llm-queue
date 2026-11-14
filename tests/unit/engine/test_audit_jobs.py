"""Bug-pinning tests for the job runner from the engine audit (EN-7, EN-8, EN-29, EN-32).

A fake `StreamQueues` stands in for the store, so they run without the Docker stack.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from llm_queue.engine.jobs import JobRunner
from llm_queue.engine.queue import Message


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
    runner = JobRunner(queues)  # type: ignore[arg-type]  # fake
    real_sleep = asyncio.sleep
    monkeypatch.setattr(asyncio, "sleep", lambda _s: real_sleep(0))
    with caplog.at_level(logging.WARNING):
        await runner._ack("j", _message(), "x")
    assert len(events(caplog, "job_ack_gave_up")) == 1
