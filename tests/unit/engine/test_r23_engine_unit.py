"""Round 3 engine findings that need no store (R3RC-7, R3FM-10, Q8 / Q9, Q33, R3FM-5).

Each test names its finding and what used to happen. Fakes stand in for the store client.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from llm_queue.engine.jobs import JobRunner
from llm_queue.engine.queue import NotReplicated
from llm_queue.engine.runtime import StoreClock, _api_kwargs
from llm_queue.engine.store_guard import ReplicaWait, parse_scopes


class _Clock:
    """A store client whose TIME answers `secs` after `delay_secs`."""

    def __init__(self) -> None:
        self.secs = 1000.0
        self.delay_secs = 0.0

    async def time(self) -> tuple[int, int]:
        await asyncio.sleep(self.delay_secs)
        return int(self.secs), int((self.secs % 1) * 1e6)


async def test_r3rc7_a_slow_time_read_keeps_the_old_offset() -> None:
    """R3RC-7 / R3FM-10: the midpoint of a TIME read that took seconds set the store clock.

    A read slowed by a failover's retries put `now()` seconds ahead for a whole beat.
    """
    client = _Clock()
    clock = StoreClock()
    assert await clock.sync(client) is None  # type: ignore[arg-type]  # a fake client
    before = clock.now()
    client.delay_secs = 0.4
    assert await clock.sync(client) == "slow"  # type: ignore[arg-type]
    assert abs(clock.now() - before - 0.4) < 0.15  # the old offset, not the slow midpoint


async def test_r3fm10_a_store_clock_jump_is_reported() -> None:
    """R3FM-10: a store clock that stepped (a promotion, a VM resume) went unnoticed."""
    client = _Clock()
    clock = StoreClock()
    await clock.sync(client)  # type: ignore[arg-type]  # a fake client
    client.secs += 30
    assert await clock.sync(client) == "jump"  # type: ignore[arg-type]


def test_q8_replica_wait_scopes_and_no_replica() -> None:
    """Owner Q8 / R3FM-11: the wait covers calls, jobs and registry, each switchable.

    A WAIT with no replica connected cannot succeed: it cost `wait_ms` per send.
    """
    wait = ReplicaWait(replicas=1, scopes=frozenset({"calls", "registry"}))
    assert wait.replicas_for("calls") == 1
    assert wait.replicas_for("jobs") == 0
    wait.connected = 0
    assert wait.replicas_for("calls") == 0
    with pytest.raises(ValueError, match="scope"):
        wait.replicas_for("nope")
    assert parse_scopes("all")[0] == frozenset({"calls", "jobs", "registry"})
    assert parse_scopes("none")[0] == frozenset()
    assert parse_scopes("calls,bad")[1]


class _Counts:
    def __init__(self) -> None:
        self.counts: list[tuple[str, dict[str, str]]] = []

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        self.counts.append((name, labels))


def test_q9_a_short_send_is_counted_and_warned_once_a_minute(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Owner Q9: fewer replicas than asked sends anyway, warns once a minute, counts."""
    caplog.set_level(logging.WARNING)
    wait, sink = ReplicaWait(replicas=2), _Counts()
    wait.short("calls", sink)
    wait.short("calls", sink)
    assert len(sink.counts) == 2
    assert caplog.text.count("replicas_short") == 1


def test_q33_converse_api_only_for_providers_that_take_it() -> None:
    """Owner Q33: a 0.1.0b1 custom provider without `converse_api` got a TypeError per call."""

    class Old:
        async def call(self, op: str, params: Any, route: Any, timing: Any) -> None: ...

    class New:
        async def call(self, *args: Any, converse_api: str = "converse") -> None: ...

    class View:
        def value(self, alias: str, name: str) -> str:
            return "stream"

    assert _api_kwargs(Old(), View(), "m") == {}  # type: ignore[arg-type]  # a fake view
    assert _api_kwargs(New(), View(), "m") == {"converse_api": "stream"}  # type: ignore[arg-type]


class _Queues:
    """Job queues whose send records its replica wait and may refuse for replicas."""

    def __init__(self, short: bool) -> None:
        self.short = short
        self.waits: list[int] = []

    async def send(self, queue: str, body: bytes, settings: Any, **kwargs: Any) -> str:
        self.waits.append(int(kwargs.get("wait_replicas", 0)))
        if self.short:
            raise NotReplicated("1 of 2 replicas")
        return "1-0"

    async def has_consumer(self, queue: str) -> bool:
        return True


async def test_r3fm5_a_job_send_waits_for_replicas_and_sends_anyway() -> None:
    """R3FM-5: a job send never waited for replicas; a lost primary lost the job unnoticed."""
    queues = _Queues(short=True)
    runner = JobRunner(queues, replica_wait=ReplicaWait(replicas=2))  # type: ignore[arg-type]
    ident = await runner.send("j", {})
    assert ident
    assert queues.waits == [2]
