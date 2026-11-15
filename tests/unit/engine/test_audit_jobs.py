"""Bug-pinning tests for the job runner (EN-7, EN-8, EN-29, EN-32 and round 3, R3JE).

A fake `StreamQueues` stands in for the store, so they run without the Docker stack.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from llm_queue import LLMQueue
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.jobs import JobRunner, JobSettings, current_job
from llm_queue.engine.keys import Keys
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


class FakeClient:
    """The few store commands the job runner sends itself (dedupe, names, returns)."""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}
        self.ttl: dict[str, float] = {}

    async def set(
        self, key: str, value: bytes, *, nx: bool = False, ex: float | None = None
    ) -> bool:
        if nx and key in self.data:
            return False
        self.data[key] = value
        if ex is not None:
            self.ttl[key] = ex
        return True

    async def delete(self, key: str) -> int:
        return int(self.data.pop(key, None) is not None)

    async def sadd(self, key: str, member: str) -> int:
        self.data.setdefault(key, set()).add(member)
        return 1

    async def smembers(self, key: str) -> Any:
        return self.data.get(key, ())

    async def expire(self, key: str, secs: float) -> bool:
        self.ttl[key] = secs
        return True

    async def hincrby(self, key: str, field: str, by: int) -> int:
        row = self.data.setdefault(key, {})
        row[field] = row.get(field, 0) + by
        return int(row[field])


class FakeQueues:
    """The parts of `StreamQueues` the job runner uses; `pending` messages are handed out."""

    def __init__(self, *, receive_secs: float = 0.0, return_secs: float = 0.0) -> None:
        self.client = FakeClient()
        self.keys = Keys("t")
        self.fails: dict[str, int] = {}
        self.failed: list[tuple[float, int]] = []  # (delay, max_receives) of each fail
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

    async def fail(self, message: Message, settings: Any, delay_secs: float = 0.0) -> str:
        self.failed.append((delay_secs, settings.max_receives))
        self.fails[message.id] = self.fails.get(message.id, 0) + 1
        return "dead" if self.fails[message.id] >= settings.max_receives else "retry"

    async def failures(self, _queue: str, message_id: str) -> int:
        return self.fails.get(message_id, 0)

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
    settings = QueueSettings(visibility_secs=0.2)  # acks are tried for one visibility time
    runner = JobRunner(queues, settings=settings)  # type: ignore[arg-type]  # fake
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
        await runner._run("inner", _message(), (asyncio.Semaphore(0),))
        await runner._run("slow", _message(), (asyncio.Semaphore(0),))
    first, second = events(caplog, "job_failed")
    assert _fields(first)["kind"] == "TimeoutError"
    assert _fields(second)["kind"] == "job_timeout"
    assert first.exc_info is not None
    assert _fields(first)["id"] == "x"


# ---------------------------------------------------------------- owner decisions Q13-Q18


async def test_q13_context_and_dedupe() -> None:
    """Q13 (R3JE-1, -2): a job could not see its id or attempt; a repeated job_id queued twice."""
    queues = _SendQueues()
    runner = JobRunner(queues)  # type: ignore[arg-type]  # fake
    assert await runner.send("j", {}, job_id="abc") == "abc"
    assert await runner.send("j", {}, job_id="abc") == "abc"
    assert len(queues.sent) == 1  # the second send queued nothing
    seen: list[Any] = []

    async def job(_payload: dict[str, Any]) -> None:
        seen.append(current_job())

    runner.register("j", job)
    queues.fails["1-0"] = 2  # two failures before this run
    await runner._run("j", _message(), (asyncio.Semaphore(0),))
    assert seen[0].job_id == "x"
    assert seen[0].attempt == 3
    assert seen[0].max_attempts == JobSettings().max_attempts
    assert current_job() is None


def test_q14_retry_delays_cover_about_an_hour() -> None:
    """Q14 (R3JE-3): a fixed 5 s x 5 dead-lettered a job 20 s into any outage."""
    limits = JobSettings()
    delays = [limits.delay_secs(n) for n in range(1, limits.max_attempts)]
    assert delays[:4] == [5, 10, 20, 40]
    assert max(delays) == 300
    assert 3_600 <= sum(delays) < 3_600 + 300


async def test_q14_retry_after_is_honoured() -> None:
    """Q14: the LLM call's `retry_after_secs` was ignored; the next try came after 5 s."""
    queues = FakeQueues()
    runner = JobRunner(queues)  # type: ignore[arg-type]  # fake

    async def job(_payload: dict[str, Any]) -> None:
        raise CallFailed("rate_limited", retry_after_secs=90)

    runner.register("j", job)
    await runner._run("j", _message(), (asyncio.Semaphore(0),))
    assert queues.failed == [(90, JobSettings().max_attempts)]


async def test_q16_payload_limits() -> None:
    """Q16 (R3JE-10): a 50 MB payload was accepted; a Decimal raised a bare TypeError."""
    from decimal import Decimal

    limits = JobSettings(max_payload_bytes=1024)
    runner = JobRunner(_SendQueues(), job_settings=limits)  # type: ignore[arg-type]  # fake
    with pytest.raises(CallFailed) as big:
        await runner.send("j", {"x": "a" * 2000})
    assert big.value.kind == "request_too_large"
    with pytest.raises(CallFailed) as odd:
        await runner.send("j", {"x": Decimal(1)})
    assert odd.value.kind == "invalid_request"


async def test_q16_process_cap_over_all_names() -> None:
    """Q16 (R3JE-8): each job name had its own limit of 4 and no process-wide one."""
    queues = FakeQueues()
    limits = JobSettings(process_concurrency=1)
    runner = JobRunner(queues, job_settings=limits)  # type: ignore[arg-type]  # fake
    release = asyncio.Event()
    running = 0
    peak = 0

    async def job(_payload: dict[str, Any]) -> None:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await release.wait()
        running -= 1

    runner.register("a", job)
    runner.register("b", job)
    queues.pending += [_message(), Message("jobs-b", "2-0", 1, _message().body)]
    await runner.start()
    await asyncio.sleep(0.3)  # time is under test: the second job must not start
    assert peak == 1
    release.set()
    await runner.stop(drain_secs=1)


async def test_q17_a_job_returned_too_often_is_dead() -> None:
    """Q17: a job longer than the drain was returned at every deploy and never ended."""
    queues = FakeQueues()
    limits = JobSettings(max_returns=2)
    runner = JobRunner(queues, job_settings=limits)  # type: ignore[arg-type]  # fake
    await runner._give_back("j", _message(), "x")
    assert len(queues.returned) == 1
    await runner._give_back("j", _message(), "x")
    assert len(queues.returned) == 1  # not returned again: dead-lettered
    assert queues.failed[-1][1] == 1


async def test_q18_job_calls_carry_the_sender_ids() -> None:
    """Q18 (R3JE-15): job calls took the context of whoever called start(), not the sender."""
    from llm_queue.context import current_context, request_context

    runner = JobRunner(_SendQueues())  # type: ignore[arg-type]  # fake
    seen: list[Any] = []

    async def job(_payload: dict[str, Any]) -> None:
        seen.append(current_context())

    runner.register("j", job)
    with request_context(user_id="u1", tenant_id="t1", priority=True):
        body = runner._body("j", "x", {})
    message = Message("jobs-j", "1-0", 1, body)
    with request_context(priority=True, user_id="starter"):
        run = runner._run("j", message, (asyncio.Semaphore(0),))
        await asyncio.create_task(run, context=contextvars.Context())
    assert (seen[0].user_id, seen[0].tenant_id, seen[0].priority) == ("u1", "t1", False)


def test_job_settings_from_env() -> None:
    """Every job limit is a setting; a bad one is refused with the variable's name."""
    env = {"LLMQ_JOB_DEDUPE_SECS": "60", "LLMQ_JOB_RETRY_FOR_SECS": "10"}
    limits = JobSettings.from_env(env)
    assert (limits.dedupe_secs, limits.retry_for_secs) == (60, 10.0)
    with pytest.raises(ConfigError, match="LLMQ_JOB_CONCURRENCY"):
        JobSettings.from_env({"LLMQ_JOB_CONCURRENCY": "0"})
    with pytest.raises(ConfigError, match="LLMQ_JOB_MAX_RETURNS"):
        JobSettings.from_env({"LLMQ_JOB_MAX_RETURNS": "x"})


async def test_q15_dead_jobs_in_status_and_api() -> None:
    """Q15 (R3JE-4): dead jobs had no status entry, no alert and no API to list or redrive."""
    from llm_queue.engine.status import AlertEvaluator, StatusReader

    queues = FakeQueues()
    body = _message().body
    letters = [{"id": "9-0", "src": "1-0", "fails": 18, "body": body}]

    async def dead_letters(_queue: str, _count: int) -> list[dict[str, Any]]:
        return letters

    async def depth(_queue: str) -> tuple[int, int, int]:
        return 3, 1, 1

    queues.dead_letters = dead_letters  # type: ignore[attr-defined]  # fake
    queues.depth = depth  # type: ignore[attr-defined]  # fake
    runner = JobRunner(queues)  # type: ignore[arg-type]  # fake
    assert await runner.dead_letters("j") == [
        {"id": "9-0", "job_id": "x", "fails": 18, "payload": {}}
    ]
    await queues.client.sadd(runner.names_key, "j")
    reader = StatusReader(MagicMock(), queues.keys, queues)  # type: ignore[arg-type]  # fake
    jobs = await reader.jobs()
    assert jobs == [{"job": "j", "waiting": 2, "dead_letters": 1}]
    alerts = AlertEvaluator(None, queues.keys).evaluate(None, {"models": [], "jobs": jobs})  # type: ignore[arg-type]  # fake
    assert [(a.name, a.on, a.labels) for a in alerts] == [("job_dead_letters", True, {"job": "j"})]
