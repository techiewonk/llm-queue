"""Jobs on the stream queue (plan E136): `register_job` / `send_job`, run at least once.

A job is any registered async function taking one JSON payload. `send_job` puts it on
`<queue_prefix>jobs-<name>`; workers of processes that registered the name run it, with
`current_job()` giving its id and attempt. A failed job is retried with an exponential delay
(5 s doubling to 5 min, for about an hour by default) and then goes to the dead letters, kept 7
days; a job returned at shutdown too often goes there as well. A held job's visibility is
extended while it runs. Jobs take no LLM budget (their own LLM calls do), and those calls carry
the sender's user and tenant ids at normal priority. Limits are `JobSettings` (`LLMQ_JOB_*`).
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import inspect
import math
import random
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, fields
from typing import Any

import orjson

from llm_queue._logging import get_logger
from llm_queue.context import current_context, request_context
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.metrics import Metrics, NullSink
from llm_queue.engine.queue import Message, NotReplicated, QueueSettings, StreamQueues
from llm_queue.engine.store_guard import ReplicaWait
from llm_queue.errors import ConfigError
from llm_queue.ids import new_id

logger = get_logger(__name__)
JobFn = Callable[[dict[str, Any]], Awaitable[Any]]
# A jobs queue holds at most this many waiting jobs: an unregistered name filled the store,
# then lost every job after the retention (RESCAN S7).
JOBS_MAX_WAITING = 10_000
CONSUMER_TTL_SECS = 30  # a consuming process marks its job queues every third of this
# An idle job loop blocks this long on its doorbell. Kept under 1 s: tuning.py only checks the
# store read timeout against the model loops' BLPOP (`idle_slow_secs`), and a BLPOP longer than
# the read timeout fails. 2 BLPOPs/s idle, against ~7 RECEIVE scripts/s before (unverified).
_BELL_WAIT_SECS = 0.5
# A job name goes into store keys: no spaces, `:` or `{}` (hash tags), and a bounded length
# (R3JE-12). Within the model queues' own rule, so `jobs-<name>` is a valid queue name.
_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_JOB_ID_MAX = 200  # characters of an app job id (it is part of the dedupe key)
_CLAIM_SECS = 60  # a dedupe claim lives this long until its send is confirmed
_MAX_STORE_SECS = 1_209_600  # the longest a dead letter may be kept (two weeks, as models)


@dataclass(frozen=True, slots=True)
class JobSettings:
    """Job limits of one process (`LLMQ_JOB_*`; owner decisions Q13-Q17).

    Retries wait `retry_base_secs`, doubling up to `retry_max_secs`, until about
    `retry_for_secs` have passed; then the job is dead and kept `dead_retention_secs`.
    """

    dedupe_secs: int = 86_400  # a send with the same job_id inside this returns the first
    retry_base_secs: float = 5.0
    retry_max_secs: float = 300.0
    retry_for_secs: float = 3_600.0
    dead_retention_secs: int = 604_800  # 7 days
    concurrency: int = 4  # running jobs per job name per process
    process_concurrency: int = 32  # running jobs per process, all names together
    max_payload_bytes: int = 1_048_576
    max_returns: int = 5  # returns at shutdown before a job is dead-lettered

    def __post_init__(self) -> None:
        """Refuse impossible values, every problem at once (`ConfigError`)."""
        problems = []
        for spec in fields(self):
            value = getattr(self, spec.name)
            if not isinstance(value, int | float) or not math.isfinite(value) or value < 0:
                problems.append(f"LLMQ_JOB_{spec.name.upper()} must be a number of 0 or more")
        if not problems:
            for name in ("retry_base_secs", "concurrency", "process_concurrency"):
                if getattr(self, name) <= 0:
                    problems.append(f"LLMQ_JOB_{name.upper()} must be above 0")
            if self.max_payload_bytes < 1024:
                problems.append("LLMQ_JOB_MAX_PAYLOAD_BYTES must be at least 1024")
            if self.retry_max_secs < self.retry_base_secs:
                problems.append("LLMQ_JOB_RETRY_MAX_SECS must be at least RETRY_BASE_SECS")
            if not 60 <= self.dead_retention_secs <= _MAX_STORE_SECS:
                problems.append(
                    f"LLMQ_JOB_DEAD_RETENTION_SECS must be 60 to {_MAX_STORE_SECS} (two weeks)"
                )
        if problems:
            raise ConfigError("invalid job settings:\n  " + "\n  ".join(problems))

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> JobSettings:
        """Read `LLMQ_JOB_<FIELD>` (for example `LLMQ_JOB_DEDUPE_SECS`); unset keeps the default."""
        values: dict[str, Any] = {}
        problems = []
        for spec in fields(cls):
            raw = env.get(f"LLMQ_JOB_{spec.name.upper()}")
            if raw is None or raw == "":
                continue
            kind = float if spec.name.endswith("_secs") and "retry" in spec.name else int
            try:
                values[spec.name] = kind(raw)
            except ValueError:
                problems.append(f"LLMQ_JOB_{spec.name.upper()} must be a number")
        if problems:
            raise ConfigError("invalid job settings:\n  " + "\n  ".join(problems))
        return cls(**values)

    def delay_secs(self, attempt: int) -> float:
        """The wait before the next try after failed attempt `attempt` (1-based)."""
        return float(min(self.retry_base_secs * 2.0 ** min(attempt - 1, 60), self.retry_max_secs))

    @property
    def max_attempts(self) -> int:
        """Tries before the dead letters: enough for the delays to cover `retry_for_secs`."""
        attempts, waited = 1, 0.0
        while waited < self.retry_for_secs:
            waited += self.delay_secs(attempts)
            attempts += 1
        return attempts


@dataclass(frozen=True, slots=True)
class JobContext:
    """The job being run: its name, id and attempt (1 on the first run).

    `job_id` is what `send_job` returned: use it as the idempotency key of the job's writes.
    `attempt == max_attempts` is the last try before the dead letters (Q13, R3JE-1).
    """

    name: str
    job_id: str
    attempt: int
    max_attempts: int


_JOB: contextvars.ContextVar[JobContext | None] = contextvars.ContextVar("llmq_job", default=None)


def current_job() -> JobContext | None:
    """The job this code runs in, or None outside a job."""
    return _JOB.get()


def _is_async(fn: object) -> bool:
    """True for an async function, a partial of one, or an object with an async `__call__`.

    R3JE-5: a plain `def` ran, then failed on its result, five times over.
    """
    while isinstance(fn, functools.partial):
        fn = fn.func
    if inspect.iscoroutinefunction(fn):
        return True
    call = inspect.getattr_static(type(fn), "__call__", None)  # inherited ones too
    return call is not None and inspect.iscoroutinefunction(call)


class JobRunner:
    """Registered jobs of one process, their senders and their worker loops."""

    def __init__(
        self,
        queues: StreamQueues,
        *,
        queue_prefix: str = "",
        settings: QueueSettings | None = None,
        job_settings: JobSettings | None = None,
        metrics: Metrics | None = None,
        replica_wait: ReplicaWait | None = None,
    ) -> None:
        """Nothing runs until `start`.

        `replica_wait` is the process's (scope `jobs`): a send waits for the replicas as a
        call's send does (R3JE-7, R3FM-5, owner Q8).
        """
        self.queues = queues
        self.queue_prefix = queue_prefix
        self.limits = job_settings or JobSettings()
        self.settings = settings or QueueSettings(
            visibility_secs=60,
            max_receives=self.limits.max_attempts,
            max_waiting=JOBS_MAX_WAITING,
            dlq_retention_secs=self.limits.dead_retention_secs,
        )
        self.metrics: Metrics = metrics or NullSink()
        self.replica_wait = replica_wait or ReplicaWait()
        self._jobs: dict[str, JobFn] = {}
        self._timeouts: dict[str, float | None] = {}
        self._concurrency: dict[str, int] = {}
        self._warned: dict[str, float] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._running: set[asyncio.Task[None]] = set()
        self._stop = asyncio.Event()
        self._paused = False
        self._started = False
        self._all: asyncio.Semaphore | None = None  # the per-process cap, made on start
        # The latest receipt of each running message, by (queue, id): a redelivery of a
        # running job moves its receipt here instead of starting a second run (R3JE-9).
        self._live: dict[tuple[str, str], Message] = {}

    def queue(self, name: str) -> str:
        """The queue of one job name."""
        return f"{self.queue_prefix}jobs-{name}"

    @property
    def names_key(self) -> str:
        """The store set of job names consumed in this environment (for `status()`)."""
        return f"{self.queues.keys.prefix}:jobs:{self.queue_prefix}"

    def register(
        self,
        name: str,
        fn: JobFn,
        *,
        timeout_secs: float | None = None,
        concurrency: int | None = None,
    ) -> None:
        """Allow this process to run job `name` (before `start`); `timeout_secs` bounds a run.

        A run past `timeout_secs` is cancelled and counts as a failure (retried, then dead).
        `concurrency` (default `LLMQ_JOB_CONCURRENCY`, 4) caps its runs in this process.
        `ConfigError` for a bad name, a function that is not async, or a call after `start`.
        """
        if not _NAME_RE.match(name):
            raise ConfigError(
                f"job name {name!r}: use 1-64 letters, digits, '_' or '-' (it is part of a key)"
            )
        if not _is_async(fn):
            # A plain `def` ran to the end (side effects done), then failed on its result:
            # five runs, then a dead letter (R3JE-5). Blocking work goes in asyncio.to_thread.
            raise ConfigError(f"job {name}: the function must be async (`async def`)")
        if self._started:
            # A name registered after `start` got no worker loop and its jobs waited unseen
            # (R3JE-6).
            raise ConfigError(f"job {name}: register jobs before start()")
        if timeout_secs is not None and timeout_secs <= 0:
            raise ConfigError(f"job {name}: timeout_secs must be above 0")
        if concurrency is not None and (isinstance(concurrency, bool) or concurrency < 1):
            raise ConfigError(f"job {name}: concurrency must be 1 or more")
        self._jobs[name] = fn
        self._timeouts[name] = timeout_secs
        self._concurrency[name] = concurrency or self.limits.concurrency

    def _body(self, name: str, ident: str, payload: Mapping[str, Any]) -> bytes:
        """The message of one job; refuses a payload that is not JSON or is too large."""
        ctx = current_context()
        # The sender's ids travel with the job: its LLM calls are counted and audited for
        # the user who asked, not for whoever started the worker (R3JE-15, Q18).
        who = {k: v for k, v in (("user_id", ctx.user_id), ("tenant_id", ctx.tenant_id)) if v}
        try:
            body = orjson.dumps({"id": ident, "name": name, "payload": payload, "ctx": who})
        except TypeError as exc:  # orjson.JSONEncodeError: a bare TypeError before (R3JE-10)
            raise CallFailed(
                "invalid_request", f"job {name}: the payload is not JSON: {exc}", request_id=ident
            ) from exc
        if len(body) > self.limits.max_payload_bytes:
            raise CallFailed(
                "request_too_large",
                f"job {name}: payload of {len(body)} bytes, above "
                f"{self.limits.max_payload_bytes}; keep large inputs in your own storage and "
                "send a reference",
                request_id=ident,
            )
        return body

    async def send(
        self, name: str, payload: Mapping[str, Any], *, job_id: str | None = None
    ) -> str:
        """Queue a job; returns its id (jobs run at least once: make them idempotent).

        A second send with the same `job_id` within `dedupe_secs` (24 h) queues nothing and
        returns that id (Q13, R3JE-2). The send waits for replicas like a call send (R3JE-7,
        R3FM-5): a primary lost right after it lost the job unnoticed. Too few replicas: sent
        anyway, warned and counted (owner Q9).
        """
        if job_id is not None and not 0 < len(job_id) <= _JOB_ID_MAX:
            raise CallFailed("invalid_request", f"job_id must be 1-{_JOB_ID_MAX} characters")
        ident = job_id or new_id()
        body = self._body(name, ident, payload)
        queue = self.queue(name)
        claim = self.queues.keys.queue(queue, f"id:{ident}")
        dedupe = job_id is not None and self.limits.dedupe_secs > 0
        # Claimed first for a short time, kept for the window only once the send worked: a
        # sender that dies between the two leaves a claim that lapses, not a lost job.
        if dedupe and not await self.queues.client.set(claim, b"1", nx=True, ex=_CLAIM_SECS):
            logger.info("job_duplicate_send", job=name, id=ident)
            return ident
        try:
            await self.queues.send(
                queue,
                body,
                self.settings,
                wait_replicas=self.replica_wait.replicas_for("jobs"),
                wait_ms=self.replica_wait.wait_ms,
            )
        except NotReplicated as exc:
            self.replica_wait.short("jobs", self.metrics, str(exc))
        except BaseException:
            if dedupe:
                with contextlib.suppress(Exception):
                    await self.queues.client.delete(claim)
            raise
        if dedupe:
            try:
                await self.queues.client.set(claim, b"1", ex=self.limits.dedupe_secs)
            except Exception as exc:  # the job is queued; only the window is shorter
                logger.warning("job_dedupe_not_kept", job=name, id=ident, error=str(exc))
        await self._check_consumer(name)
        return ident

    async def _check_consumer(self, name: str) -> None:
        """Warn (once a minute per name) when a job is sent and no process consumes it (S7)."""
        try:
            if await self.queues.has_consumer(self.queue(name)):
                return
        except Exception:  # only a warning: never fail a send that worked
            return
        self.metrics.count("llmq_jobs_no_consumer_total", job=name)
        now = time.monotonic()
        if now - self._warned.get(name, -1e9) >= 60:
            self._warned[name] = now
            logger.warning("job_no_consumer", job=name, queue=self.queue(name))

    async def dead_letters(self, name: str, count: int = 10) -> list[dict[str, Any]]:
        """Dead jobs of one name, oldest first: entry id, job id, failures and payload (Q15)."""
        out = []
        for letter in await self.queues.dead_letters(self.queue(name), count):
            try:
                body = orjson.loads(letter["body"])
            except orjson.JSONDecodeError:
                body = {}
            out.append(
                {
                    "id": letter["id"],
                    "job_id": body.get("id"),
                    "fails": letter["fails"],
                    "payload": body.get("payload"),
                }
            )
        return out

    async def redrive(self, name: str, count: int = 1000) -> int:
        """Send up to `count` dead jobs of one name back to their queue; returns how many."""
        return await self.queues.redrive(
            self.queue(name),
            count,
            self.settings.retention_secs,
            max_waiting=self.settings.max_waiting,
        )

    async def start(self) -> None:
        """One worker loop per registered job; later `register` calls are refused."""
        self._started = True
        self._all = asyncio.Semaphore(self.limits.process_concurrency)
        for name in self._jobs:
            self._tasks.append(asyncio.create_task(self._loop(name)))

    def pause(self) -> None:
        """Take no new jobs; running ones go on (`stop()` follows)."""
        self._paused = True

    async def stop(self, drain_secs: float = 10.0) -> None:
        """Stop receiving, let running jobs finish (up to `drain_secs`), then stop.

        A job still running then is cancelled and its message returned, not failed; after
        `max_returns` such returns it is dead-lettered instead (Q17).
        """
        self._stop.set()
        if self._running:
            await asyncio.wait(set(self._running), timeout=drain_secs)
        if self._tasks:
            # Let the loops end by themselves: cancelling one inside its RECEIVE, after the
            # store took a message, hid that message for the visibility time and charged it a
            # failure when the hold lapsed (EN-8). A loop waits at most `_BELL_WAIT_SECS`.
            await asyncio.wait(set(self._tasks), timeout=_BELL_WAIT_SECS + 0.5)
        tasks = [*self._tasks, *self._running]
        for task in tasks:
            task.cancel()
        # gather, not `suppress(CancelledError)` around each await: that also swallowed the
        # cancellation of `stop()` itself (a shutdown timeout), and hid errors unlogged (EN-7).
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.warning("job_task_failed", error=str(result), kind=type(result).__name__)
        self._tasks.clear()

    async def _upkeep(self, name: str) -> None:
        """Mark this process as the consumer, list the name for `status()`, trim dead letters."""
        queue = self.queue(name)
        await self.queues.mark_consumer(queue, CONSUMER_TTL_SECS)
        client = self.queues.client
        await client.sadd(self.names_key, name)
        await client.expire(self.names_key, self.settings.dlq_retention_secs)
        await self.queues.trim_dead(queue, self.settings.dlq_retention_secs)

    async def _loop(self, name: str) -> None:
        gate = asyncio.Semaphore(self._concurrency.get(name, self.limits.concurrency))
        every = self._all or asyncio.Semaphore(self.limits.process_concurrency)
        marked = -1e9
        while not self._stop.is_set():
            if self._paused:
                await asyncio.sleep(0.2)
                continue
            if time.monotonic() - marked >= CONSUMER_TTL_SECS / 3:
                try:
                    await self._upkeep(name)
                    marked = time.monotonic()
                except Exception as exc:
                    logger.warning("job_upkeep_failed", job=name, error=str(exc))
            await gate.acquire()
            try:
                await every.acquire()  # the per-process cap over every name (Q16)
            except BaseException:
                gate.release()
                raise
            held = True
            try:
                messages, dead = await self.queues.receive(self.queue(name), 1, self.settings)
                for letter in dead:
                    logger.error("job_dead_lettered", job=name, message=letter.id)
                    self.metrics.count("llmq_jobs_total", job=name, outcome="dead")
                if not messages:
                    gate.release()
                    every.release()
                    held = False
                    if not dead and not self._stop.is_set():
                        # Sleep on the doorbell every send rings, as the model loops do: the
                        # receive poll ran ~7 scripts/s per job name per idle process (EN-29).
                        # The timeout still picks up delayed retries, which ring no bell.
                        await self.queues.wait_bell([self.queue(name)], _BELL_WAIT_SECS)
                    continue
                if self._stop.is_set():  # stopped while receiving: back at once, not run (S5)
                    for message in messages:
                        await self.queues.return_(message)
                    gate.release()
                    every.release()
                    return
                message = messages[0]
                key = (message.queue, message.id)
                if key in self._live:
                    # Its hold lapsed while it runs here (a blocked loop, a store outage): keep
                    # one run and give it the new receipt; it ran twice in one process and its
                    # ack was refused as stale (R3JE-9).
                    self._live[key] = message
                    gate.release()
                    every.release()
                    held = False
                    logger.warning("job_redelivered_while_running", job=name, message=message.id)
                    continue
                self._live[key] = message
                # A fresh context: the job's calls took `priority` and the ids of whoever
                # called `start()` (R3JE-15); the sender's ids are set in `_run_once`.
                task = asyncio.create_task(
                    self._run(name, message, (gate, every)), context=contextvars.Context()
                )
                held = False  # the job releases them
                self._running.add(task)
                task.add_done_callback(self._running.discard)
            except Exception as exc:  # a store blip must not end the loop
                if held:
                    gate.release()
                    every.release()
                logger.warning("job_receive_failed", job=name, error=str(exc))
                await asyncio.sleep(1)

    def _receipt(self, message: Message) -> Message:
        """The latest receipt of a running message (a redelivery may have replaced it)."""
        return self._live.get((message.queue, message.id), message)

    async def _run(self, name: str, message: Message, gates: tuple[asyncio.Semaphore, ...]) -> None:
        try:
            await self._run_once(name, message, gates)
        finally:
            self._live.pop((message.queue, message.id), None)

    async def _attempt(self, message: Message) -> int:
        """This run's attempt number: failures so far plus one (1 when the store cannot say)."""
        try:
            return await self.queues.failures(message.queue, message.id) + 1
        except Exception:
            return 1

    async def _run_once(
        self, name: str, message: Message, gates: tuple[asyncio.Semaphore, ...]
    ) -> None:
        keep = asyncio.create_task(self._keep_visible(message))
        ident = None
        attempt = 1
        limit = asyncio.timeout(self._timeouts.get(name))
        token = None
        try:
            body = orjson.loads(message.body)
            ident = body.get("id")
            attempt = await self._attempt(message)
            who = body.get("ctx") or {}
            token = _JOB.set(JobContext(name, str(ident), attempt, self.settings.max_receives))
            with request_context(user_id=who.get("user_id"), tenant_id=who.get("tenant_id")):
                # Per-job timeout (RESCAN S6): a hung job held a worker slot for good.
                async with limit:
                    await self._jobs[name](body["payload"])
        except asyncio.CancelledError:
            if self._stop.is_set():  # shut down mid-job: back to the queue, not a failure (S5)
                with contextlib.suppress(Exception):
                    await asyncio.shield(self._give_back(name, message, ident))
            raise
        except Exception as exc:
            delay = self.limits.delay_secs(attempt)
            if isinstance(exc, CallFailed) and exc.retry_after_secs:
                delay = max(delay, exc.retry_after_secs)  # the provider said when (Q14)
            try:
                outcome = await self.queues.fail(
                    self._receipt(message), self.settings, delay_secs=delay
                )
            except Exception as fail_exc:
                outcome = f"not counted: {fail_exc}"
            # `job_timeout` only when `timeout_secs` ran out: a TimeoutError raised inside the
            # job (a database timeout) read as the job's own limit (R3JE-13). The traceback
            # and the id go with it: a job bug could not be found from the logs (R3JE-11).
            kind = "job_timeout" if limit.expired() else type(exc).__name__
            log = logger.error if outcome == "dead" else logger.warning
            log(
                "job_failed",
                job=name,
                id=ident,
                message=message.id,
                attempt=attempt,
                error=str(exc),
                kind=kind,
                outcome=outcome,
                retry_in_secs=delay if outcome == "retry" else None,
                exc_info=exc,
            )
            self.metrics.count("llmq_jobs_total", job=name, outcome=str(outcome).split(":")[0])
            return
        finally:
            if token is not None:
                _JOB.reset(token)
            keep.cancel()
            for gate in gates:
                gate.release()
        await self._ack(name, self._receipt(message), ident)

    async def _give_back(self, name: str, message: Message, ident: Any) -> None:
        """Return a job cut by shutdown; dead-letter it after `max_returns` returns (Q17).

        A job longer than the drain was returned at every deploy with no failure counted, so
        it restarted from zero each time and never ended.
        """
        receipt = self._receipt(message)
        key = self.queues.keys.queue(message.queue, "returns")
        try:
            returns = int(await self.queues.client.hincrby(key, message.id, 1))
            await self.queues.client.expire(key, self.settings.retention_secs)
        except Exception as exc:  # not counted: the job still goes back
            logger.warning("job_return_not_counted", job=name, id=ident, error=str(exc))
            returns = 0
        if self.limits.max_returns and returns >= self.limits.max_returns:
            last = QueueSettings(
                visibility_secs=self.settings.visibility_secs,
                max_receives=1,
                retention_secs=self.settings.retention_secs,
                dlq_retention_secs=self.settings.dlq_retention_secs,
                max_waiting=self.settings.max_waiting,
            )
            outcome = await self.queues.fail(receipt, last)
            logger.error("job_dead_lettered", job=name, id=ident, returns=returns, outcome=outcome)
            self.metrics.count("llmq_jobs_total", job=name, outcome="dead")
            return
        await self.queues.return_(receipt)
        logger.warning("job_returned", job=name, id=ident, returns=returns)
        self.metrics.count("llmq_jobs_total", job=name, outcome="returned")

    async def _ack(self, name: str, message: Message, ident: Any) -> None:
        """Acknowledge a finished job, retried apart from the job (RESCAN S8).

        A failed ack counted as a failed job and the job ran again. A stale receipt means the
        hold lapsed and another worker took the job: it ran twice, logged as such. Retried
        for up to a visibility time with jittered back-off (R3FM-5): three tries within about
        1 s re-ran every job that finished during a failover.
        """
        give_up = time.monotonic() + self.settings.visibility_secs
        tries = 0
        while True:
            tries += 1
            try:
                acked = await self.queues.ack(message)
            except Exception as exc:
                logger.warning("job_ack_failed", job=name, id=ident, tries=tries, error=str(exc))
                if time.monotonic() >= give_up:
                    break
                # Capped exponent: 2**tries overflowed a float after ~1,000 fast failures.
                await asyncio.sleep(random.uniform(0.05, min(2.0, 0.2 * 2 ** min(tries, 10))))
                continue
            if acked:
                logger.info("job_done", job=name, id=ident)
                self.metrics.count("llmq_jobs_total", job=name, outcome="done")
            else:
                logger.warning("job_duplicate_run", job=name, id=ident)
            return
        # Every try failed: the job runs again once its hold lapses. Say so once (EN-32).
        logger.error("job_ack_gave_up", job=name, id=ident, tries=tries)
        self.metrics.count("llmq_job_ack_failed_total", job=name)

    async def _keep_visible(self, message: Message) -> None:
        """Keep a running job hidden; a store blip must not end it (the job would run twice)."""
        while True:
            await asyncio.sleep(max(1.0, self.settings.visibility_secs / 3))
            try:
                if not await self.queues.extend(
                    self._receipt(message), self.settings.visibility_secs
                ):
                    logger.warning("job_lost_visibility", queue=message.queue, message=message.id)
            except Exception as exc:
                logger.warning("job_extend_failed", message=message.id, error=str(exc))
