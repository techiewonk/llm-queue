"""Jobs on the stream queue (plan E136): `register_job` / `send_job`, run at least once.

A job is any registered async function taking one JSON payload. `send_job` puts it on
`<queue_prefix>jobs-<name>`; workers of processes that registered the name run it. A failed
job is retried after a backoff and goes to the dead-letter stream after `max_receives`; a held
job's visibility is extended while it runs. Jobs take no LLM budget (their own LLM calls do).
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
import random
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import orjson

from llm_queue._logging import get_logger
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
ACK_TRIES = 3
# An idle job loop blocks this long on its doorbell. Kept under 1 s: tuning.py only checks the
# store read timeout against the model loops' BLPOP (`idle_slow_secs`), and a BLPOP longer than
# the read timeout fails. 2 BLPOPs/s idle, against ~7 RECEIVE scripts/s before (unverified).
_BELL_WAIT_SECS = 0.5
# A job name goes into store keys: no spaces, `:` or `{}` (hash tags), and a bounded length
# (R3JE-12). Within the model queues' own rule, so `jobs-<name>` is a valid queue name.
_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


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
        concurrency: int = 4,
        metrics: Metrics | None = None,
        replica_wait: ReplicaWait | None = None,
    ) -> None:
        """Nothing runs until `start`.

        `replica_wait` is the process's (scope `jobs`): a send waits for the replicas as a
        call's send does (R3JE-7, R3FM-5, owner Q8).
        """
        self.queues = queues
        self.queue_prefix = queue_prefix
        self.settings = settings or QueueSettings(
            visibility_secs=60, max_receives=5, max_waiting=JOBS_MAX_WAITING
        )
        self.concurrency = concurrency
        self.metrics: Metrics = metrics or NullSink()
        self.replica_wait = replica_wait or ReplicaWait()
        self._jobs: dict[str, JobFn] = {}
        self._timeouts: dict[str, float | None] = {}
        self._warned: dict[str, float] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._running: set[asyncio.Task[None]] = set()
        self._stop = asyncio.Event()
        self._paused = False
        self._started = False
        # The latest receipt of each running message, by (queue, id): a redelivery of a
        # running job moves its receipt here instead of starting a second run (R3JE-9).
        self._live: dict[tuple[str, str], Message] = {}

    def queue(self, name: str) -> str:
        """The queue of one job name."""
        return f"{self.queue_prefix}jobs-{name}"

    def register(self, name: str, fn: JobFn, *, timeout_secs: float | None = None) -> None:
        """Allow this process to run job `name` (before `start`); `timeout_secs` bounds a run.

        A run past `timeout_secs` is cancelled and counts as a failure (retried, then dead).
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
        self._jobs[name] = fn
        self._timeouts[name] = timeout_secs

    async def send(
        self, name: str, payload: Mapping[str, Any], *, job_id: str | None = None
    ) -> str:
        """Queue a job; returns its id (jobs run at least once: make them idempotent).

        Waits for replicas like a call send (R3JE-7, R3FM-5): a primary lost right after the
        send lost the job for good, unnoticed. Too few replicas: sent anyway, warned (Q9).
        """
        ident = job_id or new_id()
        body = orjson.dumps({"id": ident, "name": name, "payload": payload})
        try:
            await self.queues.send(
                self.queue(name),
                body,
                self.settings,
                wait_replicas=self.replica_wait.replicas_for("jobs"),
                wait_ms=self.replica_wait.wait_ms,
            )
        except NotReplicated as exc:
            self.replica_wait.short("jobs", self.metrics, str(exc))
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

    async def start(self) -> None:
        """One worker loop per registered job; later `register` calls are refused."""
        self._started = True
        for name in self._jobs:
            self._tasks.append(asyncio.create_task(self._loop(name)))

    def pause(self) -> None:
        """Take no new jobs; running ones go on (`stop()` follows)."""
        self._paused = True

    async def stop(self, drain_secs: float = 10.0) -> None:
        """Stop receiving, let running jobs finish (up to `drain_secs`), then stop.

        A job still running then is cancelled and its message returned, not failed.
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
        """Mark this process as the queue's consumer; drop dead letters past their retention."""
        queue = self.queue(name)
        await self.queues.mark_consumer(queue, CONSUMER_TTL_SECS)
        await self.queues.trim_dead(queue, self.settings.dlq_retention_secs)

    async def _loop(self, name: str) -> None:
        gate = asyncio.Semaphore(self.concurrency)
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
            held = True
            try:
                messages, dead = await self.queues.receive(self.queue(name), 1, self.settings)
                for letter in dead:
                    logger.error("job_dead_lettered", job=name, message=letter.id)
                if not messages:
                    gate.release()
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
                    return
                message = messages[0]
                key = (message.queue, message.id)
                if key in self._live:
                    # Its hold lapsed while it runs here (a blocked loop, a store outage): keep
                    # one run and give it the new receipt; it ran twice in one process and its
                    # ack was refused as stale (R3JE-9).
                    self._live[key] = message
                    gate.release()
                    held = False
                    logger.warning("job_redelivered_while_running", job=name, message=message.id)
                    continue
                self._live[key] = message
                task = asyncio.create_task(self._run(name, message, gate))
                held = False  # the job releases it
                self._running.add(task)
                task.add_done_callback(self._running.discard)
            except Exception as exc:  # a store blip must not end the loop
                if held:
                    gate.release()
                logger.warning("job_receive_failed", job=name, error=str(exc))
                await asyncio.sleep(1)

    def _receipt(self, message: Message) -> Message:
        """The latest receipt of a running message (a redelivery may have replaced it)."""
        return self._live.get((message.queue, message.id), message)

    async def _run(self, name: str, message: Message, gate: asyncio.Semaphore) -> None:
        try:
            await self._run_once(name, message, gate)
        finally:
            self._live.pop((message.queue, message.id), None)

    async def _run_once(self, name: str, message: Message, gate: asyncio.Semaphore) -> None:
        keep = asyncio.create_task(self._keep_visible(message))
        ident = None
        limit = asyncio.timeout(self._timeouts.get(name))
        try:
            body = orjson.loads(message.body)
            ident = body.get("id")
            # Per-job timeout (RESCAN S6): a hung job held a worker slot for good.
            async with limit:
                await self._jobs[name](body["payload"])
        except asyncio.CancelledError:
            if self._stop.is_set():  # shut down mid-job: back to the queue, not a failure (S5)
                with contextlib.suppress(Exception):
                    await asyncio.shield(self.queues.return_(self._receipt(message)))
            raise
        except Exception as exc:
            try:
                outcome = await self.queues.fail(
                    self._receipt(message), self.settings, delay_secs=5.0
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
                error=str(exc),
                kind=kind,
                outcome=outcome,
                exc_info=exc,
            )
            return
        finally:
            keep.cancel()
            gate.release()
        await self._ack(name, self._receipt(message), ident)

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
