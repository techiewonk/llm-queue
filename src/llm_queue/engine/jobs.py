"""Jobs on the stream queue (plan E136): `register_job` / `send_job`, run at least once.

A job is any registered async function taking one JSON payload. `send_job` puts it on
`<queue_prefix>jobs-<name>`; workers of processes that registered the name run it. A failed
job is retried after a backoff and goes to the dead-letter stream after `max_receives`; a held
job's visibility is extended while it runs. Jobs take no LLM budget (their own LLM calls do).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import orjson

from llm_queue._logging import get_logger
from llm_queue.engine.metrics import Metrics, NullSink
from llm_queue.engine.queue import Message, QueueSettings, StreamQueues
from llm_queue.errors import ConfigError
from llm_queue.ids import new_id

logger = get_logger(__name__)
JobFn = Callable[[dict[str, Any]], Awaitable[Any]]
# A jobs queue holds at most this many waiting jobs: an unregistered name filled the store,
# then lost every job after the retention (RESCAN S7).
JOBS_MAX_WAITING = 10_000
CONSUMER_TTL_SECS = 30  # a consuming process marks its job queues every third of this
ACK_TRIES = 3


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
    ) -> None:
        """Nothing runs until `start`."""
        self.queues = queues
        self.queue_prefix = queue_prefix
        self.settings = settings or QueueSettings(
            visibility_secs=60, max_receives=5, max_waiting=JOBS_MAX_WAITING
        )
        self.concurrency = concurrency
        self.metrics: Metrics = metrics or NullSink()
        self._jobs: dict[str, JobFn] = {}
        self._timeouts: dict[str, float | None] = {}
        self._warned: dict[str, float] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._running: set[asyncio.Task[None]] = set()
        self._stop = asyncio.Event()
        self._paused = False

    def queue(self, name: str) -> str:
        """The queue of one job name."""
        return f"{self.queue_prefix}jobs-{name}"

    def register(self, name: str, fn: JobFn, *, timeout_secs: float | None = None) -> None:
        """Allow this process to run job `name` (before `start`); `timeout_secs` bounds a run.

        A run past `timeout_secs` is cancelled and counts as a failure (retried, then dead).
        """
        if not name or not callable(fn):
            raise ConfigError("a job needs a name and an async function")
        if timeout_secs is not None and timeout_secs <= 0:
            raise ConfigError(f"job {name}: timeout_secs must be above 0")
        self._jobs[name] = fn
        self._timeouts[name] = timeout_secs

    async def send(
        self, name: str, payload: Mapping[str, Any], *, job_id: str | None = None
    ) -> str:
        """Queue a job; returns its id (jobs run at least once: make them idempotent)."""
        ident = job_id or new_id()
        body = orjson.dumps({"id": ident, "name": name, "payload": payload})
        await self.queues.send(self.queue(name), body, self.settings)
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
        """One worker loop per registered job."""
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
        for task in [*self._tasks, *self._running]:
            task.cancel()
        for task in [*self._tasks, *self._running]:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

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
            try:
                await gate.acquire()
                messages, dead = await self.queues.receive_wait(
                    self.queue(name), 1, self.settings, wait_secs=1.0
                )
                for letter in dead:
                    logger.error("job_dead_lettered", job=name, message=letter.id)
                if not messages:
                    gate.release()
                    continue
                if self._stop.is_set():  # stopped while receiving: back at once, not run (S5)
                    for message in messages:
                        await self.queues.return_(message)
                    gate.release()
                    return
                task = asyncio.create_task(self._run(name, messages[0], gate))
                self._running.add(task)
                task.add_done_callback(self._running.discard)
            except Exception as exc:  # a store blip must not end the loop
                gate.release()
                logger.warning("job_receive_failed", job=name, error=str(exc))
                await asyncio.sleep(1)

    async def _run(self, name: str, message: Message, gate: asyncio.Semaphore) -> None:
        keep = asyncio.create_task(self._keep_visible(message))
        ident = None
        try:
            body = orjson.loads(message.body)
            ident = body.get("id")
            # Per-job timeout (RESCAN S6): a hung job held a worker slot for good.
            await asyncio.wait_for(self._jobs[name](body["payload"]), self._timeouts.get(name))
        except asyncio.CancelledError:
            if self._stop.is_set():  # shut down mid-job: back to the queue, not a failure (S5)
                with contextlib.suppress(Exception):
                    await asyncio.shield(self.queues.return_(message))
            raise
        except Exception as exc:
            try:
                outcome = await self.queues.fail(message, self.settings, delay_secs=5.0)
            except Exception as fail_exc:
                outcome = f"not counted: {fail_exc}"
            kind = "timeout" if isinstance(exc, TimeoutError) else type(exc).__name__
            logger.warning("job_failed", job=name, error=str(exc), kind=kind, outcome=outcome)
            return
        finally:
            keep.cancel()
            gate.release()
        await self._ack(name, message, ident)

    async def _ack(self, name: str, message: Message, ident: Any) -> None:
        """Acknowledge a finished job, retried apart from the job (RESCAN S8).

        A failed ack counted as a failed job and the job ran again. A stale receipt means the
        hold lapsed and another worker took the job: it ran twice, logged as such.
        """
        for tries in range(1, ACK_TRIES + 1):
            try:
                acked = await self.queues.ack(message)
            except Exception as exc:
                logger.warning("job_ack_failed", job=name, id=ident, tries=tries, error=str(exc))
                await asyncio.sleep(0.2 * tries)
                continue
            if acked:
                logger.info("job_done", job=name, id=ident)
            else:
                logger.warning("job_duplicate_run", job=name, id=ident)
            return

    async def _keep_visible(self, message: Message) -> None:
        """Keep a running job hidden; a store blip must not end it (the job would run twice)."""
        while True:
            await asyncio.sleep(max(1.0, self.settings.visibility_secs / 3))
            try:
                if not await self.queues.extend(message, self.settings.visibility_secs):
                    logger.warning("job_lost_visibility", queue=message.queue, message=message.id)
            except Exception as exc:
                logger.warning("job_extend_failed", message=message.id, error=str(exc))
