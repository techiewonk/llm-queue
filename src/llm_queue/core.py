"""`LLMQueue`: the library's front door (plan §3.15).

Built from the environment (`from_env`: stores, registry, clients) or from parts. `start()`
publishes / loads the registry and runs the worker side in this process; `converse`, `chat` and
`embed` send calls through the model's queue; `*_sync` run on the direct path: on the started
queue's own loop, else on one private loop per process (E2); jobs, cancel, status, health and the
registry are here too. One started queue per process is the norm (C2): a second one is warned
about, a forked child or another event loop gets a clear error instead of hanging.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import os
import socket
import threading
import warnings
import weakref
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Mapping,
    Sequence,
)
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any, NotRequired, TypedDict, TypeVar, cast, overload

import orjson

from llm_queue._logging import get_logger
from llm_queue.clients import ClientPool, Providers
from llm_queue.context import current_context
from llm_queue.engine.envelope import Meta, Reply
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.jobs import JobFn, JobRunner, JobSettings
from llm_queue.engine.metrics import Metrics, sink_for
from llm_queue.engine.offload import ResultOffload
from llm_queue.engine.queue import QueueFull
from llm_queue.engine.runtime import Engine, EngineSettings
from llm_queue.engine.settings import InfraSettings
from llm_queue.engine.store_client import Secrets, secret_store_client
from llm_queue.engine.store_guard import store_op
from llm_queue.errors import ConfigError, LLMQueueError
from llm_queue.registry.store import RegistryStore
from llm_queue.tuning import Tuning

logger = get_logger(__name__)
_T = TypeVar("_T")
# Store connections of the private engine that serves sync calls when no queue is started
# (Celery workers, scripts): a caller-only engine needs few (D1).
SYNC_STORE_CONNECTIONS = 20
# Store connections a worker needs beyond its slots: the receive loop, the result subscriber,
# heartbeats and status reads (R3PF-8). Unverified: an estimate from the loops it runs.
STORE_POOL_MARGIN = 10
# A sync call on a started queue's loop checks this often that the loop still runs (CO-26).
_LOOP_CHECK_SECS = 1.0
# How long closing the private engine of the sync calls may take before it is logged and left.
_SYNC_CLOSE_SECS = 10.0


# ------------------------------------------------------------------- typed results (O6)


class RateStatus(TypedDict):
    """A budget of one deployment this minute."""

    limit: int | None
    used: int
    left: int | None


class ConcurrencyStatus(TypedDict):
    """Calls in flight on one deployment and its (adaptive) limit."""

    limit: float
    in_use: int


class DeploymentStatus(TypedDict):
    """One deployment of a model: switches, budgets, concurrency, breaker and latency.

    `account_enabled` and `blocked` (a routing block on its region or account) were returned
    but not declared, so a type checker refused reading them (R3AP-3).
    """

    name: str
    group: str
    enabled: bool
    account_enabled: bool
    blocked: bool
    healthy: bool
    next_probe: float | None
    outside_residency: bool
    rpm: RateStatus
    tpm: RateStatus
    concurrency: ConcurrencyStatus
    breaker: str
    latency_ms: float | None
    samples: int
    throttles_1m: int


class ModelStatus(TypedDict):
    """One model: waiting calls per class, calls in progress, dead letters, deployments."""

    model: str
    enabled: bool
    waiting: dict[str, int]
    in_progress: int
    dead_letters: int
    deployments: list[DeploymentStatus]


class JobStatus(TypedDict):
    """One job name a worker consumed lately: waiting jobs and dead letters."""

    job: str
    waiting: int
    dead_letters: int


class StatusReport(TypedDict):
    """Every model and job, the registry version and the routing switches."""

    registry_version: int
    kill_switch: bool
    blocked_regions: list[str]
    blocked_accounts: list[str]
    models: list[ModelStatus]
    jobs: NotRequired[list[JobStatus]]


class RequestStatus(TypedDict, total=False):
    """One call's status record as stored (empty when the id is unknown).

    Every value is text, as the store keeps it: convert the numbers with `int()` (`attempts`,
    `tokens_in`, `tokens_out`) and the `*_at` times (Unix seconds) with `float()`. One `*_at`
    key is there per state the call reached (R3AP-4).
    """

    request_id: str
    state: str
    model: str
    priority: str
    enqueued_at: str
    in_progress_at: str
    done_at: str
    failed_at: str
    expired_at: str
    cancelled_at: str
    served_model: str
    deployment: str
    attempts: str
    tokens_in: str
    tokens_out: str
    error: str


class Health(TypedDict):
    """Liveness, readiness, each store's state (`ok`, `read_only`, `down`) and eviction policy.

    `eviction_policy` is the state store's `maxmemory-policy` read at start (`unknown` when
    unreadable); `queue_eviction_policy` is the queue store's, when it is a separate store.
    """

    live: bool
    ready: bool
    stores: dict[str, str | None]
    eviction_policy: str
    queue_eviction_policy: NotRequired[str]


@dataclass(frozen=True, slots=True)
class EmbedResult:
    """Vectors (one per text, in order) and how the batches were served.

    With `embed(partial=True)`, a text that failed has the vector `[]` and its error in
    `errors` (by text index); otherwise `errors` is always empty. `meta.tokens_in` sums the
    input tokens the replies reported; `batches` counts provider calls and `request_ids`
    lists the answered ones, for cost and support (Q21).
    """

    vectors: list[list[float]]
    meta: Meta
    errors: dict[int, CallFailed] = field(default_factory=dict)
    batches: int = 0
    request_ids: list[str] = field(default_factory=list)


# Every queue of the process, so a fork can mark them (weak: a dropped queue is not kept).
_QUEUES: weakref.WeakSet[LLMQueue] = weakref.WeakSet()


class LLMQueue:
    """One process's LLM queue: caller, worker, jobs and admin over the stores."""

    def __init__(
        self,
        engine: Engine,
        *,
        settings: InfraSettings | None = None,
        builder: Callable[[], Engine] | None = None,
        offload: ResultOffload | None = None,
        job_settings: JobSettings | None = None,
    ) -> None:
        """Wrap a built `Engine`; `builder` makes a caller-only engine for sync calls.

        That engine is used only while this queue is not started (sync-only processes).
        `job_settings` (default: `LLMQ_JOB_*` read by `from_env`) bounds jobs.
        """
        self._engine = engine
        self.settings = settings
        self._jobs = JobRunner(
            engine.queues,
            queue_prefix=engine.settings.queue_prefix,
            metrics=engine.metrics,
            replica_wait=getattr(engine, "replica_wait", None),  # one per process (R3FM-5, Q8)
            job_settings=job_settings,
        )
        self._builder = builder
        self._offload = offload
        self._sync: _SyncRunner | None = None
        self._sync_lock = threading.Lock()
        self._start_lock: asyncio.Lock | None = None  # made on the first start(), on its loop
        self._start_loop: asyncio.AbstractEventLoop | None = None  # the loop of that lock
        self._start_error: BaseException | None = None  # why a start failed (NB-4)
        self._rolling_back = False  # start() is stopping what it began (under its lock)
        self.started = False
        self._stopped = False  # stop() began: no second stop, no start
        self._closing = False  # stop()'s drain is over: no new calls (NB-1)
        self._pid: int | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._inherited = False
        _QUEUES.add(self)

    @property
    def engine(self) -> Engine:
        """Deprecated: the engine is private (owner decision Q35); use the queue's methods.

        Kept as an alias that warns until 0.2.
        """
        warnings.warn(
            "LLMQueue.engine is private since 0.1.0b2 and will be removed in 0.2; use the "
            "LLMQueue methods (call, status, registry, ...)",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._engine

    @property
    def jobs(self) -> JobRunner:
        """Deprecated: the job runner is private (Q35); use `register_job` and `send_job`."""
        warnings.warn(
            "LLMQueue.jobs is private since 0.1.0b2 and will be removed in 0.2; use "
            "register_job / send_job",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._jobs

    # ---------------------------------------------------------------- construction

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        secrets: Secrets | None = None,
        metrics: Metrics | None = None,
        worker_id: str | None = None,
        credentials: Callable[[Any], Mapping[str, Any] | None] | None = None,
        offload: ResultOffload | None = None,
        offload_bytes: int | None = None,
        tuning: Tuning | None = None,
    ) -> LLMQueue:
        """Build from `LLMQ_*` (secret first when `secrets` is given, then the environment).

        `tuning` sets timeouts, pool sizes and loop pacing in code (`Tuning()` is the
        defaults); a bad value or combination raises `ConfigError` listing every problem.
        """
        settings = InfraSettings.from_env(environ, secrets=secrets)
        jobs = JobSettings.from_env(os.environ if environ is None else environ)
        tuning = tuning or Tuning()
        if problems := tuning.problems(queue_wait_ms=settings.queue_wait_ms):
            raise ConfigError("invalid tuning:\n  " + "\n  ".join(problems))
        sink = metrics or sink_for(settings.metrics_sink)  # one sink for both engines (O5)

        def build(cap: int = settings.store_max_connections) -> Engine:
            state = secret_store_client(
                settings.store_url, secrets, "LLMQ_STORE_PASSWORD", cap, tuning.store
            )
            queue_store = (
                secret_store_client(
                    settings.queue_store_url,
                    secrets,
                    "LLMQ_QUEUE_STORE_PASSWORD",
                    cap,
                    tuning.store,
                )
                if settings.queue_store_url
                else None
            )
            registry = RegistryStore(
                state,
                settings.key_prefix,
                config=orjson.loads(settings.registry_document()),
                live_edit=settings.registry_live_edit,
                allowed_hosts=settings.base_url_allowed_hosts,
                audit_retention_days=settings.audit_retention_days,
                history_size=tuning.registry.history_size,
                edit_retries=tuning.registry.edit_retries,
            )
            pool_kwargs: dict[str, Any] = {
                "bedrock_endpoint_url": settings.bedrock_endpoint_url,
                "sts_endpoint_url": settings.sts_endpoint_url,
                "session_name": f"llmq-{settings.environment}-{worker_id or socket.gethostname()}",
                "options": tuning.bedrock,
                "openai_options": tuning.openai,
                "warm": tuning.warm,
            }
            if credentials is not None:
                pool_kwargs["credentials"] = credentials
            if secrets is not None:
                pool_kwargs["secrets"] = secrets
            engine_settings = EngineSettings(
                queue_prefix=settings.queue_prefix,
                lease_secs=settings.lease_secs,
                result_ttl_secs=settings.result_ttl_secs,
                result_dedupe_secs=settings.result_dedupe_secs,
                status_ttl_secs=settings.status_ttl_secs,
                caller_ttl_secs=settings.caller_ttl_secs,
                drain_secs=settings.drain_secs,
                local_limit_cap=settings.local_limit_cap,
                id_hash_salt=settings.id_hash_salt or "",
                cfg_reload_secs=settings.cfg_reload_secs,
                queue_failover_secs=settings.queue_failover_secs,
                queue_wait_replicas=settings.queue_wait_replicas,
                queue_wait_ms=settings.queue_wait_ms,
                queue_wait_scopes=settings.queue_wait_scopes,
                strict=settings.strict,
                debug_capture_allowed=settings.debug_capture_from_config,
                timings=tuning.engine,
            )
            return Engine(
                state,
                registry,
                Providers(ClientPool(**pool_kwargs)),
                queue_store=queue_store,
                settings=engine_settings,
                worker_id=worker_id,
                metrics=sink,
                offload=offload,
                offload_bytes=offload_bytes,
            )

        sync_cap = min(SYNC_STORE_CONNECTIONS, settings.store_max_connections)
        return cls(
            build(),
            settings=settings,
            builder=lambda: build(sync_cap),
            offload=offload,
            job_settings=jobs,
        )

    # ---------------------------------------------------------------- guards (C2)

    def _check(self, *, same_loop: bool = True, work: bool = False) -> None:
        """Refuse a call from a forked child or (async calls) from another event loop.

        `work` marks a method that starts new work (a call, a stream, a job): it is refused
        once `stop()` has drained, instead of failing later on a closed pool (CO-6, CO-13).
        During the drain it is still served: a running job's next call and an app's follow-up
        call were refused there, which broke the graceful shutdown (NB-1).
        """
        if work and self._closing:
            raise LLMQueueError("this LLMQueue is stopped; build and start a new one to make calls")
        if self._inherited or (self._pid is not None and self._pid != os.getpid()):
            raise LLMQueueError(
                f"this LLMQueue was started in process {self._pid} and this process is a fork "
                "of it: its connections and event loop stay in the parent; build and start a "
                "new LLMQueue in the child (after the fork, e.g. in a worker_process_init hook)"
            )
        loop = self._loop
        if same_loop and (self.started or self._stopped) and loop is not None:  # FC-4: drain too
            with contextlib.suppress(RuntimeError):
                if asyncio.get_running_loop() is not loop or loop.is_closed():
                    raise LLMQueueError(
                        "this LLMQueue runs on the event loop that called start(); a call from "
                        "another event loop (asyncio.run in a thread, a new test loop) cannot use "
                        "its connections. Use the *_sync methods from other threads, or build "
                        "one queue per loop"
                    )

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        """Publish / load the registry; run the worker side unless `mode=caller`.

        A second `start()` does nothing; a stopped queue cannot start again (build a new one).
        The first started queue of the process is the LangChain default and stays so. Starts
        run one at a time, on one event loop. A start that fails part-way stops what it
        started and re-raises; the queue is then stopped (a stopped engine cannot start
        again), and a later `start()` raises `LLMQueueError` naming that first error.
        """
        self._check()
        self._refuse_if_stopped()
        loop = asyncio.get_running_loop()
        first = self._start_loop
        if self._start_lock is None or first is None or (first is not loop and first.is_closed()):
            self._start_lock, self._start_loop = asyncio.Lock(), loop
        elif first is not loop:
            # A start from a second loop contending for the lock got asyncio's RuntimeError
            # ("bound to a different event loop") or waited on a future of the wrong loop (NB-4).
            raise LLMQueueError(
                "start() is already running on another event loop; build one LLMQueue per loop"
            )
        async with self._start_lock:
            # Re-checked under the lock: two concurrent starts both passed an unlocked check
            # and started every background task twice (CO-5).
            if self.started:
                return
            self._refuse_if_stopped()
            try:
                await self._start()
            except BaseException as exc:
                # The engine's tasks kept running with started=False, and a retry started them
                # again (CO-5): stop everything this start began, then re-raise. A retry then
                # got "was stopped", which hid this error (NB-4): it is kept for the message.
                self._start_error = exc
                self._rolling_back = True  # this stop() runs under the start lock
                try:
                    await self.stop(drain_secs=0)
                except Exception as cleanup:  # the start's own error is the one to raise
                    logger.warning("start_rollback_failed", error=repr(cleanup))
                finally:
                    self._rolling_back = False
                raise

    def _refuse_if_stopped(self) -> None:
        """Refuse to start a stopped queue, naming the failed start's error when there was one."""
        if not self._stopped:
            return
        error = self._start_error
        if error is None:
            raise LLMQueueError("this LLMQueue was stopped; build a new one to start again")
        raise LLMQueueError(
            f"this LLMQueue failed to start ({error!r}) and was stopped; fix the cause and "
            "build a new one"
        ) from error

    async def _start(self) -> None:
        """The steps of `start()`, under its lock."""
        if any(q.started and not q._inherited for q in _QUEUES if q is not self):
            logger.warning(
                "second_llm_queue_started",
                hint="one LLMQueue per process shares clients and stores; this one gets its own",
            )
        mode = self.settings.mode if self.settings else "both"
        await self._engine.start(
            worker=mode in ("both", "worker"), caller=mode in ("both", "caller")
        )
        if mode in ("both", "worker"):
            self._check_store_pool()
            await self._jobs.start()
        from llm_queue.langchain import get_default_queue, set_default_queue

        try:
            get_default_queue()
        except LLMQueueError:
            set_default_queue(self)
        self._pid = os.getpid()
        self._loop = asyncio.get_running_loop()
        self.started = True

    async def _wait_for_start(self) -> None:
        """Let a `start()` running on this loop finish (or roll back) before stopping (FC-3).

        A stop() during a slow start ran every step and returned; the start then resumed, ran
        the job runner and set `started`: a queue stopped, closed and started at once, with
        tasks on closed clients and no way to stop it.
        """
        lock = self._start_lock
        if lock is None or not lock.locked() or self._rolling_back:
            return
        with contextlib.suppress(RuntimeError):  # no running loop
            if self._start_loop is not asyncio.get_running_loop():
                return
        async with lock:
            pass

    def _check_store_pool(self) -> None:
        """Warn at start when the store pool is smaller than the worker slots it serves (R3PF-8).

        Each running call holds a store connection at times; `worker_slots` may be 1000 per
        model while the pool stays 200, and the overflow waited on the pool inside the call.
        """
        cap = getattr(self.settings, "store_max_connections", None)
        view = getattr(self._engine.registry, "current", None)
        if not isinstance(cap, int) or view is None:
            return
        slots = sum(int(view.value(alias, "worker_slots")) for alias in view.registry.models)
        wanted = slots + STORE_POOL_MARGIN
        if cap < wanted:
            logger.warning(
                "store_pool_small",
                store_max_connections=cap,
                worker_slots=slots,
                hint=f"set LLMQ_STORE_MAX_CONNECTIONS to at least {wanted}",
            )

    def pause_receiving(self) -> None:
        """Fail readiness and take no new calls or jobs; running work goes on (preStop hook)."""
        self._engine.pause_receiving()
        self._jobs.pause()

    async def stop(self, drain_secs: float | None = None) -> None:
        """Shut down in order (plan §3.18) and close every client; a second call does nothing.

        `drain_secs` (default `LLMQ_DRAIN_SECS`) bounds the whole wait: jobs first (they make
        calls), then running calls get what is left of it. Calls made during the drain are
        served; new calls are refused once it is over. Every step runs even when an earlier
        one fails (each failure is logged) and the first failure is raised at the end; if the
        stop is cancelled, the steps left still close their clients, without draining.
        """
        from llm_queue.langchain import get_default_queue, set_default_queue

        await self._wait_for_start()
        if self._stopped:
            return
        self._check()
        self._stopped = True
        with contextlib.suppress(LLMQueueError):
            if get_default_queue() is self:  # another queue's default is never cleared
                set_default_queue(None)
        self.started = False
        loop = asyncio.get_running_loop()
        drain = self._engine.settings.drain_secs if drain_secs is None else drain_secs
        until = loop.time() + drain
        # One step that raised skipped every later one, and `_stopped` made a second stop()
        # return at once, so store pools, provider sessions and the S3 client leaked (CO-1).
        cancelled = False
        steps: list[tuple[str, Callable[[], Awaitable[Any]]]] = [
            ("jobs", lambda: self._jobs.stop(drain_secs=0 if cancelled else drain)),
            (
                "engine",
                lambda: self._engine.stop(
                    drain_secs=0 if cancelled else max(0.0, until - loop.time())
                ),
            ),
            ("sync", lambda: asyncio.to_thread(self.close_sync)),  # blocking: off the loop
            ("engine_close", self._engine.aclose),
            ("offload", lambda: _close_offload(self._offload)),
        ]
        failures: list[Exception] = []
        for name, step in steps:
            if name == "sync":  # the drain is over: refuse new calls from here (NB-1)
                self._closing = True
            try:
                await step()
            except asyncio.CancelledError:
                cancelled = True  # close the rest, then let the cancellation through
            except Exception as exc:
                logger.warning("stop_step_failed", step=name, error=repr(exc))
                failures.append(exc)
        if cancelled:
            raise asyncio.CancelledError
        if failures:
            raise failures[0]

    # ---------------------------------------------------------------- calls

    def _call_kwargs(self, priority: bool | None, **kw: Any) -> dict[str, Any]:
        ctx = current_context()
        out = {
            "priority": ctx.priority if priority is None else priority,
            "session_id": kw.pop("session_id", None) or ctx.session_id,
            "user_id": kw.pop("user_id", None) or ctx.user_id,
            "tenant_id": kw.pop("tenant_id", None) or ctx.tenant_id,
        }
        out.update({k: v for k, v in kw.items() if v is not None})
        return out

    async def call(
        self, model: str, params: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> Reply:
        """Any operation of a model (`converse`, `chat`, `invoke_model`, `embeddings`)."""
        self._check(work=True)
        return await self._engine.call(model, params, **self._call_kwargs(priority, **kw))

    async def converse(
        self, model: str, request: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> Reply:
        """One Bedrock Converse call (the reply's `response` is the Converse body)."""
        return await self.call(model, request, priority=priority, **kw)

    async def chat(
        self, model: str, request: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> Reply:
        """One OpenAI-compatible chat completion."""
        return await self.call(model, request, priority=priority, **kw)

    async def converse_stream(
        self, model: str, request: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream a chat call's events as they arrive (plan §3.13).

        Bedrock models yield ConverseStream events, OpenAI-compatible ones chat completion
        chunks. A failure after events were yielded raises `CallFailed` with `partial_text`.
        """
        self._check(work=True)
        kwargs = self._call_kwargs(priority, **kw)
        # aclosing: a consumer that stopped early left the inner generator to the asyncgen
        # finalizer, an untracked task (FC-12, as CL-10 in the providers).
        inner = cast(
            "AsyncGenerator[dict[str, Any], None]", self._engine.stream(model, request, **kwargs)
        )
        async with contextlib.aclosing(inner) as events:
            async for event in events:
                yield event

    chat_stream = converse_stream

    async def embed(
        self,
        model: str,
        texts: Sequence[str],
        *,
        priority: bool | None = None,
        input_type: str | None = None,
        partial: bool = False,
        **kw: Any,
    ) -> EmbedResult:
        """One vector per text, in order (batched by the model's `embed_batch_size`).

        `partial=True` keeps the batches that succeeded (see `EmbedResult.errors`); by default
        one failed batch fails the call. `request_id=X` names batch n `X:n`.
        """
        self._check(work=True)
        return await _embed(
            self._engine,
            model,
            texts,
            input_type,
            partial,
            False,
            self._call_kwargs(priority, **kw),
        )

    # ---------------------------------------------------------------- sync (direct path)

    def _run_sync(self, make: Callable[[Engine], Coroutine[Any, Any, _T]]) -> _T:
        """Run a call from sync code: on the started queue's loop, else on the private loop."""
        self._check(same_loop=False, work=True)
        loop = self._loop
        # During stop()'s drain the started loop still serves calls: `started` is already
        # False there, and a sync call built a second private engine mid-shutdown, whose calls
        # the next step cancelled (FC-4).
        draining = self._stopped and not self._closing
        if (self.started or draining) and loop is not None and not loop.is_closed():
            with contextlib.suppress(RuntimeError):
                if asyncio.get_running_loop() is loop:
                    raise LLMQueueError(
                        "a sync call on the thread that runs this queue's event loop would "
                        "deadlock; await it there instead: `await llmq.converse(...)` (or chat / "
                        "embed), and in LangChain `await chat.ainvoke(...)`"
                    )
            return _wait_on_loop(asyncio.run_coroutine_threadsafe(make(self._engine), loop), loop)
        return self._sync_runner().run(make)

    def _sync_runner(self) -> _SyncRunner:
        if threading.current_thread().name == _PrivateLoop.THREAD_NAME:
            raise LLMQueueError("a sync call from inside the sync runner would deadlock")
        _warn_if_loop_running()
        runner = self._sync
        if runner is None:
            if self._builder is None:
                raise LLMQueueError("sync calls need LLMQueue.from_env (a second, private engine)")
            with self._sync_lock:  # threads making their first sync call together build one
                # stop() closes the runner after setting `_closing`; one built later would leak
                # (CO-6). One built during the drain is closed by stop()'s sync step.
                if self._closing or self._stopped:  # no new private engine once stopping (FC-4)
                    raise LLMQueueError("this LLMQueue is stopped; build a new one to make calls")
                if self._sync is None:
                    self._sync = _SyncRunner(self._builder, self._offload)
                runner = self._sync
        return runner

    def converse_sync(
        self, model: str, request: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> Reply:
        """`converse` on the direct path from sync code (same budgets, no queue)."""
        kwargs = self._call_kwargs(priority, **kw)

        async def run(engine: Engine) -> Reply:
            return await engine.call(model, request, force_direct=True, **kwargs)

        return self._run_sync(run)

    def chat_sync(
        self, model: str, request: Mapping[str, Any], *, priority: bool | None = None, **kw: Any
    ) -> Reply:
        """`chat` on the direct path from sync code."""
        return self.converse_sync(model, request, priority=priority, **kw)

    def embed_sync(
        self,
        model: str,
        texts: Sequence[str],
        *,
        priority: bool | None = None,
        input_type: str | None = None,
        partial: bool = False,
        **kw: Any,
    ) -> EmbedResult:
        """`embed` on the direct path from sync code."""
        kwargs = self._call_kwargs(priority, **kw)
        return self._run_sync(lambda e: _embed(e, model, texts, input_type, partial, True, kwargs))

    def close_sync(self) -> None:
        """Stop the private engine of the sync calls (sync-only processes; `stop()` does too)."""
        with self._sync_lock:
            runner, self._sync = self._sync, None
        if runner is not None:
            runner.close()

    # ---------------------------------------------------------------- jobs and admin

    def register_job(
        self,
        name: str,
        fn: JobFn,
        *,
        timeout_secs: float | None = None,
        concurrency: int | None = None,
    ) -> None:
        """Allow this process to run job `name` (call before `start`).

        `concurrency` caps its runs in this process (default `LLMQ_JOB_CONCURRENCY`, 4).

        A run longer than `timeout_secs` is cancelled and counts as a failure (retried, then
        dead-lettered); None (the default) lets it run as long as it takes. `ConfigError` in
        `caller` mode, which runs no jobs: the job waited unseen (R3JE-6).
        """
        if self.settings is not None and self.settings.mode == "caller":
            raise ConfigError(f"job {name}: a process in caller mode runs no jobs")
        self._jobs.register(name, fn, timeout_secs=timeout_secs, concurrency=concurrency)

    @store_op
    async def send_job(
        self, name: str, payload: Mapping[str, Any], *, job_id: str | None = None
    ) -> str:
        """Queue a job; returns its id (jobs run at least once).

        A second send with the same `job_id` within `LLMQ_JOB_DEDUPE_SECS` (24 h) queues
        nothing and returns the id. `CallFailed("queue_full")` when 10,000 jobs of this name
        already wait; `request_too_large` above `LLMQ_JOB_MAX_PAYLOAD_BYTES` (1 MB);
        `invalid_request` for a payload that is not JSON.
        """
        self._check(work=True)
        try:
            return await self._jobs.send(name, payload, job_id=job_id)
        except QueueFull as exc:
            raise CallFailed("queue_full", str(exc), retry_after_secs=5) from exc

    @store_op
    async def job_dead_letters(self, name: str, count: int = 10) -> list[dict[str, Any]]:
        """Dead jobs of one name, oldest first: entry id, job id, failures, payload (Q15)."""
        self._check()
        return await self._jobs.dead_letters(name, count)

    @store_op
    async def redrive_jobs(self, name: str, count: int = 1000) -> int:
        """Send up to `count` dead jobs of one name back to run again; returns how many."""
        self._check()
        return await self._jobs.redrive(name, count)

    @store_op
    async def cancel(self, request_id: str) -> None:
        """Cancel a call (waiting or running)."""
        self._check()
        await self._engine.cancel(request_id)

    @overload
    async def status(self, model: None = None) -> StatusReport: ...

    @overload
    async def status(self, model: str) -> ModelStatus: ...

    @store_op
    async def status(self, model: str | None = None) -> StatusReport | ModelStatus:
        """Status of a model (`ModelStatus`) or of everything (`StatusReport`)."""
        self._check()
        return cast("StatusReport | ModelStatus", await self._engine.status(model))

    @store_op
    async def request_status(self, request_id: str) -> RequestStatus:
        """Status of one call (empty when unknown)."""
        self._check()
        return cast("RequestStatus", await self._engine.request_status(request_id))

    async def health(self) -> Health:
        """Liveness, readiness and the stores' state."""
        self._check()
        return cast("Health", await self._engine.health_check())

    @property
    def registry(self) -> RegistryStore:
        """The registry store (live edits, audit, rollback)."""
        return self._engine.registry

    def http_router(
        self,
        *,
        dependencies: Sequence[Any] = (),
        allow_writes: bool = False,
        actor: Callable[[Any], str] | None = None,
    ) -> Any:
        """A FastAPI router with status, requests, registry and health (extra `http`).

        Writes need `actor(request) -> str`, the authenticated name recorded in the audit.
        """
        from llm_queue.engine.http import status_router

        return status_router(
            self._engine, dependencies=dependencies, allow_writes=allow_writes, actor=actor
        )


async def _embed(
    engine: Engine,
    model: str,
    texts: Sequence[str],
    input_type: str | None,
    partial: bool,
    direct: bool,
    kwargs: dict[str, Any],
) -> EmbedResult:
    """`embed` on an engine: all or nothing, or per text with `partial`."""
    from llm_queue.engine.embed import embed_detailed

    if direct:
        kwargs = {**kwargs, "force_direct": True}
    out = await embed_detailed(
        engine, model, texts, input_type=input_type, partial=partial, **kwargs
    )
    alias = engine.registry.current.registry.alias_of(model)
    meta = Meta(served_model=alias, tokens_in=out.tokens_in, attempts=out.batches)
    return EmbedResult(out.vectors, meta, out.errors, out.batches, out.request_ids)


async def _close_offload(offload: ResultOffload | None) -> None:
    """Close the offload's client of the running loop, if it has one (S3Offload does)."""
    close = getattr(offload, "aclose", None)
    if close is not None:
        try:
            await close()
        except Exception as exc:  # logged, not raised: a failing S3 close vanished (CO-8)
            logger.warning("offload_close_failed", error=repr(exc))


def _wait_on_loop(future: Future[_T], loop: asyncio.AbstractEventLoop) -> _T:
    """Wait for a call sent to a started queue's loop, or fail when that loop stops running.

    A loop that was stopped (not closed) never runs the call, and `.result()` with no
    timeout blocked the sync caller forever (CO-26). A loop that is only between two
    `run_until_complete` calls is not running for a moment: it must be seen stopped on two
    polls in a row (or closed) before the call is given up (FC-13). A `KeyboardInterrupt` in
    the waiting thread cancels the call instead of leaving it running unwatched (FC-13).
    """
    stopped_polls = 0
    try:
        while True:
            try:
                return future.result(timeout=_LOOP_CHECK_SECS)
            except TimeoutError:
                if future.done():
                    # The call itself raised a TimeoutError (or a subclass such as
                    # AttemptTimeout): taken for a poll timeout, it was re-raised on every
                    # poll forever (NB-2).
                    return future.result()
                if loop.is_running():
                    stopped_polls = 0
                    continue
                stopped_polls += 1
                if stopped_polls < 2 and not loop.is_closed():
                    continue
                future.cancel()
                raise CallFailed(
                    "worker_shutdown", "the event loop of this LLMQueue stopped running"
                ) from None
            except concurrent.futures.CancelledError as exc:
                raise CallFailed("cancelled", "the call was cancelled on the queue's loop") from exc
    except BaseException:
        future.cancel()  # a no-op once done
        raise


class _PrivateLoop:
    """One event loop in a daemon thread per process, for sync calls of unstarted queues (E2)."""

    THREAD_NAME = "llmq-sync"

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(
            target=self.loop.run_forever, name=self.THREAD_NAME, daemon=True
        )
        self.thread.start()

    def submit(self, coro: Coroutine[Any, Any, _T]) -> Future[_T]:
        """Run `coro` on the private loop."""
        return asyncio.run_coroutine_threadsafe(coro, self.loop)


_private: _PrivateLoop | None = None
_private_lock = threading.Lock()


def _private_loop() -> _PrivateLoop:
    """The process's private loop, started on first use."""
    global _private
    with _private_lock:
        if _private is None or not _private.thread.is_alive():
            _private = _PrivateLoop()
        return _private


class _SyncRunner:
    """A caller-only engine of one queue on the shared private loop (clients come lazily)."""

    def __init__(self, build: Callable[[], Engine], offload: ResultOffload | None) -> None:
        self._private = _private_loop()
        self._offload = offload
        self._closing = False
        self._lock = threading.Lock()
        self._calls: set[Future[Any]] = set()

        async def boot() -> Engine:
            engine = build()
            try:
                await engine.start(worker=False, caller=True)
            except BaseException:
                # A failed start dropped the engine with its store clients open, and every
                # later sync call built and leaked another (FC-6).
                try:
                    await engine.stop(drain_secs=0)
                except Exception as exc:
                    logger.warning("sync_engine_close_failed", error=repr(exc))
                try:
                    await engine.aclose()
                except Exception as exc:
                    logger.warning("sync_engine_close_failed", error=repr(exc))
                raise
            return engine

        self._engine: Engine = self._private.submit(boot()).result()

    def run(self, fn: Callable[[Engine], Coroutine[Any, Any, _T]]) -> _T:
        """Run one call on the private loop and wait for it."""
        # The check and the add are one step under the lock that close() takes: a call
        # submitted after close() took its snapshot ran on the stopped engine (CO-7).
        with self._lock:
            if self._closing:
                raise LLMQueueError("the sync runner is closed; build a new LLMQueue")
            future = self._private.submit(fn(self._engine))
            self._calls.add(future)
        try:
            return future.result()
        except concurrent.futures.CancelledError as exc:
            # A plain Exception outside the one error family (CO-7).
            raise CallFailed("cancelled", "the sync runner was closed") from exc
        except BaseException:
            future.cancel()  # KeyboardInterrupt: do not leave the call running (FC-13)
            raise
        finally:
            self._calls.discard(future)

    def close(self) -> None:
        """Stop this queue's private engine; its calls still running are cancelled.

        Their callers get the error: before, a call running at close never returned. The
        loop itself stays for the other queues of the process.
        """
        with self._lock:
            self._closing = True
            calls = list(self._calls)
        for future in calls:
            future.cancel()

        async def stop() -> None:
            try:
                await self._engine.stop(drain_secs=1)
            finally:
                try:
                    await self._engine.aclose()
                finally:
                    await _close_offload(self._offload)

        try:
            self._private.submit(stop()).result(timeout=_SYNC_CLOSE_SECS)
        except Exception as exc:  # logged: a failed or timed-out close vanished (CO-8)
            logger.warning("sync_engine_close_failed", error=repr(exc))


def _after_fork_in_child() -> None:
    """In a forked child: started queues refuse calls; sync engines are dropped, not closed."""
    global _private, _private_lock
    _private, _private_lock = None, threading.Lock()  # the loop's thread did not survive
    for queue in list(_QUEUES):
        queue._inherited = queue.started
        queue._sync = None
        queue._sync_lock = threading.Lock()


if hasattr(os, "register_at_fork"):  # not on Windows
    os.register_at_fork(after_in_child=_after_fork_in_child)


_warned_loop = False


def _warn_if_loop_running() -> None:
    """Warn once when a sync call blocks a running event loop (use the async methods there)."""
    global _warned_loop
    if _warned_loop:
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    _warned_loop = True
    logger.warning(
        "sync_call_on_event_loop",
        hint="converse_sync / embed_sync block this event loop; await converse / embed instead",
    )
