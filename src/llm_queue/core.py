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
import contextlib
import os
import socket
import threading
import weakref
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping, Sequence
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any, NotRequired, TypedDict, TypeVar, cast, overload

import orjson
import redis.asyncio
from redis.credentials import CredentialProvider

from llm_queue._logging import get_logger
from llm_queue.clients import ClientPool, Providers
from llm_queue.context import current_context
from llm_queue.engine.envelope import MAX_MESSAGE_BYTES, Meta, Reply, message_size_bytes
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.jobs import JobFn, JobRunner
from llm_queue.engine.metrics import Metrics, sink_for
from llm_queue.engine.offload import ResultOffload
from llm_queue.engine.runtime import Engine, EngineSettings
from llm_queue.engine.settings import InfraSettings
from llm_queue.engine.store_client import store_client
from llm_queue.engine.store_guard import store_op
from llm_queue.errors import ConfigError, LLMQueueError
from llm_queue.registry import SETTINGS
from llm_queue.registry.store import RegistryStore
from llm_queue.tuning import StoreOptions, Tuning

logger = get_logger(__name__)
_T = TypeVar("_T")
Secrets = Callable[[str], "str | None"]
# Store connections of the private engine that serves sync calls when no queue is started
# (Celery workers, scripts): a caller-only engine needs few (D1).
SYNC_STORE_CONNECTIONS = 20


# ------------------------------------------------------------------- typed results (O6)


class RateStatus(TypedDict):
    """A budget of one deployment this minute."""

    limit: int | None
    used: int
    left: int | None


class DeploymentStatus(TypedDict):
    """One deployment of a model: switches, budgets, concurrency, breaker and latency."""

    name: str
    group: str
    enabled: bool
    healthy: bool
    next_probe: float | None
    outside_residency: bool
    rpm: RateStatus
    tpm: RateStatus
    concurrency: dict[str, Any]
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


class StatusReport(TypedDict):
    """Every model, the registry version and the routing switches."""

    registry_version: int
    kill_switch: bool
    blocked_regions: list[str]
    blocked_accounts: list[str]
    models: list[ModelStatus]


class RequestStatus(TypedDict, total=False):
    """One call's status record as stored, all values text (empty when the id is unknown).

    `<state>_at` timestamps (e.g. `done_at`) are there too, one per state reached.
    """

    request_id: str
    state: str
    model: str
    priority: str
    enqueued_at: str
    served_model: str
    deployment: str
    attempts: str
    tokens_in: str
    tokens_out: str
    error: str


class Health(TypedDict):
    """Liveness, readiness and each store's state (`ok`, `read_only`, `down`)."""

    live: bool
    ready: bool
    stores: dict[str, str | None]
    eviction_policy: NotRequired[str]


@dataclass(frozen=True, slots=True)
class EmbedResult:
    """Vectors (one per text, in order) and how the batches were served.

    With `embed(partial=True)`, a text whose batch failed has the vector `[]` and its error
    in `errors` (by text index); otherwise `errors` is always empty.
    """

    vectors: list[list[float]]
    meta: Meta
    errors: dict[int, CallFailed] = field(default_factory=dict)


class _FreshPassword(CredentialProvider):
    """Reads the store password from the secret on every new connection (rotation, R42)."""

    def __init__(self, secrets: Secrets, name: str, username: str | None = None) -> None:
        self._secrets = secrets
        self._name = name
        self._username = username

    def get_credentials(self) -> tuple[str] | tuple[str, str]:
        password = self._secrets(self._name) or ""
        return (self._username, password) if self._username else (password,)


def _client(
    url: str, secrets: Secrets | None, name: str, max_connections: int, options: StoreOptions
) -> redis.asyncio.Redis:
    """An async client (bytes replies) for a store URL; a password secret wins when present."""
    provider = _FreshPassword(secrets, name) if secrets is not None and secrets(name) else None
    return store_client(url, provider, max_connections=max_connections, options=options)


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
    ) -> None:
        """Wrap a built `Engine`; `builder` makes a caller-only engine for sync calls.

        That engine is used only while this queue is not started (sync-only processes).
        """
        self.engine = engine
        self.settings = settings
        self.jobs = JobRunner(engine.queues, queue_prefix=engine.settings.queue_prefix)
        self._builder = builder
        self._offload = offload
        self._sync: _SyncRunner | None = None
        self._sync_lock = threading.Lock()
        self.started = False
        self._stopped = False
        self._pid: int | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._inherited = False
        _QUEUES.add(self)

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
        tuning = tuning or Tuning()
        if problems := tuning.problems(queue_wait_ms=settings.queue_wait_ms):
            raise ConfigError("invalid tuning:\n  " + "\n  ".join(problems))
        sink = metrics or sink_for(settings.metrics_sink)  # one sink for both engines (O5)

        def build(cap: int = settings.store_max_connections) -> Engine:
            state = _client(settings.store_url, secrets, "LLMQ_STORE_PASSWORD", cap, tuning.store)
            queue_store = (
                _client(
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
        return cls(build(), settings=settings, builder=lambda: build(sync_cap), offload=offload)

    # ---------------------------------------------------------------- guards (C2)

    def _check(self, *, same_loop: bool = True) -> None:
        """Refuse a call from a forked child or (async calls) from another event loop."""
        if self._inherited or (self._pid is not None and self._pid != os.getpid()):
            raise LLMQueueError(
                f"this LLMQueue was started in process {self._pid} and this process is a fork "
                "of it: its connections and event loop stay in the parent; build and start a "
                "new LLMQueue in the child (after the fork, e.g. in a worker_process_init hook)"
            )
        loop = self._loop
        if same_loop and self.started and loop is not None:
            with contextlib.suppress(RuntimeError):
                if asyncio.get_running_loop() is not loop or loop.is_closed():
                    raise LLMQueueError(
                        "this LLMQueue runs on the event loop that called start(); a call from "
                        "another event loop (asyncio.run in a thread, a new test loop) cannot use "
                        "its connections. Use the *_sync methods from other threads, or build "
                        "one queue per loop"
                    )

    def _check_size(self, model: str, params: Mapping[str, Any]) -> None:
        """`CallFailed("invalid_request")` for a message above `max_message_bytes` (C1).

        One orjson pass over the params: 0.22 ms at 240 KB (measured 2026-11-14, Python 3.12).
        """
        limit = MAX_MESSAGE_BYTES
        if "max_message_bytes" in SETTINGS:
            view = self.engine.registry.current
            limit = int(view.value(view.registry.alias_of(model), "max_message_bytes"))
        size = message_size_bytes(params)
        if size > limit:
            raise CallFailed(
                "invalid_request", f"the message is {size} bytes, above max_message_bytes {limit}"
            )

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        """Publish / load the registry; run the worker side unless `mode=caller`.

        A second `start()` does nothing; a stopped queue cannot start again (build a new one).
        The first started queue of the process is the LangChain default and stays so.
        """
        self._check()
        if self.started:
            return
        if self._stopped:
            raise LLMQueueError("this LLMQueue was stopped; build a new one to start again")
        if any(q.started and not q._inherited for q in _QUEUES if q is not self):
            logger.warning(
                "second_llm_queue_started",
                hint="one LLMQueue per process shares clients and stores; this one gets its own",
            )
        mode = self.settings.mode if self.settings else "both"
        await self.engine.start(
            worker=mode in ("both", "worker"), caller=mode in ("both", "caller")
        )
        if mode in ("both", "worker"):
            await self.jobs.start()
        from llm_queue.langchain import get_default_queue, set_default_queue

        try:
            get_default_queue()
        except LLMQueueError:
            set_default_queue(self)
        self._pid = os.getpid()
        self._loop = asyncio.get_running_loop()
        self.started = True

    def pause_receiving(self) -> None:
        """Fail readiness and take no new calls or jobs; running work goes on (preStop hook)."""
        self.engine.pause_receiving()
        self.jobs.pause()

    async def stop(self, drain_secs: float | None = None) -> None:
        """Shut down in order (plan §3.18) and close every client; a second call does nothing.

        `drain_secs` (default `LLMQ_DRAIN_SECS`) bounds the whole wait: jobs first (they make
        calls), then running calls get what is left of it.
        """
        from llm_queue.langchain import get_default_queue, set_default_queue

        if self._stopped:
            return
        self._check()
        self._stopped = True
        with contextlib.suppress(LLMQueueError):
            if get_default_queue() is self:  # another queue's default is never cleared
                set_default_queue(None)
        self.started = False
        loop = asyncio.get_running_loop()
        drain = self.engine.settings.drain_secs if drain_secs is None else drain_secs
        until = loop.time() + drain
        await self.jobs.stop(drain_secs=drain)
        await self.engine.stop(drain_secs=max(0.0, until - loop.time()))
        await asyncio.to_thread(self.close_sync)  # blocking: off the event loop
        await self.engine.aclose()
        await _close_offload(self._offload)

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
        self._check()
        self._check_size(model, params)
        return await self.engine.call(model, params, **self._call_kwargs(priority, **kw))

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
        self._check()
        self._check_size(model, request)
        kwargs = self._call_kwargs(priority, **kw)
        async for event in self.engine.stream(model, request, **kwargs):
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
        self._check()
        return await _embed(
            self.engine, model, texts, input_type, partial, False, self._call_kwargs(priority, **kw)
        )

    # ---------------------------------------------------------------- sync (direct path)

    def _run_sync(self, make: Callable[[Engine], Coroutine[Any, Any, _T]]) -> _T:
        """Run a call from sync code: on the started queue's loop, else on the private loop."""
        self._check(same_loop=False)
        loop = self._loop
        if self.started and loop is not None and not loop.is_closed():
            with contextlib.suppress(RuntimeError):
                if asyncio.get_running_loop() is loop:
                    raise LLMQueueError(
                        "a sync call on the thread that runs this queue's event loop would "
                        "deadlock; await converse / chat / embed there instead"
                    )
            return asyncio.run_coroutine_threadsafe(make(self.engine), loop).result()
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
            self._check_size(model, request)
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

    def register_job(self, name: str, fn: JobFn) -> None:
        """Allow this process to run job `name` (call before `start`)."""
        self.jobs.register(name, fn)

    @store_op
    async def send_job(
        self, name: str, payload: Mapping[str, Any], *, job_id: str | None = None
    ) -> str:
        """Queue a job; returns its id (jobs run at least once)."""
        self._check()
        return await self.jobs.send(name, payload, job_id=job_id)

    @store_op
    async def cancel(self, request_id: str) -> None:
        """Cancel a call (waiting or running)."""
        self._check()
        await self.engine.cancel(request_id)

    @overload
    async def status(self, model: None = None) -> StatusReport: ...

    @overload
    async def status(self, model: str) -> ModelStatus: ...

    @store_op
    async def status(self, model: str | None = None) -> StatusReport | ModelStatus:
        """Status of a model (`ModelStatus`) or of everything (`StatusReport`)."""
        self._check()
        return cast("StatusReport | ModelStatus", await self.engine.status(model))

    @store_op
    async def request_status(self, request_id: str) -> RequestStatus:
        """Status of one call (empty when unknown)."""
        self._check()
        return cast("RequestStatus", await self.engine.request_status(request_id))

    async def health(self) -> Health:
        """Liveness, readiness and the stores' state."""
        self._check()
        return cast("Health", await self.engine.health_check())

    @property
    def registry(self) -> RegistryStore:
        """The registry store (live edits, audit, rollback)."""
        return self.engine.registry

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
            self.engine, dependencies=dependencies, allow_writes=allow_writes, actor=actor
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
    from llm_queue.engine.embed import embed_partial

    if direct:
        kwargs = {**kwargs, "force_direct": True}
    vectors, errors = await embed_partial(
        engine, model, texts, input_type=input_type, partial=partial, **kwargs
    )
    alias = engine.registry.current.registry.alias_of(model)
    return EmbedResult(vectors, Meta(served_model=alias), errors)


async def _close_offload(offload: ResultOffload | None) -> None:
    """Close the offload's client of the running loop, if it has one (S3Offload does)."""
    close = getattr(offload, "aclose", None)
    if close is not None:
        with contextlib.suppress(Exception):
            await close()


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
        self._calls: set[Future[Any]] = set()

        async def boot() -> Engine:
            engine = build()
            await engine.start(worker=False, caller=True)
            return engine

        self._engine: Engine = self._private.submit(boot()).result()

    def run(self, fn: Callable[[Engine], Coroutine[Any, Any, _T]]) -> _T:
        """Run one call on the private loop and wait for it."""
        if self._closing:
            raise LLMQueueError("the sync runner is closed")
        future = self._private.submit(fn(self._engine))
        self._calls.add(future)
        try:
            return future.result()
        finally:
            self._calls.discard(future)

    def close(self) -> None:
        """Stop this queue's private engine; its calls still running are cancelled.

        Their callers get the error: before, a call running at close never returned. The
        loop itself stays for the other queues of the process.
        """
        self._closing = True
        for future in list(self._calls):
            future.cancel()

        async def stop() -> None:
            await self._engine.stop(drain_secs=1)
            await self._engine.aclose()
            await _close_offload(self._offload)

        with contextlib.suppress(Exception):
            self._private.submit(stop()).result(timeout=10)


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
