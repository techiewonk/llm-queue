"""FastAPI integration (extra `http`): `install()` in one line, `serve()` and `mount()` below it.

`install(app, llmq)` wraps the app's lifespan so the queue starts before the app's own startup
and stops after its shutdown (the app's producers stop first, then running calls drain), adds
`RequestContextMiddleware` and mounts the admin, probe and drain routes under one prefix. The
drain file watcher (`LLMQ_DRAIN_FILE`) is started by `LLMQueue.start()` itself. Apps that order
their own lifespan use `serve()` inside it and `mount()` at build time.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING, Any, Literal

from llm_queue._logging import get_logger
from llm_queue.asgi import RequestContextMiddleware
from llm_queue.errors import ConfigError

if TYPE_CHECKING:
    from fastapi import FastAPI

    from llm_queue.core import LLMQueue

logger = get_logger(__name__)

DEFAULT_PREFIX = "/internal/llm-queue"
OnStartError = Literal["raise", "log"]
_ON_START_ERROR = ("raise", "log")
_INSTALLED = "llm_queue_installed"  # app.state flag: a second install() is refused


def _check_on_start_error(value: str) -> None:
    if value not in _ON_START_ERROR:
        raise ConfigError(f"on_start_error must be one of {_ON_START_ERROR}, not {value!r}")


@asynccontextmanager
async def serve(
    app: FastAPI,
    llmq: LLMQueue,
    *,
    on_start_error: OnStartError = "raise",
    drain_secs: float | None = None,
) -> AsyncIterator[LLMQueue]:
    """Start `llmq` on entry and stop it on exit, for use inside an app's own lifespan.

    `on_start_error="log"` logs a failed start (`llmq_start_failed`) and keeps the app up: the
    queue is then stopped, so queued calls fail at once with `LLMQueueError` and `ready` /
    `/health/ready` stay false. `drain_secs` bounds the stop (default `LLMQ_DRAIN_SECS`). The
    queue is also set as `app.state.llm_queue` for route handlers.
    """
    _check_on_start_error(on_start_error)
    app.state.llm_queue = llmq
    try:
        await llmq.start()
    except Exception as exc:
        if on_start_error == "raise":
            raise
        logger.error(
            "llmq_start_failed",
            error=repr(exc),
            exc_info=exc,
            hint="the app stays up; queued calls fail and readiness is false until a restart",
        )
    try:
        yield llmq
    finally:
        await llmq.stop(drain_secs=drain_secs)


def mount(
    app: FastAPI, llmq: LLMQueue, *, prefix: str = DEFAULT_PREFIX, **router_options: Any
) -> None:
    """Mount `llmq.http_router(**router_options)` (admin, probes, drain) under `prefix`.

    Call it while building the app, not in its lifespan: routes added after startup are
    missing from an OpenAPI schema already served. Without `drain_routes` in the options the
    drain routes are mounted when they can be guarded as writes (the admin token, or
    `dependencies` with `actor`); otherwise drain through `LLMQ_DRAIN_FILE`.
    """
    options = dict(router_options)
    if "drain_routes" not in options:
        options["drain_routes"] = _guarded(llmq, options)
    app.include_router(llmq.http_router(**options), prefix=prefix)


def _guarded(llmq: LLMQueue, options: Mapping[str, Any]) -> bool:
    """True when the router's write routes have a guard and an actor for the audit.

    The admin token is both (`token:<X-LLMQ-Actor>`); the app's `dependencies` need `actor`.
    The read token never writes.
    """
    if options.get("admin_token") is not None:
        return True
    if options.get("dependencies") and options.get("actor") is not None:
        return True
    reader: Callable[[], str | None] | None = getattr(llmq, "_admin_token", None)
    return reader is not None and reader() is not None


def install(
    app: FastAPI,
    llmq: LLMQueue | None = None,
    *,
    prefix: str = DEFAULT_PREFIX,
    on_start_error: OnStartError = "raise",
    drain_secs: float | None = None,
    mount_admin: bool = True,
    context_middleware: bool = True,
    context: Mapping[str, Any] | None = None,
    **router_options: Any,
) -> LLMQueue:
    """Wire a queue into a FastAPI app in one call and return it (plan 0.2.0 H2).

    `llmq=None` builds one with `LLMQueue.from_env()` (a bad setting raises here). The app's
    lifespan is wrapped by `serve()` (`on_start_error`, `drain_secs`); `context_middleware`
    adds `RequestContextMiddleware(**context)`; `mount_admin` mounts the routes under `prefix`,
    with `router_options` passed to `http_router` (tokens, `dependencies`, `allow_writes`, ...).
    """
    _check_on_start_error(on_start_error)
    if getattr(app.state, _INSTALLED, False):
        raise ConfigError("llm_queue.fastapi.install() already ran on this app")
    if llmq is None:
        from llm_queue.core import LLMQueue

        llmq = LLMQueue.from_env()
    if mount_admin:
        mount(app, llmq, prefix=prefix, **router_options)
    if context_middleware:
        app.add_middleware(RequestContextMiddleware, **dict(context or {}))
    queue = llmq
    inner: Callable[[Any], AbstractAsyncContextManager[Any]] = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(target: Any) -> AsyncIterator[Any]:
        # The queue around the app: started before its startup, stopped after its shutdown.
        async with (
            serve(target, queue, on_start_error=on_start_error, drain_secs=drain_secs),
            inner(target) as state,
        ):
            yield state

    app.router.lifespan_context = lifespan
    setattr(app.state, _INSTALLED, True)
    return queue
