"""Per-request context (priority, session, user, tenant) carried through tasks and threads.

Routers set it once with `request_context(...)` (or `RequestContextMiddleware`, `asgi.py`);
queued calls and the LangChain adapters read it when the call itself does not say. Background
work runs under `background()`, `@background_priority`, `create_background_task` or
`run_in_thread`, which keep the ids and drop the priority. Raw threads must run under
`contextvars.copy_context()` (`run_in_thread` does).
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import threading
from collections.abc import Callable, Coroutine, Iterator
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import Any, cast

from llm_queue._logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class RequestContext:
    """What the current request is: its class and the ids used for sticky sessions and records."""

    priority: bool = False
    session_id: str | None = None
    user_id: str | None = None
    tenant_id: str | None = None


_CURRENT: ContextVar[RequestContext | None] = ContextVar("llm_queue_context", default=None)
# Tasks of `create_background_task` until they finish: the loop keeps only a weak reference,
# so a task nobody holds could be collected mid-run.
_BACKGROUND_TASKS: set[asyncio.Task[Any]] = set()


def current_context() -> RequestContext:
    """The active context, or the default (normal priority, no ids)."""
    return _CURRENT.get() or RequestContext()


@contextmanager
def request_context(
    *,
    priority: bool | None = None,
    session_id: str | None = None,
    user_id: str | None = None,
    tenant_id: str | None = None,
) -> Iterator[RequestContext]:
    """Set priority and ids for everything run inside the block; unset fields are inherited."""
    base = current_context()
    ctx = replace(
        base,
        priority=base.priority if priority is None else priority,
        session_id=session_id if session_id is not None else base.session_id,
        user_id=user_id if user_id is not None else base.user_id,
        tenant_id=tenant_id if tenant_id is not None else base.tenant_id,
    )
    previous = _CURRENT.get()
    token = _CURRENT.set(ctx)
    try:
        yield ctx
    finally:
        try:
            _CURRENT.reset(token)
        except ValueError:
            # The block ended in another context (an async generator finalized by another
            # task): the token cannot be reset there, so the value it replaced is restored.
            _CURRENT.set(previous)


def background() -> AbstractContextManager[RequestContext]:
    """`request_context(priority=False)`: work inside never takes budget from waiting users.

    The ids (session, user, tenant) are kept, so the calls still count against their tenant.
    """
    return request_context(priority=False)


def background_priority[**P, R](fn: Callable[P, R]) -> Callable[P, R]:
    """Decorator: run a function or coroutine function under `background()`.

    A coroutine function gets the context for its whole run, every `await` included. A
    generator function is refused (`TypeError`): its body runs after the call returned, outside
    the context; wrap the code that iterates it instead.
    """
    if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
        raise TypeError(
            f"@background_priority cannot wrap the generator function {fn.__qualname__}: "
            "wrap the code that iterates it"
        )
    if inspect.iscoroutinefunction(fn):  # narrows `fn` to a coroutine function

        @functools.wraps(fn)
        async def run_async(*args: P.args, **kwargs: P.kwargs) -> Any:
            with background():
                return await fn(*args, **kwargs)

        return cast("Callable[P, R]", run_async)

    @functools.wraps(fn)
    def run(*args: P.args, **kwargs: P.kwargs) -> R:
        with background():
            return fn(*args, **kwargs)

    return run


def _background_copy() -> contextvars.Context:
    """A copy of the current context with the request context at background priority."""
    ctx = contextvars.copy_context()
    ctx.run(_CURRENT.set, replace(current_context(), priority=False))
    return ctx


def create_background_task[R](
    coro: Coroutine[Any, Any, R], *, name: str | None = None
) -> asyncio.Task[R]:
    """`asyncio.create_task` at background priority, kept alive until it finishes.

    The task runs in a copy of the current context (ids kept, priority off) and is held by a
    module-level set until done, so a fire-and-forget task is never collected mid-run. An
    exception it ends with is logged (`background_task_failed`).
    """
    task = asyncio.get_running_loop().create_task(coro, name=name, context=_background_copy())
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_background_task_done)
    return task


def _background_task_done(task: asyncio.Task[Any]) -> None:
    _BACKGROUND_TASKS.discard(task)
    if task.cancelled():
        return
    exc = task.exception()  # marks it retrieved: logged here, not again at collection
    if exc is not None:
        logger.warning(
            "background_task_failed", task=task.get_name(), error=repr(exc), exc_info=exc
        )


def run_in_thread(fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> threading.Thread:
    """Run `fn(*args, **kwargs)` in a new daemon thread, in a copy of this context.

    The copy keeps the ids and drops the priority (background work). Returns the started
    thread (`join()` it to wait); an exception `fn` raises is logged
    (`background_thread_failed`).
    """
    ctx = _background_copy()
    label = getattr(fn, "__qualname__", type(fn).__name__)

    def target() -> None:
        try:
            ctx.run(fn, *args, **kwargs)
        except Exception as exc:
            logger.warning("background_thread_failed", fn=label, error=repr(exc), exc_info=exc)

    thread = threading.Thread(target=target, name=f"llmq-bg:{label}", daemon=True)
    thread.start()
    return thread
