"""`RequestContextMiddleware`: every HTTP and WebSocket request runs in its own request context.

Pure ASGI (no FastAPI or Starlette import), so it works for websockets and costs one context
switch per request. Each value is a constant or a callable of the ASGI scope (sync or async);
None leaves the field inherited. Tasks and threads the request starts copy the context.
Authentication usually runs later (a route dependency), so per-user ids are mostly set by
`request_context(...)` in the route; the middleware suits per-app or per-header values.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any

from llm_queue.context import request_context

Scope = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[MutableMapping[str, Any]]]
Send = Callable[[MutableMapping[str, Any]], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]
# A field value: a constant, or a callable of the scope returning one (or an awaitable of one).
Value = Any

_TYPES = ("http", "websocket")


class RequestContextMiddleware:
    """Run each HTTP / WebSocket request inside `request_context(...)` with these values.

    `tenant_id`, `user_id`, `session_id` (str) and `priority` (bool): a constant, or a callable
    `fn(scope)` (sync or async) read per request. None (the default, or a callable returning
    None) inherits the field: the request starts from the context the server runs in, so
    `LLMQ_DEFAULT_TENANT` still applies to calls with no tenant. Lifespan and other scope
    types pass through untouched.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        tenant_id: Value = None,
        user_id: Value = None,
        session_id: Value = None,
        priority: Value = None,
    ) -> None:
        """Wrap `app`; each value is a constant or a callable of the ASGI scope."""
        self.app = app
        self.values: dict[str, Value] = {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "session_id": session_id,
            "priority": priority,
        }

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Run the request in its own context; other scope types pass through."""
        if scope.get("type") not in _TYPES:
            await self.app(scope, receive, send)
            return
        fields: dict[str, Any] = {}
        for name, value in self.values.items():
            if callable(value):
                value = value(scope)
                if inspect.isawaitable(value):
                    value = await value
            fields[name] = value
        with request_context(**fields):
            await self.app(scope, receive, send)
