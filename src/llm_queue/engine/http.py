"""HTTP views of the engine (plan §3.12, E132, E138): status, requests, registry, health.

`Handlers` are plain async functions returning (status code, JSON body), so they are tested
without a web framework; `status_router()` mounts them on FastAPI (extra `http`). Health routes
take no auth; the app puts its admin dependency on the rest. Writes (safe fields only) are off
unless the app passes `allow_writes=True`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from llm_queue.engine.store_guard import STORE_DOWN
from llm_queue.errors import ConfigError, MissingExtra
from llm_queue.registry.store import LiveEditRefused, audit_diff
from llm_queue.registry.validate import RegistryError

if TYPE_CHECKING:
    from llm_queue.engine.runtime import Engine

_REDACT = frozenset({"role_arn", "external_id_ref", "api_key_ref"})
ADMIN_HEADER = "X-LLMQ-Admin"
Response = tuple[int, dict[str, Any]]
# A store outage was a 500 with redis-py's traceback; it is a 503 like the readiness probe (EN-10).
_UNAVAILABLE: Response = (503, {"error": "state store unavailable"})


def redacted(doc: Any) -> Any:
    """The registry with role ARNs and secret references hidden."""
    if isinstance(doc, Mapping):
        return {k: "[redacted]" if k in _REDACT else redacted(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [redacted(v) for v in doc]
    return doc


class Handlers:
    """The HTTP handlers of one engine."""

    def __init__(self, engine: Engine, *, allow_writes: bool = False) -> None:
        """Writes stay refused unless `allow_writes`."""
        self.engine = engine
        self.allow_writes = allow_writes

    async def live(self) -> Response:
        """200 while the engine's loops run; 503 when one died or the heartbeat stalled (O2)."""
        live = bool((await self.engine.health_check())["live"])
        return (200 if live else 503), {"live": live}

    async def ready(self) -> Response:
        """200 when the registry is loaded and the process is not draining (stores in the body)."""
        health = await self.engine.health_check()
        return (200 if health["ready"] else 503), health

    async def status(self, model: str | None = None) -> Response:
        """Status of a model or of everything (503 while the store is unreachable)."""
        try:
            return 200, await self.engine.status(model)
        except KeyError:
            return 404, {"error": f"unknown model {model}"}
        except STORE_DOWN:
            return _UNAVAILABLE

    async def request(self, request_id: str) -> Response:
        """Status of one call (503 while the store is unreachable)."""
        try:
            found = await self.engine.request_status(request_id)
        except STORE_DOWN:
            return _UNAVAILABLE
        return (200, found) if found else (404, {"error": "unknown or expired request"})

    async def registry(self) -> Response:
        """The live registry, secrets hidden, with what is live but not in config."""
        store = self.engine.registry
        try:
            live_edits = await store.diff_with_config()
        except STORE_DOWN:
            return _UNAVAILABLE
        return 200, {
            "registry": redacted(dict(store.current.registry.raw)),
            "changes": store.changes,
            "live_edits": redacted(live_edits),
        }

    async def set(
        self, values: Mapping[str, Any], *, actor: str, model: str | None = None
    ) -> Response:
        """A live edit of safe fields (only with `allow_writes`); audited with `actor`."""
        if not self.allow_writes:
            return 403, {"error": "writes are off"}
        before = dict(self.engine.registry.current.registry.raw)
        try:
            view = await self.engine.registry.set(values, actor=f"http:{actor}", model=model)
        except (LiveEditRefused, RegistryError, ConfigError) as exc:
            return 400, {"error": str(exc)}
        except STORE_DOWN:
            return _UNAVAILABLE
        return 200, {"diff": redacted(audit_diff(before, dict(view.registry.raw)))}

    async def block(
        self, *, actor: str, region: str | None = None, account: str | None = None, on: bool = True
    ) -> Response:
        """Block or unblock a region or account (only with `allow_writes`)."""
        if not self.allow_writes:
            return 403, {"error": "writes are off"}
        try:
            await self.engine.registry.block(
                region=region, account=account, on=on, actor=f"http:{actor}"
            )
        except ConfigError as exc:
            return 400, {"error": str(exc)}
        except STORE_DOWN:
            return _UNAVAILABLE
        routing = self.engine.registry.current.registry.routing
        return 200, {
            "blocked_regions": list(routing.blocked_regions),
            "blocked_accounts": list(routing.blocked_accounts),
        }


def status_router(
    engine: Engine,
    *,
    dependencies: Sequence[Any] = (),
    allow_writes: bool = False,
    actor: Callable[[Any], str] | None = None,
) -> Any:
    """A FastAPI `APIRouter` with the handlers (`pip install 'llm-queue[http]'`).

    `dependencies` (the app's admin check) guard every route except `/health/live` and
    `/health/ready`. Writes need `dependencies` and `actor`: `actor` gets the request and
    returns the authenticated name the audit records (never a client-supplied value, which
    anyone could spoof). Write routes take JSON bodies and need the `X-LLMQ-Admin` header: a
    browser cannot send either cross-site without a CORS preflight, so a page an admin visits
    cannot post a block or an edit with the admin's cookies.
    """
    try:
        from fastapi import APIRouter, Body, Depends, Request
        from fastapi.responses import JSONResponse
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise MissingExtra("http_router", "http") from exc
    # Annotations are strings here (`from __future__ import annotations`); FastAPI resolves them
    # in this module's globals, where the optional import is not, so a `Request` parameter
    # would be read as a query field. Publish the name once it is imported.
    globals()["Request"] = Request
    if allow_writes and actor is None:
        raise ConfigError("allow_writes=True needs actor= (who made the edit, for the audit)")
    if allow_writes and not dependencies:
        raise ConfigError("allow_writes=True needs dependencies= (the app's admin check)")

    handlers = Handlers(engine, allow_writes=allow_writes)

    def who(request: Request) -> str:
        return actor(request) if actor is not None else "unknown"

    def admin_header(request: Request) -> Response | None:
        if not request.headers.get(ADMIN_HEADER, "").strip():
            return 403, {"error": f"write routes need the {ADMIN_HEADER} header"}
        return None

    router = APIRouter()
    guarded = [Depends(d) if callable(d) else d for d in dependencies]

    def reply(result: Response) -> Any:
        code, body = result
        return JSONResponse(body, status_code=code)

    @router.get("/health/live")
    async def live() -> Any:
        return reply(await handlers.live())

    @router.get("/health/ready")
    async def ready() -> Any:
        return reply(await handlers.ready())

    @router.get("/status", dependencies=guarded)
    async def status(model: str | None = None) -> Any:
        return reply(await handlers.status(model))

    @router.get("/requests/{request_id}", dependencies=guarded)
    async def request(request_id: str) -> Any:
        return reply(await handlers.request(request_id))

    @router.get("/registry", dependencies=guarded)
    async def registry() -> Any:
        return reply(await handlers.registry())

    @router.post("/registry", dependencies=guarded)
    async def set_values(
        request: Request,
        values: dict[str, Any] = Body(...),  # noqa: B008  # FastAPI's dependency style
        model: str | None = None,
    ) -> Any:
        refused = admin_header(request) if handlers.allow_writes else None
        return reply(refused or await handlers.set(values, actor=who(request), model=model))

    @router.post("/registry/block", dependencies=guarded)
    async def block(
        request: Request,
        body: dict[str, Any] = Body(...),  # noqa: B008  # {"region"|"account", "on"}
    ) -> Any:
        refused = admin_header(request) if handlers.allow_writes else None
        if refused:
            return reply(refused)
        region, account, on = body.get("region"), body.get("account"), body.get("on", True)
        wrong = [k for k in body if k not in ("region", "account", "on")]
        if (
            wrong
            or not isinstance(on, bool)
            or not all(v is None or isinstance(v, str) for v in (region, account))
        ):
            return reply((400, {"error": "the body is {region?, account?, on: true|false}"}))
        return reply(
            await handlers.block(actor=who(request), region=region, account=account, on=on)
        )

    return router
