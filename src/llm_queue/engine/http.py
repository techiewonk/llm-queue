"""HTTP views of the engine (plan §3.12, E132, E138): status, requests, registry, health.

`Handlers` are plain async functions returning (status code, JSON body), so they are tested
without a web framework; `status_router()` mounts them on FastAPI (extra `http`). Health routes
take no auth; the app puts its admin dependency on the rest. Writes (safe fields only) are off
unless the app passes `allow_writes=True`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from llm_queue._logging import get_logger
from llm_queue.engine.store_guard import STORE_DOWN
from llm_queue.errors import ConfigError, MissingExtra, PurgeRefused, UnknownModel
from llm_queue.registry.store import (
    _SECRET_FIELDS,  # one set for the audit and this view, so a new secret is hidden in both
    LiveEditRefused,
    RegistryConflict,
    audit_diff,
)
from llm_queue.registry.validate import RegistryError

if TYPE_CHECKING:
    from llm_queue.engine.runtime import Engine

logger = get_logger(__name__)
ADMIN_HEADER = "X-LLMQ-Admin"
Response = tuple[int, dict[str, Any]]
# A store outage was a 500 with redis-py's traceback; it is a 503 like the readiness probe (EN-10).
_UNAVAILABLE: Response = (503, {"error": "state store unavailable"})
# Mounted and hit before `start()` loaded the registry: a 500 with a traceback (FO-7).
_NOT_LOADED: Response = (503, {"error": "registry not loaded"})
# A lost compare-and-set is valid and retryable: 409 with `Retry-After`, not 400 (FO-6).
RETRY_AFTER_SECS = 1
# Bounds on what a client sends before anything reads the store or diffs the registry (FO-15).
MAX_ID_CHARS = 256
MAX_EDIT_PATHS = 100


def redacted(doc: Any) -> Any:
    """The registry with role ARNs and secret references hidden (lists walked too)."""
    if isinstance(doc, Mapping):
        return {k: "[redacted]" if k in _SECRET_FIELDS else redacted(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [redacted(v) for v in doc]
    return doc


def _too_long(*values: str | None) -> bool:
    return any(v is not None and len(v) > MAX_ID_CHARS for v in values)


# Purge bodies: field -> accepted types. `dry_run` defaults to true (a plan): send false to purge.
_PURGE_FIELDS: dict[str, dict[str, tuple[type, ...]]] = {
    "model": {"model": (str,), "force": (bool,), "dry_run": (bool,)},
    "deployment": {
        "model": (str,),
        "deployment": (str,),
        "force": (bool,),
        "wait": (bool,),
        "wait_secs": (int, float),
        "dry_run": (bool,),
    },
    "tenant": {"tenant": (str,), "remove": (list,), "models": (list,), "dry_run": (bool,)},
    "gc": {"force": (bool,), "dry_run": (bool,)},
}
_PURGE_REQUIRED = {
    "model": ("model",),
    "deployment": ("model", "deployment"),
    "tenant": ("tenant",),
}


def _purge_body(kind: str, body: Any) -> str | None:
    """What is wrong with a purge body, or None."""
    fields = _PURGE_FIELDS[kind]
    if not isinstance(body, Mapping):
        return "the body must be a JSON object"
    wrong = [k for k, v in body.items() if k not in fields or not isinstance(v, fields[k])]
    wrong += [k for k in _PURGE_REQUIRED.get(kind, ()) if k not in body]
    lists = [v for k, v in body.items() if k in ("remove", "models") and isinstance(v, list)]
    if wrong or any(not all(isinstance(x, str) for x in v) for v in lists):
        names = ", ".join(fields)
        return f"the body is an object of {names} ({kind}); bad or missing: {sorted(set(wrong))}"
    texts = [v for v in body.values() if isinstance(v, str)] + [x for v in lists for x in v]
    if _too_long(*texts) or any(len(v) > MAX_EDIT_PATHS for v in lists):
        return f"names have at most {MAX_ID_CHARS} characters, lists {MAX_EDIT_PATHS} items"
    return None


class Handlers:
    """The HTTP handlers of one engine."""

    def __init__(self, engine: Engine, *, allow_writes: bool = False) -> None:
        """Writes stay refused unless `allow_writes`."""
        self.engine = engine
        self.allow_writes = allow_writes

    def _loaded(self) -> bool:
        try:
            self.engine.registry.current  # noqa: B018  # raises ConfigError before the first load
        except ConfigError:
            return False
        return True

    async def live(self) -> Response:
        """200 while the engine's loops run; 503 when one died or the heartbeat stalled (O2).

        Read from the engine's own state, never the store: a store outage or a slow probe made
        liveness wait up to the probe timeout and fail the kubelet's 1 s default (FO-9).
        """
        live = self.engine._live()
        if live:
            return 200, {"live": True}
        # Why, so 50 pods do not need a log search to say which loop died (R3OB-20).
        return 503, {"live": False, "reason": self.engine._not_live_reason()}

    def metrics(self) -> tuple[bytes, str]:
        """The Prometheus exposition of the engine's sink (body, content type) (R3OB-14)."""
        from llm_queue.engine.metrics import prometheus_text

        return prometheus_text(self.engine.metrics)

    async def ready(self) -> Response:
        """200 when the registry is loaded and the process is not draining (stores in the body)."""
        health = await self.engine.health_check()
        return (200 if health["ready"] else 503), health

    async def status(self, model: str | None = None) -> Response:
        """Status of a model or of everything (404 unknown model, 503 store down or not loaded)."""
        if _too_long(model):
            return 404, {"error": "unknown model"}
        try:
            return 200, await self.engine.status(model)
        except UnknownModel:  # only this: another KeyError is a bug, not a client error (FO-17)
            return 404, {"error": f"unknown model {model}"}
        except ConfigError:
            return _NOT_LOADED
        except STORE_DOWN:
            return _UNAVAILABLE

    async def request(self, request_id: str) -> Response:
        """Status of one call (503 while the store is unreachable)."""
        if _too_long(request_id):
            return 404, {"error": "unknown or expired request"}
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
            raw = dict(store.current.registry.raw)
        except ConfigError:
            return _NOT_LOADED
        except STORE_DOWN:
            return _UNAVAILABLE
        return 200, {
            "registry": redacted(raw),
            "changes": store.changes,
            "live_edits": redacted(live_edits),
        }

    async def set(
        self, values: Mapping[str, Any], *, actor: str, model: str | None = None
    ) -> Response:
        """A live edit of safe fields (only with `allow_writes`); audited with `actor`."""
        if not self.allow_writes:
            return 403, {"error": "writes are off"}
        if len(values) > MAX_EDIT_PATHS or _too_long(model, *values):
            return 400, {"error": f"at most {MAX_EDIT_PATHS} paths of {MAX_ID_CHARS} characters"}
        if not self._loaded():
            return _NOT_LOADED
        before = dict(self.engine.registry.current.registry.raw)
        try:
            view = await self.engine.registry.set(values, actor=f"http:{actor}", model=model)
        except RegistryConflict as exc:
            return 409, {"error": str(exc), "retry_after_secs": RETRY_AFTER_SECS}
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
        if _too_long(region, account):
            return 400, {"error": f"a region or account has at most {MAX_ID_CHARS} characters"}
        if not self._loaded():
            return _NOT_LOADED
        try:
            await self.engine.registry.block(
                region=region, account=account, on=on, actor=f"http:{actor}"
            )
        except RegistryConflict as exc:
            return 409, {"error": str(exc), "retry_after_secs": RETRY_AFTER_SECS}
        except ConfigError as exc:
            return 400, {"error": str(exc)}
        except STORE_DOWN:
            return _UNAVAILABLE
        routing = self.engine.registry.current.registry.routing
        return 200, {
            "blocked_regions": list(routing.blocked_regions),
            "blocked_accounts": list(routing.blocked_accounts),
        }


class PurgeHandlers:
    """The purge routes (only with `allow_writes`): thin calls into `engine.purge`."""

    def __init__(self, engine: Engine, *, allow_writes: bool = False) -> None:
        """Purges stay refused unless `allow_writes`."""
        self.engine = engine
        self.allow_writes = allow_writes

    async def purge(self, kind: str, body: Any, *, actor: str) -> Response:
        """`kind` is model, deployment, tenant or gc; `body` a JSON object; returns the report."""
        if not self.allow_writes:
            return 403, {"error": "writes are off"}
        problem = _purge_body(kind, body)
        if problem:
            return 400, {"error": problem}
        service = self.engine.purge
        who = f"http:{actor}"
        dry = body.get("dry_run", True)
        try:
            if kind == "model":
                report = await service.model(
                    body["model"], actor=who, force=body.get("force", False), dry_run=dry
                )
            elif kind == "deployment":
                report = await service.deployment(
                    body["model"],
                    body["deployment"],
                    actor=who,
                    force=body.get("force", False),
                    wait=body.get("wait"),
                    wait_secs=body.get("wait_secs"),
                    dry_run=dry,
                )
            elif kind == "tenant":
                report = await service.tenant(
                    body["tenant"],
                    actor=who,
                    remove=body.get("remove"),
                    models=body.get("models"),
                    dry_run=dry,
                )
            else:
                report = await service.removed(
                    actor=who, force=body.get("force", False), dry_run=dry
                )
        except PurgeRefused as exc:
            return 409, {"error": str(exc)}
        except ConfigError as exc:
            return 400, {"error": str(exc)}
        except STORE_DOWN:
            return _UNAVAILABLE
        return 200, report.as_dict()

    async def scheduled(self) -> Response:
        """Auto purges waiting for their grace."""
        if not self.allow_writes:
            return 403, {"error": "writes are off"}
        try:
            return 200, {"scheduled": await self.engine.purge.scheduled()}
        except STORE_DOWN:
            return _UNAVAILABLE

    async def cancel_scheduled(self, body: Any, *, actor: str) -> Response:
        """Drop one scheduled auto purge: body `{"target": ...}`."""
        if not self.allow_writes:
            return 403, {"error": "writes are off"}
        target = body.get("target") if isinstance(body, Mapping) else None
        if not isinstance(target, str) or _too_long(target) or len(body) != 1:
            return 400, {"error": "the body is an object with only `target`"}
        try:
            removed = await self.engine.purge.cancel_scheduled(target)
        except STORE_DOWN:
            return _UNAVAILABLE
        logger.info("purge_unscheduled", target=target, by=f"http:{actor}")
        return 200, {"target": target, "cancelled": removed}


def status_router(
    engine: Engine,
    *,
    dependencies: Sequence[Any] = (),
    allow_writes: bool = False,
    actor: Callable[[Any], str] | None = None,
    metrics_route: bool = False,
) -> Any:
    """A FastAPI `APIRouter` with the handlers (`pip install 'llm-queue[http]'`).

    `metrics_route`: also `GET /metrics`, the Prometheus exposition of a `metrics_sink=
    prometheus` engine (needs the `metrics` extra), open like the health routes so a scraper
    can read it (R3OB-14: no route and no document said how to expose the registry).

    `dependencies` (the app's admin check) guard every route except `/health/live` and
    `/health/ready`. Writes need `dependencies` and `actor`: `actor` gets the request and
    returns the authenticated name the audit records (never a client-supplied value, which
    anyone could spoof). Write routes take JSON bodies and need the `X-LLMQ-Admin` header: a
    browser cannot send either cross-site without a CORS preflight, so a page an admin visits
    cannot post a block or an edit with the admin's cookies. With no `dependencies` the read
    routes (`/status`, `/requests/{id}`, `/registry`) are open; a warning is logged (FO-14).
    With `allow_writes` the purge routes (`/purge/*`, `/registry/gc`) are mounted too, under
    the same rules; their `dry_run` defaults to true.
    """
    try:
        from fastapi import APIRouter, Body, Depends, Request
        from fastapi.responses import JSONResponse
        from fastapi.responses import Response as RawResponse
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
    if not dependencies:
        # `/registry` lists accounts, regions, model ids and base URLs: open to anyone who
        # reaches the app unless it sits behind its own auth (FO-14).
        logger.warning(
            "admin_routes_unauthenticated",
            routes="/status, /requests/{id}, /registry",
            fix="pass dependencies= (the app's admin check)",
        )

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
        retry = code == 409 and "retry_after_secs" in body  # a refused purge is not retryable
        headers = {"Retry-After": str(RETRY_AFTER_SECS)} if retry else None
        return JSONResponse(body, status_code=code, headers=headers)

    @router.get("/health/live")
    async def live() -> Any:
        return reply(await handlers.live())

    if metrics_route:

        @router.get("/metrics")
        async def metrics() -> Any:
            body, content_type = handlers.metrics()
            return RawResponse(body, media_type=content_type)

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

    if allow_writes:
        purges = PurgeHandlers(engine, allow_writes=True)

        async def purge_route(kind: str, request: Request, body: Any) -> Any:
            refused = admin_header(request)
            return reply(refused or await purges.purge(kind, body, actor=who(request)))

        @router.post("/purge/model", dependencies=guarded)
        async def purge_model(request: Request, body: Any = Body(...)) -> Any:  # noqa: B008
            return await purge_route("model", request, body)

        @router.post("/purge/deployment", dependencies=guarded)
        async def purge_deployment(request: Request, body: Any = Body(...)) -> Any:  # noqa: B008
            return await purge_route("deployment", request, body)

        @router.post("/purge/tenant", dependencies=guarded)
        async def purge_tenant(request: Request, body: Any = Body(...)) -> Any:  # noqa: B008
            return await purge_route("tenant", request, body)

        @router.post("/registry/gc", dependencies=guarded)
        async def registry_gc(request: Request, body: Any = Body(...)) -> Any:  # noqa: B008
            return await purge_route("gc", request, body)

        @router.get("/purge/scheduled", dependencies=guarded)
        async def purge_scheduled(request: Request) -> Any:
            return reply(admin_header(request) or await purges.scheduled())

        @router.post("/purge/scheduled/cancel", dependencies=guarded)
        async def purge_unschedule(request: Request, body: Any = Body(...)) -> Any:  # noqa: B008
            refused = admin_header(request)
            return reply(refused or await purges.cancel_scheduled(body, actor=who(request)))

    return router
