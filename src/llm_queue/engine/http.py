"""The admin HTTP routes (extra `http`), generated from the operations table (0.2.0 R).

`status_router` mounts one route per `engine/admin/ops.py` operation with an `http` entry; path,
query or JSON body arguments are checked (bad fields: 400) and go to the same `llmq.admin`
method as the CLI. Errors map in `error_response`; health, readiness and metrics stay open.
Reads need a read or admin token or the app's `dependencies`; writes need the admin token, or
`dependencies` plus the `X-LLMQ-Admin` header (no simple cross-site request carries it), an
actor and `allow_writes`.
"""

from __future__ import annotations

import inspect
import json
import time
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

import orjson
from redis.exceptions import ResponseError

from llm_queue._logging import get_logger
from llm_queue.engine.admin.context import NotLoaded
from llm_queue.engine.admin.ops import OPERATIONS, Op, Param, op_row
from llm_queue.engine.admin.registry import redacted as redacted
from llm_queue.engine.store_guard import STORE_DOWN
from llm_queue.errors import (
    AdminRefused,
    ConfigError,
    LLMQueueError,
    LocalModeRefused,
    MissingExtra,
    QueueDisabled,
    StoreUnavailable,
    UnknownModel,
)
from llm_queue.registry.store import ACTOR_MAX_CHARS, RegistryConflict

if TYPE_CHECKING:
    from llm_queue.engine.admin import AdminServices

logger = get_logger(__name__)
ADMIN_HEADER = "X-LLMQ-Admin"  # LLMQ_ADMIN_CSRF_HEADER default
Response = tuple[int, dict[str, Any]]
# A lost compare-and-set is valid and retryable: 409 with `Retry-After`, not 400 (FO-6).
RETRY_AFTER_SECS = 1
# Bounds on what a client sends before anything reads the store or diffs the registry (FO-15).
MAX_ID_CHARS = 256
MAX_EDIT_PATHS = 100
# Strict mode (review B-21, owner Q12): an admin token is the only guard on the write routes and
# can be guessed online, so each configured token must be at least this long.
MIN_TOKEN_CHARS = 32
# One value of a JSON body (a registry edit's `values` entry), serialised (B-08).
MAX_VALUE_BYTES = 65_536
# A POST body above this answers 413 (B-08, `LLMQ_ADMIN_BODY_MAX_BYTES`).
BODY_MAX_BYTES = 1_048_576
# Store answers that mean "refusing for a while", not "bad request".
_STORE_REFUSALS = ("BUSY", "LOADING", "MASTERDOWN", "MISCONF", "NOREPLICAS", "OOM", "READONLY")
TENANT_IN_URL = (
    "a tenant name stays out of URLs: pass tenant_hash (get it with `llm-queue tenants hash "
    "NAME` or POST /tenants/hash)"
)
Token = str | Callable[[], str | None] | None


def error_response(exc: BaseException) -> Response:
    """The status code and body of an admin error (design 2.8); re-raises a non-admin error.

    Returns:
        404 `UnknownModel`; 409 `RegistryConflict` (retryable), `AdminRefused` or another
        `LLMQueueError`; 503 no registry, store outage, `LocalModeRefused` (`reason:
        local_mode`) or `QueueDisabled` (`reason: disabled`); 400 other `ConfigError`; a store
        `ResponseError` (B-07) 400 for the request itself, 503 for refused writes.
    """
    if isinstance(exc, UnknownModel):
        return 404, {"error": str(exc)}
    if isinstance(exc, RegistryConflict):
        return 409, {"error": str(exc), "retry_after_secs": RETRY_AFTER_SECS}
    if isinstance(exc, NotLoaded):
        return 503, {"error": str(exc) or "registry not loaded"}
    if isinstance(exc, (StoreUnavailable, *STORE_DOWN)):
        return 503, {"error": "state store unavailable"}
    if isinstance(exc, AdminRefused):
        return 409, {"error": str(exc)}
    if isinstance(exc, LocalModeRefused):
        return 503, {"error": str(exc), "reason": "local_mode", "mode": "local"}
    if isinstance(exc, QueueDisabled):
        return 503, {"error": str(exc), "reason": "disabled", "enabled": False}
    if isinstance(exc, ConfigError):
        return 400, {"error": str(exc)}
    if isinstance(exc, LLMQueueError):
        return 409, {"error": str(exc)}
    if isinstance(exc, ResponseError):
        words = str(exc).split(maxsplit=1)
        if words and words[0].upper() in _STORE_REFUSALS:
            return 503, {"error": "state store unavailable"}
        return 400, {"error": f"the store refused the request: {exc}"}
    raise exc


def _type_ok(param: Param, value: Any) -> bool:
    if value is None:
        return not param.required
    kind = param.kind
    if kind is bool:
        return isinstance(value, bool)
    if kind is int:
        return isinstance(value, int) and not isinstance(value, bool)
    if kind is float:
        return isinstance(value, int | float) and not isinstance(value, bool)
    if kind is list:
        return (
            isinstance(value, list)
            and len(value) <= MAX_EDIT_PATHS
            and all(isinstance(v, param.item) for v in value)
        )
    if kind is dict:
        return isinstance(value, dict) and 0 < len(value) <= MAX_EDIT_PATHS
    return isinstance(value, kind)


def _too_long(value: Any) -> bool:
    if isinstance(value, str):
        return len(value) > MAX_ID_CHARS
    if isinstance(value, list):
        return any(_too_long(v) for v in value)
    if isinstance(value, dict):
        return any(_too_long(k) or _too_big(v) for k, v in value.items())
    return False


def _too_big(value: Any) -> bool:
    """True when one body value is above `MAX_VALUE_BYTES` serialised (B-08)."""
    try:
        return len(orjson.dumps(value)) > MAX_VALUE_BYTES
    except TypeError:
        return True


def _from_query(param: Param, raw: list[str]) -> Any:
    """A query value as the parameter's type; ValueError when it is not one."""
    if param.kind is list:
        return list(raw)
    text = raw[-1]
    if param.kind is bool:
        if text.lower() not in ("true", "false", "1", "0"):
            raise ValueError(text)
        return text.lower() in ("true", "1")
    if param.kind is int:
        return int(text)
    if param.kind is float:
        return float(text)
    return text


def parse_arguments(op: Op, path: Mapping[str, str], fields: Mapping[str, Any]) -> dict[str, Any]:
    """Keyword arguments of `op` from the path and the query / body fields; ConfigError if bad.

    `dry_run` (writes with a dry run) defaults to true; `deployment` is taken from the path's
    `{account}/{name}`.
    """
    names = {p.name for p in op.params}
    allowed = names | ({"dry_run"} if op.dry_run_default is not None else set())
    path = dict(path)
    if "account" in path and "name" in path and "deployment" in names:
        path["deployment"] = f"{path.pop('account')}/{path.pop('name')}"
    unknown = sorted(set(fields) - allowed)
    if unknown:
        raise ConfigError(f"unknown fields {unknown}; {op.id} takes {sorted(allowed)}")
    out: dict[str, Any] = {}
    wrong: list[str] = []
    for param in op.params:
        if param.name in path:
            value: Any = path[param.name]
        elif param.name in fields:
            value = fields[param.name]
        elif param.required:
            wrong.append(param.name)
            continue
        else:
            continue
        if (
            not _type_ok(param, value)
            or _too_long(value)
            or (param.choices and value not in param.choices)
        ):
            wrong.append(param.name)
        elif isinstance(value, int | float) and not isinstance(value, bool) and value < 0:
            wrong.append(param.name)  # counts, limits, seconds and versions are never negative
        else:
            out[param.name] = value
    if op.dry_run_default is not None:
        dry = fields.get("dry_run", op.dry_run_default)
        if not isinstance(dry, bool):
            wrong.append("dry_run")
        out["dry_run"] = dry
    if wrong:
        raise ConfigError(f"bad or missing: {sorted(set(wrong))} ({_signature(op)})")
    return out


def _signature(op: Op) -> str:
    parts = [f"{p.name}{'' if p.required else '?'}: {p.kind.__name__}" for p in op.params]
    if op.dry_run_default is not None:
        parts.append("dry_run?: bool")
    return f"{op.id} takes {{{', '.join(parts)}}}"


async def call(admin: AdminServices, op: Op, kwargs: Mapping[str, Any]) -> Response:
    """Run one operation; its JSON response (None from a read = 404)."""
    try:
        result = admin.method(op)(**kwargs)
        if inspect.isawaitable(result):
            result = await result
    except Exception as exc:
        return error_response(exc)
    if result is None:
        return 404, {"error": "not found (unknown or expired)"}
    if not isinstance(result, dict):
        result = {"items": result}
    return 200, result


def admin_token_dependency(
    token: Token, header: str, *, read_token: Token = None, min_chars: int = 0
) -> Any:
    """A FastAPI dependency: the request's `header` must equal `token` or `read_token`.

    Constant-time compare. A callable is read per request (rotation needs no restart) on a
    worker thread, so a secrets-source refresh never blocks the loop. Fails closed with no
    token configured, or one shorter than `min_chars` (a rotation after the mount check).
    """
    import asyncio
    import hmac

    from fastapi import HTTPException, Request

    def value(source: Token) -> str | None:
        got = source() if callable(source) else source
        return str(got) if got else None

    def values() -> list[str | None]:
        return [value(token), value(read_token)]

    async def guard(request: Request) -> None:
        given = request.headers.get(header, "").encode()
        read = values() if not (callable(token) or callable(read_token)) else None
        if read is None:
            read = await asyncio.to_thread(values)
        accepted = [t for t in read if t and len(t) >= min_chars]
        # Every token is compared, so the time does not tell which one matched.
        matched = [hmac.compare_digest(given, t.encode()) for t in accepted]
        if not any(matched):
            raise HTTPException(status_code=403, detail="forbidden")

    # FastAPI resolves annotations in this module's globals, where the optional import is not.
    guard.__annotations__["request"] = Request
    return guard


async def _read_body(request: Any, limit: int) -> bytes | None:
    """The request body, None once it is above `limit` bytes (read no further; B-08)."""
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > limit:
        return None
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


def _configured(token: Token) -> bool:
    if token is None:
        return False
    return bool(token() if callable(token) else token)


def _short_tokens(**tokens: Token) -> list[str]:
    """The names of the configured tokens shorter than `MIN_TOKEN_CHARS` (never a value)."""
    out = []
    for name, token in tokens.items():
        value = token() if callable(token) else token
        if value and len(str(value)) < MIN_TOKEN_CHARS:
            out.append(name)
    return out


def status_router(
    admin: AdminServices,
    *,
    dependencies: Sequence[Any] = (),
    allow_writes: bool = False,
    actor: Callable[[Any], str] | None = None,
    metrics_route: bool = False,
    drain_routes: bool = False,
    admin_token: Token = None,
    read_token: Token = None,
    token_header: str = "X-LLMQ-Token",
    actor_header: str = "X-LLMQ-Actor",
    csrf_header: str = ADMIN_HEADER,
    insecure_reads: bool = False,
    wait_cap_secs: float = 60.0,
    dlq_bodies: bool = False,
    index_cache_secs: float = 2.0,
    strict: bool = False,
    body_max_bytes: int = BODY_MAX_BYTES,
) -> Any:
    """A FastAPI `APIRouter` of the admin operations (`pip install 'llm-queue[http]'`).

    With no guard only the open routes mount, unless `insecure_reads=True`.

    Args:
        admin: The admin services the routes call.
        dependencies: The app's admin check; guards every route but probes and `/metrics`.
        allow_writes: Mount the write routes.
        actor: Names a write's actor as `http:<actor(request)>`.
        metrics_route: Mount `GET /metrics`.
        drain_routes: Mount `GET /drain`, `POST /drain` and `POST /resume`.
        admin_token: Write and read token, a value or a callable read per request.
        read_token: Read-only token, a value or a callable read per request.
        token_header: The header carrying either token.
        actor_header: Without `actor`, the token guard's actor is `token:<value>` (or
            `token:unknown`).
        csrf_header: The header every write must carry.
        insecure_reads: Mount the read routes with no guard.
        wait_cap_secs: Cap on any `wait_secs` an HTTP client asks for.
        dlq_bodies: Let `GET /dlq/{id}?unsafe=true` return message bodies.
        index_cache_secs: How long `GET /` reuses its summary.
        strict: Refuse to mount, and later refuse requests, with a token shorter than
            `MIN_TOKEN_CHARS` (outside dev).
        body_max_bytes: The largest JSON body accepted.
    """
    try:
        from fastapi import APIRouter, Depends, Request
        from fastapi.responses import JSONResponse
        from fastapi.responses import Response as RawResponse
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise MissingExtra("http_router", "http") from exc
    # Annotations are strings here (`from __future__ import annotations`); FastAPI resolves them
    # in this module's globals, where the optional import is not, so a `Request` parameter
    # would be read as a query field. Publish the name once it is imported.
    globals()["Request"] = Request

    if strict and (short := _short_tokens(admin_token=admin_token, read_token=read_token)):
        raise ConfigError(
            f"admin tokens must be at least {MIN_TOKEN_CHARS} characters outside dev: "
            + ", ".join(short)
        )
    min_chars = MIN_TOKEN_CHARS if strict else 0
    reads_by_token = _configured(admin_token) or _configured(read_token)
    writes_by_token = _configured(admin_token)
    guarded = [Depends(d) if callable(d) else d for d in dependencies]
    # `/registry` lists accounts, regions, model ids and base URLs (FO-14): with no token and
    # no `dependencies` only the open routes (probes, metrics) are mounted, unless
    # `insecure_reads=True`. It warned and served everything before 0.2.0.
    reads = bool(guarded) or reads_by_token or insecure_reads
    if not reads:
        logger.warning(
            "admin_routes_unauthenticated",
            mounted="probes and metrics only",
            fix="set LLMQ_ADMIN_TOKEN / LLMQ_ADMIN_READ_TOKEN or pass dependencies=",
        )
    writing = allow_writes or drain_routes
    if writing and not guarded and not writes_by_token:
        raise ConfigError(
            "write routes (allow_writes / drain_routes) need LLMQ_ADMIN_TOKEN or dependencies="
        )
    if writing and actor is None and not writes_by_token:
        raise ConfigError("write routes need actor= (who made the change, for the audit)")
    read_deps = list(guarded)
    if reads_by_token:
        read_deps.append(
            Depends(
                admin_token_dependency(
                    admin_token, token_header, read_token=read_token, min_chars=min_chars
                )
            )
        )
    write_deps = list(guarded)
    if writes_by_token:
        write_deps.append(
            Depends(admin_token_dependency(admin_token, token_header, min_chars=min_chars))
        )

    def who(request: Any) -> str:
        if actor is not None:
            return f"http:{actor(request)}"
        # Cut to fit `token:` within the one actor limit (review B-06): control characters
        # in a claimed name are refused by the audit's `check_actor` (400).
        claimed = request.headers.get(actor_header, "").strip()[: ACTOR_MAX_CHARS - 6]
        return f"token:{claimed or 'unknown'}"

    def reply(result: Response) -> Any:
        code, body = result
        retry = code == 409 and "retry_after_secs" in body  # a refused purge is not retryable
        headers = {"Retry-After": str(RETRY_AFTER_SECS)} if retry else None
        return JSONResponse(body, status_code=code, headers=headers)

    engine = admin.context.engine
    router = APIRouter()
    mounted: list[dict[str, Any]] = []
    index_cache: list[Any] = [0.0, None]  # monotonic time, summary

    def mounts(op: Op) -> bool:
        if op.guard == "read" and not reads:
            return False
        if op.guard == "write" and not (guarded or writes_by_token):
            return False  # never open: e.g. `tenants.hash` (B-22) with only a read token
        return {
            "always": True,
            "writes": allow_writes,
            "drain": drain_routes,
            "metrics": metrics_route,
        }[op.mount]

    async def run(op: Op, request: Request) -> Any:
        if op.writes and not writes_by_token and not request.headers.get(csrf_header, "").strip():
            return reply((403, {"error": f"write routes need the {csrf_header} header"}))
        fields: dict[str, Any]
        if op.http and op.http[0] == "GET":
            query = request.query_params
            fields = {}
            params = {p.name: p for p in op.params}
            for name in query:
                param = params.get(name)
                if param is None:
                    fields[name] = query.get(name)
                    continue
                try:
                    fields[name] = _from_query(param, query.getlist(name))
                except ValueError:
                    return reply((400, {"error": f"bad {name} ({_signature(op)})"}))
            if "tenant" in fields and "tenant_hash" in params:
                # A name in a URL reaches proxy and access logs the library cannot redact
                # (review B-05): GET routes take the hash only.
                return reply((400, {"error": TENANT_IN_URL}))
        else:
            raw = await _read_body(request, body_max_bytes)
            if raw is None:
                return reply((413, {"error": f"the body is above {body_max_bytes} bytes"}))
            try:
                body = json.loads(raw) if raw else {}
            except ValueError:
                return reply((400, {"error": "the body must be a JSON object"}))
            if not isinstance(body, dict):
                return reply((400, {"error": "the body must be a JSON object"}))
            fields = body
        try:
            kwargs = parse_arguments(op, request.path_params, fields)
        except ConfigError as exc:
            return reply((400, {"error": str(exc)}))
        if "wait_secs" in kwargs and kwargs["wait_secs"] is not None:
            kwargs["wait_secs"] = min(float(kwargs["wait_secs"]), wait_cap_secs)
        elif "wait_secs" in {p.name for p in op.params} and op.id.startswith("purge."):
            kwargs["wait_secs"] = wait_cap_secs  # an omitted wait waited LLMQ_PURGE_WAIT_SECS
        if kwargs.get("unsafe") and not dlq_bodies:
            return reply(
                (403, {"error": "message bodies need LLMQ_ADMIN_DEAD_LETTER_BODIES_ENABLED"})
            )
        if op.writes:
            kwargs["actor"] = who(request)
        return reply(await call(admin, op, kwargs))

    def endpoint(op: Op) -> Callable[..., Any]:
        async def handle(request: Request) -> Any:
            return await run(op, request)

        handle.__name__ = op.id.replace(".", "_")
        return handle

    async def live() -> Any:
        # From the engine's own state, never the store: a slow store failed the probe (FO-9).
        if engine is None or engine._live():
            return reply((200, {"live": True}))
        return reply((503, {"live": False, "reason": engine._not_live_reason()}))

    async def ready() -> Any:
        if engine is None:
            return reply((503, {"ready": False, "not_ready_reason": "no engine"}))
        health = await engine.health_check()
        return reply(((200 if health["ready"] else 503), health))

    async def metrics() -> Any:
        from llm_queue.engine.metrics import prometheus_text

        if engine is None:
            return reply((503, {"error": "no engine in this process"}))
        body, content_type = prometheus_text(engine.metrics)
        return RawResponse(body, media_type=content_type)

    async def index() -> Any:
        now = time.monotonic()
        if index_cache[1] is None or now - index_cache[0] > index_cache_secs:
            index_cache[0], index_cache[1] = now, await admin.ops.index(routes=mounted)
        return reply((200, index_cache[1]))

    special: dict[str, Callable[..., Any]] = {
        "health.live": live,
        "health.ready": ready,
        "metrics.get": metrics,
        "ops.index": index,
    }
    for op in OPERATIONS:
        if op.http is None or not mounts(op):
            continue
        method, path = op.http
        deps = {"open": [], "read": read_deps, "write": write_deps}[op.guard]
        handler = special.get(op.id) or endpoint(op)
        router.add_api_route(
            path, handler, methods=[method], dependencies=deps, name=op.id, summary=op.summary
        )
        row = op_row(op)
        mounted.append({k: row[k] for k in (
            "method", "path", "op", "resource", "verb", "writes", "guard", "dry_run_default",
            "summary",
        )})  # fmt: skip
    return router
