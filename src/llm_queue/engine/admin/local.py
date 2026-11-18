"""`llmq.admin` of a queue in local mode (`LLMQ_EXECUTION_MODE=local`): no store behind it.

Calls run in the process and nothing is written to a store, so the operations that read or
change store state (requests, tenants by model, purges, registry versions and edits, audit,
models / deployments / blocks / queues views, jobs, dead letters) raise `LocalModeRefused`
(HTTP 503 `{"reason": "local_mode"}`, CLI exit 1). What the process itself knows stays
served: `status.get` (local slots and budgets), `ops.index` (`GET /`), `describe`, the drain
of this process, `registry.get` / `diff` / `schema` from the document, `tenants.hash`.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn

from llm_queue._version import __version__
from llm_queue.engine.admin.ops import OPERATIONS, drain_plan, op_row
from llm_queue.engine.admin.registry import redacted
from llm_queue.errors import ConfigError, LocalModeRefused

if TYPE_CHECKING:
    from llm_queue.engine.local import LocalEngine, LocalRegistry
    from llm_queue.engine.settings import InfraSettings
    from llm_queue.registry.resolve import ResolvedRegistry


@dataclass(slots=True)
class LocalContext:
    """What the local admin services read: the engine, its `LLMQueue`, settings, id hash.

    `engine` and `process` are None for the standalone CLI (`AdminServices.from_env` in
    local mode): there is no process to read then, only the settings and the document.
    """

    registry: LocalRegistry
    settings: InfraSettings | None
    hash_id: Callable[[str], str]
    engine: LocalEngine | None = None
    process: Any = None

    def view(self) -> ResolvedRegistry:
        """The loaded document (the standalone CLI checks it on first use)."""
        if self.engine is None and self.registry.changes < 0:
            return self.registry.run_on_config()
        return self.registry.current

    def needs_process(self, what: str) -> Any:
        """The `LLMQueue` this runs in; `LocalModeRefused` for the standalone CLI."""
        if self.process is None:
            raise LocalModeRefused(f"{what} outside the local-mode process")
        return self.process


def _refuse(operation: str) -> Callable[..., Coroutine[Any, Any, NoReturn]]:
    async def refused(*_: Any, **__: Any) -> NoReturn:
        raise LocalModeRefused(operation)

    return refused


class StoreOnly:
    """A resource whose operations need the store: each method raises `LocalModeRefused`."""

    def __init__(self, resource: str) -> None:
        """`resource` names it in the refusal (`purge`, `requests`, ...)."""
        self._resource = resource

    def __getattr__(self, name: str) -> Callable[..., Coroutine[Any, Any, NoReturn]]:
        if name.startswith("_"):
            raise AttributeError(name)
        return _refuse(f"{self._resource}.{name}")


class LocalRegistryAdmin(StoreOnly):
    """The document this process runs on; versions, edits and publishes need the store."""

    def __init__(self, ctx: LocalContext) -> None:
        """Over the local registry."""
        super().__init__("registry")
        self._ctx = ctx

    @property
    def current(self) -> ResolvedRegistry:
        """The document this process runs on (read-only)."""
        return self._ctx.view()

    @property
    def changes(self) -> int:
        """0 once loaded: a local document never changes."""
        return self._ctx.registry.changes

    async def get(self, model: str | None = None) -> dict[str, Any]:
        """The document (or one model's entry), secrets hidden; no live edits exist."""
        view = self._ctx.view()
        doc: Any = dict(view.registry.raw)
        if model is not None:
            doc = doc["models"][view.registry.alias_of(model)]
        return {"registry": redacted(doc), "changes": 0, "live_edits": []}

    async def diff(self) -> dict[str, Any]:
        """No live edits in local mode."""
        return {"live_edits": []}

    def schema(self) -> dict[str, Any]:
        """The registry document's JSON Schema."""
        from llm_queue.registry.json_schema import registry_schema

        return registry_schema()


class LocalTenantsAdmin(StoreOnly):
    """`tenants.hash` (it needs only the salt); listing tenants reads the store's index."""

    def __init__(self, ctx: LocalContext) -> None:
        """Over the id hash."""
        super().__init__("tenants")
        self._ctx = ctx

    def hash(self, tenant: str) -> dict[str, Any]:
        """The keyed hash of `tenant` (`LLMQ_ID_HASH_SALT`), as admin views show it."""
        if not tenant:
            raise ConfigError("give a tenant name")
        return {"tenant_hash": self._ctx.hash_id(tenant)}


class LocalStatusAdmin:
    """This process's status: per model its slots and its local budgets."""

    def __init__(self, ctx: LocalContext) -> None:
        """Over the local engine."""
        self._ctx = ctx

    async def get(
        self,
        model: str | None = None,
        *,
        tenant: str | None = None,
        tenant_hash: str | None = None,
    ) -> dict[str, Any]:
        """`StatusReport` / `ModelStatus` of this process (`mode: local`)."""
        if tenant is not None or tenant_hash is not None:
            raise LocalModeRefused("status of one tenant (it reads the request index)")
        engine = self._ctx.engine
        if engine is None:
            raise LocalModeRefused("status outside the local-mode process")
        return await engine.status(model)

    async def health(self, model: str | None = None) -> dict[str, Any]:
        """Health summary of this process's view (budgets are this process's share)."""
        from llm_queue.engine.status import health_report

        engine = self._ctx.engine
        if engine is None:
            raise LocalModeRefused("health summary outside the local-mode process")
        report = await engine.status()
        if model is not None:
            alias = engine.registry.current.registry.alias_of(model)
            report = {**report, "models": [m for m in report["models"] if m["model"] == alias]}
        return health_report(engine.registry.current, report)


class LocalDrainAdmin:
    """Drain of this process; not audited (the audit lives in the store)."""

    def __init__(self, ctx: LocalContext) -> None:
        """Acts on `ctx.process`, the `LLMQueue`."""
        self._ctx = ctx

    def get(self) -> dict[str, Any]:
        """`draining`, `since`, `held`, `running`."""
        return dict(self._ctx.needs_process("drain").drain_status())

    async def start(self, *, actor: str, dry_run: bool = True) -> dict[str, Any]:
        """Stop taking calls here (readiness fails)."""
        process = self._ctx.needs_process("drain")
        if dry_run:  # the default: the status now and `would_change`
            return drain_plan(process, draining=True)
        process.pause_receiving()
        return {**process.drain_status(), "dry_run": False}

    async def resume(self, *, actor: str, dry_run: bool = True) -> dict[str, Any]:
        """Take calls again."""
        process = self._ctx.needs_process("resume")
        if dry_run:  # the default: the status now and `would_change`
            return drain_plan(process, draining=False)
        process.resume_receiving()
        return {**process.drain_status(), "dry_run": False}


class LocalOpsAdmin:
    """The operations table and the `GET /` summary of a local-mode process."""

    def __init__(self, ctx: LocalContext) -> None:
        """Over the local engine (None for the standalone CLI)."""
        self._ctx = ctx

    def list(self) -> list[dict[str, Any]]:
        """Every operation: id, resource, verb, Python call, CLI command, route, guard."""
        return [op_row(op) for op in OPERATIONS]

    async def index(self, routes: Sequence[dict[str, Any]] | None = None) -> dict[str, Any]:
        """The live summary: library, process, the document, per model slots and budgets.

        `stores`, `jobs` and `purges` are None: local mode has no store.
        """
        ctx = self._ctx
        engine = ctx.engine
        settings = ctx.settings
        out: dict[str, Any] = {
            "library": {
                "version": __version__,
                "instance": engine.worker_id if engine is not None else None,
                "role_mode": settings.role_mode if settings else None,
                "execution_mode": "local",
                "environment": settings.environment if settings else None,
            },
            "process": None,
            "stores": None,
            "registry": None,
            "models": None,
            "jobs": None,
            "purges": None,
            "writes": any(r.get("writes") for r in (routes or ())),
            "routes": list(routes) if routes is not None else self.list(),
        }
        if engine is None:
            return out
        health = await engine.health_check()
        out["process"] = {k: health.get(k) for k in (
            "live", "ready", "not_ready_reason", "draining", "draining_since", "held", "running",
        )}  # fmt: skip
        try:
            report = await engine.status()
        except ConfigError:  # not loaded yet
            return out
        out["registry"] = {
            "version": report["registry_version"],
            "changes": 0,
            "live_edits": 0,
            "kill_switch": report["kill_switch"],
            "blocked_regions": report["blocked_regions"],
            "blocked_accounts": report["blocked_accounts"],
        }
        out["models"] = [
            {
                "model": m["model"],
                "enabled": m["enabled"],
                "waiting": sum(m["waiting"].values()),
                "in_progress": m["in_progress"],
                "concurrency": m["concurrency"],
                "deployments": len(m["deployments"]),
                "usable_deployments": sum(
                    bool(d["enabled"] and d["account_enabled"] and not d["blocked"])
                    and d["healthy"]
                    and d["breaker"] != "open"
                    for d in m["deployments"]
                ),
                "open_breakers": sum(d["breaker"] == "open" for d in m["deployments"]),
                "health": m["health"]["state"],
            }
            for m in report["models"]
        ]
        return out


# Resources every operation of which needs the store.
STORE_ONLY = (
    "audit", "models", "deployments", "blocks", "requests", "queues", "dlq", "jobs", "purge",
)  # fmt: skip
