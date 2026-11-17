"""`llmq.admin` with the library switched off (`LLMQ_ENABLED=false`, 0.2.0 H1 / FU1).

Nothing is built: no store client, no registry. The operations that read or change the store
raise `QueueDisabled` (HTTP 503 `{"reason": "disabled"}`, CLI exit 1), as local mode refuses
its store-only operations. Served: `describe`, `ops.list` / `ops.index` (`enabled: false`),
`registry.schema`, and the drain of the process (the drain file keeps working).
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn

from llm_queue._version import __version__
from llm_queue.engine.admin.ops import OPERATIONS, op_row
from llm_queue.errors import QueueDisabled

if TYPE_CHECKING:
    from llm_queue.engine.settings import InfraSettings

# Every resource of `AdminServices` whose operations all need the store.
REFUSED = (
    "audit", "models", "deployments", "blocks", "requests", "tenants", "queues", "dlq", "jobs",
    "purge", "status",
)  # fmt: skip


@dataclass(slots=True)
class DisabledContext:
    """The settings and, inside a process, its `DisabledQueue` (None for the standalone CLI)."""

    settings: InfraSettings | None
    process: Any = None
    engine: None = None  # no engine: `AdminServices.describe` reads this

    def needs_process(self, what: str) -> Any:
        """The `DisabledQueue` this runs in; `QueueDisabled` for the standalone CLI."""
        if self.process is None:
            raise QueueDisabled(f"{what} outside a process")
        return self.process


def _refuse(operation: str) -> Callable[..., Coroutine[Any, Any, NoReturn]]:
    async def refused(*_: Any, **__: Any) -> NoReturn:
        raise QueueDisabled(operation)

    return refused


class Refused:
    """A resource of a disabled library: each method raises `QueueDisabled`."""

    def __init__(self, resource: str) -> None:
        """`resource` names it in the refusal (`purge`, `requests`, ...)."""
        self._resource = resource

    def __getattr__(self, name: str) -> Callable[..., Coroutine[Any, Any, NoReturn]]:
        if name.startswith("_"):
            raise AttributeError(name)
        return _refuse(f"{self._resource}.{name}")


class DisabledRegistryAdmin(Refused):
    """Only the schema: there is no registry to read."""

    def __init__(self) -> None:
        """Every other registry operation refuses."""
        super().__init__("registry")

    def schema(self) -> dict[str, Any]:
        """The registry document's JSON Schema."""
        from llm_queue.registry.json_schema import registry_schema

        return registry_schema()


class DisabledDrainAdmin:
    """Drain of this process: readiness fails while draining; not audited (no store)."""

    def __init__(self, ctx: DisabledContext) -> None:
        """Acts on `ctx.process`, the `DisabledQueue`."""
        self._ctx = ctx

    def get(self) -> dict[str, Any]:
        """`draining`, `since`, `held` (0), `running` (0)."""
        return dict(self._ctx.needs_process("drain").drain_status())

    async def start(self, *, actor: str) -> dict[str, Any]:
        """Fail readiness here."""
        process = self._ctx.needs_process("drain")
        process.pause_receiving()
        return dict(process.drain_status())

    async def resume(self, *, actor: str) -> dict[str, Any]:
        """Ready again."""
        process = self._ctx.needs_process("resume")
        process.resume_receiving()
        return dict(process.drain_status())


class DisabledOpsAdmin:
    """The operations table and a `GET /`-shaped summary saying the library is off."""

    def __init__(self, ctx: DisabledContext) -> None:
        """Over the settings (and the process, when there is one)."""
        self._ctx = ctx

    def list(self) -> list[dict[str, Any]]:
        """Every operation: id, resource, verb, Python call, CLI command, route, guard."""
        return [op_row(op) for op in OPERATIONS]

    async def index(self, routes: Sequence[dict[str, Any]] | None = None) -> dict[str, Any]:
        """`library.enabled: false`; `process` from the queue; every store part None."""
        settings = self._ctx.settings
        process = self._ctx.process
        out: dict[str, Any] = {
            "library": {
                "version": __version__,
                "enabled": False,
                "worker": None,
                "mode": None,
                "execution_mode": "disabled",
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
        if process is not None:
            health = await process.health()
            out["process"] = {k: health.get(k) for k in (
                "live", "ready", "not_ready_reason", "draining", "draining_since",
            )}  # fmt: skip
        return out
