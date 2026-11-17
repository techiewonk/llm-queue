"""`llmq.admin`: every admin operation as one service method (0.2.0 R, owner D4).

`AdminServices` groups the services by resource (`registry`, `models`, `deployments`,
`blocks`, `requests`, `tenants`, `queues`, `dlq`, `jobs`, `purge`, `status`, `audit`, `drain`,
`ops`) plus `describe()`. The CLI and the HTTP routes are generated from the operations table
(`engine/admin/ops.py`) and call these same methods. Every write takes `actor` and is a dry run
unless `dry_run=False` (D3).

From a running queue: `llmq.admin`. Without one (a script, an admin worker):
`async with AdminServices.from_env() as admin:`; process-local operations (`drain.*`) then
refuse with `AdminRefused`. In local mode (`LLMQ_EXECUTION_MODE=local`, `for_local`) the
store-only operations raise `LocalModeRefused` (see `engine/admin/local.py`); with the library
switched off (`LLMQ_ENABLED=false`, `for_disabled`) they raise `QueueDisabled`.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Self

import orjson

from llm_queue.engine.admin.context import AdminContext, NotLoaded
from llm_queue.engine.admin.ops import BY_ID, OPERATIONS, Op, Param
from llm_queue.engine.admin.process import DrainAdmin, OpsAdmin, describe_settings
from llm_queue.engine.admin.queues import (
    DlqAdmin,
    JobsAdmin,
    PurgeAdmin,
    QueuesAdmin,
    StatusAdmin,
    TenantsAdmin,
)
from llm_queue.engine.admin.registry import (
    AuditAdmin,
    BlocksAdmin,
    DeploymentsAdmin,
    ModelsAdmin,
    RegistryAdmin,
)
from llm_queue.engine.admin.requests import RequestsAdapter
from llm_queue.engine.audit import Audit
from llm_queue.engine.settings import InfraSettings, Layers
from llm_queue.ids import keyed_hash

if TYPE_CHECKING:
    from llm_queue.engine.local import LocalEngine
    from llm_queue.engine.runtime import Engine

__all__ = ["BY_ID", "OPERATIONS", "AdminContext", "AdminServices", "NotLoaded", "Op", "Param"]


class AdminServices:
    """Every admin operation of one environment, by resource (see the module doc)."""

    registry: RegistryAdmin
    audit: AuditAdmin
    models: ModelsAdmin
    deployments: DeploymentsAdmin
    blocks: BlocksAdmin
    requests: RequestsAdapter
    tenants: TenantsAdmin
    queues: QueuesAdmin
    dlq: DlqAdmin
    jobs: JobsAdmin
    purge: PurgeAdmin
    status: StatusAdmin
    drain: DrainAdmin
    ops: OpsAdmin

    def __init__(self, ctx: AdminContext) -> None:
        """Build every service over `ctx`."""
        self.context = ctx
        self.registry = RegistryAdmin(ctx)
        self.audit = AuditAdmin(ctx)
        self.models = ModelsAdmin(ctx)
        self.deployments = DeploymentsAdmin(ctx)
        self.blocks = BlocksAdmin(ctx)
        self.requests = RequestsAdapter(ctx)
        self.tenants = TenantsAdmin(ctx, self.requests)
        self.queues = QueuesAdmin(ctx)
        self.dlq = DlqAdmin(ctx)
        self.jobs = JobsAdmin(ctx)
        self.purge = PurgeAdmin(ctx, self.requests)
        self.status = StatusAdmin(ctx, self.requests)
        self.drain = DrainAdmin(ctx)
        self.ops = OpsAdmin(ctx)
        self._owned: list[Any] = []

    def describe(self) -> dict[str, Any]:
        """This process's settings, safe to log (no password, salt, token or registry body)."""
        engine = self.context.engine
        worker = engine.worker_id if engine is not None else None
        return describe_settings(self.context.settings, worker)

    # ---------------------------------------------------------------- construction

    @classmethod
    def for_engine(
        cls, engine: Engine, *, settings: InfraSettings | None = None, process: Any = None
    ) -> AdminServices:
        """The services of a running engine (`llmq.admin`); `process` is its `LLMQueue`."""
        registry = engine.registry
        ctx = AdminContext(
            state=engine.state,
            queue_client=engine._queue_store,
            keys=engine.keys,
            registry=registry,
            queues=engine.queues,
            status=engine.status_reader,
            purge=engine.purge,
            audit=Audit(engine.state, registry.audit_key),
            hash_id=engine.hash_id,
            queue_prefix=engine.settings.queue_prefix,
            status_ttl_secs=engine.settings.status_ttl_secs,
            settings=settings,
            engine=engine,
            process=process,
            requests=engine.requests_admin,
        )
        return cls(ctx)

    @classmethod
    def for_local(
        cls, engine: LocalEngine | None, *, settings: InfraSettings | None, process: Any = None
    ) -> AdminServices:
        """The services of a local-mode queue: store-only operations raise `LocalModeRefused`.

        `engine` None (the standalone CLI): only what the settings and the document answer.
        """
        from llm_queue.engine.admin import local

        if engine is not None:
            registry, hash_id = engine.registry, engine.hash_id
        else:
            assert settings is not None
            from llm_queue.engine.local import LocalRegistry

            registry = LocalRegistry(
                orjson.loads(settings.registry_document()),
                prefix=settings.key_prefix,
                allowed_hosts=settings.base_url_allowed_hosts,
            )
            salt = settings.id_hash_salt or ""

            def hash_id(value: str) -> str:
                return keyed_hash(salt, value)

        ctx = local.LocalContext(registry, settings, hash_id, engine=engine, process=process)
        admin = cls.__new__(cls)
        parts: dict[str, Any] = {name: local.StoreOnly(name) for name in local.STORE_ONLY}
        parts.update(
            context=ctx,
            registry=local.LocalRegistryAdmin(ctx),
            tenants=local.LocalTenantsAdmin(ctx),
            status=local.LocalStatusAdmin(ctx),
            drain=local.LocalDrainAdmin(ctx),
            ops=local.LocalOpsAdmin(ctx),
            _owned=[],
        )
        vars(admin).update(parts)
        return admin

    @classmethod
    def for_disabled(cls, *, settings: InfraSettings | None, process: Any = None) -> AdminServices:
        """The services with `LLMQ_ENABLED=false`: store operations raise `QueueDisabled`.

        `describe`, `ops`, `registry.schema` and (inside a process) `drain.*` are served.
        """
        from llm_queue.engine.admin import disabled

        ctx = disabled.DisabledContext(settings, process)
        admin = cls.__new__(cls)
        parts: dict[str, Any] = {name: disabled.Refused(name) for name in disabled.REFUSED}
        parts.update(
            context=ctx,
            registry=disabled.DisabledRegistryAdmin(),
            drain=disabled.DisabledDrainAdmin(ctx),
            ops=disabled.DisabledOpsAdmin(ctx),
            _owned=[],
        )
        vars(admin).update(parts)
        return admin

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        secrets: Callable[[str], str | None] | None = None,
        defaults: Mapping[str, Any] | None = None,
        secret_names: Mapping[str, str] | None = None,
    ) -> AdminServices:
        """Standalone services from `LLMQ_*` (CLI, admin workers); close with `aclose()`.

        Read as `LLMQueue.from_env` reads its settings (same arguments): `secrets` when given,
        else the sources `LLMQ_SECRETS_SOURCE` names, then `environ`, then `defaults`.
        """
        layers = Layers(environ, secrets, defaults, secret_names)
        settings = InfraSettings.from_layers(layers)
        if not settings.enabled:  # LLMQ_ENABLED=false: nothing to talk to
            return cls.for_disabled(settings=settings)
        if settings.execution_mode == "local":  # no store to talk to
            return cls.for_local(None, settings=settings)
        ctx = AdminContext.from_layers(layers)
        admin = cls(ctx)
        admin._owned = ctx.owned
        return admin

    async def aclose(self) -> None:
        """Close the clients `from_env` made (none otherwise)."""
        async with contextlib.AsyncExitStack() as stack:
            for client in self._owned:
                stack.push_async_callback(client.aclose)
        self._owned = []

    async def __aenter__(self) -> Self:
        """Use as `async with AdminServices.from_env() as admin:`."""
        return self

    async def __aexit__(self, *_: object) -> None:
        """Close what `from_env` opened."""
        await self.aclose()

    def method(self, op: Op) -> Callable[..., Any]:
        """The bound method of an operation (`op.method`, dotted)."""
        if op.method is None:
            raise LookupError(f"{op.id} has no admin method")
        target: Any = self
        for part in op.method.split("."):
            target = getattr(target, part)
        return target  # type: ignore[no-any-return]
