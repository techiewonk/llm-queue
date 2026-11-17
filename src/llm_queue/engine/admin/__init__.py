"""`llmq.admin`: every admin operation as one service method (0.2.0 R, owner D4).

`AdminServices` groups the services by resource (`registry`, `models`, `deployments`,
`blocks`, `requests`, `tenants`, `queues`, `dlq`, `jobs`, `purge`, `status`, `audit`, `drain`,
`ops`) plus `describe()`. The CLI and the HTTP routes are generated from the operations table
(`engine/admin/ops.py`) and call these same methods. Every write takes `actor` and is a dry run
unless `dry_run=False` (D3).

From a running queue: `llmq.admin`. Without one (a script, an admin worker):
`async with AdminServices.from_env() as admin:`; process-local operations (`drain.*`) then
refuse with `AdminRefused`.
"""

from __future__ import annotations

import contextlib
import os
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
from llm_queue.engine.keys import Keys
from llm_queue.engine.purge import PurgeService, PurgeSettings, keyed_hash
from llm_queue.engine.queue import StreamQueues
from llm_queue.engine.settings import InfraSettings
from llm_queue.engine.status import StatusReader

if TYPE_CHECKING:
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
        self.status = StatusAdmin(ctx)
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
        )
        return cls(ctx)

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        secrets: Callable[[str], str | None] | None = None,
    ) -> AdminServices:
        """Standalone services from `LLMQ_*` (CLI, admin workers); close with `aclose()`."""
        from llm_queue.engine.store_client import secret_store_client
        from llm_queue.registry.store import RegistryStore
        from llm_queue.tuning import Tuning

        env = dict(os.environ if environ is None else environ)
        settings = InfraSettings.from_env(env, secrets=secrets)
        cap, options = settings.store_max_connections, Tuning().store
        lookup = secrets or env.get
        state = secret_store_client(settings.store_url, lookup, "LLMQ_STORE_PASSWORD", cap, options)
        queue_client = (
            secret_store_client(
                settings.queue_store_url, lookup, "LLMQ_QUEUE_STORE_PASSWORD", cap, options
            )
            if settings.queue_store_url
            else None
        )
        config = None
        if settings.registry or settings.registry_file:
            config = orjson.loads(settings.registry_document())
        registry = RegistryStore(
            state,
            settings.key_prefix,
            config=config,
            live_edit=settings.registry_live_edit,
            allowed_hosts=settings.base_url_allowed_hosts,
            audit_retention_days=settings.audit_retention_days,
        )
        keys = Keys(settings.key_prefix)
        queues = StreamQueues(queue_client or state, keys)
        salt = settings.id_hash_salt or ""
        purge = PurgeService(
            state,
            registry,
            queue_client=queue_client,
            queue_prefix=settings.queue_prefix,
            settings=PurgeSettings.from_infra(settings),
            salt=salt,
            status_ttl_secs=settings.status_ttl_secs,
        )
        ctx = AdminContext(
            state=state,
            queue_client=queue_client,
            keys=keys,
            registry=registry,
            queues=queues,
            status=StatusReader(state, keys, queues, settings.queue_prefix),
            purge=purge,
            audit=Audit(state, registry.audit_key),
            hash_id=lambda value: keyed_hash(salt, value),
            queue_prefix=settings.queue_prefix,
            status_ttl_secs=settings.status_ttl_secs,
            settings=settings,
        )
        admin = cls(ctx)
        admin._owned = [c for c in (queue_client, state) if c is not None]
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
