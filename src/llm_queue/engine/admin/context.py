"""What every admin service reads and writes: the stores, the registry, the audit stream.

Built from a running engine (`llmq.admin`) or from `LLMQ_*` alone (`AdminServices.from_env`,
the CLI and admin workers); the services never need to know which.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import orjson

from llm_queue.engine.audit import Audit
from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import StreamQueues
from llm_queue.engine.status import StatusReader
from llm_queue.errors import AdminRefused, ConfigError, StoreUnavailable

if TYPE_CHECKING:
    import redis.asyncio

    from llm_queue.engine.purge import PurgeService
    from llm_queue.engine.requests_admin import RequestsAdmin
    from llm_queue.engine.runtime import Engine
    from llm_queue.engine.settings import InfraSettings, Layers
    from llm_queue.registry.resolve import ResolvedRegistry
    from llm_queue.registry.store import RegistryStore


class NotLoaded(StoreUnavailable):
    """The registry is not loaded yet (before `start()`), or none is published."""


@dataclass(slots=True)
class AdminContext:
    """The parts of one environment the admin services share.

    `engine` and `process` (the `LLMQueue`) are None outside a running queue: process-local
    operations (drain, health) then refuse with `AdminRefused`. `requests` is the request index
    service (`Engine.requests_admin`, or one over these stores); None: built on first use.
    `owned`: the store clients `from_layers` opened, for the caller's `aclose()`.
    """

    state: redis.asyncio.Redis
    queue_client: redis.asyncio.Redis | None
    keys: Keys
    registry: RegistryStore
    queues: StreamQueues
    status: StatusReader
    purge: PurgeService
    audit: Audit
    hash_id: Callable[[str], str]
    queue_prefix: str
    status_ttl_secs: int
    settings: InfraSettings | None = None
    engine: Engine | None = None
    process: Any = None  # the LLMQueue: pause_receiving, resume_receiving, drain_status
    requests: RequestsAdmin | None = None
    owned: list[Any] = field(default_factory=list)

    @classmethod
    def from_layers(cls, layers: Layers) -> AdminContext:
        """The stores, registry and services of one environment from `LLMQ_*` (no engine).

        The one wiring `AdminServices.from_env`, `RequestsAdmin.from_env` and
        `PurgeService.from_env` share (review B-23: three copies drifted, and none took
        `defaults=` or `secrets_names=`). Store passwords are read as `LLMQueue.from_env`
        reads them; the clients it opens are in `owned`.
        """
        from llm_queue.engine.purge import PurgeService, PurgeSettings
        from llm_queue.engine.requests_admin import AdminSettings, RequestsAdmin
        from llm_queue.engine.settings import InfraSettings
        from llm_queue.engine.store_client import secret_store_client
        from llm_queue.ids import keyed_hash
        from llm_queue.registry.store import RegistryStore
        from llm_queue.tuning import Tuning

        settings = InfraSettings.from_layers(layers)
        cap, options = settings.store_max_connections, Tuning().store
        state = secret_store_client(
            settings.store_url, layers.get, "LLMQ_STORE_PASSWORD", cap, options
        )
        queue_client = None
        if settings.queue_store_url:
            queue_client = secret_store_client(
                settings.queue_store_url, layers.get, "LLMQ_QUEUE_STORE_PASSWORD", cap, options
            )
        config = None
        if settings.registry or settings.registry_file:
            config = orjson.loads(settings.registry_document())
        registry = RegistryStore(
            state,
            settings.key_prefix,
            config=config,
            live_edit=settings.registry_live_edit_mode,
            allowed_hosts=settings.http_allowed_hosts,
            audit_retention_days=settings.audit_retention_days,
        )
        keys = Keys(settings.key_prefix)
        queues = StreamQueues(queue_client or state, keys)
        salt = settings.id_hash_salt or ""

        def hash_id(value: str) -> str:
            return keyed_hash(salt, value)

        purge = PurgeService(
            state,
            registry,
            queue_client=queue_client,
            queue_prefix=settings.queue_prefix,
            settings=PurgeSettings.from_infra(settings),
            hash_id=hash_id,
            status_ttl_secs=settings.status_ttl_secs,
        )
        admin_settings = AdminSettings.from_infra(settings)
        requests = RequestsAdmin(
            state,
            queue_client=queue_client,
            keys=keys,
            registry=registry,
            queue_prefix=settings.queue_prefix,
            hash_id=hash_id,
            settings=admin_settings,
            status_ttl_secs=settings.status_ttl_secs,
        )
        status = StatusReader(
            state,
            keys,
            queues,
            settings.queue_prefix,
            requests=requests,
            request_index=admin_settings.request_index,
            scan_max=admin_settings.scan_max,
        )
        return cls(
            state=state,
            queue_client=queue_client,
            keys=keys,
            registry=registry,
            queues=queues,
            status=status,
            purge=purge,
            audit=Audit(state, registry.audit_key),
            hash_id=hash_id,
            queue_prefix=settings.queue_prefix,
            status_ttl_secs=settings.status_ttl_secs,
            settings=settings,
            requests=requests,
            owned=[c for c in (queue_client, state) if c is not None],
        )

    async def view(self) -> ResolvedRegistry:
        """The registry to read: the engine's loaded one, else the stored one, read now."""
        if self.engine is not None:
            try:
                return self.registry.current
            except ConfigError:
                raise NotLoaded("registry not loaded") from None
        view = await self.registry.load()
        if view is None:
            raise NotLoaded("no registry is published yet (llm-queue registry publish --yes)")
        return view

    def queue_name(self, view: ResolvedRegistry, model: str, priority: bool) -> str:
        """The queue of a model's class: `<queue_prefix><name>[-priority]`."""
        alias = view.registry.alias_of(model)
        name = self.queue_prefix + view.queue_name(alias)
        return f"{name}-priority" if priority else name

    def job_queue(self, job: str) -> str:
        """The queue of one job name."""
        return f"{self.queue_prefix}jobs-{job}"

    def needs_process(self, what: str) -> Any:
        """The `LLMQueue` this context runs in; `AdminRefused` outside one."""
        if self.process is None:
            raise AdminRefused(f"{what} is process-local: call it on the LLMQueue it acts on")
        return self.process

    def config_document(self) -> dict[str, Any] | None:
        """This process's own registry document (`LLMQ_REGISTRY[_FILE]`), None when unset."""
        if self.registry.config is not None:
            return dict(self.registry.config)
        s = self.settings
        if s is None or not (s.registry or s.registry_file):
            return None
        return dict(orjson.loads(s.registry_document()))
