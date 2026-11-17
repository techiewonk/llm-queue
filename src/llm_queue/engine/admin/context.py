"""What every admin service reads and writes: the stores, the registry, the audit stream.

Built from a running engine (`llmq.admin`) or from `LLMQ_*` alone (`AdminServices.from_env`,
the CLI and admin workers); the services never need to know which.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
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
    from llm_queue.engine.runtime import Engine
    from llm_queue.engine.settings import InfraSettings
    from llm_queue.registry.resolve import ResolvedRegistry
    from llm_queue.registry.store import RegistryStore


class NotLoaded(StoreUnavailable):
    """The registry is not loaded yet (before `start()`), or none is published."""


@dataclass(slots=True)
class AdminContext:
    """The parts of one environment the admin services share.

    `engine` and `process` (the `LLMQueue`) are None outside a running queue: process-local
    operations (drain, health) then refuse with `AdminRefused`.
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
