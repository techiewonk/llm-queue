"""One audit entry per admin write (0.2.0 R, design 4.4; review B-06, B-16).

Entries go to the registry's audit stream, next to the registry's own edits, so `audit list`
(`llm-queue audit list`, `GET /audit`) shows every change in one place. Dry runs write nothing.
Registry edits and publishes write theirs in their own Lua script; every other write (requests,
purges, dead letters, drains, model drains) goes through `Audit.write`, which checks the actor
(`check_actor`) and logs one `llmq_admin` line.
"""

from __future__ import annotations

import time
from typing import Any

import orjson
import redis.asyncio

from llm_queue._logging import get_logger
from llm_queue.registry._lua import AUDIT_MAX_ENTRIES
from llm_queue.registry.store import check_actor

logger = get_logger(__name__)


class Audit:
    """Writes `{actor, op, version, diff, ts}` entries to one audit stream."""

    def __init__(self, client: redis.asyncio.Redis, key: str) -> None:
        """`key` is the registry's audit stream (`RegistryStore.audit_key`)."""
        self._client = client
        self.key = key

    async def write(
        self, *, kind: str, actor: str, op: str, path: str, before: Any, after: Any
    ) -> None:
        """Add one entry: `op` (e.g. `dlq_redrive`), one diff row `{path, before, after}`.

        `kind` is the service that wrote it (`requests`, `purge`, `dlq`, `drain`, `models`),
        logged with it. `after` is a report's counts, never ids beyond a sample and never a
        tenant name. `ConfigError` for a bad actor (writers check it before they act, too).
        """
        check_actor(actor)
        diff = [{"path": path, "before": before, "after": after}]
        await self._client.xadd(
            self.key,
            {
                "actor": actor,
                "op": op,
                "version": "",
                "diff": orjson.dumps(diff),
                "ts": f"{time.time():.6f}",  # wall time, like the Lua audit entries
            },
            maxlen=AUDIT_MAX_ENTRIES,
            approximate=True,
        )
        logger.info("llmq_admin", kind=kind, actor=actor, op=op, path=path, after=after)
