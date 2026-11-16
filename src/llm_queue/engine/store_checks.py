"""Checks of a store's own setup, run once at start (plan C4, RESCAN A14).

Two settings break the queue in ways that show only under load: an eviction policy other than
`noeviction` lets the store drop queue messages, budgets and results when it fills (a lost call,
an overspent budget), and cluster mode spreads one call's keys over shards, where the Lua
scripts fail on their first call. `check_store` warns on the first and refuses the second.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from redis.exceptions import RedisError

from llm_queue._logging import get_logger
from llm_queue.engine._codec import text as _text
from llm_queue.engine.expiry import VOLATILE_POLICIES, decide
from llm_queue.errors import ConfigError

if TYPE_CHECKING:
    import redis.asyncio

logger = get_logger(__name__)

UNKNOWN = "unknown"


async def eviction_policy(client: redis.asyncio.Redis) -> str:
    """The store's `maxmemory-policy`, or `unknown` when neither command can read it.

    `CONFIG GET` is disabled on ElastiCache and some managed stores; `INFO memory` reports the
    policy there as `maxmemory_policy`.
    """
    try:
        reply = await client.config_get("maxmemory-policy")
        for key, value in (reply or {}).items():
            if _text(key) == "maxmemory-policy" and value:
                return _text(value)
    except (RedisError, OSError) as exc:  # a trace, not silence (EN-22)
        logger.debug("store_check_skipped", check="config_get", error=type(exc).__name__)
    try:
        info = await client.info("memory")
        if info.get("maxmemory_policy"):
            return _text(info["maxmemory_policy"])
    except (RedisError, OSError) as exc:
        logger.debug("store_check_skipped", check="info_memory", error=type(exc).__name__)
    return UNKNOWN


def eviction_warning(policy: str, key_expiry: str = "native") -> str | None:
    """A warning for a policy that may evict the library's keys; None when it cannot.

    `noeviction` and `unknown` never warn; a `volatile-*` policy is safe in `swept` key expiry
    (no key the library needs has a TTL, engine/expiry.py); `allkeys-*` never is.
    """
    if policy in ("noeviction", UNKNOWN):
        return None
    if key_expiry == "swept" and policy in VOLATILE_POLICIES:
        return None
    return (
        f"the store's maxmemory-policy is {policy}: when it fills it evicts queue messages, "
        "budgets and results (lost calls, overspent budgets); set noeviction"
    )


async def cluster_enabled(client: redis.asyncio.Redis) -> bool:
    """True when the store runs in cluster mode (`INFO cluster`: `cluster_enabled:1`)."""
    try:
        info = await client.info("cluster")
    except (RedisError, OSError) as exc:  # unreadable: the first script call reports it
        logger.debug("store_check_skipped", check="info_cluster", error=type(exc).__name__)
        return False
    return _text(info.get("cluster_enabled", 0)) == "1"


async def check_store(
    client: redis.asyncio.Redis, name: str = "state", key_expiry: str = "native"
) -> str:
    """Refuse a cluster-mode store (`ConfigError`), warn on an evicting one; return its policy.

    A store that cannot be reached is not an error here (the engine starts on its own config
    when the store is down); its policy is then `unknown`.
    """
    if await cluster_enabled(client):
        raise ConfigError(
            f"the {name} store runs in cluster mode, which llm-queue does not support: one "
            "call's keys span shards and its scripts fail; use a single primary (with replicas)"
        )
    policy = await eviction_policy(client)
    warning = eviction_warning(policy, decide(key_expiry, policy))
    if warning:
        logger.warning("store_eviction_policy", store=name, policy=policy, detail=warning)
    return policy
