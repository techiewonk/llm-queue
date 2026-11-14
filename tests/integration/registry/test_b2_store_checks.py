"""Store setup checks at start (plan C4, RESCAN A14) on real Valkey and a fake ElastiCache.

0.1.0b1 never looked: an `allkeys-lru` store silently dropped queue messages when full, and a
cluster-mode store failed only at the first call.
"""

from __future__ import annotations

from typing import Any

import pytest
import redis.asyncio
from redis.exceptions import ResponseError

from llm_queue.engine.store_checks import (
    check_store,
    cluster_enabled,
    eviction_policy,
    eviction_warning,
)
from llm_queue.errors import ConfigError

pytestmark = pytest.mark.integration


async def test_policy_read_from_the_stack(valkey: redis.asyncio.Redis) -> None:
    policy = await eviction_policy(valkey)
    assert policy != "unknown"
    assert await cluster_enabled(valkey) is False
    assert await check_store(valkey) == policy


async def test_config_get_disabled_falls_back_to_info(
    valkey: redis.asyncio.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ElastiCache refuses CONFIG GET; INFO memory still names the policy."""

    async def refused(*args: Any, **kwargs: Any) -> Any:
        raise ResponseError("unknown command 'CONFIG'")

    async def info(section: str) -> dict[str, Any]:
        return {"maxmemory_policy": "allkeys-lru"} if section == "memory" else {}

    monkeypatch.setattr(valkey, "config_get", refused)
    monkeypatch.setattr(valkey, "info", info)
    assert await eviction_policy(valkey) == "allkeys-lru"
    assert "noeviction" in (eviction_warning("allkeys-lru") or "")
    assert eviction_warning("noeviction") is None
    assert eviction_warning("unknown") is None

    async def nothing(section: str) -> dict[str, Any]:
        raise ResponseError("INFO is disabled")

    monkeypatch.setattr(valkey, "info", nothing)
    assert await eviction_policy(valkey) == "unknown"  # not an error


async def test_cluster_mode_is_refused(
    valkey: redis.asyncio.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def info(section: str) -> dict[str, Any]:
        return {"cluster_enabled": 1}

    monkeypatch.setattr(valkey, "info", info)
    with pytest.raises(ConfigError, match="cluster mode"):
        await check_store(valkey, "queue")
