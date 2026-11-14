"""Fixtures for the registry store tests: each test runs on Valkey 8.1 and on Redis 7.4.

Each test gets its own key prefix, deleted at teardown. A store that does not answer fails the
test (as in the engine tests): a silent skip hid that these tests never ran (RESCAN T6).
"""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
import redis.asyncio
from uuid_utils import uuid7

HOST = os.environ.get("LLMQ_TEST_REDIS_HOST", "127.0.0.1")
PASSWORD = os.environ.get("LLMQ_TEST_REDIS_PASSWORD", "localdev")
ENGINES = {"valkey": 6379, "redis": 6380}
FIXTURES = Path(__file__).parents[2] / "unit" / "registry" / "fixtures"


@pytest.fixture(params=sorted(ENGINES))
async def valkey(request: pytest.FixtureRequest) -> AsyncIterator[redis.asyncio.Redis]:
    """Async client on one store engine (named for the first engine; no decode_responses)."""
    client = redis.asyncio.Redis(host=HOST, port=ENGINES[request.param], password=PASSWORD)
    await client.ping()  # raises when the stack is down: fail, never skip
    yield client
    await client.aclose()


@pytest.fixture
async def prefix(valkey: redis.asyncio.Redis) -> AsyncIterator[str]:
    """Unique key prefix; every key under it is deleted at teardown."""
    value = f"llmqtest:reg:{uuid7().hex[-12:]}"
    yield value
    keys = [k async for k in valkey.scan_iter(match=f"{value}:*", count=500)]
    if keys:
        await valkey.delete(*keys)


@pytest.fixture
def full() -> dict[str, Any]:
    """The plan's full example registry."""
    data: dict[str, Any] = json.loads((FIXTURES / "full.json").read_text(encoding="utf-8"))
    return data
