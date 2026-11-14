"""Fixtures for engine tests: each test runs on Valkey 8.1 and on Redis 7.4 (plan E8).

Every test gets its own key prefix, deleted at teardown. The stack comes from
`scripts/stack.py up`; a store that does not answer fails the test (no silent skip).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any

import pytest
import redis.asyncio
from aiohttp.test_utils import TestServer
from engine_testkit import FakeEngine, Make, Scenario, create_app, registry
from uuid_utils import uuid7

from llm_queue.clients import ClientPool, Providers
from llm_queue.engine.runtime import Engine, EngineSettings
from llm_queue.engine.store_client import store_client
from llm_queue.registry.store import RegistryStore

PASSWORD = os.environ.get("LLMQ_TEST_REDIS_PASSWORD", "localdev")
ENGINES = {"valkey": 6379, "redis": 6380}


@pytest.fixture(params=sorted(ENGINES))
async def store(request: pytest.FixtureRequest) -> AsyncIterator[redis.asyncio.Redis]:
    """The library's own client (blocking pool, bytes replies) on one engine.

    redis-py's default pool raises at 100 connections; apps get `store_client`, so tests do too.
    """
    client = store_client(f"redis://:{PASSWORD}@127.0.0.1:{ENGINES[request.param]}/0")
    await client.ping()
    yield client
    await client.aclose()


@pytest.fixture
async def prefix(store: redis.asyncio.Redis) -> AsyncIterator[str]:
    """Unique key prefix; every key under it is deleted at teardown."""
    value = f"llmqtest:eng:{uuid7().hex[-12:]}"
    yield value
    keys = [k async for k in store.scan_iter(match=f"{value}:*", count=500)]
    if keys:
        await store.delete(*keys)


@pytest.fixture
async def make(store: redis.asyncio.Redis, prefix: str) -> AsyncIterator[Make]:
    servers: list[TestServer] = []
    engines: list[Engine] = []

    async def build(
        models: dict[str, Any] | None = None,
        scenario: dict[str, Any] | None = None,
        *,
        worker: bool = True,
    ) -> tuple[Engine, FakeEngine]:
        fake = FakeEngine(Scenario.from_mapping(scenario or {}))
        server = TestServer(create_app(engine=fake), host="127.0.0.1")
        await server.start_server()
        servers.append(server)
        url = str(server.make_url("")).rstrip("/")
        pool = ClientPool(
            bedrock_endpoint_url=url,
            credentials=lambda account: {
                "aws_access_key_id": account.name,
                "aws_secret_access_key": "x",
            },
        )
        reg = RegistryStore(store, prefix, config=registry(models))
        engine = Engine(
            store,
            reg,
            Providers(pool),
            settings=EngineSettings(lease_secs=3, poll_secs=0.2, sweep_secs=1),
        )
        await engine.start(worker=worker)
        engines.append(engine)
        return engine, fake

    yield build
    for engine in engines:
        await engine.stop(drain_secs=1)
        await engine.providers.aclose()
    for server in servers:
        await server.close()
