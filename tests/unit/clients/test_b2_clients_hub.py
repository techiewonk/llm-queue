"""Client hub and pool (0.1.0b2 plan E1-E5, C6, X7): shared clients, warm connections, refresh.

Before the hub every pool built its own client per (account, region); forget() dropped every
region of an account; an account edit kept the old client; `credentials=` with an expiry failed.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from clients_testkit import StartFake, registry_doc

from llm_queue.clients import (
    ClientHub,
    ClientPool,
    Providers,
    Route,
    Timing,
    WarmOptions,
    pool_size_for,
    route_for,
)
from llm_queue.clients.pool import REFRESH_BEFORE_SECS
from llm_queue.registry import ResolvedRegistry, load_registry

CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
FAST = Timing(first_response_secs=5, idle_secs=5)
NO_WARM = WarmOptions(connections=0)


def _keys(account: Any) -> dict[str, Any]:
    return {"aws_access_key_id": account.name, "aws_secret_access_key": "x"}


def _route(view: ResolvedRegistry, model: str, key: str) -> Route:
    return route_for(view, next(d for d in view.deployments(model) if d.key == key))


@pytest.fixture
async def world(start_fake: StartFake) -> AsyncIterator[Any]:
    pools: list[ClientPool] = []
    url, fake = await start_fake()
    view = load_registry(registry_doc(url))
    hub = ClientHub()

    def pool(**kw: Any) -> ClientPool:
        kw.setdefault("warm", NO_WARM)
        kw.setdefault("credentials", _keys)
        built = ClientPool(bedrock_endpoint_url=url, hub=hub, **kw)
        pools.append(built)
        return built

    yield view, hub, pool, fake
    for built in pools:
        await built.aclose()


async def test_two_pools_share_one_client_until_the_last_release(world: Any) -> None:
    view, hub, pool, _ = world
    route = _route(view, "gpt120b", "aws-a/ap-south-1")
    first, second = pool(), pool()
    client = await first.bedrock(route)
    assert await second.bedrock(route) is client  # one client, one connection pool
    assert len(hub) == 1
    await first.aclose()
    result = await Providers(second).get("bedrock").call("converse", CHAT, route, FAST)
    assert result.usage.output_tokens  # still open for the other holder
    await second.aclose()
    assert len(hub) == 0  # the last release closed it


async def test_warm_builds_clients_concurrently_with_per_key_locks(world: Any) -> None:
    view, hub, pool, _ = world
    built = pool()
    routes = [route_for(view, d) for d in view.deployments("gpt120b")]
    started = time.perf_counter()
    await built.warm(routes, slots={("aws-a", "ap-south-1"): 3})
    assert len(hub) == 2  # (aws-a, ap-south-1) and (aws-b, ap-south-1): one per account x region
    assert time.perf_counter() - started < 5
    client = await built.bedrock(routes[0])
    assert client._endpoint.http_session._max_pool_connections == pool_size_for(3) == 10


def test_pool_size_has_a_floor_and_a_cap() -> None:
    assert (pool_size_for(1), pool_size_for(64), pool_size_for(5000)) == (10, 64, 200)


async def test_forget_drops_only_that_region(world: Any) -> None:
    view, _, pool, _ = world
    doc_route = _route(view, "gpt120b", "aws-a/ap-south-1")
    other = replace(doc_route, region="us-east-1")
    built = pool()
    kept = await built.bedrock(other)
    dropped = await built.bedrock(doc_route)
    built.forget(doc_route)
    assert await built.bedrock(other) is kept
    assert await built.bedrock(doc_route) is not dropped


async def test_an_account_edit_gets_a_new_client(world: Any) -> None:
    view, _, pool, _ = world
    route = _route(view, "gpt120b", "aws-a/ap-south-1")
    built = pool()
    old = await built.bedrock(route)
    edited = replace(route, account=replace(route.account, role_arn=None, external_id_ref="X"))
    new = await built.bedrock(edited)
    assert new is not old  # was: the client of the old account fields, found by name


async def test_warm_opens_connections_without_model_calls(world: Any) -> None:
    view, _, pool, fake = world
    built = pool(warm=WarmOptions(connections=3, refresh_secs=0))
    route = _route(view, "gpt120b", "aws-a/ap-south-1")
    client = await built.bedrock(route)
    session = None
    for _ in range(100):
        session = next(iter(client._endpoint.http_session._sessions.values()), None)
        if session is not None and sum(len(c) for c in session.connector._conns.values()) >= 3:
            break
        await asyncio.sleep(0.05)
    assert session is not None
    assert sum(len(c) for c in session.connector._conns.values()) >= 3  # idle, ready
    assert not fake.snapshot()["models"]  # no model was called


async def test_credentials_with_an_expiry_are_refreshed(world: Any) -> None:
    view, _, pool, _ = world
    asked: list[float] = []

    def credentials(account: Any) -> dict[str, Any]:
        asked.append(time.time())
        soon = datetime.now(UTC) + timedelta(seconds=REFRESH_BEFORE_SECS - 60)
        return {**_keys(account), "aws_session_token": "t", "expiry_time": soon}

    built = pool(credentials=credentials)
    route = _route(view, "gpt120b", "aws-a/ap-south-1")
    result = await Providers(built).get("bedrock").call("converse", CHAT, route, FAST)
    assert result.usage.output_tokens
    # Was: TypeError (expiry_time passed to create_client). Inside the refresh window the
    # callable is asked again before signing.
    assert len(asked) >= 2
