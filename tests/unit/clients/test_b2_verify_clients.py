"""Gaps an independent check found in the clients audit fixes (CL-6, CL-8, N1, N4), per id.

Small aiohttp test servers, fakellm in process and fake leases; no network, no real model.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from clients_testkit import StartFake, registry_doc

from llm_queue.clients import (
    AttemptTimeout,
    ClientHub,
    ClientPool,
    CredentialsUnavailable,
    Providers,
    Route,
    Timing,
    WarmOptions,
    route_for,
)
from llm_queue.clients import pool as pool_module
from llm_queue.registry import ResolvedRegistry, load_registry
from llm_queue.tuning import BedrockOptions

CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
NO_WARM = WarmOptions(connections=0)


def _keys(account: Any) -> dict[str, Any]:
    return {"aws_access_key_id": account.name, "aws_secret_access_key": "x"}


def _aws_route(view: ResolvedRegistry) -> Route:
    return route_for(view, next(d for d in view.deployments("gpt120b") if d.account == "aws-a"))


def _openai_route(view: ResolvedRegistry) -> Route:
    return route_for(view, next(iter(view.deployments("local"))))


@pytest.fixture
async def view(start_fake: StartFake) -> ResolvedRegistry:
    """The test registry on an in-process fakellm."""
    url, _ = await start_fake()
    return load_registry(registry_doc(url))


async def test_a_secret_warned_at_a_call_is_not_listed_again(view: ResolvedRegistry) -> None:
    """CL-6 / N2: `problems()` compared `(account, ref)` with `(event, account)` keys.

    The check never matched, so a secret already warned when a call failed on it was listed
    again at warm. It is skipped now; a secret not warned yet is still listed.
    """
    route = _openai_route(view)
    pool = ClientPool(secrets=lambda name: None, warm=NO_WARM)
    try:
        assert pool.problems([route]) == ["account vllm: secret LLMQ_ACCOUNT_VLLM_KEY is not set"]
        with pytest.raises(CredentialsUnavailable):
            await pool.openai(route)
        assert pool.problems([route]) == []
    finally:
        await pool.aclose()


async def test_the_api_key_lookup_does_not_block_the_loop(view: ResolvedRegistry) -> None:
    """CL-8: `ClientPool.openai()` read `api_key_ref` on the event loop; a slow lookup stalled it.

    The lookup runs in a thread now; two concurrent first calls share one client.
    """

    def secrets(name: str) -> str:
        time.sleep(0.3)
        return "key"

    route = _openai_route(view)
    pool = ClientPool(secrets=secrets, warm=NO_WARM)
    gaps: list[float] = []

    async def ticker() -> None:
        last = time.monotonic()
        while True:
            await asyncio.sleep(0.01)
            now = time.monotonic()
            gaps.append(now - last)
            last = now

    task = asyncio.create_task(ticker())
    try:
        first, second = await asyncio.gather(pool.openai(route), pool.openai(route))
    finally:
        task.cancel()
        await pool.aclose()
    assert first is second
    assert max(gaps) < 0.25  # the loop kept running while the lookup slept


@pytest.fixture
async def slow_server() -> AsyncIterator[str]:
    """A Bedrock-shaped server that never answers in time (2 s per request)."""

    async def slow(request: web.Request) -> web.Response:
        await asyncio.sleep(2)
        return web.json_response({})

    app = web.Application()
    app.router.add_post("/model/{model_id:.+}/{action}", slow)
    server = TestServer(app, host="127.0.0.1")
    await server.start_server()
    yield str(server.make_url("")).rstrip("/")
    await server.close()


@pytest.mark.parametrize("transport", ["aiohttp", "botocore"])
async def test_a_slow_provider_is_still_an_attempt_timeout(
    slow_server: str, transport: str
) -> None:
    """CL-8: only a cut while waiting for credentials becomes `CredentialsUnavailable`.

    A provider that does not answer in time keeps `AttemptTimeout` (its breaker counts it).
    """
    view = load_registry(registry_doc(slow_server))
    pool = ClientPool(
        bedrock_endpoint_url=slow_server,
        credentials=_keys,
        warm=NO_WARM,
        options=BedrockOptions(transport=transport),
    )
    providers = Providers(pool)
    try:
        with pytest.raises(AttemptTimeout):
            await providers.get("bedrock").call(
                "converse", CHAT, _aws_route(view), Timing(0.3, 0.3, attempt_secs=0.3)
            )
    finally:
        await providers.aclose()


async def test_aclose_returns_once_a_retired_client_is_closed() -> None:
    """N1: aclose cancelled a retire task inside its shielded release and returned at once.

    The release went on detached, so the client was still open when aclose returned (and the
    loop could end under it). aclose now awaits that release too.
    """
    hub, closed = ClientHub(), list[str]()

    async def build() -> tuple[Any, Any]:
        async def close() -> None:
            await asyncio.sleep(0.3)
            closed.append("b")

        return object(), close

    slow = await hub.acquire(("b",), build)
    pool = ClientPool(hub=hub, warm=NO_WARM)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(pool_module, "RETIRE_GRACE_SECS", 0.0)
        pool._retire([slow])
        for _ in range(200):  # until the task is inside the slow close
            if slow.refs == 0:
                break
            await asyncio.sleep(0.01)
        assert slow.refs == 0
        await pool.aclose()
    assert closed == ["b"]  # was []: the close was still running


async def test_the_botocore_fallback_warning_names_the_redirect_risk(
    start_fake: StartFake, caplog: pytest.LogCaptureFixture
) -> None:
    """N4: an account moved to botocore by the CL-6 fallback silently lost the CL-4 guard.

    botocore follows redirects (re-sending the signed request and session token); the
    fallback warning now says so.
    """
    url, _ = await start_fake()
    view = load_registry(registry_doc(url))
    pool = ClientPool(
        bedrock_endpoint_url=url,
        credentials=lambda a: {**_keys(a), "verify": False},
        warm=NO_WARM,
    )
    try:
        await pool.bedrock(_aws_route(view))
    finally:
        await pool.aclose()
    warned = [
        r.getMessage() for r in caplog.records if "bedrock_transport_fallback" in r.getMessage()
    ]
    assert len(warned) == 1
    assert "follows HTTP redirects" in warned[0]
