"""Fixes from the clients audit (docs/internal/audit/audit-clients.md, CL-*), pinned per bug.

Each test names its finding: redirects, credentials, client retirement, quick retries, stream
release and the parity of the two Bedrock transports. Runs fakellm or small aiohttp test
servers in process; no network, no real model.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from botocore.exceptions import NoCredentialsError
from clients_testkit import TRANSPORTS, StartFake, registry_doc
from fakellm import eventstream

from llm_queue.clients import (
    AttemptTimeout,
    ClientPool,
    CredentialsUnavailable,
    Providers,
    Route,
    Timing,
    WarmOptions,
    route_for,
)
from llm_queue.errors import ErrorKind
from llm_queue.providers import classify_bedrock, is_auth_error
from llm_queue.registry import ResolvedRegistry, load_registry
from llm_queue.tuning import BedrockOptions

CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
FAST = Timing(first_response_secs=5, idle_secs=5)
PLAIN = Timing(5, 5, attempt_secs=5)
NO_WARM = WarmOptions(connections=0)


def _keys(account: Any) -> dict[str, Any]:
    return {"aws_access_key_id": account.name, "aws_secret_access_key": "x"}


def _route(view: ResolvedRegistry, model: str = "gpt120b") -> Route:
    return route_for(view, next(d for d in view.deployments(model) if d.account == "aws-a"))


async def _server(app: web.Application) -> TestServer:
    server = TestServer(app, host="127.0.0.1")
    await server.start_server()
    return server


@pytest.fixture
async def servers() -> AsyncIterator[Any]:
    """`start(app)` -> base URL of a test server; every server is closed at teardown."""
    started: list[TestServer] = []

    async def start(app: web.Application) -> str:
        server = await _server(app)
        started.append(server)
        return str(server.make_url("")).rstrip("/")

    yield start
    for server in started:
        await server.close()


def _providers(url: str, transport: str, **kw: Any) -> Providers:
    kw.setdefault("credentials", _keys)
    kw.setdefault("warm", NO_WARM)
    return Providers(
        ClientPool(bedrock_endpoint_url=url, options=BedrockOptions(transport=transport), **kw)
    )


_BOTOCORE_FOLLOWS = pytest.mark.xfail(
    reason="aiobotocore sends with aiohttp's allow_redirects=True and has no setting for it",
    strict=True,
)


@pytest.mark.parametrize("status", [301, 307, 308])
@pytest.mark.parametrize(
    "transport", ["aiohttp", pytest.param("botocore", marks=_BOTOCORE_FOLLOWS)]
)
async def test_a_redirect_is_never_followed(servers: Any, status: int, transport: str) -> None:
    """CL-4: aiohttp followed a 3xx and re-sent the signed body and session token elsewhere.

    The aiohttp transport now sends with `allow_redirects=False`: a 3xx is a FATAL error
    (code = the status) and the other host gets nothing. The botocore transport still follows
    (aiobotocore's default), pinned by the strict xfail.
    """
    seen: list[str] = []

    async def elsewhere(request: web.Request) -> web.Response:
        seen.append(request.path)
        return web.json_response({})

    other = web.Application()
    other.router.add_route("*", "/{tail:.*}", elsewhere)
    target = await servers(other)

    async def moved(request: web.Request) -> web.Response:
        return web.Response(status=status, headers={"Location": f"{target}{request.path}"})

    app = web.Application()
    app.router.add_post("/model/{model_id:.+}/{action}", moved)
    url = await servers(app)
    view = load_registry(registry_doc(url))
    providers = _providers(url, transport)
    try:
        bedrock = providers.get("bedrock")
        with pytest.raises(Exception) as info:  # noqa: PT011  # any error: classified below
            await bedrock.call("converse", CHAT, _route(view), PLAIN)
        found = bedrock.classify(info.value)
    finally:
        await providers.aclose()
    assert not seen  # the other host never got the signed request
    assert (found.kind, found.code) == (ErrorKind.FATAL, str(status))


def _broken_frame() -> bytes:
    """A ConverseStream event whose message CRC is wrong (corrupted on the wire)."""
    frame = bytearray(eventstream.event("messageStart", {"role": "assistant"}))
    frame[-1] ^= 0xFF
    return bytes(frame)


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_a_corrupt_stream_frame_is_retryable(servers: Any, transport: str) -> None:
    """CL-13: a frame failing its CRC check classified FATAL on both transports (no retry)."""

    async def corrupt(request: web.Request) -> web.StreamResponse:
        reply = web.StreamResponse(headers={"Content-Type": eventstream.CONTENT_TYPE})
        await reply.prepare(request)
        await reply.write(_broken_frame())
        await reply.write_eof()
        return reply

    app = web.Application()
    app.router.add_post("/model/{model_id:.+}/{action}", corrupt)
    url = await servers(app)
    providers = _providers(url, transport)
    try:
        bedrock = providers.get("bedrock")
        route = _route(load_registry(registry_doc(url)))
        with pytest.raises(Exception) as info:  # noqa: PT011  # any error: classified below
            await bedrock.call("converse", CHAT, route, FAST)
        assert bedrock.classify(info.value).kind is ErrorKind.RETRYABLE
    finally:
        await providers.aclose()


async def test_a_reply_that_is_not_json_is_retryable(servers: Any) -> None:
    """CL-13: a 200 with a gateway's HTML page raised `orjson.JSONDecodeError`: FATAL, no retry.

    aiohttp transport only: botocore's parser turns a non-JSON 200 into `{"message": ...}`.
    """

    async def html(request: web.Request) -> web.Response:
        return web.Response(text="<html>gateway</html>", content_type="application/json")

    app = web.Application()
    app.router.add_post("/model/{model_id:.+}/{action}", html)
    url = await servers(app)
    providers = _providers(url, "aiohttp")
    try:
        bedrock = providers.get("bedrock")
        route = _route(load_registry(registry_doc(url)))
        with pytest.raises(Exception) as info:  # noqa: PT011  # any error: classified below
            await bedrock.call("converse", CHAT, route, PLAIN)
        assert bedrock.classify(info.value).kind is ErrorKind.RETRYABLE
    finally:
        await providers.aclose()


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_extra_client_arguments_work_on_both_transports(
    start_fake: StartFake, transport: str, caplog: pytest.LogCaptureFixture
) -> None:
    """CL-6: `credentials=` with `verify` raised ConfigError on every aiohttp call.

    botocore takes such `create_client` arguments; that account now falls back to the botocore
    transport with one warning instead of raising into live calls.
    """
    url, _ = await start_fake()
    view = load_registry(registry_doc(url))
    providers = _providers(url, transport, credentials=lambda a: {**_keys(a), "verify": False})
    try:
        for _ in range(2):
            result = await providers.get("bedrock").call("converse", CHAT, _route(view), PLAIN)
            assert result.usage.output_tokens
    finally:
        await providers.aclose()
    warned = [r for r in caplog.records if "bedrock_transport_fallback" in r.getMessage()]
    assert len(warned) == (1 if transport == "aiohttp" else 0)


async def test_a_missing_api_key_fails_over_and_is_listed_at_warm(
    start_fake: StartFake, caplog: pytest.LogCaptureFixture
) -> None:
    """CL-6: a missing `api_key_ref` secret raised ConfigError (FATAL, `provider_error`).

    It is `CredentialsUnavailable` now (the worker forgets the route and tries the next one),
    and `warm` lists it at start.
    """
    url, _ = await start_fake()
    view = load_registry(registry_doc(url))
    route = route_for(view, next(iter(view.deployments("local"))))
    pool = ClientPool(secrets={}.get, warm=NO_WARM)
    try:
        assert pool.problems([route]) == ["account vllm: secret LLMQ_ACCOUNT_VLLM_KEY is not set"]
        await pool.warm([route])
        assert any("client_config_problem" in r.getMessage() for r in caplog.records)
        with pytest.raises(CredentialsUnavailable, match="LLMQ_ACCOUNT_VLLM_KEY"):
            pool.openai(route)
    finally:
        await pool.aclose()


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_no_credentials_is_retried_once_keys_appear(
    start_fake: StartFake, transport: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """CL-7: an empty credential chain cached a client holding None; it failed for good.

    The build now raises `CredentialsUnavailable` (no client cached), so the next call reads
    the chain again and succeeds once keys appear, without a restart.
    """
    from aiobotocore.session import AioSession

    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "none"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "none"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    url, _ = await start_fake()
    view = load_registry(registry_doc(url))
    providers = Providers(
        ClientPool(
            bedrock_endpoint_url=url,
            session=AioSession(),
            options=BedrockOptions(transport=transport),
            warm=NO_WARM,
        )
    )
    try:
        bedrock = providers.get("bedrock")
        with pytest.raises(CredentialsUnavailable, match="no AWS credentials"):
            await bedrock.call("converse", CHAT, _route(view), PLAIN)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws-a")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
        assert (await bedrock.call("converse", CHAT, _route(view), PLAIN)).usage.output_tokens
    finally:
        await providers.aclose()
    error = NoCredentialsError()
    assert is_auth_error(error, classify_bedrock(error))  # a raw one also drops the client


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_a_slow_refresh_is_not_cancelled_by_the_attempt_timer(
    start_fake: StartFake, transport: str
) -> None:
    """CL-8: each caller's attempt timeout cancelled the in-band refresh; it never landed.

    Inside the mandatory window every call refreshes first. The refresh (0.5 s) outlasts the
    attempt timer (0.2 s): it now runs as one shared task, so a later call finds it done.
    """
    asked: list[int] = []

    def credentials(account: Any) -> dict[str, Any]:
        asked.append(1)
        if len(asked) == 1:  # the build: keys about to expire
            soon = datetime.now(UTC) + timedelta(seconds=60)
            return {**_keys(account), "aws_session_token": "t", "expiry_time": soon}
        time.sleep(0.5)  # a slow refresh (runs in a thread)
        later = datetime.now(UTC) + timedelta(hours=1)
        return {**_keys(account), "aws_session_token": "t", "expiry_time": later}

    url, _ = await start_fake()
    view = load_registry(registry_doc(url))
    providers = _providers(url, transport, credentials=credentials)
    tight = Timing(0.2, 0.2, attempt_secs=0.2)
    try:
        bedrock = providers.get("bedrock")
        await providers.pool.bedrock(_route(view))  # built outside the tight timer
        ok = False
        for _ in range(10):
            try:
                await bedrock.call("converse", CHAT, _route(view), tight)
            except AttemptTimeout:
                continue
            ok = True
            break
        assert ok, "the refresh never landed"
        assert len(asked) == 2  # one build, one refresh: callers shared it
    finally:
        await providers.aclose()


async def test_the_credentials_callable_does_not_block_the_loop(start_fake: StartFake) -> None:
    """CL-8: `credentials=` ran on the event loop at build; a slow lookup stalled every task."""

    def credentials(account: Any) -> dict[str, Any]:
        time.sleep(0.3)
        return _keys(account)

    url, _ = await start_fake()
    view = load_registry(registry_doc(url))
    pool = ClientPool(bedrock_endpoint_url=url, credentials=credentials, warm=NO_WARM)
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
        await pool.bedrock(_route(view))
    finally:
        task.cancel()
        await pool.aclose()
    assert max(gaps) < 0.25  # the loop kept running while the callable slept
