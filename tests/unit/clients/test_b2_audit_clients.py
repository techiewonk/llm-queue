"""Fixes from the clients audit (docs/internal/audit/audit-clients.md, CL-*), pinned per bug.

Each test names its finding: redirects, credentials, client retirement, quick retries, stream
release and the parity of the two Bedrock transports. Runs fakellm or small aiohttp test
servers in process; no network, no real model.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from botocore.exceptions import NoCredentialsError
from clients_testkit import TRANSPORTS, StartFake, http_session, registry_doc
from fakellm import eventstream

from llm_queue.clients import (
    AttemptTimeout,
    ClientHub,
    ClientPool,
    CredentialsUnavailable,
    Providers,
    Route,
    Timing,
    WarmOptions,
    pool_size_for,
    route_for,
)
from llm_queue.clients import pool as pool_module
from llm_queue.errors import ErrorKind
from llm_queue.providers import classify_bedrock, is_auth_error
from llm_queue.providers.classify import is_transport_error
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
    pool = ClientPool(secrets=lambda name: None, warm=NO_WARM)
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

    Inside the mandatory window every call refreshes first. The refresh (0.8 s) outlasts the
    attempt timer (0.3 s): it now runs as one shared task, so a later call finds it done.
    """
    asked: list[int] = []

    def credentials(account: Any) -> dict[str, Any]:
        asked.append(1)
        if len(asked) == 1:  # the build: keys about to expire
            soon = datetime.now(UTC) + timedelta(seconds=60)
            return {**_keys(account), "aws_session_token": "t", "expiry_time": soon}
        time.sleep(0.8)  # a slow refresh (runs in a thread)
        later = datetime.now(UTC) + timedelta(hours=1)
        return {**_keys(account), "aws_session_token": "t", "expiry_time": later}

    url, _ = await start_fake()
    view = load_registry(registry_doc(url))
    providers = _providers(url, transport, credentials=credentials)
    tight = Timing(0.3, 0.3, attempt_secs=0.3)
    try:
        bedrock = providers.get("bedrock")
        await providers.pool.bedrock(_route(view))  # built outside the tight timer
        ok = False
        for _ in range(30):
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


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_a_dropped_stream_is_a_transport_error(start_fake: StartFake, transport: str) -> None:
    """CL-1 (clients part): a connection dropped mid-stream classifies as a transport error.

    The worker maps such an error to `worker_lost` once chunks were sent (runtime side of
    CL-1); this pins that both transports hand it a transport error to recognise.
    """
    url, _ = await start_fake(
        {"defaults": {"output_tokens": 80, "cut_after_chunks": 2, "cut_with": "close"}}
    )
    view = load_registry(registry_doc(url))
    providers = _providers(url, transport)

    async def sink(event: Any) -> None:
        return None

    try:
        with pytest.raises(Exception) as info:  # noqa: PT011  # any error: classified below
            await providers.get("bedrock").call("converse", CHAT, _route(view), FAST, sink)
    finally:
        await providers.aclose()
    assert is_transport_error(info.value), repr(info.value)
    assert classify_bedrock(info.value).kind is ErrorKind.RETRYABLE


def _fake_lease(hub: ClientHub, key: str, closed: list[str], close_secs: float = 0.0) -> Any:
    async def build() -> tuple[Any, Any]:
        async def close() -> None:
            await asyncio.sleep(close_secs)
            closed.append(key)

        return object(), close

    return hub.acquire((key,), build)


async def test_aclose_never_releases_a_retired_lease_twice() -> None:
    """CL-2: aclose released retired leases again while the retire task was releasing them.

    Lease A (shared with another pool) was released by the task, which then awaited B's slow
    close; aclose released A once more, so A was closed under the other pool.
    """
    hub, closed = ClientHub(), list[str]()
    shared = await _fake_lease(hub, "a", closed)
    await _fake_lease(hub, "a", closed)  # the other pool's hold
    slow = await _fake_lease(hub, "b", closed, close_secs=0.3)
    pool = ClientPool(hub=hub, warm=NO_WARM)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(pool_module, "RETIRE_GRACE_SECS", 0.0)
        pool._retire([shared, slow])
        for _ in range(200):  # until the task released A and is closing B
            if shared.refs == 1 and slow.refs == 0:
                break
            await asyncio.sleep(0.01)
        assert (shared.refs, slow.refs) == (1, 0)
        await pool.aclose()
    assert shared.refs == 1  # was 0: closed while the other pool still used it
    assert "a" not in closed
    for _ in range(100):
        if "b" in closed:
            break
        await asyncio.sleep(0.01)
    assert closed == ["b"]  # B's close was not cut by aclose


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_a_retired_client_waits_for_its_calls(start_fake: StartFake, transport: str) -> None:
    """CL-3: a client retired by forget() was closed after a fixed grace under a long answer.

    The grace was `read + connect` seconds whatever the call; a healthy answer longer than that
    was cut by the library. Retired clients now close when their calls end.
    """
    url, _ = await start_fake({"defaults": {"output_tokens": 30, "tokens_per_s": 30}})
    view = load_registry(registry_doc(url))
    providers = _providers(url, transport)
    route = _route(view)
    started = asyncio.Event()

    async def sink(event: Any) -> None:
        started.set()

    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(pool_module, "RETIRE_GRACE_SECS", 0.05)
            call = asyncio.create_task(
                providers.get("bedrock").call("converse", CHAT, route, FAST, sink)
            )
            await asyncio.wait_for(started.wait(), 5)
            old = await providers.pool.bedrock(route)
            providers.pool.forget(route)
            result = await call  # about 1 s, far past the 0.05 s grace
            assert result.usage.output_tokens == 30
            session = http_session(old)
            for _ in range(200):
                if session.closed:
                    break
                await asyncio.sleep(0.01)
            assert session.closed  # closed once the call ended
    finally:
        await providers.aclose()


async def _cut_server(servers: Any, hits: list[int], *, stale_first: bool) -> str:
    """A ConverseStream that drops its connection after one event, or a stale first socket."""

    async def handler(request: web.Request) -> web.StreamResponse:
        hits.append(1)
        transport = request.transport
        assert transport is not None
        if stale_first and len(hits) == 1:  # dropped before any reply: a stale keep-alive
            transport.close()
            return web.Response()
        reply = web.StreamResponse(headers={"Content-Type": eventstream.CONTENT_TYPE})
        await reply.prepare(request)
        await reply.write(eventstream.event("messageStart", {"role": "assistant"}))
        await reply.write(
            eventstream.event(
                "contentBlockDelta", {"contentBlockIndex": 0, "delta": {"text": "hi"}}
            )
        )
        if stale_first:
            await reply.write(eventstream.event("messageStop", {"stopReason": "end_turn"}))
            await reply.write_eof()
            return reply
        await asyncio.sleep(0.05)
        transport.close()  # the body is cut: the model already ran
        return reply

    app = web.Application()
    app.router.add_post("/model/{model_id:.+}/{action}", handler)
    return str(await servers(app))


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_a_body_cut_is_not_quick_retried(servers: Any, transport: str) -> None:
    """CL-9: a stream cut after the answer began was re-run at once with no sink (billed twice).

    The quick retry is for a stale socket that fails before any reply; that one still gets it.
    """
    hits: list[int] = []
    url = await _cut_server(servers, hits, stale_first=False)
    providers = _providers(url, transport)
    try:
        with pytest.raises(Exception):  # noqa: PT011, B017  # any error: counted below
            await providers.get("bedrock").call(
                "converse", CHAT, _route(load_registry(registry_doc(url))), FAST
            )
    finally:
        await providers.aclose()
    assert len(hits) == 1  # was 2: the generation ran twice

    hits.clear()
    url = await _cut_server(servers, hits, stale_first=True)
    providers = _providers(url, transport)
    try:
        result = await providers.get("bedrock").call(
            "converse", CHAT, _route(load_registry(registry_doc(url))), FAST
        )
    finally:
        await providers.aclose()
    assert result.response["output"]["message"]["content"] == [{"text": "hi"}]
    assert len(hits) == 2  # the stale socket got its one quick retry


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_a_failing_sink_releases_the_connection_at_once(
    start_fake: StartFake, transport: str
) -> None:
    """CL-10: a sink that raised left the stream generator suspended, holding its connection.

    It was closed later by the asyncgen finalizer; now the connection is free when the call
    raises.
    """
    url, _ = await start_fake({"defaults": {"output_tokens": 400, "tokens_per_s": 200}})
    view = load_registry(registry_doc(url))
    providers = _providers(url, transport)
    seen: list[Any] = []

    async def sink(event: Any) -> None:
        seen.append(event)
        if len(seen) == 2:
            raise RuntimeError("store down")

    try:
        with pytest.raises(RuntimeError, match="store down"):
            await providers.get("bedrock").call("converse", CHAT, _route(view), FAST, sink)
        session = http_session(await providers.pool.bedrock(_route(view)))
        assert not session.connector._acquired  # no extra await: released already
    finally:
        await providers.aclose()


async def test_a_cancelled_release_stays_cancelled() -> None:
    """CL-11: release() swallowed a cancellation of its own task while awaiting warm tasks.

    The cancel was lost (broke asyncio.timeout / TaskGroup); the client is still closed.
    """
    hub, closed = ClientHub(), list[str]()
    lease = await _fake_lease(hub, "a", closed)
    stubborn = asyncio.Event()

    async def warm() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            stubborn.set()
            await asyncio.sleep(0.2)  # ignores the cancel for a while

    hub.spawn(lease, warm())
    await asyncio.sleep(0)
    outer = asyncio.create_task(hub.release(lease))
    await asyncio.wait_for(stubborn.wait(), 5)
    outer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await outer
    assert outer.cancelled()
    assert closed == ["a"]


def test_an_unknown_model_is_a_404_from_the_queued_transport() -> None:
    """CL-17: the transport's `KeyError -> 404` branch was dead (KeyError was never caught).

    An unknown alias is `UnknownModel` (a ConfigError and a KeyError): 404 `model_not_found`.
    """
    import httpx

    from llm_queue.errors import UnknownModel
    from llm_queue.http_transport import _error_response

    request = httpx.Request("POST", "http://llmq.invalid/v1/chat/completions")
    reply = _error_response(request, UnknownModel("nope"))
    assert reply.status_code == 404
    assert reply.json()["error"]["code"] == "model_not_found"


def test_pool_size_follows_the_slots_without_a_cap() -> None:
    """CL-14: above 200 slots, callers queued for a socket inside the first-response timer.

    A local wait then read as a provider timeout and charged the breaker; the pool is now as
    large as the slots routed to the client (the slots already bound the calls).
    """
    assert pool_size_for(5000) == 5000
    assert pool_size_for(1) == 10


def test_keep_warm_reads_internals_that_still_exist() -> None:
    """CL-20: `_keep_warm` reads private aiobotocore / aiohttp attributes; fail loudly if gone.

    It only logged at debug level when they vanished, and warm connections silently stopped.
    """
    import inspect

    import aiohttp
    from aiobotocore.httpsession import AIOHTTPSession

    assert hasattr(AIOHTTPSession, "_get_session")
    assert "self._proxy_config" in inspect.getsource(AIOHTTPSession.__init__)
    assert "self._acquired" in inspect.getsource(aiohttp.BaseConnector.__init__)


def test_importing_the_hub_registers_no_fork_hook() -> None:
    """CL-18: `os.register_at_fork` ran at import (a side effect); now on the first acquire."""
    code = (
        "import asyncio, llm_queue.clients.hub as h\n"
        "assert not h._fork_hook\n"
        "async def build():\n"
        "    async def close(): pass\n"
        "    return object(), close\n"
        "asyncio.run(h.ClientHub().acquire(('k',), build))\n"
        "assert h._fork_hook\n"
    )
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    done = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, check=False)
    assert done.returncode == 0, done.stderr.decode()
