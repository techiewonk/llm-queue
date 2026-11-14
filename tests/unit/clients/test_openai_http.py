"""OpenAI-compatible client over httpx or aiohttp (`OpenAIOptions.http`), against a tiny server.

Pins that the aiohttp client gets the pool limits, keepalive and timeouts, that it falls back
to httpx when the SDK has no aiohttp client, that a cancelled call closes its connection, and
that 429 / 5xx / timeout / reset / refused errors classify the same on both clients. Before,
an aiohttp reset (`APITimeoutError` around `ServerDisconnectedError`) was not seen as a reset.
"""

from __future__ import annotations

import asyncio
import socket
import time
from collections.abc import AsyncIterator
from typing import Any

import openai
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from llm_queue import Tuning
from llm_queue.clients import ClientHub, ClientPool, OpenAIOptions, Providers, Route, Timing
from llm_queue.clients.timing import first
from llm_queue.errors import ErrorKind
from llm_queue.providers import classify_openai, is_connection_reset
from llm_queue.registry import Account

HTTP = ["httpx", "aiohttp"]
MESSAGES = [{"role": "user", "content": "hi"}]
CHUNK = (
    b'data: {"id":"c","object":"chat.completion.chunk","created":0,"model":"m",'
    b'"choices":[{"index":0,"delta":{"content":"a"},"finish_reason":null}]}\n\n'
)


class Server:
    """Answers by model name: ok, r429, r503, slow (5 s), reset (socket aborted)."""

    def __init__(self) -> None:
        self.url = ""

    async def chat(self, request: web.Request) -> web.StreamResponse:
        model = (await request.json())["model"]
        if model == "r429":
            error = {"message": "slow down", "type": "requests", "code": "rate_limit_exceeded"}
            return web.json_response({"error": error}, status=429, headers={"Retry-After": "7"})
        if model == "r503":
            return web.json_response({"error": {"message": "busy", "code": None}}, status=503)
        if model == "slow":
            await asyncio.sleep(5)
        if model == "reset":
            assert request.transport is not None
            request.transport.abort()
            await asyncio.sleep(1)
        message = {"role": "assistant", "content": "ok"}
        return web.json_response(
            {
                "id": "c",
                "object": "chat.completion",
                "created": 0,
                "model": model,
                "choices": [{"index": 0, "message": message, "finish_reason": "stop"}],
            }
        )


@pytest.fixture
async def server() -> AsyncIterator[Server]:
    state = Server()
    app = web.Application()
    app.router.add_post("/v1/chat/completions", state.chat)
    test_server = TestServer(app, host="127.0.0.1")
    await test_server.start_server()
    state.url = str(test_server.make_url("/v1"))
    yield state
    await test_server.close()


def _pool(url: str, **options: Any) -> tuple[ClientPool, Route]:
    account = Account(name="vllm", kind="openai", base_url=url, api_key_ref="LLMQ_ACCOUNT_K")
    pool = ClientPool(
        secrets={"LLMQ_ACCOUNT_K": "k"}.get,
        openai_options=OpenAIOptions(**options),
        hub=ClientHub(),
    )
    return pool, Route(account, "dc1", "m", "vllm/dc1")


async def test_aiohttp_client_gets_limits_keepalive_and_timeouts(server: Server) -> None:
    pool, route = _pool(
        server.url,
        http="aiohttp",
        max_connections=7,
        keepalive_secs=3,
        read_timeout_secs=42,
        connect_timeout_secs=4,
    )
    client = pool.openai(route)
    http: Any = client._client
    assert isinstance(http, openai.DefaultAioHttpClient)
    assert (http.timeout.read, http.timeout.connect) == (42, 4)
    reply = await client.chat.completions.create(model="ok", messages=MESSAGES)  # type: ignore[arg-type]
    assert reply.choices[0].message.content == "ok"
    transport: Any = http._transport
    connector = transport.client.connector  # the aiohttp session, made on first use
    assert (connector.limit, connector._keepalive_timeout) == (7, 3)
    await pool.aclose()
    assert connector.closed


async def test_aiohttp_is_the_default_and_httpx_the_fallback(
    server: Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool, route = _pool(server.url)
    assert type(pool.openai(route)._client) is openai.DefaultAioHttpClient
    await pool.aclose()
    pool, route = _pool(server.url, http="httpx")
    assert type(pool.openai(route)._client) is openai.DefaultAsyncHttpxClient
    await pool.aclose()

    class Missing:  # what the SDK defines when `httpx-aiohttp` / `aiohttp` is not installed
        def __init__(self, **_: Any) -> None:
            raise RuntimeError("To use the aiohttp client you must have installed ...")

    monkeypatch.setattr(openai, "DefaultAioHttpClient", Missing)
    pool, route = _pool(server.url, http="aiohttp")
    assert type(pool.openai(route)._client) is openai.DefaultAsyncHttpxClient
    await pool.aclose()
    monkeypatch.delattr(openai, "DefaultAioHttpClient")  # openai 1.x before the aiohttp client
    pool, route = _pool(server.url, http="aiohttp")
    assert type(pool.openai(route)._client) is openai.DefaultAsyncHttpxClient
    await pool.aclose()


def test_http_must_be_a_known_client() -> None:
    problems = Tuning(openai=OpenAIOptions(http="curl")).problems()  # type: ignore[arg-type]
    assert problems == ["openai.http must be one of httpx, aiohttp"]


@pytest.mark.parametrize("http", HTTP)
@pytest.mark.parametrize("mid_stream", [False, True])
async def test_a_cancelled_call_closes_its_connection(http: str, mid_stream: bool) -> None:
    """Cancelled waiting for headers or mid-stream: the server reads EOF on the socket.

    A raw socket server: aiohttp's server does not report a peer that went away mid-handler.
    """
    opened, closed = asyncio.Event(), asyncio.Event()

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        head = await reader.readuntil(b"\r\n\r\n")
        length = next(
            int(line.split(b":")[1])
            for line in head.split(b"\r\n")
            if line.lower().startswith(b"content-length")
        )
        await reader.readexactly(length)
        if mid_stream:
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n")
            writer.write(b"Transfer-Encoding: chunked\r\n\r\n%x\r\n%s\r\n" % (len(CHUNK), CHUNK))
            await writer.drain()
        opened.set()
        while await reader.read(1024):
            pass
        closed.set()
        writer.close()

    raw = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = raw.sockets[0].getsockname()[1]
    pool, route = _pool(f"http://127.0.0.1:{port}/v1", http=http)
    provider = Providers(pool).get("openai")
    task = asyncio.create_task(provider.call("chat", {"messages": MESSAGES}, route, Timing(9, 9)))
    await asyncio.wait_for(opened.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(closed.wait(), 5)
    await pool.aclose()
    raw.close()
    await raw.wait_closed()


async def test_a_cancel_in_the_same_turn_as_the_result_is_not_lost() -> None:
    """Python 3.11's `wait_for` returned the result and dropped the cancel (timing.first)."""
    read: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    task = asyncio.create_task(first(read, 5, "chunk"))
    await asyncio.sleep(0)  # the task now waits on the read
    read.set_result(1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def _refused_url() -> str:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return f"http://127.0.0.1:{port}/v1"


@pytest.mark.parametrize("http", HTTP)
@pytest.mark.parametrize(
    ("model", "expected"),
    [
        ("r429", (ErrorKind.THROTTLE, "rate_limit_exceeded", 7.0, False)),
        ("r503", (ErrorKind.RETRYABLE, "InternalServerError", None, False)),
        ("slow", (ErrorKind.RETRYABLE, "APITimeoutError", None, False)),
        ("reset", (ErrorKind.RETRYABLE, "APIConnectionError", None, True)),
        ("refused", (ErrorKind.RETRYABLE, "APIConnectionError", None, False)),
    ],
)
async def test_errors_classify_the_same_on_both_clients(
    server: Server, http: str, model: str, expected: tuple[Any, ...]
) -> None:
    url = _refused_url() if model == "refused" else server.url
    # aiohttp's whole-connect timer is the pool timeout (= read here): keep it above a refused
    # connect, which takes ~2 s on Windows.
    read = 0.3 if model == "slow" else 5
    pool, route = _pool(url, http=http, read_timeout_secs=read, connect_timeout_secs=5)
    started = time.monotonic()
    with pytest.raises(openai.APIError) as info:
        await pool.openai(route).chat.completions.create(model=model, messages=MESSAGES)  # type: ignore[arg-type]
    elapsed = time.monotonic() - started
    error = classify_openai(info.value)
    got = (error.kind, error.code, error.retry_after_secs, is_connection_reset(info.value))
    assert got == expected
    if model == "slow":
        assert elapsed < 3  # the read timeout is honoured (the server would answer in 5 s)
    await pool.aclose()
