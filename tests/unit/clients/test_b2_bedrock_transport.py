"""The aiohttp Bedrock transport against the botocore one (`BedrockOptions.transport`).

Both run against the same fakellm: every error row classifies the same way (so the worker picks
the same `CallFailed` kind), streams yield the same events and cut at the same text, request
bodies match botocore's serializer, refreshed credentials re-sign the next call, Retry-After
reaches the classifier, and a cancelled stream closes its connection.
"""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import orjson
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from botocore.serialize import create_serializer
from botocore.session import get_session
from clients_testkit import TRANSPORTS, StartFake, http_session, registry_doc
from fakellm import ERRORS
from fakellm.scenario import STREAM_EXCEPTIONS

from llm_queue.clients import ClientPool, Providers, Route, Timing, WarmOptions, route_for
from llm_queue.clients.bedrock_http import BedrockHTTP
from llm_queue.providers import is_auth_error, is_target_error, request_error_kind
from llm_queue.registry import ResolvedRegistry, load_registry
from llm_queue.tuning import BedrockOptions, Tuning

MODEL = "openai.gpt-oss-120b-1:0"
CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
FAST = Timing(first_response_secs=5, idle_secs=5)
PLAIN = Timing(5, 5, attempt_secs=5)
TITAN = {"body": {"inputText": "a b c d", "dimensions": 256}}


def _keys(account: Any) -> dict[str, Any]:
    return {"aws_access_key_id": account.name, "aws_secret_access_key": "x"}


def _route(view: ResolvedRegistry, model: str) -> Route:
    return route_for(view, next(d for d in view.deployments(model) if d.account == "aws-a"))


@pytest.fixture
async def both(start_fake: StartFake) -> AsyncIterator[Any]:
    """`make(scenario)` -> (view, {transport: Providers}) on one fakellm."""
    built: list[Providers] = []

    async def make(scenario: dict[str, Any] | None = None, **kw: Any) -> Any:
        url, fake = await start_fake(scenario)
        view = load_registry(registry_doc(url))
        out = {}
        for transport in TRANSPORTS:
            pool = ClientPool(
                bedrock_endpoint_url=url,
                credentials=kw.get("credentials", _keys),
                options=BedrockOptions(transport=transport),
                warm=WarmOptions(connections=0),
            )
            out[transport] = Providers(pool)
            built.append(out[transport])
        return view, out, fake

    yield make
    for providers in built:
        await providers.aclose()


def _decision(provider: Any, exc: BaseException) -> tuple[Any, ...]:
    """Everything the worker reads from a provider error to pick its `CallFailed` kind."""
    info = provider.classify(exc)
    return (
        info.kind,
        info.code,
        info.retry_after_secs,
        is_auth_error(exc, info),
        is_target_error(exc, info),
        request_error_kind(exc, info),
        getattr(exc, "response", {}).get("Error", {}).get("Message"),
    )


def test_transport_setting_is_checked() -> None:
    assert Tuning(bedrock=BedrockOptions(transport="aiohttp")).problems() == []
    assert Tuning(bedrock=BedrockOptions(transport="curl")).problems() == [
        "bedrock.transport must be one of botocore, aiohttp"
    ]


@pytest.mark.parametrize("error", sorted(ERRORS))
@pytest.mark.parametrize(
    ("op", "model", "params", "timing"),
    [
        ("converse", "gpt120b", CHAT, PLAIN),
        ("converse", "gpt120b", CHAT, FAST),  # over ConverseStream
        ("invoke_model", "titan", TITAN, FAST),
    ],
    ids=["converse", "stream", "invoke"],
)
async def test_errors_classify_the_same_on_both_transports(
    both: Any, error: str, op: str, model: str, params: Any, timing: Timing
) -> None:
    view, providers, _ = await both({"defaults": {"force_error": error}})
    route = _route(view, model)
    decisions = {}
    for transport, built in providers.items():
        bedrock = built.get("bedrock")
        with pytest.raises(Exception) as info:  # noqa: PT011  # any error: both transports compared
            await bedrock.call(op, params, route, timing)
        decisions[transport] = _decision(bedrock, info.value)
    assert decisions["aiohttp"] == decisions["botocore"]
    assert decisions["aiohttp"][1] == ERRORS[error].wire_code(error)


@pytest.mark.parametrize("reasoning", ["none", "both"])
async def test_streams_and_replies_are_the_same(both: Any, reasoning: str) -> None:
    view, providers, _ = await both({"defaults": {"output_tokens": 40, "reasoning": reasoning}})
    route = _route(view, "gpt120b")
    events: dict[str, list[Any]] = {}
    results = {}
    for transport, built in providers.items():
        bedrock = built.get("bedrock")
        events[transport] = [e async for e in bedrock.stream(CHAT, route, FAST)]
        plain = await bedrock.call("converse", CHAT, route, PLAIN)
        streamed = await bedrock.call("converse", CHAT, route, FAST)
        results[transport] = (plain.response, plain.usage, streamed.response, streamed.usage)
    assert events["aiohttp"] == events["botocore"]
    assert results["aiohttp"] == results["botocore"]
    if reasoning == "both":  # blob members come back as bytes on both
        content = results["aiohttp"][0]["output"]["message"]["content"]
        assert any(
            isinstance(b.get("reasoningContent", {}).get("redactedContent"), bytes) for b in content
        )


async def test_tool_use_and_invoke_are_the_same(both: Any) -> None:
    view, providers, _ = await both()
    tools = {
        "tools": [
            {
                "toolSpec": {
                    "name": "lookup",
                    "inputSchema": {
                        "json": {"type": "object", "properties": {"n": {"type": "integer"}}}
                    },
                }
            }
        ]
    }
    out = {}
    for transport, built in providers.items():
        bedrock = built.get("bedrock")
        tool = await bedrock.call(
            "converse", {**CHAT, "toolConfig": tools}, _route(view, "gpt120b"), FAST
        )
        invoke = await bedrock.call("invoke_model", TITAN, _route(view, "titan"), FAST)
        out[transport] = (tool.response, invoke.response, invoke.usage)
    assert out["aiohttp"] == out["botocore"]


@pytest.mark.parametrize("cut_with", sorted(STREAM_EXCEPTIONS | {"close"}))
async def test_a_cut_stream_fails_the_same_after_the_same_text(both: Any, cut_with: str) -> None:
    view, providers, _ = await both(
        {"defaults": {"output_tokens": 80, "cut_after_chunks": 2, "cut_with": cut_with}}
    )
    route = _route(view, "gpt120b")
    seen: dict[str, list[Any]] = {}
    decisions = {}
    for transport, built in providers.items():
        bedrock = built.get("bedrock")
        got: list[Any] = []

        async def sink(event: Any, into: list[Any] = got) -> None:
            into.append(event)

        with pytest.raises(Exception) as info:  # noqa: PT011  # any error: both transports compared
            await bedrock.call("converse", CHAT, route, FAST, sink)
        seen[transport] = got
        decisions[transport] = _decision(bedrock, info.value)
    assert seen["aiohttp"] == seen["botocore"]
    text = "".join(
        e["contentBlockDelta"]["delta"].get("text", "")
        for e in seen["aiohttp"]
        if "contentBlockDelta" in e
    )
    # An abrupt close can lose the buffered chunks (then the reset gets its one quick retry).
    assert len(text) == (0 if cut_with == "close" else 64)  # the partial text the caller gets
    assert decisions["aiohttp"][:3] == decisions["botocore"][:3]


async def test_refreshed_credentials_sign_the_next_call(both: Any) -> None:
    # Each refresh hands out new keys; inside the mandatory window every call refreshes first.
    issued: dict[str, int] = {}

    def credentials(account: Any) -> dict[str, Any]:
        n = issued[account.name] = issued.get(account.name, 0) + 1
        soon = datetime.now(UTC) + timedelta(seconds=60)  # inside the last 2 minutes
        return {"aws_access_key_id": f"k{n}", "aws_secret_access_key": "x", "expiry_time": soon}

    scenario: dict[str, Any] = {"models": {f"{MODEL}@k{n}/ap-south-1": {} for n in range(1, 5)}}
    view, providers, fake = await both(scenario, credentials=credentials)
    route = _route(view, "gpt120b")
    for transport, built in providers.items():
        issued.clear()
        fake.stats.clear()
        await built.get("bedrock").call("converse", CHAT, route, PLAIN)
        await built.get("bedrock").call("converse", CHAT, route, PLAIN)
        signed = sorted(k.split("@")[1] for k in fake.snapshot()["models"])
        assert issued["aws-a"] >= 3, transport  # built, then refreshed before each call
        assert "k1/ap-south-1" not in signed, transport  # the expired keys never signed


async def _retry_after_app() -> TestServer:
    async def throttled(request: web.Request) -> web.Response:
        return web.json_response(
            {"message": "Too many requests"},
            status=429,
            headers={"x-amzn-ErrorType": "ThrottlingException", "Retry-After": "7"},
        )

    app = web.Application()
    app.router.add_post("/model/{model_id:.+}/{action}", throttled)
    server = TestServer(app, host="127.0.0.1")
    await server.start_server()
    return server


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_retry_after_reaches_the_classifier(transport: str) -> None:
    server = await _retry_after_app()
    url = str(server.make_url("")).rstrip("/")
    view = load_registry(registry_doc(url))
    providers = Providers(
        ClientPool(
            bedrock_endpoint_url=url,
            credentials=_keys,
            options=BedrockOptions(transport=transport),
            warm=WarmOptions(connections=0),
        )
    )
    try:
        bedrock = providers.get("bedrock")
        for timing in (PLAIN, FAST):
            with pytest.raises(Exception) as info:  # noqa: PT011  # any error: both transports compared
                await bedrock.call("converse", CHAT, _route(view, "gpt120b"), timing)
            assert bedrock.classify(info.value).retry_after_secs == 7
    finally:
        await providers.aclose()
        await server.close()


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_a_cancelled_stream_closes_its_connection(
    start_fake: StartFake, transport: str
) -> None:
    url, _ = await start_fake({"defaults": {"output_tokens": 400, "tokens_per_s": 40}})
    view = load_registry(registry_doc(url))
    providers = Providers(
        ClientPool(
            bedrock_endpoint_url=url,
            credentials=_keys,
            options=BedrockOptions(transport=transport),
            warm=WarmOptions(connections=0),
        )
    )
    route = _route(view, "gpt120b")
    first = asyncio.Event()

    async def sink(event: Any) -> None:
        first.set()

    try:
        task = asyncio.create_task(
            providers.get("bedrock").call("converse", CHAT, route, FAST, sink)
        )
        await asyncio.wait_for(first.wait(), 5)
        session = http_session(await providers.pool.bedrock(route))
        assert len(session.connector._acquired) == 1
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not session.connector._acquired
        assert not sum(len(c) for c in session.connector._conns.values())  # closed, not pooled
    finally:
        await providers.aclose()


def test_request_bodies_match_botocore() -> None:
    image = b"\x89PNG\r\n\x1a\nimage"
    request: dict[str, Any] = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"text": "look"},
                    {"image": {"format": "png", "source": {"bytes": image}}},
                    {"document": {"format": "txt", "name": "d", "source": {"bytes": "plain text"}}},
                ],
            },
            {
                "role": "assistant",
                "content": [{"reasoningContent": {"redactedContent": b"\x00\x01"}}],
            },
        ],
        "system": [{"text": "be brief"}],
        "inferenceConfig": {"maxTokens": 10, "temperature": 0.5},
        "additionalModelRequestFields": {"top_k": 5},
    }
    model = get_session().get_service_model("bedrock-runtime")
    wire = create_serializer("rest-json").serialize_to_request(
        {**request, "modelId": MODEL}, model.operation_model("Converse")
    )
    ours = BedrockHTTP._json(dict(request))
    assert orjson.loads(ours) == orjson.loads(wire["body"])
    assert request["messages"][0]["content"][2]["document"]["source"]["bytes"] == "plain text"
    sent = orjson.loads(ours)["messages"][0]["content"]
    assert base64.b64decode(sent[1]["image"]["source"]["bytes"]) == image
