"""C2: every provider attempt keeps the provider's request id (`Result.request_id`).

fakellm sends one id per response as `x-amzn-RequestId` and `x-request-id` (as Bedrock and
OpenAI do) and keeps the ids it sent; each path must report the id of its own response.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Literal

import pytest
from aiohttp.test_utils import TestServer
from clients_testkit import TRANSPORTS, Engine, Scenario, create_app, registry_doc
from fakellm import REQUEST_IDS

from llm_queue.clients import ClientPool, OpenAIOptions, Providers, Timing, WarmOptions, route_for
from llm_queue.registry import load_registry
from llm_queue.tuning import BedrockOptions

CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
OPENAI_CHAT = {"messages": [{"role": "user", "content": "hello"}]}
TITAN = {"body": {"inputText": "a b c d", "dimensions": 256}}
FAST = Timing(first_response_secs=5, idle_secs=5)  # Bedrock: over ConverseStream
PLAIN = Timing(5, 5, attempt_secs=5)  # Bedrock: plain Converse


@pytest.fixture
async def fake() -> AsyncIterator[TestServer]:
    server = TestServer(create_app(engine=Engine(Scenario.from_mapping({}))), host="127.0.0.1")
    await server.start_server()
    yield server
    await server.close()


Http = Literal["httpx", "aiohttp"]  # `OpenAIOptions.http`


def _providers(server: TestServer, transport: str, http: Http = "httpx") -> Providers:
    url = str(server.make_url("")).rstrip("/")
    return Providers(
        ClientPool(
            bedrock_endpoint_url=url,
            options=BedrockOptions(transport=transport),
            credentials=lambda a: {"aws_access_key_id": a.name, "aws_secret_access_key": "x"},
            secrets={"LLMQ_ACCOUNT_VLLM_KEY": "key-v"}.get,
            openai_options=OpenAIOptions(http=http),
            warm=WarmOptions(connections=0),
        )
    )


def _route(server: TestServer, model: str) -> Any:
    view = load_registry(registry_doc(str(server.make_url("")).rstrip("/")))
    return route_for(view, view.deployments(model)[0])


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize(
    ("op", "model", "params", "timing", "sink"),
    [
        ("converse", "gpt120b", CHAT, PLAIN, False),
        ("converse", "gpt120b", CHAT, FAST, True),
        ("invoke_model", "titan", TITAN, FAST, False),
    ],
    ids=["converse", "converse_stream", "invoke_model"],
)
async def test_bedrock_result_has_the_request_id(
    fake: TestServer, transport: str, op: str, model: str, params: Any, timing: Timing, sink: bool
) -> None:
    providers = _providers(fake, transport)
    events: list[Any] = []

    async def collect(event: Any) -> None:
        events.append(event)

    try:
        bedrock = providers.get("bedrock")
        result = await bedrock.call(
            op, params, _route(fake, model), timing, collect if sink else None
        )
    finally:
        await providers.aclose()
    assert result.request_id
    assert result.request_id == fake.app[REQUEST_IDS][-1]
    assert bool(events) is sink


@pytest.mark.parametrize("http", ["httpx", "aiohttp"])
async def test_openai_chat_result_has_the_request_id(fake: TestServer, http: Http) -> None:
    providers = _providers(fake, "botocore", http)
    try:
        result = await providers.get("openai").call(
            "chat", OPENAI_CHAT, _route(fake, "local"), FAST
        )
    finally:
        await providers.aclose()
    assert result.request_id
    assert result.request_id == fake.app[REQUEST_IDS][-1]


async def test_openai_embeddings_result_has_the_request_id(fake: TestServer) -> None:
    doc = registry_doc(str(fake.make_url("")).rstrip("/"))
    doc["models"]["local-emb"] = {
        "type": "embedding",
        "provider": "openai",
        "model_id": "text-embed",
        "deployments": {"vllm": {"confirmed": ["dc1"]}},
    }
    view = load_registry(doc)
    providers = _providers(fake, "botocore")
    try:
        result = await providers.get("openai").call(
            "embeddings",
            {"input": ["a", "b"]},
            route_for(view, view.deployments("local-emb")[0]),
            FAST,
        )
    finally:
        await providers.aclose()
    assert result.request_id == fake.app[REQUEST_IDS][-1]
