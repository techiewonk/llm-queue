"""`LLMQueue` and the LangChain adapters end to end (plan step A14), on fakellm.

Built with `LLMQueue.from_env` exactly as an app builds it: converse with the request context,
embeddings, jobs, sync calls on the direct path, and the four LangChain adapters (Bedrock chat
with tools, Bedrock embeddings, OpenAI-compatible chat and embeddings). No real LLM is called.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import redis.asyncio
from aiohttp.test_utils import TestServer
from engine_testkit import FakeEngine, Scenario, create_app, registry
from fakellm import REQUEST_IDS

from llm_queue import CallFailed, LLMQueue, OpenAIOptions, Tuning, request_context
from llm_queue.http_transport import _error_response

pytestmark = pytest.mark.integration


def _doc(url: str) -> dict[str, Any]:
    doc: dict[str, Any] = registry()
    doc["accounts"]["vllm"] = {
        "kind": "openai",
        "base_url": f"{url}/v1",
        "api_key_ref": "LLMQ_ACCOUNT_VLLM_KEY",
    }
    doc["models"]["local"] = {
        "type": "chat",
        "provider": "openai",
        "model_id": "gpt-oss-120b",
        "deployments": {"vllm": {"confirmed": ["dc1"]}},
    }
    # Disabled, with a fallback model of the other provider (Q40): calls are converted.
    doc["models"]["to-openai"] = {
        "type": "chat",
        "provider": "bedrock",
        "model_id": "meta.llama3-8b-instruct-v1:0",
        "enabled": False,
        "model_fallback": {"on": ["exhausted"], "models": ["local"]},
        "deployments": {"aws-a": {"confirmed": ["r1"]}},
    }
    doc["models"]["to-bedrock"] = {
        "type": "chat",
        "provider": "openai",
        "model_id": "gpt-oss-20b",
        "enabled": False,
        "model_fallback": {"on": ["exhausted"], "models": ["small"]},
        "deployments": {"vllm": {"confirmed": ["dc1"]}},
    }
    doc["models"]["local-emb"] = {
        "type": "embedding",
        "provider": "openai",
        "model_id": "text-embed",
        "deployments": {"vllm": {"confirmed": ["dc1"]}},
    }
    return doc


@pytest.fixture
async def llmq(
    store: redis.asyncio.Redis, prefix: str, request: pytest.FixtureRequest
) -> AsyncIterator[LLMQueue]:
    fake = FakeEngine(Scenario.from_mapping({}))
    server = TestServer(create_app(engine=fake), host="127.0.0.1")
    await server.start_server()
    url = str(server.make_url("")).rstrip("/")
    port = store.connection_pool.connection_kwargs["port"]
    # A plain name (FO-20 refuses the fixture's `:`); its keys are deleted below.
    key_prefix = prefix.replace(":", "-")
    env = {
        "LLMQ_STORE_URL": f"redis://:localdev@127.0.0.1:{port}/0",
        "LLMQ_ENVIRONMENT": "test",
        "LLMQ_KEY_PREFIX": key_prefix,
        "LLMQ_REGISTRY": json.dumps(_doc(url)),
        "LLMQ_BEDROCK_ENDPOINT_URL": url,
        "LLMQ_METRICS_SINK": "none",
    }
    queue = LLMQueue.from_env(
        env,
        secrets={"LLMQ_ACCOUNT_VLLM_KEY": "key-v"}.get,
        credentials=lambda account: {
            "aws_access_key_id": account.name,
            "aws_secret_access_key": "x",
        },
        tuning=Tuning(openai=OpenAIOptions(http=getattr(request, "param", "httpx"))),
    )
    seen: list[dict[str, Any]] = []

    async def ingest(payload: dict[str, Any]) -> None:
        vectors = await queue.embed("cohere", payload["texts"], input_type="search_query")
        seen.append({"payload": payload, "vectors": len(vectors.vectors)})

    queue.register_job("ingest", ingest)
    queue.seen = seen  # type: ignore[attr-defined]
    queue.fakellm = server  # type: ignore[attr-defined]
    await queue.start()
    yield queue
    await queue.stop()
    await server.close()
    keys = [k async for k in store.scan_iter(match=f"{key_prefix}:*", count=500)]
    if keys:
        await store.delete(*keys)


def _sent_ids(llmq: LLMQueue) -> list[str]:
    """The request ids fakellm sent (`x-amzn-RequestId` / `x-request-id`), newest last."""
    return list(llmq.fakellm.app[REQUEST_IDS])  # type: ignore[attr-defined]


def _assert_request_id(llmq: LLMQueue, metadata: dict[str, Any]) -> None:
    """A LangChain reply's boto-shaped `ResponseMetadata` names the provider's request id."""
    boto = metadata["ResponseMetadata"]
    assert boto["RequestId"] in _sent_ids(llmq)
    assert boto["RequestId"] == metadata["llmq"]["provider_request_id"]
    assert boto["HTTPStatusCode"] == 200
    assert boto["RetryAttempts"] == metadata["llmq"]["attempts"] - 1
    assert "HTTPHeaders" not in boto


async def test_c2_provider_request_id_reaches_the_caller(llmq: LLMQueue) -> None:
    """C2: queued Converse, a queued ConverseStream and an OpenAI chat carry the provider's id."""
    request = {"messages": [{"role": "user", "content": [{"text": "hi"}]}]}
    reply = await llmq.converse("big", request)
    assert reply.meta.provider_request_id == _sent_ids(llmq)[-1]
    events = [e async for e in llmq.converse_stream("big", request, meta_event=True)]
    meta = events[-1]["llmq"]["meta"]
    assert meta["provider_request_id"] == _sent_ids(llmq)[-1]
    chat = await llmq.chat("local", {"messages": [{"role": "user", "content": "hi"}]})
    assert chat.meta.provider_request_id == _sent_ids(llmq)[-1]
    ids = [reply.meta.provider_request_id, meta["provider_request_id"]]
    assert len({*ids, chat.meta.provider_request_id}) == 3


async def test_converse_with_request_context(llmq: LLMQueue) -> None:
    with request_context(priority=True, user_id="ann", session_id="s-1"):
        reply = await llmq.converse(
            "big", {"messages": [{"role": "user", "content": [{"text": "hi"}]}]}
        )
    assert reply.ok
    status = await llmq.request_status(reply.request_id)
    assert status["priority"] == "1"
    assert (await llmq.health())["ready"]


async def test_converse_stream(llmq: LLMQueue) -> None:
    """Every event in order; the streamed text and usage equal the non-stream reply."""
    request = {"messages": [{"role": "user", "content": [{"text": "hi"}]}]}
    with request_context(priority=True):
        events = [e async for e in llmq.converse_stream("big", request)]
    whole = await llmq.converse("big", request)
    assert whole.response is not None
    kinds = [next(iter(e)) for e in events]
    assert kinds[0] == "messageStart"
    assert kinds[-3:] == ["contentBlockStop", "messageStop", "metadata"]
    assert set(kinds[1:-3]) == {"contentBlockDelta"}
    text = "".join(e["contentBlockDelta"]["delta"]["text"] for e in events[1:-3])
    assert text == whole.response["output"]["message"]["content"][0]["text"]
    assert events[-2]["messageStop"]["stopReason"] == "end_turn"
    assert events[-1]["metadata"]["usage"] == whole.response["usage"]


async def test_embed_and_jobs(llmq: LLMQueue) -> None:
    result = await llmq.embed("cohere", ["a", "b", "c"], input_type="search_query")
    assert len(result.vectors) == 3
    await llmq.send_job("ingest", {"texts": ["x", "y"]})
    for _ in range(50):
        if llmq.seen:  # type: ignore[attr-defined]
            break
        await asyncio.sleep(0.1)
    assert llmq.seen == [{"payload": {"texts": ["x", "y"]}, "vectors": 2}]  # type: ignore[attr-defined]


async def test_sync_calls_run_direct(llmq: LLMQueue, store: redis.asyncio.Redis) -> None:
    """Sync calls never go through the queue: no message is added to the model's streams."""
    request = {"messages": [{"role": "user", "content": [{"text": "sync"}]}]}
    assert (await llmq.converse("big", request)).ok  # queued: creates the streams
    queued = await _messages_sent(llmq, store, "big")
    assert queued >= 1  # the counter sees the queued call
    reply = await asyncio.to_thread(llmq.converse_sync, "big", request)
    assert reply.ok
    vectors = await asyncio.to_thread(llmq.embed_sync, "titan", ["one"])
    assert len(vectors.vectors) == 1
    assert await _messages_sent(llmq, store, "big") == queued
    assert await _messages_sent(llmq, store, "titan") == 0


async def _messages_sent(llmq: LLMQueue, store: redis.asyncio.Redis, alias: str) -> int:
    """Messages ever added to a model's normal and priority streams (XINFO entries-added)."""
    engine = llmq._engine
    total = 0
    for priority in (False, True):
        name = engine._queue_name(engine.registry.current, alias, priority)
        key = engine.keys.queue(name, "s")
        if await store.exists(key):
            info: dict[Any, Any] = await store.xinfo_stream(key)
            total += int(info.get("entries-added", info.get(b"entries-added", 0)))
    return total


async def test_langchain_bedrock_chat_and_tools(llmq: LLMQueue) -> None:
    from langchain_core.messages import AIMessage
    from langchain_core.tools import tool

    from llm_queue.langchain import QueuedChatBedrockConverse

    model = QueuedChatBedrockConverse(model_alias="big", llmq=llmq)  # type: ignore[call-arg]
    assert model.model_id == "openai.gpt-oss-120b-1:0"  # looked up from the registry
    message = await model.ainvoke("hello")
    assert str(message.content).startswith("fakellm")
    assert message.response_metadata["llmq"]["deployment"] == "aws-a/r1"
    assert "deployment" not in message.response_metadata  # under `llmq` only (0.2.0)
    _assert_request_id(llmq, message.response_metadata)

    @tool
    def lookup(query: str) -> str:
        """Look something up."""
        return query

    called = await model.bind_tools([lookup]).ainvoke("find it")
    assert isinstance(called, AIMessage)
    assert called.tool_calls[0]["name"] == "lookup"
    sync_reply = await asyncio.to_thread(model.invoke, "sync hello")
    assert str(sync_reply.content).startswith("fakellm")


async def test_langchain_queue_streaming(llmq: LLMQueue) -> None:
    """Q49: `queue_streaming=True` streams tokens; the joined chunks equal `ainvoke`; Q1 kept."""
    import functools
    import operator

    from llm_queue import LLMQueueError
    from llm_queue.langchain import QueuedChatBedrockConverse, QueuedChatOpenAI

    bedrock = QueuedChatBedrockConverse(model_alias="big", llmq=llmq, queue_streaming=True)  # type: ignore[call-arg]
    openai_chat = QueuedChatOpenAI(model="local", llmq=llmq, queue_streaming=True)
    for model in (bedrock, openai_chat):
        chunks = [chunk async for chunk in model.astream("hello")]
        assert len(chunks) > 2  # the provider's events, not one whole message
        assert [n for n, c in enumerate(chunks) if c.usage_metadata] == [len(chunks) - 1]
        whole = functools.reduce(operator.add, chunks)
        text: Any = whole.text  # a method on langchain-core 0.3, a property on 1.x
        assert str(text if isinstance(text, str) else text()).startswith("fakellm")
        assert whole.response_metadata["finish_reason"] == "stop"
        assert whole.response_metadata["llmq"]["served_model"] in ("big", "local")
        _assert_request_id(llmq, whole.response_metadata)
        assert whole.usage_metadata is not None
        assert whole.usage_metadata["output_tokens"] > 0
        with pytest.raises(LLMQueueError) as refused:  # a sync call on the queue's own loop
            model.invoke("hello")
        assert "would deadlock" in str(refused.value)


async def test_langchain_bedrock_embeddings(llmq: LLMQueue) -> None:
    from llm_queue.langchain import QueuedBedrockEmbeddings

    emb = QueuedBedrockEmbeddings(
        model_alias="cohere", model_id="cohere.embed-english-v3", llmq=llmq
    )
    docs = await emb.aembed_documents(["a", "b"])
    query = await emb.aembed_query("q")
    assert len(docs) == 2
    assert len(query) == 1024


@pytest.mark.parametrize("llmq", ["httpx", "aiohttp"], indirect=True)
async def test_langchain_openai_chat_and_embeddings(llmq: LLMQueue) -> None:
    from llm_queue.langchain import QueuedChatOpenAI, QueuedOpenAIEmbeddings

    chat = QueuedChatOpenAI(model="local", llmq=llmq)
    message = await chat.ainvoke("hello")
    assert str(message.content).startswith("fakellm")
    assert message.response_metadata["llmq"]["served_model"] == "local"
    _assert_request_id(llmq, message.response_metadata)
    emb = QueuedOpenAIEmbeddings(model="local-emb", llmq=llmq)
    vectors = await emb.aembed_documents(["a", "b", "c"])
    assert len(vectors) == 3


async def test_q40_a_fallback_of_the_other_provider_is_converted(llmq: LLMQueue) -> None:
    """Q40: a fallback model of the other provider got the caller's request unchanged (a
    Converse body at an OpenAI server, or the other way round) and the caller got the other
    shape back. The request and reply are now converted; a stream does not fall back.
    """
    converse = {
        "system": [{"text": "be brief"}],
        "messages": [{"role": "user", "content": [{"text": "hi"}]}],
        "inferenceConfig": {"maxTokens": 50, "temperature": 0.2},
    }
    reply = await llmq.converse("to-openai", converse)
    assert reply.meta.served_model == "local"
    assert reply.response is not None
    assert reply.response["output"]["message"]["content"][0]["text"]
    assert reply.response["stopReason"] == "end_turn"
    assert reply.meta.tokens_out
    assert reply.response["usage"]["outputTokens"] == reply.meta.tokens_out
    chat = {
        "messages": [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "hi"},
        ],
        "max_tokens": 50,
    }
    reply = await llmq.chat("to-bedrock", chat)
    assert reply.meta.served_model == "small"
    assert reply.response is not None
    choice = reply.response["choices"][0]
    assert choice["message"]["content"]
    assert choice["finish_reason"] == "stop"
    assert reply.meta.tokens_out
    assert reply.response["usage"]["completion_tokens"] == reply.meta.tokens_out
    with pytest.raises(CallFailed) as info:
        async for _ in llmq.converse_stream("to-openai", converse):
            pass
    assert info.value.kind == "model_unavailable"


def test_error_mapping() -> None:
    request = httpx.Request("POST", "http://llmq.invalid/v1/chat/completions")
    limited = _error_response(request, CallFailed("rate_limited", "busy", retry_after_secs=2))
    assert limited.status_code == 429
    assert limited.headers["retry-after"] == "2"
    assert _error_response(request, CallFailed("deadline_exceeded")).status_code == 504
    assert _error_response(request, CallFailed("model_unavailable")).status_code == 503
    assert _error_response(request, CallFailed("request_too_large")).status_code == 400
