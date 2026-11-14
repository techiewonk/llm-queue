"""Real-service shapes fakellm adds for 0.1.0b2 (RESCAN T4), checked through the real SDKs.

Reasoning blocks, split tool input, in-stream exceptions, stalls and closes, auth / quota
errors with their real messages, `force_message`, cache usage, a heavy latency tail and
prompt-length latency. Each test fails on the fakellm of 0.1.0b1.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from contextlib import AsyncExitStack
from typing import Any

import openai
import pytest
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.exceptions import ClientError, EventStreamError, ReadTimeoutError
from fakellm import ERRORS, Engine, Scenario
from fakellm.scenario import STREAM_EXCEPTIONS
from fakellm_testkit import Fake, MakeBedrock, StartFake
from openai import AsyncOpenAI
from types_aiobotocore_bedrock_runtime import BedrockRuntimeClient

GPT = "openai.gpt-oss-120b-1:0"
MSGS: Any = [{"role": "user", "content": [{"text": "hello there"}]}]
OA_MSGS: Any = [{"role": "user", "content": "hi there"}]
TOOLS: Any = {
    "tools": [
        {
            "toolSpec": {
                "name": "lookup",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {"query": {"type": "string"}, "limit": {"type": "integer"}},
                    }
                },
            }
        }
    ]
}
OA_TOOLS: Any = [
    {
        "type": "function",
        "function": {
            "name": "lookup",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}, "limit": {"type": "integer"}},
            },
        },
    }
]
OpenAIClient = Callable[[Fake], AsyncOpenAI]


async def _bedrock(stack: AsyncExitStack, fake: Fake, read_timeout: float = 60) -> Any:
    client = get_session().create_client(
        "bedrock-runtime",
        region_name="ap-south-1",
        endpoint_url=fake.url,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=AioConfig(
            retries={"total_max_attempts": 1, "mode": "standard"}, read_timeout=read_timeout
        ),
    )
    return await stack.enter_async_context(client)


async def _events(client: BedrockRuntimeClient, **kwargs: Any) -> list[dict[str, Any]]:
    reply = await client.converse_stream(modelId=GPT, messages=MSGS, **kwargs)
    try:
        return [dict(event) async for event in reply["stream"]]
    finally:
        reply["stream"].close()  # a failed stream must not leave its connection to the GC


async def test_converse_reasoning_blocks(start_fake: StartFake, make_bedrock: MakeBedrock) -> None:
    """`reasoning: both` puts a reasoningText and a redactedContent block before the text."""
    client = await make_bedrock(await start_fake({"defaults": {"reasoning": "both"}}))
    reply = await client.converse(modelId=GPT, messages=MSGS)
    content: list[Any] = reply["output"]["message"]["content"]
    assert [next(iter(b)) for b in content] == ["reasoningContent", "reasoningContent", "text"]
    thought = content[0]["reasoningContent"]["reasoningText"]
    assert thought["text"].startswith("fakellm reasoning")
    assert thought["signature"]
    assert isinstance(content[1]["reasoningContent"]["redactedContent"], bytes)
    plain = await make_bedrock(await start_fake())
    without = await plain.converse(modelId=GPT, messages=MSGS)
    assert reply["usage"]["outputTokens"] > without["usage"]["outputTokens"]


async def test_stream_events_reasoning_then_text(
    start_fake: StartFake, make_bedrock: MakeBedrock
) -> None:
    """Every ConverseStream event in order; the stream rebuilds exactly the Converse reply."""
    client = await make_bedrock(
        await start_fake({"defaults": {"reasoning": "both", "output_tokens": 20}})
    )
    whole = await client.converse(modelId=GPT, messages=MSGS)
    events = await _events(client)
    kinds = [next(iter(e)) for e in events]
    assert kinds[0] == "messageStart"
    assert kinds[-2:] == ["messageStop", "metadata"]
    assert kinds.count("contentBlockStop") == 3
    reasoning, signature, redacted, text = "", "", b"", ""
    for event in events:
        delta = event.get("contentBlockDelta", {}).get("delta", {})
        part = delta.get("reasoningContent", {})
        reasoning += part.get("text", "")
        signature = part.get("signature", signature)
        redacted += part.get("redactedContent", b"")
        text += delta.get("text", "")
    blocks: list[Any] = whole["output"]["message"]["content"]
    assert reasoning == blocks[0]["reasoningContent"]["reasoningText"]["text"]
    assert signature == blocks[0]["reasoningContent"]["reasoningText"]["signature"]
    assert redacted == blocks[1]["reasoningContent"]["redactedContent"]
    assert text == blocks[2]["text"]
    assert events[-1]["metadata"]["usage"] == whole["usage"]


async def test_stream_tool_input_split_across_deltas(
    start_fake: StartFake, make_bedrock: MakeBedrock
) -> None:
    """Tool input arrives in several deltas, as Bedrock sends it; joined it equals Converse."""
    client = await make_bedrock(await start_fake())
    whole = await client.converse(modelId=GPT, messages=MSGS, toolConfig=TOOLS)
    events = await _events(client, toolConfig=TOOLS)
    pieces = [
        e["contentBlockDelta"]["delta"]["toolUse"]["input"]
        for e in events
        if "contentBlockDelta" in e
    ]
    assert len(pieces) > 1
    assert (
        json.loads("".join(pieces)) == whole["output"]["message"]["content"][0]["toolUse"]["input"]
    )


@pytest.mark.parametrize("name", sorted(STREAM_EXCEPTIONS))
async def test_stream_exception_mid_stream(
    name: str, start_fake: StartFake, make_bedrock: MakeBedrock
) -> None:
    """`cut_with: <exception>` ends the stream with that exception; `force_message` is used."""
    fake = await start_fake(
        {
            "defaults": {
                "output_tokens": 60,
                "cut_after_chunks": 2,
                "cut_with": name,
                "force_message": "custom text",
            }
        }
    )
    client = await make_bedrock(fake)
    with pytest.raises(EventStreamError) as info:
        await _events(client)
    assert info.value.response["Error"]["Code"] == name
    assert info.value.response["Error"]["Message"] == "custom text"


async def test_stream_stall_and_close(start_fake: StartFake, stack: AsyncExitStack) -> None:
    """`stall` stops sending (the idle timer fires); `close` drops the connection mid-body."""
    stalled = await _bedrock(
        stack,
        await start_fake(
            {"defaults": {"output_tokens": 60, "cut_after_chunks": 1, "cut_with": "stall"}}
        ),
        read_timeout=0.5,
    )
    with pytest.raises((ReadTimeoutError, TimeoutError)):  # aiohttp timeout, unwrapped
        await _events(stalled)
    closed = await _bedrock(
        stack,
        await start_fake(
            {"defaults": {"output_tokens": 60, "cut_after_chunks": 1, "cut_with": "close"}}
        ),
    )
    with pytest.raises(Exception) as info:  # noqa: PT011  # botocore wraps the aiohttp error
        await _events(closed)
    assert not isinstance(info.value, ClientError | ReadTimeoutError)


@pytest.mark.parametrize(
    "name",
    [
        "InvalidSignatureException",
        "InvalidSignatureClockSkew",
        "ExpiredTokenException",
        "UnrecognizedClientException",
        "ServiceQuotaExceededException",
    ],
)
async def test_auth_and_quota_errors_real_messages(
    name: str, start_fake: StartFake, make_bedrock: MakeBedrock
) -> None:
    """Auth and quota errors carry the AWS code and text the classifier reads."""
    client = await make_bedrock(await start_fake({"defaults": {"force_error": name}}))
    with pytest.raises(ClientError) as info:
        await client.converse(modelId=GPT, messages=MSGS)
    error = info.value.response["Error"]
    assert error["Code"] == ERRORS[name].wire_code(name)
    assert error["Message"] == ERRORS[name].message
    if name == "InvalidSignatureClockSkew":
        assert error["Message"].startswith("Signature expired")


async def test_force_message_on_http_errors(
    start_fake: StartFake, make_bedrock: MakeBedrock, make_openai: OpenAIClient
) -> None:
    fake = await start_fake(
        {"defaults": {"force_error": "ValidationException", "force_message": "Input is too long"}}
    )
    client = await make_bedrock(fake)
    with pytest.raises(ClientError, match="Input is too long"):
        await client.converse(modelId=GPT, messages=MSGS)
    with pytest.raises(openai.BadRequestError, match="Input is too long"):
        await make_openai(fake).chat.completions.create(model="m", messages=OA_MSGS)


async def test_openai_insufficient_quota(start_fake: StartFake, make_openai: OpenAIClient) -> None:
    """insufficient_quota: 429 with its own type / code and no Retry-After (waiting won't help)."""
    client = make_openai(
        await start_fake({"defaults": {"force_error": "ServiceQuotaExceededException"}})
    )
    with pytest.raises(openai.RateLimitError) as info:
        await client.chat.completions.create(model="m", messages=OA_MSGS)
    assert info.value.status_code == 429
    assert info.value.code == "insufficient_quota"
    assert "exceeded your current quota" in info.value.message
    assert "retry-after" not in info.value.response.headers


async def _read_text(stream: Any, parts: list[str]) -> None:
    async for chunk in stream:
        if chunk.choices and chunk.choices[0].delta.content:
            parts.append(chunk.choices[0].delta.content)


async def test_openai_mid_stream_error_event(
    start_fake: StartFake, make_openai: OpenAIClient
) -> None:
    """A `data: {"error": ...}` event mid-stream: the SDK raises APIError without a status."""
    client = make_openai(
        await start_fake(
            {
                "defaults": {
                    "output_tokens": 60,
                    "cut_after_chunks": 1,
                    "cut_with": "internalServerException",
                }
            }
        )
    )
    stream = await client.chat.completions.create(model="m", messages=OA_MSGS, stream=True)
    parts: list[str] = []
    with pytest.raises(openai.APIError) as info:
        await _read_text(stream, parts)
    assert parts  # something reached the caller before the error
    assert not isinstance(info.value, openai.APIStatusError)
    assert info.value.message == ERRORS["InternalServerException"].message


async def test_openai_stream_tool_calls_split(
    start_fake: StartFake, make_openai: OpenAIClient
) -> None:
    """Tool calls stream as id + name first, then the arguments in several pieces."""
    client = make_openai(await start_fake())
    whole = await client.chat.completions.create(model="m", messages=OA_MSGS, tools=OA_TOOLS)
    calls = whole.choices[0].message.tool_calls or []
    stream = await client.chat.completions.create(
        model="m", messages=OA_MSGS, tools=OA_TOOLS, stream=True
    )
    pieces, ids, finish = [], [], None
    async for chunk in stream:
        for call in chunk.choices[0].delta.tool_calls or []:
            ids.append(call.id)
            pieces.append(call.function.arguments if call.function else "")
        finish = chunk.choices[0].finish_reason or finish
    assert len(pieces) > 2
    assert ids[0] == calls[0].id
    assert set(ids[1:]) == {None}
    assert "".join(p or "" for p in pieces) == calls[0].function.arguments  # type: ignore[union-attr]
    assert finish == "tool_calls"


@pytest.mark.parametrize("mode", ["stall", "close"])
async def test_openai_stream_stall_and_close(mode: str, start_fake: StartFake) -> None:
    fake = await start_fake(
        {"defaults": {"output_tokens": 60, "cut_after_chunks": 1, "cut_with": mode}}
    )
    client = AsyncOpenAI(base_url=f"{fake.url}/v1", api_key="t", max_retries=0, timeout=0.5)
    try:
        stream = await client.chat.completions.create(model="m", messages=OA_MSGS, stream=True)
        with pytest.raises(Exception) as info:  # noqa: PT011  # httpx read / protocol errors
            await _read_text(stream, [])
        assert not isinstance(info.value, openai.APIStatusError)
    finally:
        await client.close()
        await asyncio.sleep(0)


async def test_cache_usage_tokens(
    start_fake: StartFake, make_bedrock: MakeBedrock, make_openai: OpenAIClient
) -> None:
    """Bedrock reports cache tokens beside inputTokens; OpenAI counts them inside prompt_tokens."""
    fake = await start_fake({"defaults": {"cache_read_tokens": 100, "cache_write_tokens": 7}})
    reply = await (await make_bedrock(fake)).converse(modelId=GPT, messages=MSGS)
    usage = reply["usage"]
    assert usage["cacheReadInputTokens"] == 100
    assert usage["cacheWriteInputTokens"] == 7
    assert usage["totalTokens"] == usage["inputTokens"] + usage["outputTokens"] + 107
    chat = await make_openai(fake).chat.completions.create(model="m", messages=OA_MSGS)
    assert chat.usage is not None
    assert chat.usage.prompt_tokens_details is not None
    assert chat.usage.prompt_tokens_details.cached_tokens == 100
    assert chat.usage.prompt_tokens >= 101


class _Clock:
    def __init__(self) -> None:
        self.slept: list[float] = []

    def __call__(self) -> float:
        return 0.0

    async def sleep(self, secs: float) -> None:
        self.slept.append(secs)


async def test_latency_tail_and_prompt_length() -> None:
    """A heavy tail hits about `tail_rate` of calls; latency grows with the input tokens."""
    clock = _Clock()
    raw = {"defaults": {"latency_ms": {"p50": 10, "p95": 20}, "tail_rate": 0.05, "tail_ms": 5000}}
    engine = Engine(Scenario.from_mapping(raw), seed=3, clock=clock, sleep=clock.sleep)
    for _ in range(2000):
        await engine.call("k")
    tail = sum(1 for s in clock.slept if s >= 5.0)
    assert 50 < tail < 150
    # The tail RNG is separate: without the tail the main sequence is unchanged.
    plain_clock, tail_off = _Clock(), {"defaults": {"latency_ms": {"p50": 10, "p95": 20}}}
    plain = Engine(
        Scenario.from_mapping(tail_off), seed=3, clock=plain_clock, sleep=plain_clock.sleep
    )
    for _ in range(2000):
        await plain.call("k")
    assert [a for a, b in zip(clock.slept, plain_clock.slept, strict=True) if a != b] == [
        s for s in clock.slept if s >= 5.0
    ]
    long_clock = _Clock()
    per_k = {"defaults": {"latency_ms": {"p50": 0, "p95": 0}, "latency_per_1k_input_ms": 100}}
    sized = Engine(Scenario.from_mapping(per_k), clock=long_clock, sleep=long_clock.sleep)
    await sized.call("k", input_tokens=1000)
    await sized.call("k", input_tokens=20000)
    assert long_clock.slept == pytest.approx([0.1, 2.0])
