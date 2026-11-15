"""Token streaming in the LangChain chat adapters (`queue_streaming=True`, owner Q49, Q1).

A fake queue replays scripted provider events, so these run without a store: text deltas,
tool-call chunks, usage once on the last chunk, mapped stop reasons, a mid-stream `CallFailed`
with its partial text, closing the queue's stream when the consumer stops, and the queue's own
refusal of a sync call surfaced unchanged.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import operator
from collections.abc import AsyncIterator, Mapping
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("langchain_core")

from langchain_core.messages import AIMessageChunk
from langchain_core.outputs import ChatGenerationChunk

from llm_queue import CallFailed, LLMQueueError

# The message `LLMQueue._run_sync` refuses a sync call on the queue's own loop with (Q1).
Q1_MESSAGE = (
    "a sync call on the thread that runs this queue's event loop would deadlock; await it "
    "there instead: `await llmq.converse(...)` (or chat / embed), and in LangChain "
    "`await chat.ainvoke(...)`"
)
BEDROCK_ID = "openai.gpt-oss-120b-1:0"
META = {"served_model": "big", "deployment": "aws-a/r1", "fallback": None, "attempts": 1}
USAGE = {"inputTokens": 7, "outputTokens": 5, "cacheReadInputTokens": 0}


class FakeQueue:
    """The adapters' queue methods; streams replay `events`, then raise `fail` if set."""

    registry = None

    def __init__(self, events: list[dict[str, Any]], fail: Exception | None = None) -> None:
        self.events = events
        self.fail = fail
        self.calls: list[tuple[str, str, dict[str, Any], dict[str, Any]]] = []
        self.closed = False
        self.sync_error: Exception | None = None
        self.gate: asyncio.Event | None = None  # set: the stream waits on it after one event

    async def _stream(
        self, op: str, model: str, request: Mapping[str, Any], kw: dict[str, Any]
    ) -> AsyncIterator[dict[str, Any]]:
        self.calls.append((op, model, dict(request), kw))
        try:
            for n, event in enumerate(self.events):
                if n == 1 and self.gate is not None:
                    await self.gate.wait()
                yield event
            if self.fail is not None:
                raise self.fail
        finally:
            self.closed = True

    def converse_stream(
        self, model: str, request: Mapping[str, Any], **kw: Any
    ) -> AsyncIterator[dict[str, Any]]:
        return self._stream("converse_stream", model, request, kw)

    def chat_stream(
        self, model: str, request: Mapping[str, Any], **kw: Any
    ) -> AsyncIterator[dict[str, Any]]:
        return self._stream("chat_stream", model, request, kw)

    def _reply(self, response: dict[str, Any]) -> Any:
        meta = SimpleNamespace(served_model="big", model_dump=lambda **_: dict(META))
        return SimpleNamespace(response=response, meta=meta)

    async def converse(self, model: str, request: Mapping[str, Any], **kw: Any) -> Any:
        self.calls.append(("converse", model, dict(request), kw))
        return self._reply(_converse_body("whole"))

    def converse_sync(self, model: str, request: Mapping[str, Any], **kw: Any) -> Any:
        if self.sync_error is not None:
            raise self.sync_error
        return self._reply(_converse_body("whole"))

    async def chat(self, model: str, request: Mapping[str, Any], **kw: Any) -> Any:
        self.calls.append(("chat", model, dict(request), kw))
        return self._reply(_chat_body("whole"))

    def chat_sync(self, model: str, request: Mapping[str, Any], **kw: Any) -> Any:
        if self.sync_error is not None:
            raise self.sync_error
        return self._reply(_chat_body("whole"))

    async def embed(self, *_: Any, **__: Any) -> Any: ...

    def embed_sync(self, *_: Any, **__: Any) -> Any: ...


def _converse_body(text: str) -> dict[str, Any]:
    return {
        "output": {"message": {"role": "assistant", "content": [{"text": text}]}},
        "stopReason": "end_turn",
        "usage": {"inputTokens": 1, "outputTokens": 1, "totalTokens": 2},
        "metrics": {"latencyMs": 1},
    }


def _chat_body(text: str) -> dict[str, Any]:
    return {
        "id": "c0",
        "object": "chat.completion",
        "created": 0,
        "model": "m",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }


def _bedrock_events(meta: bool = True) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = [
        {"messageStart": {"role": "assistant"}},
        {"contentBlockDelta": {"delta": {"text": "Hel"}, "contentBlockIndex": 0}},
        {"contentBlockDelta": {"delta": {"text": "lo"}, "contentBlockIndex": 0}},
        {"contentBlockStop": {"contentBlockIndex": 0}},
        {
            "contentBlockStart": {
                "start": {"toolUse": {"toolUseId": "t1", "name": "lookup"}},
                "contentBlockIndex": 1,
            }
        },
        {"contentBlockDelta": {"delta": {"toolUse": {"input": '{"q": '}}, "contentBlockIndex": 1}},
        {"contentBlockDelta": {"delta": {"toolUse": {"input": '"x"}'}}, "contentBlockIndex": 1}},
        {"contentBlockStop": {"contentBlockIndex": 1}},
        {"messageStop": {"stopReason": "tool_use"}},
        {"metadata": {"usage": {**USAGE, "totalTokens": 12}, "metrics": {"latencyMs": 9}}},
    ]
    if meta:
        events.append({"llmq": {"usage": USAGE, "meta": META}})
    return events


def _openai_chunk(delta: dict[str, Any] | None, finish: str | None = None) -> dict[str, Any]:
    choices = [] if delta is None else [{"index": 0, "delta": delta, "finish_reason": finish}]
    return {"id": "c1", "object": "chat.completion.chunk", "created": 0, "model": "m-id",
            "choices": choices}  # fmt: skip


def _openai_events() -> list[dict[str, Any]]:
    call = {"index": 0, "id": "call_1", "type": "function",
            "function": {"name": "lookup", "arguments": ""}}  # fmt: skip
    usage_chunk = _openai_chunk(None)
    usage_chunk["usage"] = {"prompt_tokens": 7, "completion_tokens": 5, "total_tokens": 12}
    return [
        _openai_chunk({"role": "assistant", "content": "Hel"}),
        _openai_chunk({"content": "lo"}),
        _openai_chunk({"tool_calls": [call]}),
        _openai_chunk({"tool_calls": [{"index": 0, "function": {"arguments": '{"q": '}}]}),
        _openai_chunk({"tool_calls": [{"index": 0, "function": {"arguments": '"x"}'}}]}),
        _openai_chunk({}, finish="tool_calls"),
        usage_chunk,
        {"llmq": {"usage": USAGE, "meta": META}},
    ]


def _bedrock(queue: FakeQueue, **kw: Any) -> Any:
    pytest.importorskip("langchain_aws")
    from llm_queue.langchain.bedrock import QueuedChatBedrockConverse

    return QueuedChatBedrockConverse(
        model_alias="big", model_id=BEDROCK_ID, region_name="us-east-1", llmq=queue, **kw
    )


def _openai(queue: FakeQueue, **kw: Any) -> Any:
    pytest.importorskip("langchain_openai")
    from llm_queue.langchain.openai import QueuedChatOpenAI

    return QueuedChatOpenAI(model="m", llmq=queue, **kw)


async def _collect(model: Any, text: str = "hi") -> list[AIMessageChunk]:
    return [chunk async for chunk in model.astream(text)]


def _usage_positions(chunks: list[AIMessageChunk]) -> list[int]:
    return [n for n, chunk in enumerate(chunks) if chunk.usage_metadata]


# ------------------------------------------------------------------------------ off (default)


async def test_streaming_off_by_default_yields_one_whole_message() -> None:
    queue = FakeQueue(_bedrock_events())
    chunks = await _collect(_bedrock(queue))
    assert "".join(c.text for c in chunks) == "whole"
    assert [c[0] for c in queue.calls] == ["converse"]
    openai_queue = FakeQueue(_openai_events())
    chunks = await _collect(_openai(openai_queue))
    assert "".join(c.text for c in chunks) == "whole"
    assert [c[0] for c in openai_queue.calls] == ["chat"]


# ------------------------------------------------------------------------------ Bedrock


async def test_bedrock_streams_text_tools_usage_and_stop_reason() -> None:
    queue = FakeQueue(_bedrock_events())
    chunks = await _collect(_bedrock(queue, queue_streaming=True, max_tokens=50))
    (op, alias, request, kw) = queue.calls[0]
    assert (op, alias) == ("converse_stream", "big")
    assert "modelId" not in request
    assert request["messages"][0]["content"][0]["text"] == "hi"
    assert request["inferenceConfig"]["maxTokens"] == 50
    assert kw["meta_event"] is True
    assert [c.text for c in chunks if c.text] == ["Hel", "lo"]
    tool_chunks = [tc for c in chunks for tc in c.tool_call_chunks]
    assert tool_chunks[0]["id"] == "t1"
    assert tool_chunks[0]["name"] == "lookup"
    assert {tc["index"] for tc in tool_chunks} == {1}
    assert "".join(tc["args"] or "" for tc in tool_chunks) == '{"q": "x"}'
    assert _usage_positions(chunks) == [len(chunks) - 1]  # once, on the last chunk
    whole = functools.reduce(operator.add, chunks)
    assert whole.tool_calls == [
        {"name": "lookup", "args": {"q": "x"}, "id": "t1", "type": "tool_call"}
    ]
    assert whole.usage_metadata is not None
    assert whole.usage_metadata["input_tokens"] == 7
    assert whole.usage_metadata["output_tokens"] == 5
    assert whole.response_metadata["stopReason"] == "tool_use"
    assert whole.response_metadata["finish_reason"] == "tool_calls"
    assert whole.response_metadata["deployment"] == "aws-a/r1"
    assert whole.response_metadata["model_name"] == BEDROCK_ID


async def test_bedrock_usage_from_the_closing_event_when_the_provider_sent_none() -> None:
    events = [e for e in _bedrock_events() if "metadata" not in e]
    chunks = await _collect(_bedrock(FakeQueue(events), queue_streaming=True))
    assert _usage_positions(chunks) == [len(chunks) - 1]
    assert chunks[-1].usage_metadata is not None
    assert chunks[-1].usage_metadata["total_tokens"] == 12
    # A queue without the closing event: the provider's usage still comes last.
    chunks = await _collect(_bedrock(FakeQueue(_bedrock_events(meta=False)), queue_streaming=True))
    assert _usage_positions(chunks) == [len(chunks) - 1]


async def test_bedrock_astream_events_streams() -> None:
    """LangGraph's message mode streams through `ainvoke` with a streaming handler."""
    queue = FakeQueue(_bedrock_events())
    model = _bedrock(queue, queue_streaming=True)
    tokens = [
        event["data"]["chunk"].text
        async for event in model.astream_events("hi", version="v2")
        if event["event"] == "on_chat_model_stream"
    ]
    assert "Hel" in tokens
    assert queue.calls[0][0] == "converse_stream"


async def test_bedrock_disable_streaming_for_tools_is_kept() -> None:
    queue = FakeQueue(_bedrock_events())
    model = _bedrock(queue, queue_streaming=True, disable_streaming="tool_calling")
    lookup = {"name": "lookup", "description": "Look up.", "parameters": {"type": "object"}}
    await _collect(model.bind_tools([lookup]))
    assert queue.calls[0][0] == "converse"


# ------------------------------------------------------------------------------ OpenAI


async def test_openai_streams_text_tools_usage_and_finish_reason() -> None:
    queue = FakeQueue(_openai_events())
    chunks = await _collect(_openai(queue, queue_streaming=True, model_alias="local"))
    (op, alias, request, kw) = queue.calls[0]
    assert (op, alias) == ("chat_stream", "local")
    assert not {"model", "stream", "stream_options"} & set(request)
    assert request["messages"] == [{"role": "user", "content": "hi"}]
    assert kw["meta_event"] is True
    assert [c.text for c in chunks if c.text] == ["Hel", "lo"]
    tool_chunks = [tc for c in chunks for tc in c.tool_call_chunks]
    assert tool_chunks[0]["id"] == "call_1"
    assert tool_chunks[0]["name"] == "lookup"
    assert {tc["index"] for tc in tool_chunks} == {0}
    assert _usage_positions(chunks) == [len(chunks) - 1]
    whole = functools.reduce(operator.add, chunks)
    assert whole.tool_calls[0]["args"] == {"q": "x"}
    assert whole.usage_metadata is not None
    assert whole.usage_metadata["total_tokens"] == 12
    assert whole.response_metadata["finish_reason"] == "tool_calls"
    assert whole.response_metadata["served_model"] == "big"


async def test_openai_alias_defaults_to_the_model() -> None:
    queue = FakeQueue(_openai_events())
    await _collect(_openai(queue, queue_streaming=True))
    assert queue.calls[0][1] == "m"


# ------------------------------------------------------------------------------ failures


@pytest.mark.parametrize("which", ["bedrock", "openai"])
async def test_call_failed_mid_stream_keeps_the_partial_text(which: str) -> None:
    events = _bedrock_events()[:3] if which == "bedrock" else _openai_events()[:2]
    failure = CallFailed("provider_error", "reset", partial_text="Hello")
    queue = FakeQueue(events, fail=failure)
    make = _bedrock if which == "bedrock" else _openai
    model = make(queue, queue_streaming=True)
    seen: list[str] = []

    async def consume() -> None:
        async for chunk in model.astream("hi"):
            seen.append(chunk.text)

    with pytest.raises(CallFailed) as caught:
        await consume()
    assert caught.value is failure
    assert caught.value.partial_text == "Hello"
    assert "".join(seen) == "Hello"
    assert queue.closed


async def test_consumer_that_stops_closes_the_queue_stream() -> None:
    from langchain_core.messages import HumanMessage

    queue = FakeQueue(_bedrock_events())
    model = _bedrock(queue, queue_streaming=True)
    stream = model._astream([HumanMessage("hi")])
    async with contextlib.aclosing(stream):
        async for _chunk in stream:
            break
    assert queue.closed  # at once, not left to the async generator finalizer


async def test_cancelled_consumer_closes_the_queue_stream() -> None:
    queue = FakeQueue(_openai_events())
    queue.gate = asyncio.Event()  # never set: the stream waits after its first event
    model = _openai(queue, queue_streaming=True)
    first = asyncio.Event()

    async def consume() -> None:
        async for _chunk in model.astream("hi"):
            first.set()

    task = asyncio.create_task(consume())
    await asyncio.wait_for(first.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert queue.closed


async def test_a_queue_without_stream_methods_is_named() -> None:
    queue = FakeQueue([])
    model = _bedrock(queue, queue_streaming=True)
    model.llmq = SimpleNamespace(**{n: getattr(queue, n) for n in ("converse", "converse_sync")})
    with pytest.raises(LLMQueueError, match=r"needs a queue with converse_stream\(\)"):
        await _collect(model)


# ------------------------------------------------------------------------------ Q1 (sync)


@pytest.mark.parametrize("which", ["bedrock", "openai"])
@pytest.mark.parametrize("streaming", [False, True])
def test_sync_calls_surface_the_queue_refusal_unchanged(which: str, streaming: bool) -> None:
    """Q1: the queue refuses a sync call on its own loop; the adapter keeps its message."""
    queue = FakeQueue([])
    queue.sync_error = LLMQueueError(Q1_MESSAGE)
    make = _bedrock if which == "bedrock" else _openai
    model = make(queue, queue_streaming=streaming)
    for run in (lambda: model.invoke("hi"), lambda: list(model.stream("hi"))):
        with pytest.raises(LLMQueueError) as caught:
            run()
        assert str(caught.value) == Q1_MESSAGE
    assert not [c for c in queue.calls if c[0].endswith("_stream")]  # sync never streams


def test_sync_stream_yields_one_whole_message() -> None:
    queue = FakeQueue(_bedrock_events())
    chunks = list(_bedrock(queue, queue_streaming=True).stream("hi"))
    assert "".join(c.text for c in chunks) == "whole"


def test_result_chunk_keeps_tool_calls() -> None:
    from langchain_core.messages import AIMessage
    from langchain_core.outputs import ChatGeneration, ChatResult

    from llm_queue.langchain._stream import result_chunk

    message = AIMessage(
        content="", tool_calls=[{"name": "f", "args": {"a": 1}, "id": "x", "type": "tool_call"}]
    )
    chunk = result_chunk(ChatResult(generations=[ChatGeneration(message=message)]))
    assert isinstance(chunk, ChatGenerationChunk)
    assert isinstance(chunk.message, AIMessageChunk)
    assert chunk.message.tool_calls[0]["args"] == {"a": 1}
