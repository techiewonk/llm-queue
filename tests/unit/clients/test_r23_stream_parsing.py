"""Stream assembly and parsing fixes of the round-3 review (R3PF-1, R3PF-4, R3ST-*, R3SE-1).

Each test names its finding: quadratic text joins, every stream event kept until the end,
partial tool JSON, chunks without `delta`, tool calls without `index`, non-object frames,
the wire shape of OpenAI chunks, and caller params that override protected fields.
"""

from __future__ import annotations

import gc
import time
import weakref
from collections.abc import AsyncGenerator, Callable, Mapping
from types import SimpleNamespace
from typing import Any

import pytest
from botocore.eventstream import EventStreamBuffer
from fakellm import eventstream
from openai.types.chat import ChatCompletionChunk

from llm_queue.clients import BedrockProvider, ClientPool, Timing
from llm_queue.clients.bedrock_http import _event
from llm_queue.clients.providers import OpenAIProvider, _chat_body, _converse_body
from llm_queue.engine.streaming import ChunkWriter, text_of
from llm_queue.providers import StreamTruncated

FAST = Timing(5, 5)


def _delta(index: int, part: dict[str, Any]) -> dict[str, Any]:
    return {"contentBlockDelta": {"contentBlockIndex": index, "delta": part}}


def _text_events(n: int) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = [{"messageStart": {"role": "assistant"}}]
    events += [_delta(0, {"text": "abcdefghijklmnop"}) for _ in range(n)]
    events.append({"messageStop": {"stopReason": "end_turn"}})
    return events


def _best_secs(fn: Callable[[], Any], reps: int = 3) -> float:
    best = float("inf")
    for _ in range(reps):
        started = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - started)
    return best


def test_streamed_text_is_joined_in_linear_time() -> None:
    """R3PF-1: each delta built a new string (`text + delta`): quadratic in the answer.

    Parts are joined once now: 8x the deltas take about 8x the time, not about 64x.
    """
    small, large = _text_events(4_000), _text_events(32_000)
    assert (
        _converse_body(large)["output"]["message"]["content"][0]["text"]
        == "abcdefghijklmnop" * 32_000
    )
    ratio = _best_secs(lambda: _converse_body(large)) / _best_secs(lambda: _converse_body(small))
    # Linear is about 8x, the old quadratic join 64x; 40 keeps it caught with room for a
    # noisy shared runner (GitHub CI measured 25x once).
    assert ratio < 40, f"32,000 deltas took {ratio:.1f}x the time of 4,000"


class _Event(dict[str, Any]):
    """A dict that can be weakly referenced (to see whether the provider keeps it)."""


class _Chunk(SimpleNamespace):
    """A chat chunk that can be weakly referenced."""


async def test_bedrock_stream_events_are_not_kept_until_the_end() -> None:
    """R3PF-4: every ConverseStream event stayed in a list until the stream ended (45x text).

    Each event is folded in as it arrives: an event two steps back is already freed.
    """
    refs: list[weakref.ref[_Event]] = []
    kept: list[bool] = []

    async def stream(*_: Any) -> AsyncGenerator[Mapping[str, Any], None]:
        for raw in _text_events(20):
            event = _Event(raw)
            refs.append(weakref.ref(event))
            yield event
            del event

    async def sink(_: Any) -> None:
        gc.collect()
        if len(refs) > 2:
            kept.append(refs[-3]() is not None)

    provider = BedrockProvider(ClientPool())
    provider.stream = stream  # type: ignore[method-assign,assignment]  # a fake stream, no client
    result = await provider._converse_stream({}, None, FAST, sink)  # type: ignore[arg-type]
    assert result.response["output"]["message"]["content"][0]["text"]
    assert kept
    assert not any(kept)


def _choice(**fields: Any) -> SimpleNamespace:
    fields.setdefault("index", 0)
    fields.setdefault("delta", SimpleNamespace(content=None, tool_calls=None, refusal=None))
    fields.setdefault("logprobs", None)
    fields.setdefault("finish_reason", None)
    return SimpleNamespace(**fields)


def _text_chunk(text: str, finish: str | None = None) -> _Chunk:
    delta = SimpleNamespace(content=text, tool_calls=None, refusal=None)
    choice = _choice(delta=delta, finish_reason=finish)
    return _Chunk(id="c", model="m", created=1, usage=None, choices=[choice])


async def test_openai_chunks_are_not_kept_until_the_end() -> None:
    """R3PF-4: every chat chunk (2.4 KB each) stayed in a list until the stream ended."""
    refs: list[weakref.ref[_Chunk]] = []
    kept: list[bool] = []

    async def stream(*_: Any) -> AsyncGenerator[Any, None]:
        for i in range(20):
            chunk = _text_chunk("x", "stop" if i == 19 else None)
            refs.append(weakref.ref(chunk))
            yield chunk
            del chunk

    async def sink(_: Any) -> None:
        gc.collect()
        if len(refs) > 2:
            kept.append(refs[-3]() is not None)

    provider = OpenAIProvider(ClientPool())
    provider.stream = stream  # type: ignore[method-assign,assignment]  # a fake stream, no client
    result = await provider._chat({}, None, FAST, sink)  # type: ignore[arg-type]
    assert result.response["choices"][0]["message"]["content"] == "x" * 20
    assert kept
    assert not any(kept)


def test_a_cut_tool_input_keeps_the_reply() -> None:
    """R3ST-4: partial tool JSON (cut by max_tokens) raised JSONDecodeError, FATAL.

    The call failed as `provider_error` after the whole stream reached the caller. The raw
    text is kept as the tool's input now. A text delta and a tool delta on one index raised
    `KeyError: 'toolUse'`; that reply is assembled too.
    """
    start = {"contentBlockStart": {"contentBlockIndex": 0, "start": {"toolUse": {"name": "f"}}}}
    body = _converse_body(
        [
            {"messageStart": {"role": "assistant"}},
            start,
            _delta(0, {"toolUse": {"input": '{"city": "Mum'}}),
            {"messageStop": {"stopReason": "max_tokens"}},
        ]
    )
    assert body["stopReason"] == "max_tokens"
    assert body["output"]["message"]["content"][0]["toolUse"]["input"] == '{"city": "Mum'
    mixed = _converse_body(
        [
            _delta(0, {"text": "a"}),
            _delta(0, {"toolUse": {"input": "{}"}}),
            {"messageStop": {"stopReason": "tool_use"}},
        ]
    )
    assert mixed["output"]["message"]["content"][0]["toolUse"]["input"] == {}


def test_a_chunk_without_delta_is_skipped() -> None:
    """R3ST-5: Azure content-filter chunks have no `delta`: AttributeError, FATAL, every call."""
    chunks = [
        _text_chunk("hi"),
        _Chunk(id="c", model="m", created=1, usage=None, choices=[_choice(delta=None)]),
        _Chunk(id="c", model="m", created=1, usage=None, choices=[SimpleNamespace(index=0)]),
        _text_chunk("!", "stop"),
    ]
    assert _chat_body(chunks)["choices"][0]["message"]["content"] == "hi!"


def test_tool_calls_without_index_stay_apart() -> None:
    """R3ST-11: calls sent without `index` merged into one (name `f1f2`, arguments `{}{}`)."""

    def call(ident: str | None, name: str | None, args: str) -> SimpleNamespace:
        function = SimpleNamespace(name=name, arguments=args)
        return SimpleNamespace(index=None, id=ident, function=function)

    def chunk(*calls: SimpleNamespace, finish: str | None = None) -> _Chunk:
        delta = SimpleNamespace(content=None, tool_calls=list(calls), refusal=None)
        choice = _choice(delta=delta, finish_reason=finish)
        return _Chunk(id="c", model="m", created=1, usage=None, choices=[choice])

    body = _chat_body(
        [
            chunk(call("a", "f1", '{"x"')),
            chunk(call(None, None, ": 1}")),
            chunk(call("b", "f2", "{}"), finish="tool_calls"),
        ]
    )
    calls = body["choices"][0]["message"]["tool_calls"]
    assert [(c["id"], c["function"]["name"], c["function"]["arguments"]) for c in calls] == [
        ("a", "f1", '{"x": 1}'),
        ("b", "f2", "{}"),
    ]


def _frame(event_type: str, payload: bytes) -> Any:
    raw = eventstream.event(event_type, {})
    buffer = EventStreamBuffer()
    buffer.add_data(raw)
    message = next(iter(buffer))
    message.payload = payload  # a proxy's non-object payload
    return message


@pytest.mark.parametrize("payload", [b'"text"', b"[1, 2]"])
def test_a_stream_frame_that_is_not_an_object_is_retryable(payload: bytes) -> None:
    """R3ST-14: a frame whose JSON is not an object raised AttributeError, FATAL.

    It is `StreamTruncated` (retryable) now, and `text_of` reads `delta: null` as no text.
    """
    with pytest.raises(StreamTruncated):
        _event(_frame("contentBlockDelta", payload))
    assert text_of({"contentBlockDelta": {"delta": None}}) == ""


async def test_openai_events_keep_the_wire_shape_and_choice_zero() -> None:
    """R3ST-10: events were dumped without nulls (`finish_reason` missing: KeyError in apps).

    `text_of` also read `choices[0]` whatever its index, mixing choices into `partial_text`.
    """
    chunk = ChatCompletionChunk.model_validate(
        {
            "id": "c",
            "object": "chat.completion.chunk",
            "created": 1,
            "model": "m",
            "choices": [{"index": 0, "delta": {"content": None}, "finish_reason": None}],
        }
    )
    store: Any = SimpleNamespace()  # never used: the flush is replaced below
    writer = ChunkWriter(store, "k", "a", flush_ms=60_000, max_bytes=1 << 20, ttl_secs=1)
    written: list[Any] = []

    async def keep() -> None:
        written.extend(writer._buffer)

    writer._flush = keep  # type: ignore[method-assign]  # no store in a unit test
    await writer.add(chunk)
    assert written[0]["choices"][0]["finish_reason"] is None
    assert written[0]["choices"][0]["delta"]["content"] is None
    two = {
        "choices": [
            {"index": 1, "delta": {"content": "B"}},
            {"index": 0, "delta": {"content": "A"}},
        ]
    }
    assert text_of(two) == "A"
