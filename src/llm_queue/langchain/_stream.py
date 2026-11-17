"""Token streaming shared by the queued chat adapters (`queue_streaming=True`, owner Q49).

Each adapter turns one provider event into a `ChatGenerationChunk` with its parent's own parser;
this module drives the queue's stream: it closes it when the consumer stops (aclosing), holds
the chunk that carries usage back so usage arrives once, on the last chunk, and ends with a chunk
that has how the queue served the call. A `CallFailed` from the queue (with `partial_text` when
part of the answer was yielded) is raised unchanged. Needs langchain-core only.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

from langchain_core.messages import AIMessage, AIMessageChunk
from langchain_core.messages.ai import UsageMetadata
from langchain_core.outputs import ChatGenerationChunk

if TYPE_CHECKING:
    from langchain_core.callbacks import AsyncCallbackManagerForLLMRun
    from langchain_core.outputs import ChatResult

# The library's own last event (`stream(meta_event=True)`): {"llmq": {"usage", "meta"}}.
LLMQ_EVENT = "llmq"

# Bedrock `stopReason` -> the OpenAI `finish_reason` both adapters report on the last chunk.
_FINISH_REASONS = {
    "end_turn": "stop",
    "stop_sequence": "stop",
    "max_tokens": "length",
    "model_context_window_exceeded": "length",
    "tool_use": "tool_calls",
    "guardrail_intervened": "content_filter",
    "content_filtered": "content_filter",
}

Convert = Callable[[dict[str, Any]], ChatGenerationChunk | None]
Closing = Callable[[Mapping[str, Any], bool], ChatGenerationChunk]


def queue_metadata(meta: Mapping[str, Any]) -> dict[str, Any]:
    """The `response_metadata` keys the queued adapters add: `llmq` and `ResponseMetadata`.

    `llmq` is the queue's `Meta` (served model, deployment, fallback, attempts, timings,
    provider request id). `ResponseMetadata` has boto's shape, so code that reads the AWS
    request id and retries from a direct `ChatBedrockConverse` reply reads them here too:
    `RetryAttempts` counts the queue's attempts after the first. No `HTTPHeaders`.
    """
    attempts = int(meta.get("attempts") or 0)
    return {
        LLMQ_EVENT: dict(meta),
        "ResponseMetadata": {
            "RequestId": str(meta.get("provider_request_id") or ""),
            "HTTPStatusCode": 200,
            "RetryAttempts": max(0, attempts - 1),
        },
    }


def finish_reason(stop_reason: str | None) -> str | None:
    """A Bedrock `stopReason` as an OpenAI `finish_reason` (unknown reasons pass through)."""
    if stop_reason is None:
        return None
    return _FINISH_REASONS.get(stop_reason, stop_reason)


def usage_metadata(usage: Mapping[str, Any] | None) -> UsageMetadata | None:
    """LangChain usage from the library's usage (Converse keys); `None` when it has no counts.

    `input_tokens` includes cached input, as LangChain counts it.
    """
    if not usage or (usage.get("inputTokens") is None and usage.get("outputTokens") is None):
        return None
    read = int(usage.get("cacheReadInputTokens") or 0)
    write = int(usage.get("cacheWriteInputTokens") or 0)
    tokens_in = int(usage.get("inputTokens") or 0) + read + write
    tokens_out = int(usage.get("outputTokens") or 0)
    out = UsageMetadata(
        input_tokens=tokens_in, output_tokens=tokens_out, total_tokens=tokens_in + tokens_out
    )
    if read or write:
        out["input_token_details"] = {"cache_read": read, "cache_creation": write}
    return out


def _last(chunk: ChatGenerationChunk) -> ChatGenerationChunk:
    """Mark the stream's last chunk (langchain-core 1.x adds an empty one otherwise)."""
    if "chunk_position" in type(chunk.message).model_fields:
        chunk.message.chunk_position = "last"  # type: ignore[attr-defined]
    return chunk


def _has_usage(chunk: ChatGenerationChunk) -> bool:
    return bool(getattr(chunk.message, "usage_metadata", None))


async def _emit(
    chunk: ChatGenerationChunk, run_manager: AsyncCallbackManagerForLLMRun | None
) -> ChatGenerationChunk:
    if run_manager is not None:
        await run_manager.on_llm_new_token(chunk.text, chunk=chunk)
    return chunk


async def queued_chunks(
    events: AsyncIterator[dict[str, Any]],
    convert: Convert,
    closing: Closing,
    run_manager: AsyncCallbackManagerForLLMRun | None,
) -> AsyncIterator[ChatGenerationChunk]:
    """Chunks of one queued stream; `closing(llmq_info, had_usage)` builds the last one.

    A consumer that stops early closes the queue's stream at once (its call is cancelled),
    instead of leaving it to the async generator finalizer.
    """
    held: ChatGenerationChunk | None = None
    stream = cast("AsyncGenerator[dict[str, Any], None]", events)
    async with contextlib.aclosing(stream):
        async for event in stream:
            if LLMQ_EVENT in event and len(event) == 1:
                last = closing(event[LLMQ_EVENT] or {}, held is not None)
                if held is not None:
                    last, held = held + last, None
                yield await _emit(_last(last), run_manager)
                continue
            chunk = convert(event)
            if chunk is None:
                continue
            if _has_usage(chunk):  # usage goes on the last chunk, once
                held = chunk if held is None else held + chunk
                continue
            yield await _emit(chunk, run_manager)
    if held is not None:  # a queue without the closing event
        yield await _emit(_last(held), run_manager)


def result_chunk(result: ChatResult) -> ChatGenerationChunk:
    """A whole `ChatResult` as one chunk (paths the queue cannot stream token by token)."""
    generation = result.generations[0]
    message = generation.message
    tool_calls = message.tool_calls if isinstance(message, AIMessage) else []
    chunk = AIMessageChunk(
        content=message.content,
        id=message.id,
        additional_kwargs=dict(message.additional_kwargs),
        response_metadata=dict(message.response_metadata),
        usage_metadata=getattr(message, "usage_metadata", None),
        tool_call_chunks=[
            {
                "name": call["name"],
                "args": json.dumps(call["args"]),
                "id": call.get("id"),
                "index": index,
                "type": "tool_call_chunk",
            }
            for index, call in enumerate(tool_calls)
        ],
    )
    return ChatGenerationChunk(message=chunk, generation_info=generation.generation_info)


def should_stream(
    model: Any,
    *,
    async_api: bool,
    run_manager: Any = None,
    **kwargs: Any,
) -> bool:
    """`BaseChatModel._should_stream` for a queued adapter.

    Async only, and only with `queue_streaming=True`: the queue has no sync stream, so a sync
    `stream()` runs `invoke()` (one chunk, on the sync path, which refuses a call made on the
    queue's own event loop with its own message, Q1). The adapters force `streaming=False`
    (their sync path cannot stream), so that field is not read here.
    """
    if not async_api or not model.queue_streaming:
        return False
    if model.disable_streaming is True:
        return False
    if model.disable_streaming == "tool_calling" and kwargs.get("tools"):
        return False
    if "stream" in kwargs:
        return bool(kwargs["stream"])
    from langchain_core.tracers._streaming import _StreamingCallbackHandler

    handlers = run_manager.handlers if run_manager else []
    return any(isinstance(h, _StreamingCallbackHandler) for h in handlers)
