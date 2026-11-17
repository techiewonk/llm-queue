"""Queued OpenAI-compatible adapters: `ChatOpenAI` / `OpenAIEmbeddings` over `QueuedHTTPTransport`.

Both SDK clients get an httpx client whose transport answers from the LLM queue (D88), so every
LangChain/SDK code path is queued and parsed unchanged. SDK streaming and retries are forced off:
the queue's workers own retry and fallback (`queue_streaming=True` streams through
`LLMQueue.chat_stream` instead of the SDK). Chat replies get how the queue served
them in `response_metadata["llmq"]` (the `Meta`) plus a boto-shaped `ResponseMetadata` whose
`RequestId` is the server's `x-request-id`.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from typing import TYPE_CHECKING, Any, Literal, cast

import httpx
import openai
from langchain_core.messages import AIMessageChunk, BaseMessageChunk
from langchain_core.outputs import ChatGenerationChunk
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from pydantic import Field, model_validator

from llm_queue.errors import LLMQueueError
from llm_queue.http_transport import META_KEY, QUEUED_BASE_URL, QueuedHTTPTransport
from llm_queue.langchain._client import checked_values, resolve_queue, streaming_queue
from llm_queue.langchain._stream import (
    queue_metadata,
    queued_chunks,
    result_chunk,
    should_stream,
    usage_metadata,
)

if TYPE_CHECKING:
    from langchain_core.callbacks import (
        AsyncCallbackManagerForLLMRun,
        CallbackManagerForLLMRun,
    )
    from langchain_core.messages import BaseMessage
    from langchain_core.outputs import ChatResult

# Placeholder only: the SDK requires a key, but workers hold the real provider credentials.
_PLACEHOLDER_API_KEY = "llmq-queued"


def _queued_client_values(values: Any) -> Any:
    """Point both SDK clients at a queued transport built from the raw constructor values."""
    values = checked_values(values)
    if not isinstance(values, dict):
        return values
    transport = QueuedHTTPTransport(
        values.get("llmq"),
        values.get("model_alias"),
        priority=values.get("priority"),
        allow_fallback=values.get("allow_fallback", "all"),
        timeout_secs=values.get("queue_timeout_secs"),
    )
    values["http_async_client"] = httpx.AsyncClient(transport=transport, base_url=QUEUED_BASE_URL)
    values["http_client"] = httpx.Client(transport=transport, base_url=QUEUED_BASE_URL)
    values.pop("openai_api_base", None)
    values["base_url"] = QUEUED_BASE_URL
    values["openai_proxy"] = None  # an OPENAI_PROXY env var would conflict with http_client
    if values.get("api_key") is None and values.get("openai_api_key") is None:
        values["api_key"] = _PLACEHOLDER_API_KEY
    values["max_retries"] = 0
    return values


class _ClosesClients:
    """`close()` / `aclose()` for the two httpx clients each queued adapter owns."""

    def close(self) -> None:
        """Close the sync httpx client (the async one needs `aclose()`); safe to repeat."""
        if (client := getattr(self, "http_client", None)) is not None:
            client.close()

    async def aclose(self) -> None:
        """Close both httpx clients; safe to repeat."""
        self.close()
        if (client := getattr(self, "http_async_client", None)) is not None:
            await client.aclose()


class QueuedChatOpenAI(_ClosesClients, ChatOpenAI):
    """`ChatOpenAI` whose HTTP calls are answered by the LLM queue's `chat` op.

    `model_alias` defaults to the request's `model`; `queue_streaming=True` streams tokens
    in `astream()`. Only `/chat/completions` and `/embeddings` are queued (R3AP-13).
    """

    llmq: Any = Field(default=None, exclude=True)
    model_alias: str | None = None
    allow_fallback: Literal["all", "regions", "none"] = "all"
    priority: bool | None = None
    queue_timeout_secs: float | None = None
    queue_streaming: bool = False

    @model_validator(mode="before")
    @classmethod
    def _queue_defaults(cls, values: Any) -> Any:
        values = _queued_client_values(values)
        if isinstance(values, dict):
            values["streaming"] = False  # the SDK path never streams; `queue_streaming` does
            if not values.get("queue_streaming"):
                values["disable_streaming"] = True
            # langchain-openai 0.3.26+ posts to `/responses` when asked to, or by itself for
            # `reasoning`, built-in tools or `output_version="responses/v1"`; the queue answered
            # 404, which read as a missing model (R3AP-13).
            if values.get("use_responses_api"):
                raise ValueError(
                    "use_responses_api=True is not supported: the queue serves "
                    "/chat/completions and /embeddings only"
                )
            if "use_responses_api" in ChatOpenAI.model_fields:
                values["use_responses_api"] = False
            values.setdefault("name", ChatOpenAI.__name__)  # trace run name as the parent's
        return values

    def _create_chat_result(
        self, response: Any, generation_info: dict[str, Any] | None = None
    ) -> ChatResult:
        """Parse like the parent, then add the queue's `llmq_meta` (`queue_metadata`)."""
        result = super()._create_chat_result(response, generation_info)
        if isinstance(response, dict):
            meta = response.get(META_KEY)
        else:
            meta = (getattr(response, "model_extra", None) or {}).get(META_KEY)
        if isinstance(meta, dict):
            added = queue_metadata(meta)
            for generation in result.generations:
                generation.message.response_metadata.update(added)
        return result

    def _should_stream(self, *, async_api: bool, run_manager: Any = None, **kwargs: Any) -> bool:
        """Stream only with `queue_streaming=True` and only on the async API (see the class)."""
        return should_stream(self, async_api=async_api, run_manager=run_manager, **kwargs)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        """The parent's sync path; a queue refusal (Q1) is raised as the queue's own error."""
        try:
            return super()._generate(messages, stop, run_manager, **kwargs)
        except openai.APIStatusError as exc:
            raise (_queue_error(exc) or exc) from None

    async def _astream(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        """Token streaming through `chat_stream` (`queue_streaming=True`).

        The request is built and each chunk parsed by the installed langchain-openai, as for
        its own `astream`. The queue always reports usage, so `stream_usage` is ignored. A
        structured-output request (`response_format`) is answered whole, as one chunk.
        """
        kwargs.pop("stream_usage", None)
        kwargs.pop("stream_options", None)
        payload = self._get_request_payload(messages, stop=stop, **kwargs)
        if "response_format" in payload:  # parsed by the SDK from a whole reply
            yield result_chunk(await self._agenerate(messages, stop, run_manager, **kwargs))
            return
        for key in ("stream", "stream_options"):  # the queue's provider sets both
            payload.pop(key, None)
        model = payload.pop("model", None)  # the registry decides the provider model id
        alias = self.model_alias or model or self.model_name
        queue = streaming_queue(resolve_queue(self.llmq), "chat_stream")
        chunk_class: type[BaseMessageChunk] = AIMessageChunk

        def convert(event: dict[str, Any]) -> ChatGenerationChunk | None:
            nonlocal chunk_class
            chunk = self._convert_chunk_to_generation_chunk(event, chunk_class, None)
            if chunk is not None:
                chunk_class = type(chunk.message)
            return chunk

        def closing(info: Mapping[str, Any], had_usage: bool) -> ChatGenerationChunk:
            usage = None if had_usage else usage_metadata(info.get("usage"))
            message = AIMessageChunk(
                content="",
                response_metadata=queue_metadata(info.get("meta") or {}),
                usage_metadata=usage,
            )
            return ChatGenerationChunk(message=message)

        events = queue.chat_stream(
            alias,
            payload,
            priority=self.priority,
            allow_fallback=self.allow_fallback,
            timeout_secs=self.queue_timeout_secs,
            meta_event=True,
        )
        # aclosing: a consumer that closes this generator closes the queue's stream now.
        chunks = cast(
            "AsyncGenerator[ChatGenerationChunk, None]",
            queued_chunks(events, convert, closing, run_manager),
        )
        async with contextlib.aclosing(chunks):
            async for chunk in chunks:
                yield chunk


def _queue_error(exc: openai.APIStatusError) -> LLMQueueError | None:
    """The queue's own error behind an SDK status error, when it was not a failed call.

    The transport answers a plain `LLMQueueError` (a sync call refused on the queue's own
    loop, Q1; no queue set) as a 500, which the SDK raised as an `InternalServerError`.
    """
    if exc.response.headers.get("x-llmq-error") != LLMQueueError.__name__:
        return None
    body = exc.body
    message = body.get("message") if isinstance(body, dict) else None
    return LLMQueueError(message if isinstance(message, str) else str(exc))


class QueuedOpenAIEmbeddings(_ClosesClients, OpenAIEmbeddings):
    """`OpenAIEmbeddings` whose HTTP calls are answered by the LLM queue's `embed` op.

    `check_embedding_ctx_length` defaults to False because the queue embeds text, not tokens,
    and non-OpenAI servers do not share OpenAI's tokenizer.
    """

    llmq: Any = Field(default=None, exclude=True)
    model_alias: str | None = None
    priority: bool | None = None
    queue_timeout_secs: float | None = None
    check_embedding_ctx_length: bool = False

    @model_validator(mode="before")
    @classmethod
    def _queue_defaults(cls, values: Any) -> Any:
        return _queued_client_values(values)
