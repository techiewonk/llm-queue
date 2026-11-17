"""Queued Bedrock adapters: `ChatBedrockConverse` and `BedrockEmbeddings` that call the LLM queue.

The chat class runs the parent's own `_generate` against a placeholder client that hands the
Converse request to the queue, so messages, tools, cache points, reasoning and usage are built
and parsed exactly as the installed langchain-aws does (0.2 and 1.x lines alike). No boto3
client is ever created: both classes get a placeholder client that refuses direct calls.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Mapping
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Literal, cast

from langchain_aws import BedrockEmbeddings, ChatBedrockConverse
from langchain_aws.chat_models import bedrock_converse as _converse_module
from langchain_core import __version__ as _core_version
from langchain_core.messages import AIMessageChunk
from langchain_core.outputs import ChatGenerationChunk
from pydantic import Field, model_validator

from llm_queue.errors import LLMQueueError
from llm_queue.langchain._client import (
    checked_values,
    get_default_queue,
    resolve_queue,
    streaming_queue,
)
from llm_queue.langchain._stream import (
    finish_reason,
    queue_metadata,
    queued_chunks,
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

    from llm_queue.langchain._client import QueueClient, ReplyLike

# Per-call Converse handler of the placeholder client (a ContextVar: concurrent calls on one
# model instance never see each other's handler).
_Handler = Callable[[dict[str, Any]], dict[str, Any]]
_HANDLER: ContextVar[_Handler | None] = ContextVar("llmq_converse_handler", default=None)
# langchain-core 1.x merges a repeated `model_provider` (langchain-aws 1.x sets it on every
# stream chunk); 0.3 would concatenate it, and langchain-aws 0.2 never sets it.
_SETS_PROVIDER = not _core_version.startswith("0.")


class _Captured(Exception):  # control flow, not an error
    """Raised by the capturing handler to stop the parent right after it built the request."""

    def __init__(self, request: dict[str, Any]) -> None:
        super().__init__("captured")
        self.request = request


class _QueueOnlyClient:
    """Placeholder for the boto3 client: stops the parent creating one and refuses direct calls.

    `converse` answers only inside a queued call (through the current handler); any other path
    that would reach Bedrock directly (e.g. a parent `_stream`) fails loudly.
    """

    def converse(self, **request: Any) -> dict[str, Any]:
        handler = _HANDLER.get()
        if handler is None:
            self._refuse()
        assert handler is not None
        return handler(request)

    def _refuse(self, *_args: Any, **_kwargs: Any) -> Any:
        raise LLMQueueError("this model sends every call through the LLM queue")

    converse_stream = invoke_model = get_inference_profile = _refuse


def _queue_only_values(values: Any, fields: Any, *slots: str) -> Any:
    """Fill the given boto3 client slots with the placeholder unless the caller supplied them.

    Only slots that are fields of the installed langchain-aws class: 0.2.24 has no
    `bedrock_client`, and an unknown key moves into `model_kwargs`, which reach the Converse
    request (the placeholder then broke envelope serialization in an app).
    """
    if isinstance(values, dict):
        for slot in slots:
            if slot in fields:
                values.setdefault(slot, _QueueOnlyClient())
    return values


def _looks_like_bedrock_id(model_id: str) -> bool:
    """Bedrock ids are `provider.model[...]` (optionally region-prefixed) or ARNs."""
    return "." in model_id or model_id.startswith("arn:")


def _alias_values(values: Any) -> Any:
    """Resolve `model_alias` / `model_id` and reject a queue alias passed as the model id.

    Without `model_id`, it is looked up from the registry of the given queue, else of the
    started default queue.
    """
    if not isinstance(values, dict):
        return values
    model_id = values.get("model_id", values.get("model"))
    alias = values.get("model_alias")
    if model_id is None and alias is not None:
        queue = values.get("llmq")
        if queue is None:
            with contextlib.suppress(LLMQueueError):
                queue = get_default_queue()
        registry = getattr(queue, "registry", None)
        if registry is None:
            raise ValueError(
                f"model_alias={alias!r} needs model_id='<Bedrock model id>', llmq=<LLMQueue>, "
                "or a started LLMQueue so it can be looked up from the registry"
            )
        values["model_id"] = registry.current.model(alias).model_id
    elif isinstance(model_id, str) and alias is None and not _looks_like_bedrock_id(model_id):
        raise ValueError(
            f"model={model_id!r} looks like a queue alias, not a Bedrock model id; pass "
            f"model_alias={model_id!r} and model_id='<Bedrock model id>' "
            "(e.g. 'openai.gpt-oss-120b-1:0'), or model_alias plus llmq=<LLMQueue>"
        )
    return values


def _service_tier(tier: Any) -> dict[str, Any]:
    return tier if isinstance(tier, dict) else {"type": tier}


class QueuedChatBedrockConverse(ChatBedrockConverse):
    """`ChatBedrockConverse` whose Converse calls go through the LLM queue.

    `model_alias` is the registry alias (`gpt120b`); `model_id` is the Bedrock id (looked up
    from the registry when omitted) and drives the parent's provider-specific request building.
    `priority=None` takes the class from `request_context`. `queue_streaming=True` makes
    `astream()` (and LangGraph's message streaming) yield tokens as they arrive, through
    `LLMQueue.converse_stream`; off by default (one full message). Sync `stream()` always
    yields one full message: the queue has no sync stream.

    Replies carry how the queue served them in `response_metadata["llmq"]` (the `Meta`) and
    boto's `response_metadata["ResponseMetadata"]` (`RequestId` is Bedrock's request id).
    """

    llmq: Any = Field(default=None, exclude=True)
    model_alias: str | None = None
    allow_fallback: Literal["all", "regions", "none"] = "all"
    priority: bool | None = None
    # langchain-aws 1.x types it as these values (0.2.x has no such field: ours adds it).
    service_tier: Literal["priority", "default", "flex", "reserved"] | None = None
    queue_timeout_secs: float | None = None
    queue_streaming: bool = False

    @model_validator(mode="before")
    @classmethod
    def _queue_defaults(cls, values: Any) -> Any:
        values = _queue_only_values(
            _alias_values(checked_values(values)), cls.model_fields, "client", "bedrock_client"
        )
        if isinstance(values, dict):
            if not values.get("queue_streaming"):
                values["disable_streaming"] = True
            # Traces (Langfuse, LangSmith) keep the parent's run name, so dashboards still match.
            values.setdefault("name", ChatBedrockConverse.__name__)
        return values

    def _queue(self) -> QueueClient:
        return resolve_queue(self.llmq)

    def _should_stream(self, *, async_api: bool, run_manager: Any = None, **kwargs: Any) -> bool:
        """Stream only with `queue_streaming=True` and only on the async API (see the class)."""
        return should_stream(self, async_api=async_api, run_manager=run_manager, **kwargs)

    @property
    def _alias(self) -> str:
        return self.model_alias or self.model_id

    def _run_parent(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None,
        kwargs: dict[str, Any],
        handler: _Handler,
    ) -> ChatResult:
        token = _HANDLER.set(handler)
        try:
            return super()._generate(messages, stop, None, **kwargs)
        finally:
            _HANDLER.reset(token)

    def _capture(
        self, messages: list[BaseMessage], stop: list[str] | None, kwargs: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """(Converse request without `modelId`, kwargs for the parse pass) from the parent."""
        call_kwargs = dict(kwargs)
        tier = call_kwargs.pop("service_tier", None)  # langchain-aws 0.2 has no such kwarg

        def capture(request: dict[str, Any]) -> dict[str, Any]:
            raise _Captured(request)

        try:
            self._run_parent(messages, stop, call_kwargs, capture)
        except _Captured as captured:
            request = dict(captured.request)
        else:  # pragma: no cover - the parent always calls converse
            raise LLMQueueError("ChatBedrockConverse did not build a Converse request")
        request.pop("modelId", None)  # the queue alias picks the model and target
        if tier is not None:
            request["serviceTier"] = _service_tier(tier)
        elif self.service_tier and "serviceTier" not in request:
            request["serviceTier"] = _service_tier(self.service_tier)
        return request, call_kwargs

    def _parse(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None,
        call_kwargs: dict[str, Any],
        result: ReplyLike,
    ) -> ChatResult:
        """Parse like the parent (same call, canned reply), then add how the queue served it."""
        response = result.response or {}
        chat = self._run_parent(messages, stop, call_kwargs, lambda _request: response)
        added = queue_metadata(result.meta.model_dump())
        for generation in chat.generations:
            generation.message.response_metadata.update(added)
        served = result.meta.served_model
        if served and served != self._alias:
            # Model fallback: report the model that answered (tracers price by `llm_output`).
            chat.llm_output = {**(chat.llm_output or {}), "model_name": self._model_id_of(served)}
        return chat

    def _model_id_of(self, served: str) -> str:
        """The Bedrock id of the model that answered (this one unless it fell back)."""
        if not served or served == self._alias:
            return self.model_id
        registry = getattr(self._queue(), "registry", None)
        return registry.current.model(served).model_id if registry else served

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Sync path: `converse_sync` (direct under the shared limiter, see README)."""
        request, call_kwargs = self._capture(messages, stop, kwargs)
        result = self._queue().converse_sync(
            self._alias,
            request,
            priority=self.priority,
            allow_fallback=self.allow_fallback,
            timeout_secs=self.queue_timeout_secs,
        )
        return self._parse(messages, stop, call_kwargs, result)

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Native async path: one queued Converse call (the parent only has a thread fallback)."""
        request, call_kwargs = self._capture(messages, stop, kwargs)
        result = await self._queue().converse(
            self._alias,
            request,
            priority=self.priority,
            allow_fallback=self.allow_fallback,
            timeout_secs=self.queue_timeout_secs,
        )
        return self._parse(messages, stop, call_kwargs, result)

    async def _astream(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        """Token streaming through `converse_stream` (`queue_streaming=True`).

        The request is built and each ConverseStream event parsed by the installed
        langchain-aws, as for its own `astream`. The last chunk has the usage, `stopReason`
        with the OpenAI-style `finish_reason`, and how the queue served the call.
        """
        request, _ = self._capture(messages, stop, kwargs)
        queue = streaming_queue(self._queue(), "converse_stream")
        stop_reason: list[str] = []

        def convert(event: dict[str, Any]) -> ChatGenerationChunk | None:
            message = _converse_module._parse_stream_event(event)
            if message is None:
                return None
            if "messageStop" in event:
                stop_reason.append(event["messageStop"].get("stopReason") or "")
            if _SETS_PROVIDER:  # langchain-aws 1.x marks every chunk, as its own _stream does
                message.response_metadata["model_provider"] = "bedrock_converse"
            return ChatGenerationChunk(message=message)

        def closing(info: Mapping[str, Any], had_usage: bool) -> ChatGenerationChunk:
            meta = dict(info.get("meta") or {})
            metadata: dict[str, Any] = {
                **queue_metadata(meta),
                "model_name": self._model_id_of(str(meta.get("served_model") or "")),
            }
            if stop_reason and (reason := finish_reason(stop_reason[-1] or None)):
                metadata["finish_reason"] = reason
            usage = None if had_usage else usage_metadata(info.get("usage"))
            message = AIMessageChunk(content="", response_metadata=metadata, usage_metadata=usage)
            return ChatGenerationChunk(message=message)

        events = queue.converse_stream(
            self._alias,
            request,
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


class QueuedBedrockEmbeddings(BedrockEmbeddings):
    """`BedrockEmbeddings` whose calls go through the queue's `embed` op (batched per provider).

    `model_alias` is the queue alias (defaults to `model_id`; the queue builds the provider
    body). Queries use `query_input_type` (`"search_query"`), documents `document_input_type`
    (`"search_document"`); the provider ignores it where the model has no such field. An index
    built with `embed_query` for its documents keeps matching with
    `document_input_type="search_query"`. `normalize` is applied locally.
    """

    llmq: Any = Field(default=None, exclude=True)
    model_alias: str | None = None
    priority: bool | None = None
    queue_timeout_secs: float | None = None
    query_input_type: str = "search_query"
    document_input_type: str = "search_document"

    @model_validator(mode="before")
    @classmethod
    def _queue_defaults(cls, values: Any) -> Any:
        return _queue_only_values(checked_values(values), cls.model_fields, "client")

    def _queue(self) -> QueueClient:
        return resolve_queue(self.llmq)

    def _prepare(self, texts: list[str]) -> list[str]:
        """Same newline handling as the parent, which says newlines hurt embedding quality."""
        return [text.replace(os.linesep, " ") for text in texts]

    def _finish(self, vectors: list[list[float]]) -> list[list[float]]:
        return [self._normalize_vector(v) for v in vectors] if self.normalize else vectors

    def _sync(self, texts: list[str], input_type: str) -> list[list[float]]:
        result = self._queue().embed_sync(
            self.model_alias or self.model_id,
            self._prepare(texts),
            priority=self.priority,
            input_type=input_type,
            timeout_secs=self.queue_timeout_secs,
        )
        return self._finish(result.vectors)

    async def _async(self, texts: list[str], input_type: str) -> list[list[float]]:
        result = await self._queue().embed(
            self.model_alias or self.model_id,
            self._prepare(texts),
            priority=self.priority,
            input_type=input_type,
            timeout_secs=self.queue_timeout_secs,
        )
        return self._finish(result.vectors)

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed documents on the sync path (`embed_sync`)."""
        return self._sync(texts, self.document_input_type)

    def embed_query(self, text: str) -> list[float]:
        """Embed one query on the sync path (`embed_sync`)."""
        return self._sync([text], self.query_input_type)[0]

    async def aembed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed documents through the queue as one call (the parent fans out per text)."""
        return await self._async(texts, self.document_input_type)

    async def aembed_query(self, text: str) -> list[float]:
        """Embed one query through the queue."""
        return (await self._async([text], self.query_input_type))[0]
