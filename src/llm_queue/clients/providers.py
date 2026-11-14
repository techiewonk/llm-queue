"""Providers on routes: one attempt of an operation, its usage, and error classification.

Bedrock chat uses plain Converse unless the caller streams or the model asks for
ConverseStream (`converse_api`, plan P7); OpenAI-compatible chat always streams underneath and is
assembled into a chat.completion, so the attempt timers apply. A stream that ends without its
end marker is `StreamTruncated` (retryable). A dropped connection gets one quick retry inside
the attempt when nothing reached the caller (C7). `register_provider` adds a custom kind.
"""

from __future__ import annotations

import base64
import contextlib
import inspect
import time
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import orjson

from llm_queue.clients.pool import ClientPool, Route
from llm_queue.clients.timing import Timing, first, opened, paced
from llm_queue.errors import ConfigError, ErrorInfo
from llm_queue.providers.classify import (
    StreamTruncated,
    classify_bedrock,
    classify_openai,
    is_body_cut,
    is_connection_reset,
)

_TOKEN_HEADER = "x-amzn-bedrock-input-token-count"


@dataclass(frozen=True, slots=True)
class Usage:
    """Tokens of one attempt as the provider reported them; None when it reported nothing.

    `input_tokens` excludes cache reads (Bedrock's meaning; OpenAI's `prompt_tokens` include
    them, so they are subtracted there), so `cache_read_tokens` is never counted twice.
    """

    input_tokens: int | None = None
    output_tokens: int | None = None
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0


@dataclass(frozen=True, slots=True)
class Result:
    """One successful attempt: the provider-native reply and its usage."""

    response: dict[str, Any]
    usage: Usage


Sink = Callable[[Any], Awaitable[None]]
"""Receives each streamed event of an attempt (streaming to the caller, plan §3.13)."""


class Provider(Protocol):
    """One provider kind: runs an operation on a route and classifies its failures."""

    name: str

    async def call(
        self,
        op: str,
        params: Mapping[str, Any],
        route: Route,
        timing: Timing,
        sink: Sink | None = None,
        *,
        converse_api: str = "converse",
    ) -> Result:
        """Run one attempt; chat operations pass each streamed event to `sink`.

        `converse_api` is the model's setting: `converse` or `stream` (Bedrock only).
        """
        ...

    def classify(self, exc: BaseException) -> ErrorInfo:
        """Throttle / retryable / fatal."""
        ...


async def _reset_retry(
    attempt: Callable[[Sink | None], Awaitable[Result]], sink: Sink | None
) -> Result:
    """Run `attempt`; on a connection dropped before any answer arrived, run it once more.

    The first failure never reaches the worker, so it charges no breaker and no retry budget;
    the second one is classified as usual. Only a stale socket that failed before the reply
    qualifies: once a stream event arrived (sent to `sink` or not) or the body was being read,
    the model already ran, and running it again billed the generation twice (CL-9).
    """
    received = False

    async def forward(event: Any) -> None:
        nonlocal received
        received = True
        if sink is not None:
            await sink(event)

    try:
        return await attempt(forward)
    except Exception as exc:
        if received or is_body_cut(exc) or not is_connection_reset(exc):
            raise
    return await attempt(forward)


# ------------------------------------------------------------------------------- Bedrock


def _converse_body(events: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Assemble ConverseStream events into the body `converse` returns.

    Keeps reasoning (text, signature, redacted bytes) and citations; raises `StreamTruncated`
    when the stream ended without `messageStop`.
    """
    role = "assistant"
    blocks: dict[int, dict[str, Any]] = {}
    tool_input: dict[int, str] = {}
    citations: dict[int, list[Any]] = {}
    body: dict[str, Any] = {}
    stopped = False
    for event in events:
        if "messageStart" in event:
            role = event["messageStart"].get("role", role)
        elif "contentBlockStart" in event:
            index = event["contentBlockStart"].get("contentBlockIndex", 0)
            start = event["contentBlockStart"].get("start", {})
            if "toolUse" in start:
                blocks[index] = {"toolUse": dict(start["toolUse"])}
                tool_input[index] = ""
        elif "contentBlockDelta" in event:
            index = event["contentBlockDelta"].get("contentBlockIndex", 0)
            delta = event["contentBlockDelta"].get("delta", {})
            if "text" in delta:
                block = blocks.setdefault(index, {"text": ""})
                block["text"] = block.get("text", "") + delta["text"]
            elif "toolUse" in delta:
                tool_input[index] = tool_input.get(index, "") + delta["toolUse"].get("input", "")
                blocks.setdefault(index, {"toolUse": {}})
            elif "reasoningContent" in delta:
                content = blocks.setdefault(index, {"reasoningContent": {}})["reasoningContent"]
                part = delta["reasoningContent"]
                if "redactedContent" in part:
                    content["redactedContent"] = (
                        content.get("redactedContent", b"") + part["redactedContent"]
                    )
                if "text" in part or "signature" in part:
                    reasoning = content.setdefault("reasoningText", {})
                    if "text" in part:
                        reasoning["text"] = reasoning.get("text", "") + part["text"]
                    if "signature" in part:
                        reasoning["signature"] = part["signature"]
            elif "citation" in delta:
                citations.setdefault(index, []).append(delta["citation"])
        elif "messageStop" in event:
            stopped = True
            body["stopReason"] = event["messageStop"].get("stopReason")
            extra = event["messageStop"].get("additionalModelResponseFields")
            if extra is not None:
                body["additionalModelResponseFields"] = extra
        elif "metadata" in event:
            for key in ("usage", "metrics", "trace"):
                if key in event["metadata"]:
                    body[key] = event["metadata"][key]
    if not stopped:
        raise StreamTruncated("the Bedrock stream ended before messageStop")
    for index, text in tool_input.items():
        blocks[index]["toolUse"]["input"] = orjson.loads(text) if text else {}
    for index, cited in citations.items():  # the non-streamed Converse shape of a cited answer
        text = blocks.get(index, {}).get("text", "")
        blocks[index] = {"citationsContent": {"content": [{"text": text}], "citations": cited}}
    content = [blocks[i] for i in sorted(blocks)]
    return {"output": {"message": {"role": role, "content": content}}, **body}


def _bedrock_usage(usage: Mapping[str, Any] | None) -> Usage:
    if not usage:
        return Usage()
    return Usage(
        input_tokens=usage.get("inputTokens"),
        output_tokens=usage.get("outputTokens"),
        cache_read_tokens=int(usage.get("cacheReadInputTokens") or 0),
        cache_write_tokens=int(usage.get("cacheWriteInputTokens") or 0),
    )


class BedrockProvider:
    """Bedrock runtime: `converse` (plain or over ConverseStream) and `invoke_model`."""

    name = "bedrock"

    def __init__(self, pool: ClientPool) -> None:
        """Use `pool` for clients."""
        self._pool = pool

    async def stream(
        self, params: Mapping[str, Any], route: Route, timing: Timing
    ) -> AsyncGenerator[Mapping[str, Any], None]:
        """ConverseStream events of one attempt, each within the attempt timers."""
        async with self._pool.borrow_bedrock(route) as client:
            request = {**params, "modelId": route.model_id}
            started = time.monotonic()
            reply = await first(
                client.converse_stream(**request), timing.first_response_secs, "first response"
            )
            stream = reply["stream"]
            try:
                async for event in paced(stream, opened(timing, started), started=False):
                    yield event
            finally:
                # Always release the HTTP connection: a stream given up on an idle timeout (or
                # a cancelled call) left it to garbage collection (aiohttp warned from __del__).
                with contextlib.suppress(Exception):
                    stream.close()

    async def call(
        self,
        op: str,
        params: Mapping[str, Any],
        route: Route,
        timing: Timing,
        sink: Sink | None = None,
        *,
        converse_api: str = "converse",
    ) -> Result:
        """Run one `converse` or `invoke_model` attempt.

        Plain Converse (IAM `bedrock:InvokeModel` only) when nothing streams to a `sink`,
        `converse_api` is `converse` and `timing.attempt_secs` bounds the whole answer; else
        ConverseStream with the first-response and idle timers (a caller that sets no attempt
        timer keeps the stream's hung-region detection).
        """
        if op == "converse":
            plain = converse_api != "stream" and timing.attempt_secs is not None
            if sink is None and plain:
                return await _reset_retry(lambda _: self._converse(params, route, timing), None)
            # A sink is always passed: it marks that the stream answered (no quick retry then).
            return await _reset_retry(
                lambda s: self._converse_stream(params, route, timing, s), sink
            )
        if op == "invoke_model":
            return await _reset_retry(lambda _: self._invoke(params, route, timing), None)
        raise ValueError(f"Bedrock does not support op {op!r}")

    async def _converse(self, params: Mapping[str, Any], route: Route, timing: Timing) -> Result:
        async with self._pool.borrow_bedrock(route) as client:
            pending = client.converse(**{**params, "modelId": route.model_id})
            limit = timing.attempt_secs or timing.first_response_secs
            reply = await first(pending, limit, "response")
        body: dict[str, Any] = {k: v for k, v in reply.items() if k != "ResponseMetadata"}
        return Result(body, _bedrock_usage(body.get("usage")))

    async def _converse_stream(
        self, params: Mapping[str, Any], route: Route, timing: Timing, sink: Sink | None
    ) -> Result:
        events = []
        # aclosing: a sink that raised (or a cancel during it) left the generator suspended and
        # its connection held until the asyncgen finalizer ran (CL-10).
        async with contextlib.aclosing(self.stream(params, route, timing)) as stream:
            async for event in stream:
                events.append(event)
                if sink is not None:
                    await sink(event)
        body = _converse_body(events)
        return Result(body, _bedrock_usage(body.get("usage")))

    async def _invoke(self, params: Mapping[str, Any], route: Route, timing: Timing) -> Result:
        request = {**params, "modelId": route.model_id}
        if isinstance(request.get("body"), Mapping | list):
            request["body"] = orjson.dumps(request["body"])
        request.setdefault("contentType", "application/json")
        request.setdefault("accept", "application/json")
        async with self._pool.borrow_bedrock(route) as client:
            raw = await first(
                client.invoke_model(**request), timing.first_response_secs, "first response"
            )
            async with raw["body"] as stream:
                data = await first(stream.read(), timing.idle_secs, "body")
        headers = raw.get("ResponseMetadata", {}).get("HTTPHeaders", {})
        counted = headers.get(_TOKEN_HEADER)
        response = {
            "body_b64": base64.b64encode(data).decode("ascii"),
            "contentType": raw.get("contentType", ""),
        }
        return Result(response, Usage(input_tokens=int(counted) if counted else None))

    def classify(self, exc: BaseException) -> ErrorInfo:
        """By Bedrock error family (`classify_bedrock`)."""
        return classify_bedrock(exc)


def decode_invoke_body(response: Mapping[str, Any]) -> Any:
    """The model body of an `invoke_model` result: parsed JSON, or raw bytes."""
    data = base64.b64decode(response["body_b64"])
    if "json" in str(response.get("contentType", "")).lower():
        return orjson.loads(data)
    return data


# ------------------------------------------------------------------------------- OpenAI

# Keyword names each SDK method accepts, read once per resource class (plan P1).
_ACCEPTED: dict[Any, frozenset[str]] = {}


def split_params(method: Callable[..., Any], params: Mapping[str, Any]) -> dict[str, Any]:
    """`params` with every key `method` does not take moved into `extra_body` (plan P1).

    Servers like vLLM take `top_k`, `chat_template_kwargs`, `guided_json`, ...: passed as
    keywords they raised `TypeError`; in `extra_body` the SDK sends them in the JSON body. An
    `extra_body` already in `params` wins over a moved key of the same name.
    """
    owner = getattr(method, "__func__", method)
    accepted = _ACCEPTED.get(owner)
    if accepted is None:
        accepted = _ACCEPTED[owner] = frozenset(inspect.signature(method).parameters)
    out = {k: v for k, v in params.items() if k in accepted}
    moved = {k: v for k, v in params.items() if k not in accepted}
    if moved:
        out["extra_body"] = {**moved, **(params.get("extra_body") or {})}
    return out


def _choice_body(slot: dict[str, Any]) -> dict[str, Any]:
    message: dict[str, Any] = {"role": "assistant", "content": "".join(slot["text"]) or None}
    if slot["reasoning"]:
        message["reasoning_content"] = "".join(slot["reasoning"])
    if slot["refusal"]:
        message["refusal"] = "".join(slot["refusal"])
    if slot["calls"]:
        message["tool_calls"] = [slot["calls"][i] for i in sorted(slot["calls"])]
    return {
        "index": slot["index"],
        "message": message,
        "finish_reason": slot["finish"],
        "logprobs": slot["logprobs"],
    }


def _chat_body(chunks: list[Any]) -> dict[str, Any]:
    """Assemble chat.completion.chunk objects into one chat.completion body.

    Every choice (`n` > 1) separately, with reasoning, refusal and logprobs; raises
    `StreamTruncated` when a choice never got its `finish_reason` (the stream was cut).
    """
    slots: dict[int, dict[str, Any]] = {}
    usage = None
    ident, model, created = "", "", int(time.time())
    for chunk in chunks:
        ident, model, created = chunk.id or ident, chunk.model or model, chunk.created or created
        if chunk.usage is not None:
            usage = chunk.usage.model_dump(mode="json", exclude_none=True)
        for choice in chunk.choices or []:
            slot = slots.get(choice.index)
            if slot is None:
                slot = slots[choice.index] = {
                    "index": choice.index,
                    "text": [],
                    "reasoning": [],
                    "refusal": [],
                    "calls": {},
                    "logprobs": None,
                    "finish": None,
                }
            delta = choice.delta
            if delta.content:
                slot["text"].append(delta.content)
            extra = getattr(delta, "model_extra", None) or {}
            reasoning = extra.get("reasoning_content") or extra.get("reasoning")
            if reasoning:  # vLLM / DeepSeek-style servers: not an SDK field
                slot["reasoning"].append(reasoning)
            if getattr(delta, "refusal", None):
                slot["refusal"].append(delta.refusal)
            for call in delta.tool_calls or []:
                tool = slot["calls"].setdefault(
                    call.index,
                    {"id": "", "type": "function", "function": {"name": "", "arguments": ""}},
                )
                if call.id:
                    tool["id"] = call.id
                if call.function is not None:
                    tool["function"]["name"] += call.function.name or ""
                    tool["function"]["arguments"] += call.function.arguments or ""
            if getattr(choice, "logprobs", None) is not None:
                part = choice.logprobs.model_dump(mode="json", exclude_none=True)
                merged = slot["logprobs"] = slot["logprobs"] or {}
                for key, items in part.items():
                    merged.setdefault(key, []).extend(items or [])
            if choice.finish_reason:
                slot["finish"] = choice.finish_reason
    if not slots or any(slot["finish"] is None for slot in slots.values()):
        raise StreamTruncated("the chat stream ended before finish_reason")
    body: dict[str, Any] = {
        "id": ident,
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [_choice_body(slots[i]) for i in sorted(slots)],
    }
    if usage is not None:
        body["usage"] = usage
    return body


def _openai_usage(usage: Mapping[str, Any] | None) -> Usage:
    if not usage:
        return Usage()
    cached = int((usage.get("prompt_tokens_details") or {}).get("cached_tokens") or 0)
    prompt = usage.get("prompt_tokens")
    return Usage(
        # `prompt_tokens` include the cached ones: without the subtraction they counted twice
        # under `tpm_count_cached` and once too often without it.
        input_tokens=None if prompt is None else max(0, int(prompt) - cached),
        output_tokens=usage.get("completion_tokens", 0),
        cache_read_tokens=cached,
    )


class OpenAIProvider:
    """OpenAI-compatible servers: `chat` (over streaming) and `embeddings`."""

    name = "openai"

    def __init__(self, pool: ClientPool) -> None:
        """Use `pool` for clients."""
        self._pool = pool

    async def stream(
        self, params: Mapping[str, Any], route: Route, timing: Timing
    ) -> AsyncGenerator[Any, None]:
        """Chat completion chunks of one attempt, each within the attempt timers."""
        async with self._pool.borrow_openai(route) as client:
            create = client.chat.completions.create
            request = split_params(
                create,
                {
                    **params,
                    "model": route.model_id,
                    "stream": True,
                    "stream_options": {"include_usage": True},
                },
            )
            started = time.monotonic()
            reply = await first(create(**request), timing.first_response_secs, "first response")
            try:
                async for chunk in paced(reply, opened(timing, started), started=False):
                    yield chunk
            finally:  # release the connection on a timeout or cancel too (as for Bedrock)
                with contextlib.suppress(Exception):
                    await reply.close()

    async def call(
        self,
        op: str,
        params: Mapping[str, Any],
        route: Route,
        timing: Timing,
        sink: Sink | None = None,
        *,
        converse_api: str = "converse",
    ) -> Result:
        """Run one `chat` or `embeddings` attempt (`converse_api` is Bedrock's; ignored)."""
        if op == "chat":
            return await _reset_retry(lambda s: self._chat(params, route, timing, s), sink)
        if op == "embeddings":
            return await _reset_retry(lambda _: self._embeddings(params, route, timing), None)
        raise ValueError(f"OpenAI-compatible provider does not support op {op!r}")

    async def _chat(
        self, params: Mapping[str, Any], route: Route, timing: Timing, sink: Sink | None
    ) -> Result:
        chunks = []
        async with contextlib.aclosing(self.stream(params, route, timing)) as stream:  # CL-10
            async for chunk in stream:
                chunks.append(chunk)
                if sink is not None:
                    await sink(chunk)
        body = _chat_body(chunks)
        return Result(body, _openai_usage(body.get("usage")))

    async def _embeddings(self, params: Mapping[str, Any], route: Route, timing: Timing) -> Result:
        async with self._pool.borrow_openai(route) as client:
            create = client.embeddings.create
            reply = await first(
                create(**split_params(create, {**params, "model": route.model_id})),
                timing.first_response_secs,
                "response",
            )
        body = reply.model_dump(mode="json", exclude_unset=True)
        return Result(body, _openai_usage(body.get("usage")))

    def classify(self, exc: BaseException) -> ErrorInfo:
        """By HTTP status (`classify_openai`)."""
        return classify_openai(exc)


# ------------------------------------------------------------------------------- registry

ProviderFactory = Callable[[ClientPool], Provider]
_FACTORIES: dict[str, ProviderFactory] = {"bedrock": BedrockProvider, "openai": OpenAIProvider}


def register_provider(name: str, factory: ProviderFactory) -> None:
    """Add a provider kind (pair it with `registry.register_account_kind`)."""
    if not name or not callable(factory):
        raise ConfigError("register_provider needs a name and a callable factory")
    _FACTORIES[name] = factory


class Providers:
    """One provider instance per kind over a shared `ClientPool`."""

    def __init__(self, pool: ClientPool) -> None:
        """Build lazily over `pool`."""
        self.pool = pool
        self._built: dict[str, Provider] = {}

    def get(self, name: str) -> Provider:
        """The provider for a kind; `ConfigError` for an unknown one."""
        if name not in self._built:
            if name not in _FACTORIES:
                raise ConfigError(f"no provider registered for {name!r}")
            self._built[name] = _FACTORIES[name](self.pool)
        return self._built[name]

    async def aclose(self) -> None:
        """Close the pool's clients."""
        await self.pool.aclose()
