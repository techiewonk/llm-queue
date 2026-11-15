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
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable, Mapping
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


class _ConverseAssembler:
    """Folds ConverseStream events, one at a time, into the body `converse` returns.

    Every event used to be kept in a list until the stream ended (2.7 MB for a 4,000-delta
    answer of 60 KB, R3PF-4), and text was joined with `+` per delta, quadratic in the answer
    (R3PF-1). Text, reasoning, redacted bytes and tool input are now parts joined once.
    """

    __slots__ = ("blocks", "body", "citations", "parts", "redacted", "role", "stopped", "tools")

    def __init__(self) -> None:
        self.role = "assistant"
        self.blocks: dict[int, dict[str, Any]] = {}
        self.parts: dict[tuple[int, str], list[str]] = {}  # (index, "text" | "reasoning")
        self.redacted: dict[int, list[bytes]] = {}
        self.tools: dict[int, list[str]] = {}
        self.citations: dict[int, list[Any]] = {}
        self.body: dict[str, Any] = {}
        self.stopped = False

    def add(self, event: Mapping[str, Any]) -> None:
        """Fold one event in."""
        if "contentBlockDelta" in event:
            index = event["contentBlockDelta"].get("contentBlockIndex", 0)
            delta = event["contentBlockDelta"].get("delta") or {}
            if "text" in delta:
                self.blocks.setdefault(index, {"text": ""})
                self.parts.setdefault((index, "text"), []).append(delta["text"])
            elif "toolUse" in delta:
                self.blocks.setdefault(index, {"toolUse": {}})
                piece = (delta["toolUse"] or {}).get("input") or ""
                self.tools.setdefault(index, []).append(piece)
            elif "reasoningContent" in delta:
                block = self.blocks.setdefault(index, {"reasoningContent": {}})
                reasoning = block.setdefault("reasoningContent", {})
                part = delta["reasoningContent"] or {}
                if "redactedContent" in part:
                    self.redacted.setdefault(index, []).append(part["redactedContent"])
                if "text" in part or "signature" in part:
                    text = reasoning.setdefault("reasoningText", {})
                    if "text" in part:
                        text.setdefault("text", "")
                        self.parts.setdefault((index, "reasoning"), []).append(part["text"])
                    if "signature" in part:
                        text["signature"] = part["signature"]
            elif "citation" in delta:
                self.citations.setdefault(index, []).append(delta["citation"])
        elif "messageStart" in event:
            self.role = event["messageStart"].get("role", self.role)
        elif "contentBlockStart" in event:
            index = event["contentBlockStart"].get("contentBlockIndex", 0)
            start = event["contentBlockStart"].get("start") or {}
            if "toolUse" in start:
                self.blocks[index] = {"toolUse": dict(start["toolUse"])}
                self.tools[index] = []
        elif "messageStop" in event:
            self.stopped = True
            self.body["stopReason"] = event["messageStop"].get("stopReason")
            extra = event["messageStop"].get("additionalModelResponseFields")
            if extra is not None:
                self.body["additionalModelResponseFields"] = extra
        elif "metadata" in event:
            for key in ("usage", "metrics", "trace"):
                if key in event["metadata"]:
                    self.body[key] = event["metadata"][key]

    def result(self) -> dict[str, Any]:
        """The assembled body; `StreamTruncated` when the stream ended before `messageStop`."""
        if not self.stopped:
            raise StreamTruncated("the Bedrock stream ended before messageStop")
        blocks = self.blocks
        for (index, kind), parts in self.parts.items():
            if kind == "text":
                blocks[index]["text"] = "".join(parts)
            else:
                blocks[index]["reasoningContent"]["reasoningText"]["text"] = "".join(parts)
        for index, chunks in self.redacted.items():
            blocks[index]["reasoningContent"]["redactedContent"] = b"".join(chunks)
        for index, pieces in self.tools.items():
            # A text block at a tool's index (a broken server) takes the tool too: the
            # KeyError here failed a call whose whole stream had reached the caller (R3ST-4).
            blocks[index].setdefault("toolUse", {})["input"] = _tool_input("".join(pieces))
        for index, cited in self.citations.items():  # the non-streamed shape of a cited answer
            text = blocks.get(index, {}).get("text", "")
            blocks[index] = {"citationsContent": {"content": [{"text": text}], "citations": cited}}
        content = [blocks[i] for i in sorted(blocks)]
        return {"output": {"message": {"role": self.role, "content": content}}, **self.body}


def _tool_input(text: str) -> Any:
    """A streamed tool input as JSON; the raw text when it is not JSON.

    A tool call cut by `stopReason: max_tokens` ends with partial JSON. `orjson.loads` raised,
    classified FATAL, after the whole stream had reached the caller: the caller got
    `provider_error`, the usage was not charged and the breaker was (R3ST-4). Converse takes
    any JSON document as `input`, so the text is kept as a string.
    """
    if not text:
        return {}
    try:
        return orjson.loads(text)
    except orjson.JSONDecodeError:
        return text


def _converse_body(events: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Assemble ConverseStream events into the body `converse` returns.

    Keeps reasoning (text, signature, redacted bytes) and citations; raises `StreamTruncated`
    when the stream ended without `messageStop`.
    """
    assembler = _ConverseAssembler()
    for event in events:
        assembler.add(event)
    return assembler.result()


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
        assembler = _ConverseAssembler()  # folded as they come, no list of events (R3PF-4)
        # aclosing: a sink that raised (or a cancel during it) left the generator suspended and
        # its connection held until the asyncgen finalizer ran (CL-10).
        async with contextlib.aclosing(self.stream(params, route, timing)) as stream:
            async for event in stream:
                assembler.add(event)
                if sink is not None:
                    await sink(event)
        body = assembler.result()
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
        calls = []
        for key in sorted(slot["calls"]):
            call = slot["calls"][key]
            name, arguments = call.pop("parts")
            call["function"] = {"name": "".join(name), "arguments": "".join(arguments)}
            calls.append(call)
        message["tool_calls"] = calls
    return {
        "index": slot["index"],
        "message": message,
        "finish_reason": slot["finish"],
        "logprobs": slot["logprobs"],
    }


# Tool calls a server sent without `index` sort after the indexed ones, in arrival order.
_NO_INDEX = 1 << 30


class _ChatAssembler:
    """Folds chat.completion.chunk objects, one at a time, into one chat.completion body.

    Every choice (`n` > 1) separately, with reasoning, refusal and logprobs. Every chunk used
    to be kept until the stream ended (2.4 KB each, 9.6 MB for a 4,000-chunk answer, R3PF-4);
    each one is now folded in as it arrives and only the text parts are kept.
    """

    __slots__ = ("created", "ident", "model", "slots", "usage")

    def __init__(self) -> None:
        self.slots: dict[int, dict[str, Any]] = {}
        self.usage: dict[str, Any] | None = None
        self.ident, self.model, self.created = "", "", int(time.time())

    def add(self, chunk: Any) -> None:
        """Fold one chunk in."""
        self.ident = chunk.id or self.ident
        self.model = chunk.model or self.model
        self.created = chunk.created or self.created
        if chunk.usage is not None:
            self.usage = chunk.usage.model_dump(mode="json", exclude_none=True)
        for choice in chunk.choices or []:
            slot = self.slots.get(choice.index)
            if slot is None:
                slot = self.slots[choice.index] = {
                    "index": choice.index,
                    "text": [],
                    "reasoning": [],
                    "refusal": [],
                    "calls": {},
                    "last_call": None,
                    "logprobs": None,
                    "finish": None,
                }
            # Azure's asynchronous content-filter chunks have no `delta` (or `delta: null`):
            # reading `.content` raised AttributeError, FATAL, after the whole stream (R3ST-5).
            delta = getattr(choice, "delta", None)
            if delta is not None:
                self._delta(slot, delta)
            if getattr(choice, "logprobs", None) is not None:
                part = choice.logprobs.model_dump(mode="json", exclude_none=True)
                merged = slot["logprobs"] = slot["logprobs"] or {}
                for key, items in part.items():
                    merged.setdefault(key, []).extend(items or [])
            if getattr(choice, "finish_reason", None):
                slot["finish"] = choice.finish_reason

    @staticmethod
    def _delta(slot: dict[str, Any], delta: Any) -> None:
        content = getattr(delta, "content", None)
        if content:
            slot["text"].append(content)
        extra = getattr(delta, "model_extra", None) or {}
        reasoning = extra.get("reasoning_content") or extra.get("reasoning")
        if reasoning:  # vLLM / DeepSeek-style servers: not an SDK field
            slot["reasoning"].append(reasoning)
        if getattr(delta, "refusal", None):
            slot["refusal"].append(delta.refusal)
        calls = slot["calls"]
        for call in getattr(delta, "tool_calls", None) or []:
            key = call.index
            if key is None:
                # Some gateways send tool calls without `index`: every call merged into one
                # (name `f1f2`, arguments `{}{}`, R3ST-11). A new id starts a new call.
                key = slot["last_call"]
                if key is None or (call.id and calls[key]["id"] and call.id != calls[key]["id"]):
                    key = _NO_INDEX + len(calls)
            slot["last_call"] = key
            tool = calls.get(key)
            if tool is None:
                tool = calls[key] = {"id": "", "type": "function", "parts": ([], [])}
            if call.id:
                tool["id"] = call.id
            if call.function is not None:
                tool["parts"][0].append(call.function.name or "")
                tool["parts"][1].append(call.function.arguments or "")

    def result(self) -> dict[str, Any]:
        """The chat.completion body; `StreamTruncated` when a choice never got `finish_reason`."""
        slots = self.slots
        if not slots or any(slot["finish"] is None for slot in slots.values()):
            raise StreamTruncated("the chat stream ended before finish_reason")
        body: dict[str, Any] = {
            "id": self.ident,
            "object": "chat.completion",
            "created": self.created,
            "model": self.model,
            "choices": [_choice_body(slots[i]) for i in sorted(slots)],
        }
        if self.usage is not None:
            body["usage"] = self.usage
        return body


def _chat_body(chunks: Iterable[Any]) -> dict[str, Any]:
    """Assemble chat.completion.chunk objects into one chat.completion body.

    Every choice (`n` > 1) separately, with reasoning, refusal and logprobs; raises
    `StreamTruncated` when a choice never got its `finish_reason` (the stream was cut).
    """
    assembler = _ChatAssembler()
    for chunk in chunks:
        assembler.add(chunk)
    return assembler.result()


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
        assembler = _ChatAssembler()  # folded as they come, no list of chunks (R3PF-4)
        async with contextlib.aclosing(self.stream(params, route, timing)) as stream:  # CL-10
            async for chunk in stream:
                assembler.add(chunk)
                if sink is not None:
                    await sink(chunk)
        body = assembler.result()
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
