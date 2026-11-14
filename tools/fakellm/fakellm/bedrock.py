"""Fake Bedrock runtime: `converse`, `converse-stream` and `invoke` under `/model/{modelId}/`.

The region comes from the SigV4 credential scope and the account from its access key id (never
verified), so every account and region can share one endpoint URL. Errors carry
`x-amzn-ErrorType` like the real service. Tokens are reserved (input + maxTokens) when a call
starts and settled to the real usage when it ends, as Bedrock does.
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
from typing import Any
from urllib.parse import unquote

from aiohttp import web

from fakellm import eventstream
from fakellm.behaviour import Engine, Outcome
from fakellm.errors import ERRORS, STREAM_ERROR_ROWS
from fakellm.scenario import Behaviour
from fakellm.state import ENGINE
from fakellm.synth import (
    collect_text,
    count_tokens,
    digest,
    embedding,
    reply_text,
    tool_arguments,
)

_SCOPE = re.compile(r"Credential=([^/,\s]+)/\d{8}/([^/,\s]+)/[^/,\s]+/aws4_request")
_TITAN_DIMS = frozenset({256, 512, 1024})


def region_from_auth(authorization: str) -> str:
    """Region from a SigV4 `Authorization` header, or "unknown" when absent."""
    match = _SCOPE.search(authorization)
    return match.group(2) if match else "unknown"


def account_from_auth(authorization: str) -> str | None:
    """The access key id of a SigV4 `Authorization` header (the fake account), or None."""
    match = _SCOPE.search(authorization)
    return match.group(1) if match else None


def bedrock_error(name: str, message: str | None = None) -> web.Response:
    """Bedrock-shaped error: status from the table, `x-amzn-ErrorType`, `{"message"}` body."""
    spec = ERRORS[name]
    return web.json_response(
        {"message": message or spec.message},
        status=spec.status,
        headers={"x-amzn-ErrorType": spec.wire_code(name)},
    )


def _target(request: web.Request) -> tuple[Engine, str]:
    engine = request.app[ENGINE]
    model_id = request.match_info["model_id"]
    # botocore percent-encodes ':' and '/'; decode whatever the router left encoded.
    model_id = unquote(model_id) if "%" in model_id else model_id
    auth = request.headers.get("Authorization", "")
    return engine, engine.resolve_bedrock_key(
        model_id, region_from_auth(auth), account_from_auth(auth)
    )


async def _json_body(request: web.Request) -> Any:
    try:
        return json.loads(await request.read() or b"null")
    except ValueError:
        return None


def _converse_content(body: dict[str, Any], tool_mode: str) -> tuple[list[Any], str, str]:
    messages = body["messages"]
    tools = (body.get("toolConfig") or {}).get("tools") or []
    last = (messages[-1].get("content") or []) if isinstance(messages[-1], dict) else []
    answered_tool = any(isinstance(b, dict) and "toolResult" in b for b in last)
    seed = {k: body.get(k) for k in ("messages", "system", "toolConfig")}
    text = reply_text(seed)
    if tools and tool_mode == "echo" and not answered_tool:
        choice = ((body.get("toolConfig") or {}).get("toolChoice") or {}).get("tool") or {}
        specs = [t.get("toolSpec") or {} for t in tools if isinstance(t, dict)]
        spec = next((s for s in specs if s.get("name") == choice.get("name")), specs[0])
        schema = (spec.get("inputSchema") or {}).get("json")
        block = {
            "toolUse": {
                "toolUseId": f"tooluse_{digest(seed)[:22]}",
                "name": spec.get("name", "tool"),
                "input": tool_arguments(schema, collect_text(last) or text),
            }
        }
        return [block], "tool_use", json.dumps(block)
    return [{"text": text}], "end_turn", text


def _reasoning_blocks(body: dict[str, Any], mode: str) -> list[dict[str, Any]]:
    """gpt-oss / Claude style reasoning blocks placed before the answer (`reasoning` knob)."""
    seed = digest({k: body.get(k) for k in ("messages", "system")})
    blocks: list[dict[str, Any]] = []
    if mode in ("text", "both"):
        text = f"fakellm reasoning {seed[:16]}: the user asked something; answer it briefly."
        signature = f"sig-{seed[:12]}"
        blocks.append(
            {"reasoningContent": {"reasoningText": {"text": text, "signature": signature}}}
        )
    if mode in ("redacted", "both"):
        blocks.append({"reasoningContent": {"redactedContent": f"redacted-{seed[:24]}".encode()}})
    return blocks


def _json_blocks(content: list[Any]) -> list[Any]:
    """Content as JSON: `redactedContent` bytes become base64, as the service sends blobs."""
    out = []
    for block in content:
        redacted = (block.get("reasoningContent") or {}).get("redactedContent")
        if isinstance(redacted, bytes):
            block = {"reasoningContent": {"redactedContent": base64.b64encode(redacted).decode()}}
        out.append(block)
    return out


def _reply(body: dict[str, Any], outcome: Outcome) -> tuple[list[Any], str, str, int, int]:
    """Content blocks, stop reason, output text and token counts of one Converse reply.

    Reasoning blocks (if any) come first; `out_text` is the answer only, but the reasoning
    text counts in the output tokens.
    """
    behaviour = outcome.behaviour
    content, stop_reason, out_text = _converse_content(body, behaviour.tool_calls)
    if behaviour.output_tokens and stop_reason == "end_turn":
        out_text = (out_text + " ") * (behaviour.output_tokens * 4 // (len(out_text) + 1) + 1)
        out_text = out_text[: behaviour.output_tokens * 4]
        content = [{"text": out_text}]
    input_tokens = count_tokens(collect_text([body.get("system"), body["messages"]]))
    output_tokens = count_tokens(out_text)
    max_tokens = (body.get("inferenceConfig") or {}).get("maxTokens")
    if stop_reason == "end_turn" and isinstance(max_tokens, int) and output_tokens > max_tokens:
        out_text = out_text[: max(1, max_tokens) * 4]
        content = [{"text": out_text}]
        stop_reason, output_tokens = "max_tokens", max_tokens
    reasoning = _reasoning_blocks(body, behaviour.reasoning)
    for block in reasoning:
        output_tokens += count_tokens(collect_text(block))
    return reasoning + content, stop_reason, out_text, input_tokens, output_tokens


def _reserve(body: dict[str, Any]) -> int:
    """Tokens Bedrock holds when a call starts: the input plus maxTokens."""
    max_tokens = (body.get("inferenceConfig") or {}).get("maxTokens")
    inputs = count_tokens(collect_text([body.get("system"), body.get("messages")]))
    return inputs + (max_tokens if isinstance(max_tokens, int) else 0)


def _usage(input_tokens: int, output_tokens: int, behaviour: Behaviour) -> dict[str, int]:
    """Bedrock usage: `inputTokens` is the uncached part; the total adds the cache tokens."""
    read, write = behaviour.cache_read_tokens, behaviour.cache_write_tokens
    return {
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "totalTokens": input_tokens + output_tokens + read + write,
        "cacheReadInputTokens": read,
        "cacheWriteInputTokens": write,
    }


async def _start(
    request: web.Request,
) -> tuple[Engine, str, dict[str, Any], Outcome | web.Response]:
    """Validate a Converse body and run the engine; the outcome, or an error response."""
    engine, key = _target(request)
    body = await _json_body(request)
    messages = body.get("messages") if isinstance(body, dict) else None
    if not isinstance(body, dict) or not isinstance(messages, list) or not messages:
        engine.record_error(key)
        error = bedrock_error("ValidationException", "messages must be a non-empty list")
        return engine, key, {}, error
    inputs = count_tokens(collect_text([body.get("system"), messages]))
    outcome = await engine.call(key, _reserve(body), inputs)
    if outcome.error:
        return engine, key, body, bedrock_error(outcome.error, outcome.behaviour.force_message)
    return engine, key, body, outcome


async def converse(request: web.Request) -> web.Response:
    """Bedrock Converse: deterministic text (or a toolUse block), usage and metrics."""
    engine, key, body, outcome = await _start(request)
    if isinstance(outcome, web.Response):
        return outcome
    content, stop_reason, _, input_tokens, output_tokens = _reply(body, outcome)
    speed = outcome.behaviour.tokens_per_s
    if speed > 0:
        await asyncio.sleep(output_tokens / speed)
    engine.settle(key, outcome, input_tokens + output_tokens)
    reply: dict[str, Any] = {
        "output": {"message": {"role": "assistant", "content": _json_blocks(content)}},
        "stopReason": stop_reason,
        "metrics": {"latencyMs": outcome.latency_ms},
    }
    if not outcome.behaviour.usage_missing:
        reply["usage"] = _usage(input_tokens, output_tokens, outcome.behaviour)
    return web.json_response(reply)


async def converse_stream(request: web.Request) -> web.StreamResponse:
    """Bedrock ConverseStream: the same reply as Converse, as paced event-stream chunks.

    The first event goes out after the latency (time to first response); text then follows at
    `tokens_per_s`, 8 tokens per chunk, tool input in 16-character pieces. Usage arrives in the
    final `metadata` event. `cut_after_chunks` ends the stream early (see `_cut`).
    """
    engine, key, body, outcome = await _start(request)
    if isinstance(outcome, web.Response):
        return outcome
    content, stop_reason, _, input_tokens, output_tokens = _reply(body, outcome)
    behaviour = outcome.behaviour
    response = web.StreamResponse(headers={"Content-Type": eventstream.CONTENT_TYPE})
    await response.prepare(request)
    await response.write(eventstream.event("messageStart", {"role": "assistant"}))
    chunks = 0
    for index, block in enumerate(content):
        for event_type, payload in _block_events(index, block):
            if event_type == "contentBlockDelta" and "text" in payload["delta"]:
                if behaviour.cut_after_chunks is not None and chunks >= behaviour.cut_after_chunks:
                    engine.settle(key, outcome, input_tokens)
                    return await _cut(request, response, behaviour)
                chunks += 1
            await response.write(eventstream.event(event_type, payload))
            text = payload.get("delta", {}).get("text")
            if text and behaviour.tokens_per_s > 0:
                await asyncio.sleep(count_tokens(text) / behaviour.tokens_per_s)
    await response.write(eventstream.event("messageStop", {"stopReason": stop_reason}))
    metadata: dict[str, Any] = {"metrics": {"latencyMs": outcome.latency_ms}}
    if not behaviour.usage_missing:
        metadata["usage"] = _usage(input_tokens, output_tokens, behaviour)
    await response.write(eventstream.event("metadata", metadata))
    engine.settle(key, outcome, input_tokens + output_tokens)
    await response.write_eof()
    return response


def _block_events(index: int, block: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Stream events of one content block: start (tool use only), deltas, stop."""
    events: list[tuple[str, dict[str, Any]]] = []

    def delta(value: dict[str, Any]) -> None:
        events.append(("contentBlockDelta", {"contentBlockIndex": index, "delta": value}))

    if "toolUse" in block:
        tool = block["toolUse"]
        start = {"toolUse": {"toolUseId": tool["toolUseId"], "name": tool["name"]}}
        events.append(("contentBlockStart", {"contentBlockIndex": index, "start": start}))
        raw = json.dumps(tool["input"])
        for i in range(0, len(raw), 16):  # the service splits tool input across deltas
            delta({"toolUse": {"input": raw[i : i + 16]}})
    elif "reasoningContent" in block:
        part = block["reasoningContent"]
        if "redactedContent" in part:
            blob = base64.b64encode(part["redactedContent"]).decode()
            delta({"reasoningContent": {"redactedContent": blob}})
        else:
            text = part["reasoningText"]["text"]
            for i in range(0, len(text), 32):
                delta({"reasoningContent": {"text": text[i : i + 32]}})
            delta({"reasoningContent": {"signature": part["reasoningText"]["signature"]}})
    else:
        text = block["text"]
        for i in range(0, len(text), 32):  # 8 tokens of 4 characters
            delta({"text": text[i : i + 32]})
    events.append(("contentBlockStop", {"contentBlockIndex": index}))
    return events


async def _cut(
    request: web.Request, response: web.StreamResponse, behaviour: Behaviour
) -> web.StreamResponse:
    """End a stream early: an in-stream exception, a stall (never another byte) or a close."""
    if behaviour.cut_with == "stall":
        await asyncio.Event().wait()  # the client's idle timer has to notice
    if behaviour.cut_with == "close":
        if request.transport is not None:
            request.transport.abort()  # no end of the chunked body: a broken connection
        return response
    row = ERRORS[STREAM_ERROR_ROWS[behaviour.cut_with]]
    await response.write(
        eventstream.exception(behaviour.cut_with, behaviour.force_message or row.message)
    )
    await response.write_eof()
    return response


async def invoke(request: web.Request) -> web.Response:
    """Bedrock InvokeModel for Titan v2 and Cohere v3 embedding bodies (deterministic)."""
    engine, key = _target(request)
    body = await _json_body(request)
    problem = None
    if not isinstance(body, dict):
        problem = "body must be a JSON object"
    elif "inputText" in body:
        dims = body.get("dimensions")
        if not isinstance(body["inputText"], str) or not body["inputText"]:
            problem = "inputText must be a non-empty string"
        elif dims is not None and dims not in _TITAN_DIMS:
            problem = f"dimensions must be one of {sorted(_TITAN_DIMS)}"
    elif "texts" in body:
        texts = body["texts"]
        if not isinstance(texts, list) or not texts or not all(isinstance(t, str) for t in texts):
            problem = "texts must be a non-empty list of strings"
        elif len(texts) > 96:
            problem = "texts accepts at most 96 items"
        elif "input_type" not in body:
            problem = "input_type is required"
    else:
        problem = "fakellm supports Titan v2 (inputText) and Cohere v3 (texts) bodies"
    if problem is not None:
        engine.record_error(key)
        return bedrock_error("ValidationException", problem)
    assert isinstance(body, dict)  # narrowed by the checks above
    outcome = await engine.call(key, count_tokens(collect_text(body)))
    if outcome.error:
        return bedrock_error(outcome.error, outcome.behaviour.force_message)
    if "inputText" in body:
        text = body["inputText"]
        tokens = count_tokens(text)
        payload: dict[str, Any] = {
            "embedding": embedding(text, body.get("dimensions") or outcome.behaviour.dims),
            "inputTextTokenCount": tokens,
        }
    else:
        texts = body["texts"]
        tokens = sum(count_tokens(t) for t in texts)
        vectors = [embedding(t, outcome.behaviour.dims) for t in texts]
        payload = {
            "id": digest(body)[:32],
            "texts": texts,
            "response_type": "embeddings_floats",
            "embeddings": {"float": vectors} if body.get("embedding_types") else vectors,
        }
    engine.settle(key, outcome, tokens)
    headers = {"X-Amzn-Bedrock-Invocation-Latency": str(outcome.latency_ms)}
    if outcome.behaviour.usage_missing:
        payload.pop("inputTextTokenCount", None)
    else:
        headers["X-Amzn-Bedrock-Input-Token-Count"] = str(tokens)
        headers["X-Amzn-Bedrock-Output-Token-Count"] = "0"
    return web.json_response(payload, headers=headers)


def add_routes(app: web.Application) -> None:
    """Register the Bedrock runtime routes (model ids may contain `/` and `:`)."""
    app.router.add_post("/model/{model_id:.+}/converse-stream", converse_stream)
    app.router.add_post("/model/{model_id:.+}/converse", converse)
    app.router.add_post("/model/{model_id:.+}/invoke", invoke)
