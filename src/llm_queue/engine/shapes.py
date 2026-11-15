"""Convert chat requests and replies between Bedrock Converse and OpenAI chat completions (Q40).

A model's fallback may be a model of the other provider. Its call is still written in the
caller's model's shape (`converse` for Bedrock, `chat` for OpenAI-compatible) and the caller
reads its reply in that shape, so the worker converts the request to the target's shape before
the provider call and the reply back after it. Used by the runtime (`_params`, `_attempt`,
`_static_cap`) and at fallback choice (`unconvertible`).

Converted: messages (text, images, tool use and tool results), the system prompt, tools and
the tool choice, `inferenceConfig` (maxTokens, temperature, topP, stopSequences) and
`max_tokens` / `max_completion_tokens`, `temperature`, `top_p`, `stop`; the reply's message,
tool calls, stop reason and usage. Fields with no counterpart are dropped (Converse
`additionalModelRequestFields`, guardrails, cache points, reasoning blocks of past turns; OpenAI
`response_format`, `logprobs`, `n`, penalties, `seed`): the target's own `inference` defaults
still apply. A request that cannot be carried over (a document or video block, an image that is
a URL and not inline bytes, an image inside a tool result) is refused by `unconvertible`, so
that target is skipped. Streams are not converted (the runtime does not fall back across shapes
for a stream).
"""

from __future__ import annotations

import base64
import binascii
import time
from collections.abc import Mapping, Sequence
from typing import Any

import orjson

CONVERSE = "converse"
CHAT = "chat"
SHAPES = (CONVERSE, CHAT)

# Converse image formats and their MIME types (the data URL of an OpenAI image part).
_MIME = {"png": "image/png", "jpeg": "image/jpeg", "gif": "image/gif", "webp": "image/webp"}
_FORMAT = {mime: fmt for fmt, mime in _MIME.items()} | {"image/jpg": "jpeg"}
# Stop reasons, both ways. Converse values not listed end as `stop`; OpenAI as `end_turn`.
_TO_OPENAI_STOP = {
    "end_turn": "stop",
    "stop_sequence": "stop",
    "max_tokens": "length",
    "tool_use": "tool_calls",
    "guardrail_intervened": "content_filter",
    "content_filtered": "content_filter",
}
_TO_CONVERSE_STOP = {
    "stop": "end_turn",
    "length": "max_tokens",
    "tool_calls": "tool_use",
    "function_call": "tool_use",
    "content_filter": "content_filtered",
}


class ShapeError(ValueError):
    """A request part has no counterpart in the target's shape (the target is skipped)."""


def converts(source: str, target: str) -> bool:
    """True when a call of shape `source` served in shape `target` needs a conversion."""
    return source != target and source in SHAPES and target in SHAPES


def unconvertible(params: Mapping[str, Any], source: str, target: str) -> str | None:
    """Why `params` cannot be sent in shape `target`; None when it can (or needs nothing)."""
    if not converts(source, target):
        return None
    try:
        request(params, source, target)
    except ShapeError as exc:
        return str(exc)
    return None


def request(params: Mapping[str, Any], source: str, target: str) -> dict[str, Any]:
    """`params` (shape `source`) in shape `target`; a copy as is when no conversion is needed.

    `ShapeError` names the first part with no counterpart.
    """
    if not converts(source, target):
        return dict(params)
    if source == CONVERSE:
        return _converse_to_chat(params)
    return _chat_to_converse(params)


def reply(response: Mapping[str, Any], source: str, target: str, *, model: str) -> dict[str, Any]:
    """A reply of shape `target` (the served model's) in shape `source` (the caller's).

    `model` is the served model's alias, for the OpenAI reply's `model` field.
    """
    if not converts(source, target):
        return dict(response)
    if source == CONVERSE:
        return _chat_reply_to_converse(response)
    return _converse_reply_to_chat(response, model)


# ------------------------------------------------------------------------ Converse -> OpenAI


def _texts(blocks: Any, where: str) -> list[str]:
    """The text of Converse system / text-only blocks; cache points dropped."""
    out: list[str] = []
    for block in blocks or ():
        if not isinstance(block, Mapping):
            raise ShapeError(f"{where}: a block is not an object")
        if "text" in block:
            out.append(str(block["text"]))
        elif "guardContent" in block:
            out.append(str(((block["guardContent"] or {}).get("text") or {}).get("text", "")))
        elif "cachePoint" in block:
            continue
        else:
            raise ShapeError(f"{where}: a `{_kind(block)}` block has no OpenAI counterpart")
    return out


def _kind(block: Mapping[str, Any]) -> str:
    return next(iter(block), "empty")


def _image_part(image: Mapping[str, Any], where: str) -> dict[str, Any]:
    """A Converse image block as an OpenAI `image_url` part (a base64 data URL)."""
    fmt = str(image.get("format", ""))
    data = (image.get("source") or {}).get("bytes")
    if fmt not in _MIME or not isinstance(data, bytes | bytearray):
        raise ShapeError(f"{where}: only an inline png, jpeg, gif or webp image converts")
    url = f"data:{_MIME[fmt]};base64,{base64.b64encode(data).decode('ascii')}"
    return {"type": "image_url", "image_url": {"url": url}}


def _tool_result_text(result: Mapping[str, Any], where: str) -> str:
    parts: list[str] = []
    for item in result.get("content") or ():
        if "text" in item:
            parts.append(str(item["text"]))
        elif "json" in item:
            parts.append(orjson.dumps(item["json"]).decode())
        else:
            raise ShapeError(f"{where}: a tool result `{_kind(item)}` has no OpenAI counterpart")
    text = "\n".join(parts)
    # OpenAI tool messages carry no status: an error result says so in its text.
    return f"error: {text}" if result.get("status") == "error" else text


def _converse_to_chat(params: Mapping[str, Any]) -> dict[str, Any]:
    messages: list[dict[str, Any]] = []
    system = _texts(params.get("system"), "system")
    if system:
        messages.append({"role": "system", "content": "\n".join(system)})
    for i, message in enumerate(params.get("messages") or ()):
        where = f"messages[{i}]"
        role = message.get("role")
        blocks = message.get("content") or ()
        if isinstance(blocks, str):
            blocks = [{"text": blocks}]
        if role == "assistant":
            text: list[str] = []
            calls: list[dict[str, Any]] = []
            for block in blocks:
                if "text" in block:
                    text.append(str(block["text"]))
                elif "toolUse" in block:
                    use = block["toolUse"]
                    calls.append(
                        {
                            "id": use.get("toolUseId"),
                            "type": "function",
                            "function": {
                                "name": use.get("name"),
                                "arguments": orjson.dumps(use.get("input") or {}).decode(),
                            },
                        }
                    )
                elif "reasoningContent" in block or "cachePoint" in block:
                    continue  # a past turn's reasoning is not sent back to another model
                else:
                    raise ShapeError(f"{where}: a `{_kind(block)}` block has no OpenAI counterpart")
            out: dict[str, Any] = {"role": "assistant", "content": "".join(text) or None}
            if calls:
                out["tool_calls"] = calls
            messages.append(out)
            continue
        if role != "user":
            raise ShapeError(f"{where}: role `{role}` has no OpenAI counterpart")
        parts: list[dict[str, Any]] = []
        for block in blocks:
            if "toolResult" in block:
                # Tool messages follow the assistant's tool calls directly in OpenAI's shape.
                result = block["toolResult"]
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": result.get("toolUseId"),
                        "content": _tool_result_text(result, where),
                    }
                )
            elif "text" in block:
                parts.append({"type": "text", "text": str(block["text"])})
            elif "image" in block:
                parts.append(_image_part(block["image"], where))
            elif "cachePoint" in block:
                continue
            elif "guardContent" in block:
                parts.append({"type": "text", "text": "\n".join(_texts([block], where))})
            else:
                raise ShapeError(f"{where}: a `{_kind(block)}` block has no OpenAI counterpart")
        if len(parts) == 1 and parts[0]["type"] == "text":
            messages.append({"role": "user", "content": parts[0]["text"]})
        elif parts:
            messages.append({"role": "user", "content": parts})
    out = {"messages": messages}
    config = params.get("inferenceConfig") or {}
    for source_name, target_name in (
        ("maxTokens", "max_tokens"),
        ("temperature", "temperature"),
        ("topP", "top_p"),
        ("stopSequences", "stop"),
    ):
        if config.get(source_name) is not None:
            out[target_name] = config[source_name]
    tool_config = params.get("toolConfig")
    if tool_config:
        tools = [
            {
                "type": "function",
                "function": {
                    "name": spec.get("name"),
                    **({"description": spec["description"]} if spec.get("description") else {}),
                    "parameters": (spec.get("inputSchema") or {}).get("json") or {},
                },
            }
            for tool in tool_config.get("tools") or ()
            if (spec := tool.get("toolSpec"))
        ]
        if tools:
            out["tools"] = tools
        choice = tool_config.get("toolChoice") or {}
        if "auto" in choice:
            out["tool_choice"] = "auto"
        elif "any" in choice:
            out["tool_choice"] = "required"
        elif "tool" in choice:
            out["tool_choice"] = {"type": "function", "function": {"name": choice["tool"]["name"]}}
    return out


def _converse_reply_to_chat(response: Mapping[str, Any], model: str) -> dict[str, Any]:
    message = (response.get("output") or {}).get("message") or {}
    text: list[str] = []
    calls: list[dict[str, Any]] = []
    for block in message.get("content") or ():
        if "text" in block:
            text.append(str(block["text"]))
        elif "toolUse" in block:
            use = block["toolUse"]
            calls.append(
                {
                    "id": use.get("toolUseId"),
                    "type": "function",
                    "function": {
                        "name": use.get("name"),
                        "arguments": orjson.dumps(use.get("input") or {}).decode(),
                    },
                }
            )
    out_message: dict[str, Any] = {"role": "assistant", "content": "".join(text) or None}
    if calls:
        out_message["tool_calls"] = calls
    usage = response.get("usage") or {}
    prompt = int(usage.get("inputTokens") or 0)
    completion = int(usage.get("outputTokens") or 0)
    chat_usage: dict[str, Any] = {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": int(usage.get("totalTokens") or prompt + completion),
    }
    if usage.get("cacheReadInputTokens"):
        chat_usage["prompt_tokens_details"] = {"cached_tokens": usage["cacheReadInputTokens"]}
    stop = str(response.get("stopReason") or "end_turn")
    return {
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": out_message,
                "finish_reason": _TO_OPENAI_STOP.get(stop, "stop"),
            }
        ],
        "usage": chat_usage,
    }


# ------------------------------------------------------------------------ OpenAI -> Converse


def _content_text(content: Any, where: str) -> str:
    """The text of an OpenAI content (a string or text parts)."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    texts: list[str] = []
    for part in content:
        if part.get("type") != "text":
            raise ShapeError(f"{where}: a `{part.get('type')}` part has no Converse counterpart")
        texts.append(str(part.get("text", "")))
    return "\n".join(texts)


def _image_block(part: Mapping[str, Any], where: str) -> dict[str, Any]:
    """An OpenAI `image_url` part with a base64 data URL as a Converse image block."""
    url = str((part.get("image_url") or {}).get("url", ""))
    head, _, data = url.partition(",")
    mime = head.removeprefix("data:").removesuffix(";base64")
    if not head.startswith("data:") or not head.endswith(";base64") or mime not in _FORMAT:
        raise ShapeError(f"{where}: only an inline png, jpeg, gif or webp data URL converts")
    try:
        raw = base64.b64decode(data, validate=True)
    except (binascii.Error, ValueError):
        raise ShapeError(f"{where}: the image's base64 is invalid") from None
    return {"image": {"format": _FORMAT[mime], "source": {"bytes": raw}}}


def _user_blocks(content: Any, where: str) -> list[dict[str, Any]]:
    if isinstance(content, str):
        return [{"text": content}]
    blocks: list[dict[str, Any]] = []
    for part in content or ():
        kind = part.get("type")
        if kind == "text":
            blocks.append({"text": str(part.get("text", ""))})
        elif kind == "image_url":
            blocks.append(_image_block(part, where))
        else:
            raise ShapeError(f"{where}: a `{kind}` part has no Converse counterpart")
    return blocks


def _arguments(raw: Any, where: str) -> Any:
    if isinstance(raw, Mapping):
        return dict(raw)
    try:
        return orjson.loads(raw or "{}")
    except orjson.JSONDecodeError:
        raise ShapeError(f"{where}: tool call arguments are not JSON") from None


def _chat_to_converse(params: Mapping[str, Any]) -> dict[str, Any]:
    system: list[dict[str, Any]] = []
    messages: list[dict[str, Any]] = []

    def add(role: str, blocks: Sequence[dict[str, Any]]) -> None:
        # Converse wants user and assistant turns in turn: tool results and the user's next
        # words join one user message.
        if not blocks:
            return
        if messages and messages[-1]["role"] == role:
            messages[-1]["content"].extend(blocks)
        else:
            messages.append({"role": role, "content": list(blocks)})

    for i, message in enumerate(params.get("messages") or ()):
        where = f"messages[{i}]"
        role = message.get("role")
        if role in ("system", "developer"):
            text = _content_text(message.get("content"), where)
            if text:
                system.append({"text": text})
        elif role == "user":
            add("user", _user_blocks(message.get("content"), where))
        elif role == "assistant":
            blocks: list[dict[str, Any]] = []
            text = _content_text(message.get("content"), where)
            if text:
                blocks.append({"text": text})
            for call in message.get("tool_calls") or ():
                function = call.get("function") or {}
                blocks.append(
                    {
                        "toolUse": {
                            "toolUseId": call.get("id"),
                            "name": function.get("name"),
                            "input": _arguments(function.get("arguments"), where),
                        }
                    }
                )
            add("assistant", blocks)
        elif role == "tool":
            result = {
                "toolUseId": message.get("tool_call_id"),
                "content": [{"text": _content_text(message.get("content"), where)}],
            }
            add("user", [{"toolResult": result}])
        else:
            raise ShapeError(f"{where}: role `{role}` has no Converse counterpart")
    out: dict[str, Any] = {"messages": messages}
    if system:
        out["system"] = system
    config: dict[str, Any] = {}
    tokens = params.get("max_completion_tokens")
    if tokens is None:
        tokens = params.get("max_tokens")
    for value, name in (
        (tokens, "maxTokens"),
        (params.get("temperature"), "temperature"),
        (params.get("top_p"), "topP"),
        (params.get("stop"), "stopSequences"),
    ):
        if value is not None:
            config[name] = [value] if name == "stopSequences" and isinstance(value, str) else value
    if config:
        out["inferenceConfig"] = config
    tools = [
        {
            "toolSpec": {
                "name": function.get("name"),
                **({"description": function["description"]} if function.get("description") else {}),
                "inputSchema": {"json": function.get("parameters") or {"type": "object"}},
            }
        }
        for tool in params.get("tools") or ()
        if tool.get("type", "function") == "function" and (function := tool.get("function"))
    ]
    if tools:
        tool_config: dict[str, Any] = {"tools": tools}
        choice = params.get("tool_choice")
        if choice == "auto":
            tool_config["toolChoice"] = {"auto": {}}
        elif choice == "required":
            tool_config["toolChoice"] = {"any": {}}
        elif isinstance(choice, Mapping) and (choice.get("function") or {}).get("name"):
            tool_config["toolChoice"] = {"tool": {"name": choice["function"]["name"]}}
        # "none" has no Converse counterpart: the tools stay listed (past tool turns need
        # them) and the model chooses.
        out["toolConfig"] = tool_config
    return out


def _chat_reply_to_converse(response: Mapping[str, Any]) -> dict[str, Any]:
    choices = response.get("choices") or [{}]
    choice = choices[0] or {}
    message = choice.get("message") or {}
    blocks: list[dict[str, Any]] = []
    content = message.get("content")
    text = content if isinstance(content, str) else _reply_text(content)
    if text:
        blocks.append({"text": text})
    for call in message.get("tool_calls") or ():
        function = call.get("function") or {}
        try:
            arguments = _arguments(function.get("arguments"), "reply")
        except ShapeError:
            arguments = {"arguments": function.get("arguments")}  # the model's own text, kept
        blocks.append(
            {
                "toolUse": {
                    "toolUseId": call.get("id"),
                    "name": function.get("name"),
                    "input": arguments,
                }
            }
        )
    usage = response.get("usage") or {}
    prompt = int(usage.get("prompt_tokens") or 0)
    completion = int(usage.get("completion_tokens") or 0)
    converse_usage: dict[str, Any] = {
        "inputTokens": prompt,
        "outputTokens": completion,
        "totalTokens": int(usage.get("total_tokens") or prompt + completion),
    }
    cached = (usage.get("prompt_tokens_details") or {}).get("cached_tokens")
    if cached:
        converse_usage["cacheReadInputTokens"] = cached
    finish = str(choice.get("finish_reason") or "stop")
    return {
        "output": {"message": {"role": "assistant", "content": blocks}},
        "stopReason": _TO_CONVERSE_STOP.get(finish, "end_turn"),
        "usage": converse_usage,
    }


def _reply_text(content: Any) -> str:
    """The text of a reply content that came as parts (some OpenAI-compatible servers)."""
    if not content:
        return ""
    return "".join(str(p.get("text", "")) for p in content if isinstance(p, Mapping))
