"""Inference defaults and limits merged into a request, per provider (plan §3.9 rules 1-8).

The registry's `inference` values fill what the call did not set; `inference_limits.locked`
names parameters the call may not change; `inference_limits.max_tokens` caps the output
(`clamp` or `reject`). Values are written in the provider's own request shape.
"""

from __future__ import annotations

import copy
import math
from collections.abc import Mapping
from typing import Any

from llm_queue.errors import LLMQueueError

# Registry name -> (Converse path, OpenAI field).
_CHAT_BEDROCK = {
    "max_tokens": ("inferenceConfig", "maxTokens"),
    "temperature": ("inferenceConfig", "temperature"),
    "top_p": ("inferenceConfig", "topP"),
    "stop": ("inferenceConfig", "stopSequences"),
    "top_k": ("additionalModelRequestFields", "top_k"),
    "frequency_penalty": ("additionalModelRequestFields", "frequency_penalty"),
    "presence_penalty": ("additionalModelRequestFields", "presence_penalty"),
}
_WRITTEN = ("inferenceConfig", "additionalModelRequestFields", "body")
_CHAT_OPENAI = (
    "max_tokens",
    "temperature",
    "top_p",
    "stop",
    "seed",
    "frequency_penalty",
    "presence_penalty",
    # Not a chat.completions argument: the provider moves it into `extra_body` (plan P1).
    "top_k",
)


class InferenceRejected(LLMQueueError):
    """The call asked for more than `inference_limits` allow and `on_exceed` is `reject`."""


def _get(params: Mapping[str, Any], path: tuple[str, str]) -> Any:
    section = params.get(path[0])
    return section.get(path[1]) if isinstance(section, Mapping) else None


def _section(params: dict[str, Any], name: str) -> dict[str, Any]:
    """The sub-dict `name` of a request, made a dict first.

    A request may carry `"inferenceConfig": None`: `setdefault` returned that None and the
    write raised `TypeError` into the call loop (EN-2).
    """
    current = params.get(name)
    section = dict(current) if isinstance(current, Mapping) else {}
    params[name] = section
    return section


def _set(params: dict[str, Any], path: tuple[str, str], value: Any) -> None:
    _section(params, path[0])[path[1]] = value


def whole_tokens(value: Any, name: str = "max_tokens") -> int:
    """A token count as an int: ints, whole floats and numeric strings are accepted.

    OpenAI-compatible servers (vLLM) coerce `100000.0` or `"100000"`, so a cap that read only
    ints let those through (FO-3). Anything else is refused as `invalid_request`.
    """
    number: float | int
    if isinstance(value, bool):
        raise InferenceRejected(f"{name} must be a whole number, not {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            number = float(value.strip())
        except ValueError:
            raise InferenceRejected(f"{name} must be a whole number") from None
    elif isinstance(value, float):
        number = value
    else:
        raise InferenceRejected(f"{name} must be a whole number, not {type(value).__name__}")
    if not math.isfinite(number) or number != int(number):
        raise InferenceRejected(f"{name} must be a whole number")
    return int(number)


def _capped(current: Any, cap: int, limits: Mapping[str, Any], name: str) -> int:
    """`current` checked against `cap`: the cap when absent or above it (`clamp`), else itself.

    A call that sets no value gets the cap, so the provider's own (often larger) default does
    not apply on a capped model (FO-3).
    """
    if current is None:
        return cap
    value = whole_tokens(current, name)
    if value > cap:
        if limits.get("on_exceed") == "reject":
            raise InferenceRejected(f"{name} {value} is above the limit {cap}")
        return cap
    return value


def apply(
    provider: str,
    op: str,
    params: Mapping[str, Any],
    defaults: Mapping[str, Any],
    limits: Mapping[str, Any],
) -> dict[str, Any]:
    """The request with defaults filled, locked values enforced and `max_tokens` capped."""
    # Only the top level and the sub-dicts written below are copied (the messages are never
    # changed): a deepcopy of the whole request was 1.86 ms per call at 217 KB (measured).
    out = dict(params)
    for key in _WRITTEN:
        if isinstance(out.get(key), Mapping):
            out[key] = dict(out[key])
    locked = set(limits.get("locked", ()))
    if provider == "bedrock" and op == "converse":
        for name, path in _CHAT_BEDROCK.items():
            if name in defaults and (name in locked or _get(out, path) is None):
                value = defaults[name]
                if name == "stop" and isinstance(value, str):
                    value = [value]
                _set(out, path, value)
        extra = defaults.get("extra")
        if extra:
            fields = _section(out, "additionalModelRequestFields")
            for key, value in extra.items():
                fields.setdefault(key, value)
        cap = limits.get("max_tokens")
        if cap is not None:
            current = _get(out, _CHAT_BEDROCK["max_tokens"])
            _set(out, _CHAT_BEDROCK["max_tokens"], _capped(current, cap, limits, "max_tokens"))
        return out
    if provider == "openai" and op == "chat":
        for name in _CHAT_OPENAI:
            if name in defaults and (name in locked or out.get(name) is None):
                out[name] = defaults[name]
        for key, value in (defaults.get("extra") or {}).items():
            out.setdefault(key, value)
        # `max_completion_tokens` is the same limit under its newer OpenAI name: a lock on
        # `max_tokens` drops it, and the cap applies to it too, or a caller got unlimited
        # output on a capped model by sending it instead (FO-2).
        if "max_tokens" in locked:
            out.pop("max_completion_tokens", None)
        cap = limits.get("max_tokens")
        if cap is not None:
            if out.get("max_completion_tokens") is not None:
                out["max_completion_tokens"] = _capped(
                    out["max_completion_tokens"], cap, limits, "max_completion_tokens"
                )
                if out.get("max_tokens") is not None:
                    out["max_tokens"] = _capped(out["max_tokens"], cap, limits, "max_tokens")
            else:
                out["max_tokens"] = _capped(out.get("max_tokens"), cap, limits, "max_tokens")
        return out
    if op == "embeddings" and "dimensions" in defaults:
        out.setdefault("dimensions", defaults["dimensions"])
    if op == "invoke_model" and isinstance(out.get("body"), Mapping):
        body = dict(out["body"])
        names = ("dimensions", "normalize", "input_type", "truncate", "embedding_types")
        for name in names:
            if name in defaults and (name in locked or name not in body):
                body[name] = defaults[name]
        if "inputText" in body:  # Titan takes dimensions / normalize only
            body = {k: v for k, v in body.items() if k not in ("input_type", "truncate")}
        else:  # Cohere takes input_type / truncate / embedding_types only
            body = {k: v for k, v in body.items() if k not in ("dimensions", "normalize")}
        out["body"] = body
    return out


def merge_params(request: Mapping[str, Any], extra: Mapping[str, Any]) -> dict[str, Any]:
    """Merge a deployment's `params` (guardrail, model fields) into a request, one level deep."""
    out = dict(request)
    for key, value in extra.items():
        if isinstance(value, Mapping) and isinstance(out.get(key), Mapping):
            out[key] = {**out[key], **value}
        else:
            out[key] = copy.deepcopy(value)
    return out
