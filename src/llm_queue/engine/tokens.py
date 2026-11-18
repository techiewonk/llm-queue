"""Token accounting (plan §3.7a): the estimate before a call and what goes back after it.

The estimate is characters / 4 of every text (tool definitions included) plus the output
reservation; after the call the budget is corrected to the real usage. Used by admission.
"""

# Binary blocks (images, documents, video, audio) are not read as text: each costs a fixed
# `BINARY_BLOCK_TOKENS`, and the real usage corrects it after the call. Characters / 4 fits
# English; Indic and CJK scripts take more tokens per character, so a model serving them is
# better estimated by its own tokenizer: `register_token_estimator("name", fn)` once at import,
# then the model's `token_estimator` setting "name" (per model or deployment; plan C8).

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from llm_queue.engine import hooks
from llm_queue.errors import ConfigError

if TYPE_CHECKING:  # an annotation only: importing it pulled the provider stack in (EN-28)
    from llm_queue.clients.providers import Usage

Estimator = Callable[[str, Mapping[str, Any]], int]
_ESTIMATORS: dict[str, Estimator] = {}
# Request fields that carry numbers or switches, not text the model reads.
_SKIP = frozenset(
    {
        "inferenceConfig",
        "modelId",
        "max_tokens",
        "max_completion_tokens",
        "temperature",
        "top_p",
        "seed",
    }
)
# Content blocks that carry binary data (Converse) or a data URL (OpenAI): their base64 text is
# not tokens. Unverified starting value: a ~1 MP image is about 1,300 tokens on Claude.
_BINARY = frozenset({"image", "document", "video", "audio", "image_url", "input_audio", "file"})
BINARY_BLOCK_TOKENS = 1000


def register_token_estimator(name: str, fn: Estimator) -> None:
    """Add an estimator chosen by the `token_estimator` setting: fn(op, params) -> input tokens.

    The setting is per model (or deployment), so one model can use a real tokenizer (e.g. for
    Indic or CJK text) while the others keep characters / 4.
    """
    if not name or not callable(fn):
        raise ConfigError("register_token_estimator needs a name and a callable")
    _ESTIMATORS[name] = fn


def _count(node: Any, totals: list[int]) -> None:
    """Add characters (`totals[0]`) and binary blocks (`totals[1]`) under `node`."""
    if isinstance(node, str):
        totals[0] += len(node)
    elif isinstance(node, Mapping):
        for key, value in node.items():
            if key in _BINARY and isinstance(value, Mapping):
                totals[1] += 1
            elif key not in _SKIP:
                _count(value, totals)
    elif isinstance(node, list | tuple):
        for value in node:
            _count(value, totals)


def input_tokens(op: str, params: Mapping[str, Any], estimator: str | None = None) -> int:
    """Estimated input tokens of a request (every text, tool schemas included).

    Binary blocks count `BINARY_BLOCK_TOKENS` each instead of their encoded size.
    """
    if estimator:
        fn = _ESTIMATORS.get(estimator)
        if fn is None:
            # Registry validation checks only the name's form: say once that it is unknown
            # (it fell back to characters / 4 silently, EN-15).
            hooks.warn_missing("token_estimator", estimator)
        else:
            # A custom estimator that raises falls back to characters / 4: it escaped into the
            # call loop, which returned the message at once with no failure counted (EN-2).
            value = hooks.call_safely(
                "token_estimator", estimator, lambda: int(fn(op, params)), default=None
            )
            if isinstance(value, int):
                return max(1, value)
    totals = [0, 0]
    _count(params, totals)
    return max(1, math.ceil(totals[0] / 4) + totals[1] * BINARY_BLOCK_TOKENS)


def max_tokens_of(op: str, params: Mapping[str, Any]) -> int | None:
    """The `max_tokens` a request carries, if any (Converse or OpenAI shape)."""
    if op == "converse":
        config = params.get("inferenceConfig")
        values = [config.get("maxTokens") if isinstance(config, Mapping) else None]
    else:
        values = [params.get("max_tokens"), params.get("max_completion_tokens")]
    # A whole float or a numeric string counts too (vLLM coerces them), and the larger of the
    # two OpenAI names is reserved (FO-2, FO-3). Anything else reads as not set.
    found = [n for n in map(_whole, values) if n is not None and n > 0]
    return max(found) if found else None


def _whole(value: Any) -> int | None:
    """`value` as a whole number of tokens, or None when it is not one."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        number = float(value.strip()) if isinstance(value, str) else float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if math.isfinite(number) and number == int(number) else None


@dataclass(frozen=True, slots=True)
class Estimate:
    """What a call reserves: input tokens and the output reservation (already weighted)."""

    input: int
    output: int

    @property
    def total(self) -> int:
        """Tokens taken from TPM at admission."""
        return self.input + self.output


def estimate(
    op: str,
    params: Mapping[str, Any],
    *,
    embedding: bool,
    default_output_tokens: int,
    output_weight: float = 1.0,
    estimator: str | None = None,
) -> Estimate:
    """The reservation of one call: input, plus `max_tokens` x the output weight for chat."""
    inputs = input_tokens(op, params, estimator)
    if embedding:
        return Estimate(inputs, 0)
    output = max_tokens_of(op, params) or default_output_tokens
    return Estimate(inputs, math.ceil(output * output_weight))


def used_tokens(
    usage: Usage, *, output_weight: float = 1.0, count_cached: bool = False
) -> int | None:
    """Tokens a finished call cost against TPM, or None when the provider reported nothing.

    Bedrock: input + cache writes + output x the burndown rate; cache reads do not count.
    """
    if usage.input_tokens is None and usage.output_tokens is None:
        return None
    total = (usage.input_tokens or 0) + usage.cache_write_tokens
    total += math.ceil((usage.output_tokens or 0) * output_weight)
    if count_cached:
        total += usage.cache_read_tokens
    return total


@dataclass(frozen=True, slots=True)
class Refund:
    """What goes back to the budgets after an attempt (negative tokens: charge more)."""

    requests: int
    tokens: int


def refund(reserved: Estimate, outcome: str, used: int | None = None) -> Refund:
    """The refund rule of plan §3.7a.

    `ok`: correct to the real usage (nothing back when usage is missing); `unsent` (it never
    reached the provider): the request and every token back; `throttle` (a 429): every token
    back, the request kept, since the provider counted it in its minute (given back, it was
    sent again in that minute: gate BL1 saw 640 against rpm 600); `fail` (timeout, 5xx, cut
    stream): the output part back, the input kept; `cut` (the client gave up mid-answer):
    nothing back.
    """
    if outcome == "ok":
        return Refund(0, 0 if used is None else reserved.total - used)
    if outcome == "unsent":
        return Refund(1, reserved.total)
    if outcome == "throttle":
        return Refund(0, reserved.total)
    if outcome == "fail":
        return Refund(0, reserved.output)
    return Refund(0, 0)
