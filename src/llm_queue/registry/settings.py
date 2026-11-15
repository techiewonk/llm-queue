"""Every registry setting: where it may be set, its type and range, and its library default.

One table drives validation (unknown names, levels, types, ranges) and resolution (defaults), so
a new setting is one row here. Used by `registry.schema` (checks) and `registry.resolve`.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

# Levels, most general first: `defaults`, an account's `settings`, the model, a deployment.
D, A, M, R = "D", "A", "M", "R"
LEVEL_NAMES = {D: "defaults", A: "account", M: "model", R: "deployment"}

Check = Callable[[Any], "str | None"]

STRATEGIES = ("ordered", "round_robin", "weighted", "headroom")
# `\Z`, not `$`: `$` also matches before one trailing newline, so `"a\n"` passed (R3CF-4).
_CUSTOM_RE = re.compile(r"^custom:[A-Za-z0-9_.-]{1,64}\Z")
_HOOK_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}\Z")
# Every number in the registry stays exact as a double: Lua reads numbers as doubles, and
# orjson refuses integers above 64 bits, so `rpm: 2**64` failed boot with a raw TypeError and
# `2**53 + 1` lost precision (R3CF-2, owner Q42). Settings with a real limit are tighter.
MAX_NUMBER = 2**53 - 1
DAY_SECS = 86_400  # the longest any timeout or wait may be (Q42)


def _is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _shown(value: float) -> str:
    """A bound as people read it: `86400`, `0.5`, `9007199254740991` (not `9.0072e+15`)."""
    return str(int(value)) if float(value).is_integer() else f"{value:g}"


def number(
    lo: float | None = None,
    hi: float | None = None,
    *,
    integer: bool = False,
    lo_open: bool = False,
) -> Check:
    """A finite number (a whole number when `integer`) inside [lo, hi].

    `lo_open` makes the low end strict. With no `hi` the bound is `MAX_NUMBER` (2**53 - 1),
    and with no `lo` it is `-MAX_NUMBER` (R3CF-2). `NaN` and infinities are refused first:
    they passed every range check, since every comparison with `NaN` is false (R3CF-1).
    """
    top = MAX_NUMBER if hi is None else hi
    bottom = -MAX_NUMBER if lo is None else lo

    def check(value: Any) -> str | None:
        if not _is_number(value):
            return "must be a whole number (an integer)" if integer else "must be a number"
        if not math.isfinite(value):
            return "must be a finite number"
        if integer and not isinstance(value, int):
            return "must be a whole number (an integer, no decimal point)"
        if value <= bottom if lo_open else value < bottom:
            return f"must be {'above' if lo_open else 'at least'} {_shown(bottom)}"
        if value > top:
            return f"must be at most {_shown(top)}"
        return None

    return check


def one_of(*values: str) -> Check:
    """One of the given words."""

    def check(value: Any) -> str | None:
        return None if value in values else f"must be one of {', '.join(values)}"

    return check


def boolean(value: Any) -> str | None:
    """True or false."""
    return None if isinstance(value, bool) else "must be true or false"


def text(value: Any) -> str | None:
    """A non-empty string."""
    return None if isinstance(value, str) and value else "must be a non-empty string"


def strategy(value: Any) -> str | None:
    """A built-in balancing strategy or `custom:<name>`."""
    if value in STRATEGIES or (isinstance(value, str) and _CUSTOM_RE.match(value)):
        return None
    return f"must be one of {', '.join(STRATEGIES)} or custom:<name>"


def hook_name(value: Any) -> str | None:
    """The name of a registered hook."""
    if isinstance(value, str) and _HOOK_RE.match(value):
        return None
    return "must be a hook name: 1-64 letters, digits, `_`, `.` or `-`"


def text_list(value: Any) -> str | None:
    """A list of non-empty strings."""
    if isinstance(value, list) and all(isinstance(v, str) and v for v in value):
        return None
    return "must be a list of non-empty strings"


def float_embeddings(value: Any) -> str | None:
    """Cohere `embedding_types`: `["float"]` only, the one type the reply parser reads.

    `["int8"]` made every call get paid for, then fail as unreadable; `["float", "int8"]`
    dropped the int8 vectors silently (R3JE-22).
    """
    return None if value == ["float"] else 'must be ["float"] (the only type returned)'


def any_object(value: Any) -> str | None:
    """Any JSON object (passed through unchecked)."""
    return None if isinstance(value, dict) else "must be an object"


class ObjCheck:
    """An object whose keys are among `fields`, each checked; `required` keys must be present.

    `deprecated` names are accepted and dropped with a warning (`registry.schema` reads
    `fields` and `deprecated` to do it, and drops unknown fields with a warning in lenient
    mode: a newer pod's sub-field made every older pod refuse the stored document, R3CF-8).
    """

    def __init__(
        self,
        fields: Mapping[str, Check],
        required: tuple[str, ...] = (),
        deprecated: Mapping[str, str] | None = None,
    ) -> None:
        """`deprecated` maps a removed name to the reason shown in its warning."""
        self.fields = fields
        self.required = required
        self.deprecated: Mapping[str, str] = deprecated or {}

    def __call__(self, value: Any) -> str | None:
        """Every problem of `value`, joined; None when it is valid."""
        if not isinstance(value, dict):
            return "must be an object"
        problems = [f"needs `{key}`" for key in self.required if key not in value]
        for key, item in value.items():
            sub = self.fields.get(key)
            if sub is None:
                if key in self.deprecated:
                    continue
                known = ", ".join(sorted(self.fields))
                problems.append(f"unknown field `{key}` (known: {known})")
                continue
            problem = sub(item)
            if problem:
                problems.append(f"`{key}` {problem}")
        return "; ".join(problems) or None


def obj(
    fields: Mapping[str, Check],
    *,
    required: tuple[str, ...] = (),
    deprecated: Mapping[str, str] | None = None,
) -> ObjCheck:
    """An object check (see `ObjCheck`)."""
    return ObjCheck(fields, required, deprecated)


def _tags(value: Any) -> str | None:
    if isinstance(value, dict) and all(isinstance(v, str) or _is_number(v) for v in value.values()):
        return None
    return "must be an object of text or number values"


def _causes(value: Any) -> str | None:
    allowed = ("exhausted", "broken", "error")
    if isinstance(value, list) and value and all(v in allowed for v in value):
        return None
    return f"must be a non-empty list of {', '.join(allowed)}"


def _fallback_models(value: Any) -> str | None:
    if not isinstance(value, list) or not value:
        return "must be a non-empty list"
    for item in value:
        if isinstance(item, str) and item:
            continue
        entry = obj({"model": text, "require_headroom": boolean}, required=("model",))(item)
        if entry:
            return f"each entry is an alias or {{model, require_headroom}}: {entry}"
    return None


def _sticky_key(value: Any) -> str | None:
    if value == "session" or (isinstance(value, str) and _CUSTOM_RE.match(value)):
        return None
    return "must be session or custom:<name>"


def _stop(value: Any) -> str | None:
    return None if isinstance(value, str) or text_list(value) is None else "must be text or a list"


_EXTRA_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}\Z")
# Names the OpenAI SDK reads as its own arguments (headers, body, query, timeout), names that
# change what the provider keeps (`store` saves prompt and answer), and the request's own
# fields: an `inference.extra` entry must never reach any of them.
EXTRA_REFUSED = frozenset(
    {
        "extra_headers",
        "extra_body",
        "extra_query",
        "timeout",
        "store",
        "metadata",
        "user",
        "model",
        "messages",
        "input",
        "stream",
        "stream_options",
        "api_key",
        "base_url",
        "tools",
        "tool_choice",
    }
)


def _extra(value: Any) -> str | None:
    """Provider-specific inference fields: plain lowercase names, never an SDK argument."""
    if not isinstance(value, dict):
        return "must be an object"
    for key in value:
        if not _EXTRA_NAME_RE.match(key):
            return f"`{key}` is not a plain lowercase name"
        if key in EXTRA_REFUSED:
            return f"`{key}` is refused (an SDK argument or a request field, not a model option)"
    return None


# Inference parameters and the model type they belong to (rules of §3.9).
MAX_OUTPUT_TOKENS = 10_000_000  # far above any model's output limit (Q42)
CHAT_INFERENCE: Mapping[str, Check] = {
    "max_tokens": number(1, MAX_OUTPUT_TOKENS, integer=True),
    "temperature": number(0, 2),
    "top_p": number(0, 1),
    "stop": _stop,
    "top_k": number(1, 1_000_000, integer=True),
    "seed": number(integer=True),
    "frequency_penalty": number(-2, 2),
    "presence_penalty": number(-2, 2),
    "extra": _extra,
}
EMBED_INFERENCE: Mapping[str, Check] = {
    "dimensions": number(1, 65_536, integer=True),
    "normalize": boolean,
    "input_type": text,
    "truncate": text,
    "embedding_types": float_embeddings,
}
INFERENCE_NAMES = frozenset(CHAT_INFERENCE) | frozenset(EMBED_INFERENCE)


def _locked(value: Any) -> str | None:
    if text_list(value) is None and all(v in INFERENCE_NAMES for v in value):
        return None
    return "must be a list of inference parameter names"


HEALTH_DEFAULTS = {
    "disable_errors": 3,
    "disable_window_secs": 300,
    "probe_after_secs": 900,
    "probe_max_secs": 7200,
    "slow_ratio": 3.0,
    "slow_min_secs": 5,
    "slow_min_samples": 20,
}
ALERT_DEFAULTS = {"quota_used_pct": 90, "dlq_messages": 1}
# Alert settings removed in 0.1.0b2 (they did nothing): accepted with a warning until 0.2, so a
# 0.1.0b1 document still loads (owner Q34, R3AP-5).
REMOVED_ALERTS = dict.fromkeys(
    ("quota_used_mins", "spill_pct", "spill_window_secs", "waiting_age_secs"),
    "removed in 0.1.0b2 (it did nothing); ignored, and refused from 0.2",
)
# A queue's messages and dead letters (which hold the prompt) live at most two weeks.
MAX_RETENTION_SECS = 1_209_600
QUEUE_DEFAULTS = {
    "visibility_secs": 60,
    "max_receives": 10,
    # A message holds the prompt: kept one day unless the model says otherwise (owner Q27).
    "retention_secs": 86_400,
    "dlq_retention_secs": 86_400,
}
# Explicit queue names. Dots are allowed so the default `llm-<alias>` always fits (R3CF-5).
QUEUE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,71}\Z")  # leaves room for "-priority"
JOB_QUEUE_PREFIX = "jobs-"  # the job queues' names: a model queue may not take one


def queue_name(value: Any) -> str | None:
    """An explicit queue (stream) name: short and plain, and not a job queue's name."""
    if not (isinstance(value, str) and QUEUE_NAME_RE.match(value)):
        return "must be 1-71 letters, digits, `_`, `.` or `-` (it becomes a store key)"
    if value.startswith(JOB_QUEUE_PREFIX):
        return f"must not start with `{JOB_QUEUE_PREFIX}` (the job queues use it)"
    return None


_PCT = number(0, 100, integer=True)
_POS = number(0, lo_open=True)
_SECS = number(0, DAY_SECS, lo_open=True)  # timeouts and waits: at most one day (Q42)


@dataclass(frozen=True, slots=True)
class Spec:
    """One setting: the levels it may be set at, its check, its default and how levels combine.

    `default` may be a callable taking the model type (`chat` / `embedding`); `merge` objects
    combine key by key across levels instead of the most specific one replacing the rest.
    `deploy_only` settings never change by live edit; `protected` ones only with
    `registry_live_edit=all` (both read by `registry.store`).
    """

    levels: frozenset[str]
    check: Check
    default: Any = None
    merge: bool = False
    deploy_only: bool = False
    protected: bool = False
    by_type: Callable[[str], Any] | None = field(default=None)


def _spec(levels: str, check: Check, default: Any = None, **kw: Any) -> Spec:
    return Spec(frozenset(levels.split(",")), check, default, **kw)


SETTINGS: Mapping[str, Spec] = {
    "enabled": _spec("A,M,R", boolean, True),
    "rpm": _spec("D,M,R", number(1, integer=True)),
    "tpm": _spec("D,M,R", number(1, integer=True)),
    "rate_limits": _spec(
        "D,A,M,R",
        one_of("both", "rpm", "tpm", "off"),
        by_type=lambda kind: "both" if kind == "chat" else "rpm",
    ),
    "tpm_output_weight": _spec("D,A,M,R", number(1, 1000), 1),
    "tpm_count_cached": _spec("D,A,M,R", boolean, False),
    "default_output_tokens": _spec("D,A,M,R", number(1, MAX_OUTPUT_TOKENS, integer=True), 1000),
    "shared_rpm": _spec("A", number(1, integer=True)),
    "shared_tpm": _spec("A", number(1, integer=True)),
    "rate_window": _spec("D,A,M,R", one_of("smooth", "fixed"), "smooth"),
    "rate_burst": _spec("D,A,M,R", number(0, 1), 0.1),
    "stream_flush_ms": _spec("M,R", number(0, 1000, integer=True), 30),
    "max_concurrency": _spec("D,A,M,R", number(1, 100_000, integer=True)),
    "priority_reserve": _spec(
        "D,A,M", number(0, 1), by_type=lambda kind: 0.2 if kind == "chat" else 0.0
    ),
    "normal_min_share": _spec("D,M", number(0, 1), 0.1),
    "timeout_secs": _spec("D,A,M", _SECS, 120),
    "timeout_max_secs": _spec("D,M", _SECS, 900),
    "max_wait_secs": _spec("D,M", _SECS),
    "attempt_timeout_secs": _spec("D,A,M,R", _SECS, 60),
    "throttle_retry_attempts": _spec("D,A,M,R", number(1, 20, integer=True), 6),
    "retry_base_secs": _spec("D,A,M,R", number(0, DAY_SECS), 1.0),
    # `factor ** tries` overflowed a float at 1e300 on the second retry (R3CF-3).
    "retry_multiplier": _spec("D,A,M,R", number(1, 100), 4.0),
    "retry_spread": _spec("D,A,M,R", number(1, 100), 2.5),
    "retry_max_secs": _spec("D,A,M,R", _SECS, 30.0),
    "breaker_threshold": _spec("D,A,M,R", number(1, 10_000, integer=True), 5),
    "breaker_cooldown_secs": _spec("D,A,M,R", _SECS, 15.0),
    # Failures count toward the threshold only within this window (one after it restarts it).
    "breaker_window_secs": _spec("D,A,M,R", _SECS, 60.0),
    # A probe call that never reported back stops blocking others after this.
    "breaker_probe_secs": _spec("D,A,M,R", _SECS, 60.0),
    "balance": _spec("D,M", strategy, "ordered"),
    "balance_opts": _spec(
        "D,M", obj({"pick": one_of("percent", "max", "spread")}), {"pick": "percent"}, merge=True
    ),
    "balance_priority": _spec("M", strategy),
    "max_attempts": _spec("D,M", number(1, 100, integer=True), 4),
    "retry_budget_pct": _spec("D,M", _PCT, 20),
    "retry_budget_min": _spec("D,M", number(0, 1_000_000, integer=True), 10),
    "stream_idle_secs": _spec("D,M", _SECS, 30),
    "stream_max_bytes": _spec("M", number(1024, 1_073_741_824, integer=True), 8_388_608),
    # A call whose messages (inline images and documents included) are larger fails as
    # `invalid_request` before it is queued.
    "max_message_bytes": _spec("D,M", number(1024, 268_435_456, integer=True), 4_194_304),
    # Non-stream chat calls: plain Converse (IAM `bedrock:InvokeModel`), or ConverseStream
    # (`bedrock:InvokeModelWithResponseStream`) for its first-response timer.
    "converse_api": _spec("D,M", one_of("converse", "stream"), "converse"),
    # False for a model without tool use: a call served by it as a fallback model loses the
    # caller's tool fields (`toolConfig`, `tools`, `tool_choice`) instead of being refused.
    "supports_tools": _spec("M", boolean, True),
    # 0 means no limit, not "never queue" (R3CF-16).
    "max_waiting": _spec("D,M", number(0, 100_000_000, integer=True), 50_000),
    "sticky_move_after_secs": _spec("D,M", _SECS, 1),
    "require_headroom_pct": _spec("D,M", _PCT, 20),
    "tags": _spec("R", _tags),
    "weight": _spec("R", number(0), 1),
    "sticky": _spec(
        "D,M",
        obj({"key": _sticky_key, "idle_ttl_secs": _SECS}, required=("key",)),
    ),
    "fallback": _spec(
        "D,M",
        obj(
            {"on": _causes, "after_secs": number(0, DAY_SECS), "regions": one_of("account", "any")},
            required=("on",),
        ),
    ),
    "model_fallback": _spec(
        "M",
        obj(
            {"on": _causes, "after_secs": number(0, DAY_SECS), "models": _fallback_models},
            required=("on", "models"),
        ),
        protected=True,
    ),
    "store_down": _spec("D,M", one_of("pause", "static_cap", "fail_fast"), "pause"),
    "queue_pct": _spec("D,M", _PCT, 100),
    "worker_slots": _spec("D,M", number(1, 1000, integer=True), 20, deploy_only=True),
    "queue": _spec(
        "D,M",
        obj(
            {
                "name": queue_name,
                "visibility_secs": number(1, 43_200, integer=True),
                "max_receives": number(1, 1000, integer=True),
                "retention_secs": number(60, MAX_RETENTION_SECS, integer=True),
                "dlq_retention_secs": number(60, MAX_RETENTION_SECS, integer=True),
            }
        ),
        QUEUE_DEFAULTS,
        merge=True,
    ),
    "embed_batch_size": _spec("M", number(1, 4096, integer=True), 1),
    # Longest text, in characters, an embed call sends; 0 turns the check off. None: the
    # family's own limit (Titan 50,000, Cohere 2,048; OpenAI counts tokens, not checked).
    "embed_max_chars": _spec("M", number(0, 10_000_000, integer=True)),
    "inference": _spec("D,A,M,R", obj({**CHAT_INFERENCE, **EMBED_INFERENCE}), merge=True),
    "inference_limits": _spec(
        "D,A,M,R",
        obj(
            {
                "max_tokens": number(1, MAX_OUTPUT_TOKENS, integer=True),
                "locked": _locked,
                "on_exceed": one_of("clamp", "reject"),
            }
        ),
        merge=True,
    ),
    "params": _spec("R", any_object, protected=True),
    "health": _spec(
        "D,A,M,R",
        obj(
            {
                "disable_errors": number(1, 10_000, integer=True),
                "disable_window_secs": _SECS,
                "probe_after_secs": _SECS,
                "probe_max_secs": _SECS,
                "slow_ratio": number(1, 1000, lo_open=True),
                "slow_min_secs": number(0, DAY_SECS),
                "slow_min_samples": number(1, 1_000_000, integer=True),
            }
        ),
        HEALTH_DEFAULTS,
        merge=True,
    ),
    "alerts": _spec(
        "D,M",
        obj(
            {"quota_used_pct": number(1, 100), "dlq_messages": number(1, integer=True)},
            deprecated=REMOVED_ALERTS,
        ),
        ALERT_DEFAULTS,
        merge=True,
    ),
    "admission": _spec("D,M", hook_name),
    "fallback_policy": _spec("D,M", hook_name),
    "token_estimator": _spec("D,M", hook_name),
    "probe": _spec("D,M", hook_name),
    "debug_capture": _spec("M", boolean, False, protected=True),
}

# Defaults of the optional fields inside the two fallback rules.
FALLBACK_AFTER_SECS = 2.0
MODEL_FALLBACK_AFTER_SECS = 5.0


def settings_at(level: str) -> frozenset[str]:
    """Names of the settings that may be set at `level`."""
    return frozenset(name for name, spec in SETTINGS.items() if level in spec.levels)
