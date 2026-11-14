"""Tuning: the library's timeouts, pool sizes and pacing, passed in code with safe defaults.

Each value used to be a constant in its module. An app passes `Tuning` to
`LLMQueue.from_env(tuning=...)`, and only the values it sets change; every default is today's
measured value (the comment next to each says why). `problems()` lists every bad value and
every bad combination at once, and `from_env` refuses to start on any (`ConfigError`), so a
bad value never reaches a live call.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, fields
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class StoreOptions:
    """The Valkey / Redis client of every process (`engine/store_client.py`).

    The pool size stays `LLMQ_STORE_MAX_CONNECTIONS`: it follows the deployment's slots.
    """

    retries: int = 3  # with the jittered backoff: about 1.5 s before a call sees the error
    connect_timeout_secs: float = 5.0  # a node that does not answer in a failover fails fast
    # A reply that never comes (half-open connection, packets dropped in a failover) fails the
    # command instead of hanging its task: one-hour soak, two calls held for 50 minutes. Must
    # stay above every blocking command (BLPOP, WAIT, XREAD BLOCK): see `problems()`.
    read_timeout_secs: float = 15.0
    health_check_secs: float = 30.0  # a connection idle this long is pinged before use
    pool_wait_secs: float = 10.0  # a burst waits this long for a free connection


@dataclass(frozen=True, slots=True)
class BedrockOptions:
    """The Bedrock and STS clients (`clients/pool.py`)."""

    read_timeout_secs: float = 300.0  # a long generation; each attempt has its own timer too
    connect_timeout_secs: float = 10.0
    max_pool_connections: int = 100  # per client (one per account x region)
    role_session_secs: int = 3600  # assumed-role session; 3600 is the role-chaining maximum
    # How Bedrock calls are sent: "botocore" (aiobotocore clients) or "aiohttp" (orjson body,
    # botocore only for credentials, SigV4 and stream frames: `clients/bedrock_http.py`).
    # aiohttp is the default: measured vs botocore, converse +78-104 % calls/s, client CPU per
    # call -55-61 %, memory per client 669 -> 87 KiB (2026-11-14, sequential ABAB rounds).
    transport: str = "aiohttp"  # a str: apps pass it from config; problems() checks it


@dataclass(frozen=True, slots=True)
class OpenAIOptions:
    """The OpenAI-compatible clients' timeouts, connection pool and HTTP client (one per account).

    `http="aiohttp"` sends through the SDK's aiohttp client with the same limits and timeouts;
    aiohttp has no idle cap, so `max_keepalive_connections` is not used there. Without one
    (openai older than 1.9x, or no `httpx-aiohttp` / `aiohttp`), httpx is used and a warning logged.
    """

    read_timeout_secs: float = 300.0  # a long generation; each attempt has its own timer too
    connect_timeout_secs: float = 10.0
    max_connections: int = 100
    max_keepalive_connections: int = 20
    keepalive_secs: float = 15.0
    http: Literal["httpx", "aiohttp"] = "aiohttp"


@dataclass(frozen=True, slots=True)
class WarmOptions:
    """Warm connections per Bedrock client (E4); pass `connections=0` to turn it off.

    After a client is built, `connections` unsigned HEAD requests go to its endpoint at once
    through the client's own aiohttp session, so DNS, TCP and TLS are done before the first
    call. They are not billable (no model is invoked; AWS answers an unsigned request with an
    error status and keeps the connection). Every `refresh_secs` the same requests run again,
    so the connections outlive AWS's ~20 s idle close and aiohttp's `keepalive_secs`.
    """

    connections: int = 2
    refresh_secs: float = 10.0  # below `keepalive_secs` and AWS's 20 s idle timeout; 0 = off
    keepalive_secs: float = 15.0  # aiohttp keeps an idle pooled connection this long


@dataclass(frozen=True, slots=True)
class EngineTimings:
    """Pacing of the engine's loops (`engine/runtime.py`, `engine/streaming.py`)."""

    # Receive loop when idle: every 50 ms, then every 1 s after 20 empty polls. A send rings
    # its queue's doorbell, which wakes a blocked worker at once, so slow polls add no latency
    # (idle polling cost ~96 receive scripts/s per pod with 4 models, measured).
    idle_poll_secs: float = 0.05
    idle_slow_secs: float = 1.0
    idle_after_empty: int = 20
    result_recheck_secs: float = 60.0  # a waiting caller's last-resort read of its result
    # A store probe slower than this reports "down": a blackholed store made readiness take
    # 41 s; a cold TLS connect to a remote ElastiCache took 1.2 s (measured).
    health_probe_secs: float = 3.0
    stream_read_block_ms: int = 100  # an opened chunk stream joins the shared read within this
    # Admission scripts (engine/_admission_lua.py), sent with every call.
    admission_probe_wait_ms: int = 200  # a breaker's probe is in flight: check again after this
    admission_slot_wait_ms: int = 50  # no free slot: retry after this ...
    admission_slot_jitter_ms: int = 150  # ... plus up to this, at random (spreads the retries)
    admission_wait_ms: int = 100  # a refusal with no better estimate retries after this
    limiter_cut_ms: int = 1000  # a throttle cuts the adaptive limit at most once per this
    limiter_cut_factor: float = 0.7  # ... to this fraction of itself (0-1)
    budget_key_ms: int = 180_000  # budget and limiter keys live this long (over 2 minutes)
    # A throttled deployment is skipped for a jittered backoff from 1-2 s doubling to 30 s per
    # throttle in a row (or Retry-After when longer), for every pod (P2).
    throttle_base_ms: int = 1000
    throttle_max_ms: int = 30_000
    slow_stale_ms: int = 300_000  # a latency average this old no longer marks a deployment slow
    job_ttl_ms: int = 3_600_000  # a claim record made by a worker lives this long


@dataclass(frozen=True, slots=True)
class RegistryOptions:
    """The registry store (`registry/store.py`).

    Audit retention stays `LLMQ_AUDIT_RETENTION_DAYS`.
    """

    history_size: int = 20  # versions kept for `registry rollback`
    edit_retries: int = 3  # a live edit that raced another one is retried this often (max 10)


@dataclass(frozen=True, slots=True)
class Tuning:
    """All of the above; `Tuning()` is the defaults. Passed to `LLMQueue.from_env(tuning=)`."""

    store: StoreOptions = field(default_factory=StoreOptions)
    bedrock: BedrockOptions = field(default_factory=BedrockOptions)
    engine: EngineTimings = field(default_factory=EngineTimings)
    registry: RegistryOptions = field(default_factory=RegistryOptions)
    openai: OpenAIOptions = field(default_factory=OpenAIOptions)
    warm: WarmOptions = field(default_factory=WarmOptions)

    def problems(self, *, queue_wait_ms: int = 0) -> list[str]:
        """Every bad value and bad combination, one line each (empty when all is well).

        `queue_wait_ms` is `LLMQ_QUEUE_WAIT_MS`: WAIT blocks a connection that long, so the
        store read timeout must be above it.
        """
        out: list[str] = []
        parts = {
            "store": StoreOptions,
            "bedrock": BedrockOptions,
            "engine": EngineTimings,
            "registry": RegistryOptions,
            "openai": OpenAIOptions,
            "warm": WarmOptions,
        }
        wrong = [
            f"{name} must be {kind.__name__}(...), not {type(getattr(self, name)).__name__}"
            for name, kind in parts.items()
            if type(getattr(self, name)) is not kind
        ]
        if wrong:  # `Tuning(store=None)`: nothing below can be read
            return wrong
        for part in (self.store, self.bedrock, self.engine, self.registry, self.openai, self.warm):
            out += _check_part(part)
        # Each combination is checked once its own fields are valid (a bad field is already
        # listed; comparing it would only repeat the same problem).
        bad = {line.split(" ", 1)[0] for line in out}
        store, engine = self.store, self.engine
        blocking = {
            "engine.idle_slow_secs": ("BLPOP", engine.idle_slow_secs),
            "LLMQ_QUEUE_WAIT_MS": ("WAIT", queue_wait_ms / 1000),
            "engine.stream_read_block_ms": ("XREAD BLOCK", engine.stream_read_block_ms / 1000),
        }
        for name, (command, secs) in blocking.items():
            if {"store.read_timeout_secs", name} & bad:
                continue
            if store.read_timeout_secs < secs + 1:
                out.append(
                    f"store.read_timeout_secs ({store.read_timeout_secs:g}) must be at least 1 s "
                    f"above {name} ({command}, {secs:g} s), or that command times out"
                )
        if "engine.limiter_cut_factor" not in bad and engine.limiter_cut_factor >= 1:
            out.append("engine.limiter_cut_factor must be below 1 (a cut makes the limit smaller)")
        if "engine.budget_key_ms" not in bad and engine.budget_key_ms < 130_000:
            out.append(
                "engine.budget_key_ms must be at least 130000 (a fixed window spans 2 minutes)"
            )
        throttle = {"engine.throttle_base_ms", "engine.throttle_max_ms"}
        if not throttle & bad and engine.throttle_base_ms > engine.throttle_max_ms:
            out.append("engine.throttle_base_ms must not be above engine.throttle_max_ms")
        idle = {"engine.idle_poll_secs", "engine.idle_slow_secs"}
        if not idle & bad and engine.idle_poll_secs > engine.idle_slow_secs:
            out.append("engine.idle_poll_secs must not be above engine.idle_slow_secs")
        retries = self.registry.edit_retries
        if "registry.edit_retries" not in bad and retries > _EDIT_RETRIES_MAX:
            # Unbounded, a publish at boot could retry for hours (NB-3).
            out.append(
                f"registry.edit_retries must be {_EDIT_RETRIES_MAX} or less "
                "(each try backs off up to 2 s)"
            )
        session = self.bedrock.role_session_secs
        if "bedrock.role_session_secs" not in bad and not 900 <= session <= 43200:
            out.append("bedrock.role_session_secs must be 900 to 43200 (STS limits)")
        return out


BEDROCK_TRANSPORTS = ("botocore", "aiohttp")
# Live-edit tries at most: with the 2 s backoff cap, a publish backs off 12.5 s at most (NB-3).
_EDIT_RETRIES_MAX = 10
# Fields where 0 is valid: no store retries; no warm connections, or no refresh (off).
_ZERO_OK = frozenset({"retries", "connections", "refresh_secs"})
# Fields that name a choice instead of a number, by `section.field`: keyed by the bare field
# name, a future number field called `http` or `transport` in another section was checked as
# a choice (CO-23).
_CHOICES = {"bedrock.transport": BEDROCK_TRANSPORTS, "openai.http": ("httpx", "aiohttp")}


def _check_part(part: Any) -> list[str]:
    """Type and range of each field: ints are whole numbers, every number finite and above 0."""
    section = {
        StoreOptions: "store",
        BedrockOptions: "bedrock",
        EngineTimings: "engine",
        RegistryOptions: "registry",
        OpenAIOptions: "openai",
        WarmOptions: "warm",
    }[type(part)]
    out: list[str] = []
    for spec in fields(part):
        value = getattr(part, spec.name)
        name = f"{section}.{spec.name}"
        whole = spec.type in ("int", int)
        if (choices := _CHOICES.get(name)) is not None:
            if value not in choices:
                out.append(f"{name} must be one of {', '.join(choices)}")
        elif isinstance(value, bool) or not isinstance(value, int | float):
            out.append(f"{name} must be a number, not {type(value).__name__}")
        elif whole and not isinstance(value, int):
            out.append(f"{name} must be a whole number")
        elif not math.isfinite(value):
            out.append(f"{name} must be finite")
        elif value < 0 or (value == 0 and spec.name not in _ZERO_OK):
            out.append(f"{name} must be {'0 or more' if spec.name in _ZERO_OK else 'above 0'}")
    return out
