"""Tuning: every timeout, pool size and pacing value is a typed setting with a safe default.

These pin that the defaults are valid, that bad values and bad combinations are all listed at
once and refused by `LLMQueue.from_env`, and that each value reaches the client it tunes.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from llm_queue import (
    BedrockOptions,
    ConfigError,
    EngineTimings,
    LLMQueue,
    RegistryOptions,
    StoreOptions,
    Tuning,
)
from llm_queue.engine.cross_checks import start_warnings
from llm_queue.engine.store_client import store_client
from llm_queue.registry import load_registry

REGISTRY = {
    "registry_version": 1,
    "accounts": {"aws-main": {"kind": "aws"}},
    "models": {
        "chat": {
            "type": "chat",
            "provider": "bedrock",
            "model_id": "openai.gpt-oss-20b-1:0",
            "rpm": 10,
            "deployments": {"aws-main": {"confirmed": ["ap-south-1"]}},
        }
    },
}
ENV = {
    "LLMQ_STORE_URL": "redis://127.0.0.1:6379/0",
    "LLMQ_REGISTRY": json.dumps(REGISTRY),
    "LLMQ_METRICS_SINK": "none",
    "LLMQ_ENVIRONMENT": "test",
}


def test_defaults_are_valid() -> None:
    """`Tuning()` (what every app gets by default) has no problem, even with a 100 ms WAIT."""
    assert Tuning().problems(queue_wait_ms=100) == []


def test_every_bad_value_is_listed_at_once() -> None:
    """One fix-and-rerun: wrong types, zero, negative and non-finite values in every part."""
    tuning = Tuning(
        store=StoreOptions(retries=-1, read_timeout_secs=0),
        bedrock=BedrockOptions(max_pool_connections=2.5),  # type: ignore[arg-type]
        engine=EngineTimings(idle_poll_secs=float("nan")),
        registry=RegistryOptions(history_size="20"),  # type: ignore[arg-type]
    )
    problems = tuning.problems()
    assert any("store.retries" in p for p in problems)
    assert any("store.read_timeout_secs must be above 0" in p for p in problems)
    assert any("bedrock.max_pool_connections must be a whole number" in p for p in problems)
    assert any("engine.idle_poll_secs must be finite" in p for p in problems)
    assert any("registry.history_size must be a number" in p for p in problems)


def test_read_timeout_must_be_above_every_blocking_command() -> None:
    """A WAIT, BLPOP or XREAD BLOCK longer than the read timeout would time out every time."""
    tuning = Tuning(store=StoreOptions(read_timeout_secs=4))
    assert tuning.problems(queue_wait_ms=100) == []
    problems = tuning.problems(queue_wait_ms=5000)
    assert len(problems) == 1
    assert "LLMQ_QUEUE_WAIT_MS" in problems[0]
    slow_idle = Tuning(
        store=StoreOptions(read_timeout_secs=4), engine=EngineTimings(idle_slow_secs=5)
    )
    assert any("idle_slow_secs (BLPOP" in p for p in slow_idle.problems())


def test_other_combinations() -> None:
    """Idle polls faster than the slow pace; an assumed-role session within STS limits."""
    assert Tuning(engine=EngineTimings(idle_poll_secs=2)).problems() == [
        "engine.idle_poll_secs must not be above engine.idle_slow_secs"
    ]
    assert Tuning(bedrock=BedrockOptions(role_session_secs=60)).problems() == [
        "bedrock.role_session_secs must be 900 to 43200 (STS limits)"
    ]


def test_from_env_refuses_bad_tuning() -> None:
    """Bad tuning stops start-up with `ConfigError` naming every problem, never a live call."""
    bad = Tuning(store=StoreOptions(read_timeout_secs=0.5, retries=-1))
    with pytest.raises(ConfigError) as info:
        LLMQueue.from_env(ENV, tuning=bad)
    assert "store.retries" in str(info.value)
    assert "store.read_timeout_secs" in str(info.value)


def test_values_reach_their_clients() -> None:
    """Store options reach the connection pool; engine, Bedrock and registry options reach
    the engine, the client pool and the registry store.
    """
    tuning = Tuning(
        store=StoreOptions(read_timeout_secs=20, connect_timeout_secs=2, pool_wait_secs=3),
        bedrock=BedrockOptions(read_timeout_secs=120, role_session_secs=1800),
        engine=EngineTimings(result_recheck_secs=30, stream_read_block_ms=50),
        registry=RegistryOptions(history_size=5, edit_retries=7),
    )
    queue = LLMQueue.from_env(ENV, tuning=tuning)
    engine: Any = queue._engine
    pool = engine.state.connection_pool
    assert pool.connection_kwargs["socket_timeout"] == 20
    assert pool.connection_kwargs["socket_connect_timeout"] == 2
    assert pool.timeout == 3
    assert engine.settings.timings.result_recheck_secs == 30
    assert engine.chunks._block_ms == 50
    assert engine.providers.pool._read_secs == 120
    assert engine.providers.pool._options.role_session_secs == 1800
    assert engine.registry._history_size == 5
    assert engine.registry._edit_retries == 7


def test_store_client_defaults() -> None:
    """Without options the store client gets the defaults, read timeout included (there was
    none: a reply that never came hung its task for good).
    """
    client = store_client("redis://127.0.0.1:6379/0")
    kwargs = client.connection_pool.connection_kwargs
    assert kwargs["socket_timeout"] == StoreOptions().read_timeout_secs
    assert kwargs["socket_connect_timeout"] == StoreOptions().connect_timeout_secs


def test_a_read_timeout_is_not_retried() -> None:
    """Scenario S5 bug: redis-py's Retry retries TimeoutError by default (`retry_on_error`
    only adds to the list), so a half-open store hung each command 4 x 15 s = 60 s, and a
    timed-out Lua write could run twice. Only connection errors and READONLY are retried.
    """
    from redis.exceptions import ConnectionError as StoreConnectionError
    from redis.exceptions import ReadOnlyError
    from redis.exceptions import TimeoutError as StoreTimeoutError

    client = store_client("redis://127.0.0.1:6379/0")
    retry = client.connection_pool.connection_kwargs["retry"]
    supported = retry._supported_errors
    assert StoreTimeoutError not in supported
    assert set(supported) == {StoreConnectionError, ReadOnlyError}


def test_admission_script_timings_are_checked() -> None:
    """The admission script's pacing is tunable: a limiter "cut" that grows the limit, or
    budget keys that expire inside a fixed two-minute window, are refused at start-up.
    """
    assert Tuning(engine=EngineTimings(limiter_cut_factor=1.2)).problems() == [
        "engine.limiter_cut_factor must be below 1 (a cut makes the limit smaller)"
    ]
    assert Tuning(engine=EngineTimings(budget_key_ms=60_000)).problems() == [
        "engine.budget_key_ms must be at least 130000 (a fixed window spans 2 minutes)"
    ]
    assert Tuning(engine=EngineTimings(admission_probe_wait_ms=0)).problems() == [
        "engine.admission_probe_wait_ms must be above 0"
    ]


def test_choice_fields_are_keyed_by_section(monkeypatch: pytest.MonkeyPatch) -> None:
    """CO-23: `_CHOICES` was keyed by bare field name.

    A number field named `http` or `transport` in another section would have been checked as
    a choice. The option classes now also use slots, like the package's other dataclasses.
    """
    from dataclasses import make_dataclass

    from llm_queue import tuning as tuning_module

    bad = Tuning(bedrock=BedrockOptions(transport="x")).problems()
    assert bad == ["bedrock.transport must be one of botocore, aiohttp"]
    assert not hasattr(Tuning(), "__dict__")
    assert not hasattr(EngineTimings(), "__dict__")
    section = make_dataclass("EngineTimings", [("http", int)], frozen=True)
    monkeypatch.setattr(tuning_module, "EngineTimings", section)  # an engine field `http`
    assert tuning_module._check_part(section(http=-1)) == ["engine.http must be above 0"]
    assert tuning_module._check_part(section(http=5)) == []


def test_edit_retries_has_an_upper_bound() -> None:
    """NB-3: `registry.edit_retries` had no upper bound; 20 meant hours of conflict backoff."""
    assert Tuning(registry=RegistryOptions(edit_retries=10)).problems() == []
    assert Tuning(registry=RegistryOptions(edit_retries=11)).problems() == [
        "registry.edit_retries must be 10 or less (each try backs off up to 2 s)"
    ]


def test_r3cf15_upper_bounds_and_pairs() -> None:
    """R3CF-15: `problems()` returned [] for a 1 ms job TTL, 10**30 ms keys, history 1 ..."""
    from llm_queue.tuning import OpenAIOptions, WarmOptions

    tuning = Tuning(
        engine=EngineTimings(job_ttl_ms=1, budget_key_ms=10**30, result_recheck_secs=1e9),
        registry=RegistryOptions(history_size=1),
        store=StoreOptions(retries=10**9),
        warm=WarmOptions(refresh_secs=100, keepalive_secs=1),
        openai=OpenAIOptions(max_keepalive_connections=500),
        bedrock=BedrockOptions(read_timeout_secs=0.001),
    )
    text = "\n".join(tuning.problems())
    for fragment in (
        "engine.result_recheck_secs must be at most 86400",
        "engine.budget_key_ms must be at most 86400000",
        "engine.job_ttl_ms must be at least 60000",
        "registry.history_size must be at least 2",
        "store.retries must be at most 100",
        "warm.refresh_secs must be below warm.keepalive_secs",
        "openai.max_keepalive_connections must not be above",
        "bedrock.read_timeout_secs must be at least 1",
    ):
        assert fragment in text


def test_r3cf10_result_ttl_below_the_recheck_is_refused() -> None:
    """R3CF-10: `LLMQ_RESULT_TTL_SECS=1` was accepted while `result_recheck_secs` is 60: a caller
    that missed its wake-up found its result gone. Both names are in the one problem line.
    """
    assert Tuning().problems(result_ttl_secs=300) == []
    [line] = Tuning().problems(result_ttl_secs=30)
    assert "LLMQ_RESULT_TTL_SECS (30)" in line
    assert "engine.result_recheck_secs (60)" in line
    with pytest.raises(ConfigError, match="LLMQ_RESULT_TTL_SECS"):
        LLMQueue.from_env({**ENV, "LLMQ_RESULT_TTL_SECS": "30"})


def test_r3cf10_registry_pairs_with_the_process_are_warned() -> None:
    """R3CF-10: an attempt timeout above the client's read timeout (the client cut the call
    first) and a call timeout above `LLMQ_STATUS_TTL_SECS` passed with no word at start.
    """
    doc = json.loads(json.dumps(REGISTRY))
    doc["models"]["chat"].update(attempt_timeout_secs=800, timeout_secs=900, breaker_probe_secs=900)
    timeouts = {"bedrock": 300.0}
    lines = start_warnings(load_registry(doc), read_timeouts=timeouts, status_ttl_secs=600)
    assert len(lines) == 2
    assert "attempt_timeout_secs (800)" in lines[0]
    assert "bedrock.read_timeout_secs (300)" in lines[0]
    assert "timeout_secs (900)" in lines[1]
    assert "LLMQ_STATUS_TTL_SECS (600)" in lines[1]
    view = load_registry(REGISTRY)
    assert start_warnings(view, read_timeouts=timeouts, status_ttl_secs=1800) == []
