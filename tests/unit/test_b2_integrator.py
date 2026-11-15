"""0.1.0b2 integration of the clients, runtime and registry work (handoffs), without a store.

Each test names the handoff it pins; each failed on the merged code before the change.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from llm_queue import CallFailed, OpenAIOptions, Tuning, WarmOptions
from llm_queue.clients.timing import Timing, opened
from llm_queue.core import LLMQueue
from llm_queue.engine.admission import Admission
from llm_queue.engine.http import Handlers
from llm_queue.engine.keys import Keys
from llm_queue.engine.offload import S3Offload
from llm_queue.engine.requests import RequestStore
from llm_queue.engine.runtime import EngineSettings
from llm_queue.errors import ConfigError
from llm_queue.registry import check, load_registry
from llm_queue.tuning import EngineTimings

FIXTURES = Path(__file__).parent / "registry" / "fixtures"


def _small() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((FIXTURES / "smallest.json").read_text(encoding="utf-8"))
    return data


def test_tuning_carries_openai_and_warm_options() -> None:
    """6: OpenAI and warm-up options could not be set from `Tuning` (no field to pass them)."""
    tuning = Tuning()
    assert tuning.openai == OpenAIOptions()
    assert tuning.warm == WarmOptions()
    assert Tuning(warm=WarmOptions(connections=0, refresh_secs=0)).problems() == []
    assert any("warm.connections" in p for p in Tuning(warm=WarmOptions(connections=-1)).problems())
    bad = Tuning(openai=OpenAIOptions(max_connections=0)).problems()
    assert any("openai.max_connections" in p for p in bad)


def test_throttle_pacing_is_tunable_and_reaches_the_scripts() -> None:
    """6: throttle backoff, slow-mark staleness and claim TTL were module constants."""
    timings = EngineTimings(throttle_base_ms=500, throttle_max_ms=8000, job_ttl_ms=60_000)
    admission = Admission(MagicMock(), Keys("p"), timings=timings)
    assert admission._t["throttle_base_ms"] == 500
    assert admission._t["throttle_max_ms"] == 8000
    assert admission._t["slow_stale_ms"] == 300_000
    assert admission._t["job_ttl_ms"] == 60_000
    wrong = Tuning(engine=EngineTimings(throttle_base_ms=9000, throttle_max_ms=8000))
    assert any("throttle_base_ms must not be above" in p for p in wrong.problems())


def test_first_response_budget_covers_the_open() -> None:
    """K7: the first event got a whole `first_response_secs` again after a slow open."""
    started = time.monotonic() - 0.4
    left = opened(Timing(1.0, 30.0), started).first_response_secs
    assert 0.5 < left < 0.61
    assert opened(Timing(0.1, 30.0), started).first_response_secs == 0.0


def test_id_hash_salt_is_not_in_repr() -> None:
    """13: `repr(EngineSettings)` printed the salt of every id hash."""
    assert "s3cret" not in repr(EngineSettings(id_hash_salt="s3cret"))


def test_supports_tools_is_a_validated_model_setting() -> None:
    """7: `supports_tools` was read with getattr and could never be set in the registry."""
    doc = _small()
    alias = next(iter(doc["models"]))
    assert load_registry(doc).value(alias, "supports_tools") is True
    doc["models"][alias]["supports_tools"] = False
    assert load_registry(doc).value(alias, "supports_tools") is False
    doc["models"][alias]["supports_tools"] = "no"
    _, errors, _ = check(doc)
    assert any("supports_tools" in e for e in errors)


def test_dedupe_refuses_an_offload_that_deletes_on_read() -> None:
    """16: with result_dedupe_secs, the second reader of an offloaded result found it deleted."""
    client = MagicMock()
    with pytest.raises(ConfigError, match="delete_after_read=False"):
        RequestStore(
            client, Keys("p"), result_dedupe_secs=60, offload=S3Offload("b"), offload_bytes=1
        )
    keep = S3Offload("b", delete_after_read=False)
    RequestStore(client, Keys("p"), result_dedupe_secs=60, offload=keep, offload_bytes=1)
    # No dedupe: deleting on read is right.
    RequestStore(client, Keys("p"), offload=S3Offload("b"), offload_bytes=1)


async def test_liveness_route_reports_dead_loops() -> None:
    """18: /health/live answered 200 even with a dead background loop.

    Liveness reads the engine's `_live()` only, never the store-probing health check (FO-9).
    """
    for live, code in ((True, 200), (False, 503)):
        engine = SimpleNamespace(_live=lambda live=live: live)
        status, body = await Handlers(engine).live()  # type: ignore[arg-type]
        assert (status, body) == (code, {"live": live})


def test_register_job_takes_a_timeout() -> None:
    """17: `LLMQueue.register_job` could not pass the job runner's `timeout_secs`."""
    engine = SimpleNamespace(
        queues=None,
        settings=SimpleNamespace(queue_prefix="", queue_wait_replicas=0, queue_wait_ms=100),
        metrics=MagicMock(),
    )
    queue = LLMQueue(engine)  # type: ignore[arg-type]

    async def job(payload: Any) -> None:
        return None

    queue.register_job("j", job, timeout_secs=2.5)
    assert queue._jobs._timeouts["j"] == 2.5
    assert queue._jobs.metrics is engine.metrics  # the engine's sink, not a NullSink


def test_new_kinds_are_call_failed_kinds() -> None:
    """1: `invalid_request`, `context_length_exceeded`, `worker_shutdown` were not in KINDS."""
    from llm_queue import KINDS

    for kind in ("invalid_request", "context_length_exceeded", "worker_shutdown"):
        assert kind in KINDS
        assert CallFailed(kind).kind == kind


async def test_send_job_at_the_limit_is_queue_full(monkeypatch: pytest.MonkeyPatch) -> None:
    """send_job raised the queue's internal `QueueFull` (not an LLMQueueError) at the limit."""
    from llm_queue.engine.queue import QueueFull

    engine = SimpleNamespace(
        queues=None,
        settings=SimpleNamespace(queue_prefix="", queue_wait_replicas=0, queue_wait_ms=100),
        metrics=None,
    )
    queue = LLMQueue(engine)  # type: ignore[arg-type]

    async def full(*args: Any, **kwargs: Any) -> str:
        raise QueueFull("jobs-j", 10_000)

    monkeypatch.setattr(queue._jobs, "send", full)
    with pytest.raises(CallFailed) as info:
        await queue.send_job("j", {})
    assert info.value.kind == "queue_full"
