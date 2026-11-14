"""0.1.0b2 engine fixes (RESCAN A1-A13, P2-P10, C3, C9, O1-O4, R3, R10; PLAN A4), on fakellm.

Each test names the bug it pins; each failed on the code before its fix.
"""

from __future__ import annotations

import asyncio
import copy
import dataclasses
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
import redis.asyncio
from engine_testkit import CHAT, GPT, SMALL, Make, stats
from redis.exceptions import ResponseError

from llm_queue.engine import runtime
from llm_queue.engine.envelope import Envelope
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.queue import QueueSettings
from llm_queue.engine.runtime import Engine, EngineSettings, _Backoff

pytestmark = pytest.mark.integration
SLOW = {"latency_ms": {"p50": 3000, "p95": 3000}}
LONG = {"small": {"attempt_timeout_secs": 8}}  # 3 s answers are not attempt timeouts


class Recorder:
    """A metrics sink that keeps every point."""

    def __init__(self) -> None:
        self.points: list[tuple[str, str, float, dict[str, str]]] = []

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        self.points.append(("count", name, value, labels))

    def observe(self, name: str, value: float, **labels: str) -> None:
        self.points.append(("observe", name, value, labels))

    def gauge(self, name: str, value: float, **labels: str) -> None:
        self.points.append(("gauge", name, value, labels))

    def total(self, name: str) -> float:
        return sum(v for kind, n, v, _ in self.points if n == name and kind == "count")


async def _until(check: Callable[[], Awaitable[bool]], secs: float = 5.0) -> None:
    for _ in range(int(secs / 0.05)):
        if await check():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met in time")


async def _running(engine: Engine, rid: str) -> runtime._Held:
    async def check() -> bool:
        held = engine._held.get(rid)
        return held is not None and held.run is not None

    await _until(check)
    return engine._held[rid]


async def test_redelivery_to_the_same_pod_runs_once(make: Make) -> None:
    """A1: a second delivery to the pod running the call overwrote its record and ran it twice."""
    engine, fake = await make(LONG, {"models": {f"{SMALL}@r1": SLOW}})
    task = asyncio.create_task(engine.call("small", CHAT, request_id="redo"))
    held = await _running(engine, "redo")
    assert held.message is not None
    first = held.message.generation
    await engine.queues.return_(held.message)  # its hold lapsed: visible again

    async def taken_again() -> bool:
        return held.message is not None and held.message.generation > first

    await _until(taken_again)
    reply = await task
    assert reply.ok
    assert stats(fake, f"{SMALL}@r1")["calls"] == 1

    async def calm() -> bool:
        return not engine._held

    await _until(calm)
    name = engine._queue_name(engine.registry.current, "small", False)
    assert (await engine.queues.depth(name))[0] == 0  # acked with the new receipt


async def test_drain_timeout_gives_running_calls_back(
    make: Make, store: redis.asyncio.Redis
) -> None:
    """C3: a call still running when the drain ran out stayed hidden until its visibility lapsed."""
    caller, _ = await make(worker=False)
    first, _ = await make(LONG, {"models": {f"{SMALL}@r1": SLOW}})
    task = asyncio.create_task(caller.call("small", CHAT, request_id="drain", timeout_secs=9))
    held = await _running(first, "drain")
    assert held.message is not None
    message = held.message
    await first.stop(drain_secs=0.2)
    fails = await store.hget(first.keys.queue(message.queue, "fails"), message.id)
    assert fails is None  # given back, not failed
    assert not await store.exists(first.keys.model("small", "inprog:drain"))  # refunded
    await make()  # another pod takes it at once
    reply = await task
    assert reply.ok
    assert EngineSettings().drain_secs == 25


async def test_inference_rejected_is_an_invalid_request(make: Make) -> None:
    """P4: `on_exceed: reject` raised out of the worker and looped the message to dead-letter."""
    engine, _ = await make(
        {"small": {"inference_limits": {"max_tokens": 100, "on_exceed": "reject"}}}
    )
    params = {**CHAT, "inferenceConfig": {"maxTokens": 500}}
    for direct in (False, True):
        with pytest.raises(CallFailed) as info:
            await engine.call("small", params, timeout_secs=4, force_direct=direct)
        assert info.value.kind == "invalid_request"


async def test_fallback_model_gets_its_own_limits(make: Make) -> None:
    """P6: a fallback model was sent the caller's model-specific fields and was refused."""
    engine, _ = await make(
        {"small": {"inference_limits": {"max_tokens": 100, "on_exceed": "reject"}}}
    )
    env = Envelope(
        request_id="p6",
        op="converse",
        model="big",
        params={
            **CHAT,
            "inferenceConfig": {"maxTokens": 5000},
            "additionalModelRequestFields": {"reasoning_effort": "high"},
        },
        caller="c",
        timeout_secs=5,
    )
    request = engine._params(engine.registry.current, "small", env, None)
    assert request["inferenceConfig"]["maxTokens"] == 100
    assert "reasoning_effort" not in request.get("additionalModelRequestFields", {})


async def test_throttle_waits_for_its_window(make: Make) -> None:
    """P2: a throttle was retried at once, ignoring Retry-After, up to throttle_retry_attempts."""
    engine, fake = await make(
        scenario={"models": {f"{SMALL}@r1": {"force_error": "ThrottlingException"}}}
    )
    with pytest.raises(CallFailed):
        await engine.call("small", CHAT, timeout_secs=3)
    assert stats(fake, f"{SMALL}@r1")["calls"] <= 3


async def test_hung_first_response_counts_against_the_breaker(
    make: Make, store: redis.asyncio.Redis
) -> None:
    """P3: a first-response timeout shortened by the deadline was never the provider's failure."""
    engine, _ = await make(
        {"big": {"attempt_timeout_secs": 4, "timeout_secs": 6}},
        {"models": {f"{GPT}@r1": {"hang": True}}},
    )
    reply = await engine.call("big", CHAT)
    assert reply.meta.deployment == "aws-a/r2"
    failures = await store.hget(engine.keys.breaker("big", "aws-a/r1"), "failures")
    assert int(failures or 0) == 1


async def test_model_error_moves_without_breaker_charge(
    make: Make, store: redis.asyncio.Redis
) -> None:
    """P9: a ModelErrorException charged the region's breaker though the model failed."""
    engine, _ = await make(
        scenario={"models": {f"{GPT}@r1": {"force_error": "ModelErrorException"}}}
    )
    reply = await engine.call("big", CHAT)
    assert reply.meta.deployment == "aws-a/r2"
    failures = await store.hget(engine.keys.breaker("big", "aws-a/r1"), "failures")
    assert int(failures or 0) == 0


async def test_target_error_on_the_only_deployment_ends_at_once(make: Make) -> None:
    """P10: the model missing on a model's one deployment was retried with backoff to the end."""
    engine, fake = await make(
        scenario={"models": {f"{SMALL}@r1": {"force_error": "ResourceNotFoundException"}}}
    )
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, timeout_secs=5)
    assert info.value.kind == "provider_error"
    assert stats(fake, f"{SMALL}@r1")["calls"] == 1


async def test_credential_backoff_resets_after_success(make: Make) -> None:
    """PLAN A4: the credential backoff only grew; one success never reset it."""
    engine, _ = await make()
    dep = engine.registry.current.deployments("small")[0]
    engine._bad_accounts[(dep.account, dep.region)] = _Backoff(0.0, 40.0)  # lapsed, long backoff
    assert (await engine.call("small", CHAT)).ok
    assert (dep.account, dep.region) not in engine._bad_accounts


async def test_cancel_key_is_checked_by_the_heartbeat(make: Make) -> None:
    """C9: a cancel published while the subscriber was away was lost; the call ran to its end."""
    engine, _ = await make(LONG, {"models": {f"{SMALL}@r1": SLOW}})
    task = asyncio.create_task(engine.call("small", CHAT, request_id="cx"))
    await _running(engine, "cx")
    await engine.state.set(engine.keys.request("cx", "cancel"), "1", ex=60)  # no publish
    with pytest.raises(CallFailed) as info:
        await asyncio.wait_for(task, 2.5)
    assert info.value.kind == "cancelled"


async def test_cancel_gives_the_estimate_back(make: Make, store: redis.asyncio.Redis) -> None:
    """A11: a cancelled call kept its request and tokens charged (refund 0)."""
    engine, _ = await make(
        {"small": {"rpm": 100, "rate_window": "smooth", "attempt_timeout_secs": 8}},
        {"models": {f"{SMALL}@r1": SLOW}},
    )
    task = asyncio.create_task(engine.call("small", CHAT, request_id="give"))
    await _running(engine, "give")
    rate = engine.keys.rate("small", "aws-a/r1")
    assert int(await store.hget(rate, "seen_calls") or 0) == 1
    await engine.cancel("give")
    with pytest.raises(CallFailed):
        await task

    async def refunded() -> bool:
        return int(await store.hget(rate, "seen_calls") or 0) == 0

    await _until(refunded, 3)


async def test_retry_keys_expire_under_the_served_model(
    make: Make, store: redis.asyncio.Redis
) -> None:
    """A3: `retries:{minute}` kept no TTL in a minute without a retry."""
    engine, _ = await make()
    await engine.call("small", CHAT)
    keys = [k async for k in store.scan_iter(match=f"{engine.keys.prefix}:{{m:small}}:retries:*")]
    assert keys
    for key in keys:
        assert await store.ttl(key) > 0


async def test_bad_message_uses_the_models_queue_settings(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A12: a bad message was failed with default queue settings (10 receives, not the model's)."""
    engine, _ = await make({"small": {"queue": {"max_receives": 2}}})
    seen: list[QueueSettings] = []
    real = engine.queues.fail

    async def fail(message: Any, settings: QueueSettings, delay_secs: float = 0.0) -> str:
        seen.append(settings)
        return str(await real(message, settings, delay_secs))

    monkeypatch.setattr(engine.queues, "fail", fail)
    name = engine._queue_name(engine.registry.current, "small", False)
    await engine.queues.send(
        name, b"not json", engine._queue_settings(engine.registry.current, "small")
    )

    async def failed() -> bool:
        return bool(seen)

    await _until(failed, 4)
    assert seen[0].max_receives == 2


async def test_sweeper_runs_below_one_second(make: Make, monkeypatch: pytest.MonkeyPatch) -> None:
    """A13: `sweep_secs` below 1 s set its lock with ex=0, an error: nothing was swept."""
    engine, _ = await make()
    engine.settings = dataclasses.replace(engine.settings, sweep_secs=0.3)
    swept: list[str] = []
    real = engine.admission.sweep

    async def counting(alias: str) -> int:
        swept.append(alias)
        return int(await real(alias))

    monkeypatch.setattr(engine.admission, "sweep", counting)

    async def ran() -> bool:
        return bool(swept)

    await _until(ran, 3)


async def test_refusals_are_counted_once_per_call(make: Make) -> None:
    """O4: a held call counted one refusal per admission poll."""
    engine, _ = await make({"small": {"rpm": 1, "rate_window": "smooth"}})
    sink = Recorder()
    engine.metrics = sink
    await engine.call("small", CHAT)
    with pytest.raises(CallFailed):
        await engine.call("small", CHAT, timeout_secs=2)
    assert sink.total("llmq_admission_refusals_total") == 1


async def test_health_is_cached_and_catches_refusals(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O1: an OOM reply raised out of readiness; O2: liveness stayed true with dead loops."""
    engine, _ = await make()
    assert (await engine.health_check())["stores"]["state"] == "ok"

    async def oom(*args: Any, **kwargs: Any) -> Any:
        raise ResponseError("OOM command not allowed when used memory > 'maxmemory'.")

    monkeypatch.setattr(engine.state, "set", oom)
    stores = await engine._probe_stores()
    assert stores["state"] == "down"
    assert (await engine.health_check())["stores"]["state"] == "down"  # the cached state
    monkeypatch.undo()

    async def boom() -> None:
        raise RuntimeError("loop died")

    dead = asyncio.create_task(boom())
    with pytest.raises(RuntimeError):
        await dead
    engine._tasks.append(dead)
    assert (await engine.health_check())["live"] is False


async def test_new_model_gets_a_worker_loop(make: Make) -> None:
    """R3: a model added by a newer registry got no receive loop on running pods."""
    engine, _ = await make()
    doc = copy.deepcopy(dict(engine.registry.current.registry.raw))
    doc["models"]["extra"] = copy.deepcopy(doc["models"]["small"])
    doc["models"]["extra"]["model_id"] = "openai.gpt-oss-extra-1:0"
    doc["registry_version"] = int(doc["registry_version"]) + 1
    await engine.registry.publish(doc, actor="test")
    await engine.registry.load()
    reply = await engine.call("extra", CHAT, timeout_secs=5)
    assert reply.ok


async def test_stale_config_reload_is_single_flight(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R10: every call that met `stale_config` reloaded the registry on its own (a storm)."""
    engine, _ = await make()
    await asyncio.sleep(0.3)  # the reload loop's first refresh (at start) is done
    loads = 0
    real = engine.registry.load

    async def slow_load() -> Any:
        nonlocal loads
        loads += 1
        await asyncio.sleep(0.2)
        return await real()

    monkeypatch.setattr(engine.registry, "load", slow_load)
    await asyncio.gather(*(engine._reload_registry(engine.registry.changes) for _ in range(10)))
    assert loads == 1


async def test_changed_epoch_reregisters(make: Make, store: redis.asyncio.Redis) -> None:
    """A2: a pod that found the store's new epoch (set by another pod) re-registered nothing."""
    engine, _ = await make(LONG, {"models": {f"{SMALL}@r1": SLOW}})
    task = asyncio.create_task(engine.call("small", CHAT, request_id="ep"))
    await _running(engine, "ep")
    entry = engine.keys.model("small", "inprog:ep")
    await store.delete(entry)
    await store.set(engine.keys.epoch, "another-pods-epoch")
    await engine._check_epoch()
    assert await store.exists(entry)
    assert (await task).ok


async def test_lost_grant_is_reregistered_by_the_heartbeat(
    make: Make, store: redis.asyncio.Redis
) -> None:
    """A6: RENEW's count was ignored; a running call whose entry was lost stayed unprotected."""
    engine, _ = await make(LONG, {"models": {f"{SMALL}@r1": SLOW}})
    task = asyncio.create_task(engine.call("small", CHAT, request_id="lost"))
    await _running(engine, "lost")
    entry = engine.keys.model("small", "inprog:lost")
    await store.delete(entry)

    async def back() -> bool:
        return bool(await store.exists(entry))

    await _until(back, 2.5)
    assert (await task).ok


async def test_tenant_is_hashed_in_the_call_record(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X10: the raw tenant id went into every `llmq_call_finished` log line."""
    engine, _ = await make()
    lines: list[dict[str, Any]] = []
    real = runtime.logger.info

    def capture(event: str, **kw: Any) -> None:
        if event == "llmq_call_finished":
            lines.append(kw)
        real(event, **kw)

    monkeypatch.setattr(runtime.logger, "info", capture)
    await engine.call("small", CHAT, tenant_id="acme-corp")
    assert lines
    assert lines[0]["tenant"] == engine.hash_id("acme-corp")
