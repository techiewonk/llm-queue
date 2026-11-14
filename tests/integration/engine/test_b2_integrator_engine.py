"""0.1.0b2 integration of the clients, runtime and registry work (handoffs), on fakellm.

Each test names the handoff it pins; each failed on the merged code before the change.
"""

from __future__ import annotations

import asyncio
from typing import Any

import orjson
import pytest
import redis.asyncio
from engine_testkit import CHAT, SMALL, Make, stats

from llm_queue.engine import store_checks, tokens
from llm_queue.engine.envelope import VERSION, Envelope
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.streaming import ChunkReader, ChunkWriter
from llm_queue.errors import ConfigError

pytestmark = pytest.mark.integration


class Recorder:
    """A metrics sink that keeps every point."""

    def __init__(self) -> None:
        self.points: list[tuple[str, float, dict[str, str]]] = []

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        self.points.append((name, value, labels))

    def observe(self, name: str, value: float, **labels: str) -> None:
        self.points.append((name, value, labels))

    def gauge(self, name: str, value: float, **labels: str) -> None:
        self.points.append((name, value, labels))

    def last(self, name: str) -> float | None:
        values = [v for n, v, _ in self.points if n == name]
        return values[-1] if values else None


async def test_plain_converse_unless_the_model_streams(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """3: every call went over ConverseStream (no attempt timer, no `converse_api` passed)."""
    engine, _ = await make({"big": {"converse_api": "stream"}})
    provider = engine.providers.get("bedrock")
    seen: list[tuple[str, Any, bool]] = []
    real = provider.call

    async def call(op: str, params: Any, route: Any, timing: Any, sink: Any = None, **kw: Any):  # type: ignore[no-untyped-def]
        seen.append((kw.get("converse_api", "?"), timing, sink is not None))
        return await real(op, params, route, timing, sink, **kw)

    monkeypatch.setattr(provider, "call", call)
    assert (await engine.call("small", CHAT)).ok
    assert (await engine.call("big", CHAT)).ok
    events = [e async for e in engine.stream("small", CHAT)]
    assert events
    (plain, timing, _), (streamed, _, _), (_, _, sink) = seen
    assert (plain, streamed, sink) == ("converse", "stream", True)
    assert timing.attempt_secs == timing.first_response_secs  # the plain call's own timer


async def test_bad_request_finishes_as_invalid_request(make: Make) -> None:
    """4: a validation error from the provider reached the caller as `provider_error`."""
    engine, fake = await make(
        scenario={"models": {f"{SMALL}@r1": {"force_error": "ValidationException"}}}
    )
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, timeout_secs=5)
    assert info.value.kind == "invalid_request"
    assert stats(fake, f"{SMALL}@r1")["calls"] == 1  # never retried


async def test_clients_are_warmed_together_and_sized(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """5: clients were built one by one at start, each with the fixed pool size."""
    engine, _ = await make()
    calls: list[tuple[list[Any], dict[tuple[str, str], int]]] = []

    async def warm(routes: Any, slots: Any = None) -> None:
        calls.append((list(routes), dict(slots)))

    monkeypatch.setattr(engine.providers.pool, "warm", warm)
    await engine._warm_clients()
    ((routes, slots),) = calls
    assert len(routes) == 6  # big r1 r2 r3, small, titan, cohere: one call for all
    # Four models route to aws-a r1 with the default 20 worker slots each.
    assert slots == {("aws-a", "r1"): 80, ("aws-a", "r2"): 20, ("aws-a", "r3"): 20}


async def test_store_checks_run_at_start(
    make: Make, store: redis.asyncio.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    """8: the eviction policy was never read and a cluster-mode store was not refused."""
    engine, _ = await make()
    health = await engine.health_check()
    assert health["eviction_policy"] == await store_checks.eviction_policy(store)

    async def clustered(client: Any) -> bool:
        return True

    monkeypatch.setattr(store_checks, "cluster_enabled", clustered)
    with pytest.raises(ConfigError, match="cluster mode"):
        await make()


async def test_registry_state_is_exported_as_gauges(make: Make) -> None:
    """9: a pod running on its own config (stored registry refused) showed only in a log."""
    engine, _ = await make()
    sink = Recorder()
    engine.metrics = sink
    engine.registry.stored_refused = True
    engine.registry.on_own_config = True
    engine.registry.invalid_count = 3
    engine._registry_gauges()
    assert sink.last("llmq_registry_stored_refused") == 1.0
    assert sink.last("llmq_registry_on_own_config") == 1.0
    assert sink.last("llmq_registry_lib_too_old") == 0.0
    assert sink.last("llmq_registry_invalid_reads") == 3.0


async def test_reservation_above_tpm_is_too_large_at_once(make: Make) -> None:
    """10: input under the TPM but input + output over it waited until its deadline."""
    engine, _ = await make({"small": {"tpm": 1000, "default_output_tokens": 500}})
    params = {"messages": [{"role": "user", "content": [{"text": "word " * 600}]}]}
    estimate = engine._estimate(
        engine.registry.current,
        "small",
        Envelope(
            request_id="x", op="converse", model="small", params=params, caller="c", timeout_secs=5
        ),
        params,
    )
    assert estimate.input < 1000 < estimate.total  # only the whole reservation is too large
    assert isinstance(estimate, tokens.Estimate)
    loop = asyncio.get_running_loop()
    began = loop.time()
    with pytest.raises(CallFailed) as info:
        await engine.call("small", params, timeout_secs=8)
    assert info.value.kind == "request_too_large"
    assert loop.time() - began < 4


async def test_disabled_model_skips_a_disabled_fallback(make: Make) -> None:
    """11: a disabled model's calls went to its first fallback even when that one was disabled."""
    rule = {"on": ["exhausted"], "models": [{"model": "big"}]}
    engine, fake = await make(
        {"small": {"enabled": False, "model_fallback": rule}, "big": {"enabled": False}}
    )
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, timeout_secs=5)
    assert info.value.kind == "model_unavailable"
    assert "no fallback model is enabled" in str(info.value)
    assert not fake.snapshot()["models"]


async def test_message_cap_covers_every_path(make: Make) -> None:
    """12: only the front door checked the size: direct, embed and job calls skipped it."""
    engine, fake = await make(
        {"small": {"max_message_bytes": 2048}, "titan": {"max_message_bytes": 2048}}
    )
    big = {"messages": [{"role": "user", "content": [{"text": "x" * 4096}]}]}
    for direct in (False, True):
        with pytest.raises(CallFailed) as info:
            await engine.call("small", big, timeout_secs=5, force_direct=direct)
        assert info.value.kind == "invalid_request"
    with pytest.raises(CallFailed) as info:
        await engine.embed("titan", ["y" * 4096])
    assert info.value.kind == "invalid_request"
    assert not fake.snapshot()["models"]


async def test_newer_message_is_passed_on_quietly(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """14: a message from a newer library came back every 5 s, logged each time, no metric."""
    engine, _ = await make()
    sink = Recorder()
    engine.metrics = sink
    delays: list[float] = []
    real = engine.queues.return_

    async def return_(message: Any, delay_secs: float = 0.0) -> bool:
        delays.append(delay_secs)
        return bool(await real(message, delay_secs))

    monkeypatch.setattr(engine.queues, "return_", return_)
    view = engine.registry.current
    body = orjson.dumps({"v": VERSION + 1, "request_id": "new", "model": "small"})
    await engine.queues.send(
        engine._queue_name(view, "small", False), body, engine._queue_settings(view, "small")
    )
    for _ in range(80):
        if delays:
            break
        await asyncio.sleep(0.05)
    assert delays == [15.0]
    assert sink.last("llmq_newer_messages_total") == 1.0


async def test_stream_chunks_carry_bytes(store: redis.asyncio.Redis, prefix: str) -> None:
    """15: bytes in a stream event (Bedrock redacted reasoning) reached the caller as "b'...'"."""
    key = f"{prefix}:{{r:bytes}}:chunks"
    writer = ChunkWriter(store, key, "a", flush_ms=0, max_bytes=1 << 20, ttl_secs=60)
    event = {"contentBlockDelta": {"delta": {"reasoningContent": {"redactedContent": b"\x00\xff"}}}}
    await writer.add(event)
    reader = ChunkReader(store, block_ms=50)
    queue = reader.open(key)
    try:
        entry = await asyncio.wait_for(queue.get(), 2)
    finally:
        await reader.aclose()
    assert entry["d"] == [event]


async def test_fallback_model_without_tools_loses_tool_fields(make: Make) -> None:
    """7: `supports_tools: false` could not be set, so tool fields always went to the target."""
    engine, _ = await make({"small": {"supports_tools": False}})
    tools = {"tools": [{"toolSpec": {"name": "t", "inputSchema": {"json": {}}}}]}
    env = Envelope(
        request_id="t",
        op="converse",
        model="big",
        params={**CHAT, "toolConfig": tools},
        caller="c",
        timeout_secs=5,
    )
    view = engine.registry.current
    assert "toolConfig" not in engine._params(view, "small", env, None)
    assert "toolConfig" in engine._params(view, "big", env, None)
