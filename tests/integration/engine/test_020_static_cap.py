"""0.2.0 `store_down: static_cap` on the local executor (review L-5): a queue engine's stop drains
and ends those calls, and their budget wait is capped short (`LLMQ_STATIC_CAP_WAIT_SECS`).
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import time
from typing import Any

import pytest
import redis.asyncio
from engine_testkit import CHAT, SMALL, Make, registry
from redis.exceptions import ConnectionError as StoreConnectionError

from llm_queue import LLMQueue
from llm_queue.engine.errors import CallFailed

pytestmark = pytest.mark.integration


def _store_down(engine: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    async def cut(*args: Any, **kwargs: Any) -> Any:
        raise StoreConnectionError("Connection closed by server.")

    monkeypatch.setattr(engine.requests, "register_send", cut)


async def test_stop_drains_then_ends_static_cap_calls(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`Engine.stop` never waited for or cancelled the calls on the `static_cap` path."""
    engine, _ = await make(
        {"small": {"store_down": "static_cap"}}, {"models": {f"{SMALL}@r1": {"hang": True}}}
    )
    _store_down(engine, monkeypatch)
    call = asyncio.create_task(engine.call("small", CHAT, timeout_secs=30))
    for _ in range(100):
        if engine.local.running:
            break
        await asyncio.sleep(0.02)
    assert engine.local.running
    started = time.monotonic()
    await engine.stop(drain_secs=0.5)
    assert time.monotonic() - started < 3
    with pytest.raises(CallFailed) as info:
        await asyncio.wait_for(call, 1)
    assert info.value.kind == "worker_shutdown"


async def test_a_static_cap_budget_wait_is_capped(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Owner Q-12: waits and retries stay, but a budget wait ends after the cap."""
    engine, _ = await make(
        {"small": {"store_down": "static_cap", "rpm": 1, "rate_window": "smooth"}}
    )
    engine.settings = dataclasses.replace(engine.settings, static_cap_wait_secs=0.5)
    _store_down(engine, monkeypatch)
    assert (await engine.call("small", CHAT)).ok
    started = time.monotonic()
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, timeout_secs=8, max_wait_secs=6)
    assert info.value.kind == "rate_limited"
    assert time.monotonic() - started < 2


async def test_start_warns_when_static_cap_runs_with_one_process(
    store: redis.asyncio.Redis, prefix: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Owner Q-12: each process would take a static_cap model's whole budget in an outage."""
    kwargs = store.connection_pool.connection_kwargs
    key_prefix = prefix.replace(":", "-")
    env = {
        "LLMQ_STORE_URL": f"redis://:{kwargs['password']}@127.0.0.1:{kwargs['port']}/0",
        "LLMQ_ENVIRONMENT": "test",
        "LLMQ_REGISTRY": json.dumps(registry({"small": {"store_down": "static_cap"}})),
        "LLMQ_METRICS_SINK_MODE": "none",
        "LLMQ_KEY_PREFIX": key_prefix,
        "LLMQ_ROLE_MODE": "caller",
    }
    try:
        for count, warned in (("1", True), ("4", False)):
            caplog.clear()
            llmq = LLMQueue.from_env({**env, "LLMQ_LOCAL_PROCESS_COUNT": count})
            with caplog.at_level(logging.WARNING, logger="llm_queue"):
                await llmq.start()
            try:
                assert ("static_cap_process_count_unset" in caplog.text) is warned
            finally:
                await llmq.stop(drain_secs=0)
    finally:
        keys = [k async for k in store.scan_iter(match=f"{key_prefix}*", count=500)]
        if keys:
            await store.delete(*keys)
