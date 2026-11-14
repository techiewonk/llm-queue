"""0.1.0b2 speed work (plan B): fewer store round trips and no idle waits on the hot path.

Each test pins one change on Valkey and Redis: a stream joins the shared chunk read at once, a
stream returns on its `end` entry, the folded scripts and the doorbell give-back.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
import redis.asyncio
from engine_testkit import CHAT, Make

from llm_queue.engine.streaming import ChunkReader

pytestmark = pytest.mark.integration


async def test_a_new_stream_joins_the_blocked_read_at_once(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """B1: a stream opened while the shared read blocked waited for that read to end."""
    reader = ChunkReader(store, block_ms=5000, wake_key=f"{prefix}:chunkwake:w")
    first = f"{prefix}:{{r:a}}:chunks"
    second = f"{prefix}:{{r:b}}:chunks"
    try:
        reader.open(first)
        await asyncio.sleep(0.2)  # the shared read now blocks on `first` alone
        queue = reader.open(second)
        await asyncio.sleep(0.05)
        await store.xadd(second, {"seq": 1, "att": "a", "t": "end", "d": b"{}"})
        started = time.monotonic()
        entry = await asyncio.wait_for(queue.get(), 2)
        assert entry["t"] == "end"
        assert time.monotonic() - started < 1.0  # not the 5 s block
    finally:
        await reader.aclose()


async def test_stream_returns_on_its_end_entry(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B2: the stream waited for the call task (result write and read) after its `end`."""
    engine, _ = await make()
    await engine.call("small", CHAT)  # warm the client
    real = engine.requests.put_result

    async def late(*args: Any, **kw: Any) -> bool:
        await asyncio.sleep(1.5)
        return await real(*args, **kw)

    monkeypatch.setattr(engine.requests, "put_result", late)
    started = time.monotonic()
    events = [event async for event in engine.stream("small", CHAT)]
    assert events
    assert time.monotonic() - started < 1.2  # the result is still 1.5 s away
    assert engine._tails  # the call task finishes in the background
    for _ in range(60):
        if not engine._tails:
            break
        await asyncio.sleep(0.05)
    assert not engine._tails
