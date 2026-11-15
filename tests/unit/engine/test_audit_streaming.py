"""Bug-pinning tests for chunk streams from the engine audit (EN-7, EN-18, EN-19).

Fake store clients stand in for Valkey, so they run without the Docker stack.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from llm_queue.engine.streaming import ChunkReader, ChunkWriter


def events(caplog: pytest.LogCaptureFixture, name: str) -> list[logging.LogRecord]:
    """The captured records of one structured event."""
    return [r for r in caplog.records if getattr(r, "llmq", {}).get("event") == name]


class FailingPipe:
    """A pipeline whose execute raises (the store is down)."""

    async def __aenter__(self) -> FailingPipe:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def xadd(self, *_: Any, **__: Any) -> None:
        return None

    def expire(self, *_: Any) -> None:
        return None

    async def execute(self) -> list[Any]:
        raise ConnectionError("store down")


class Client:
    """A store client: a pipeline that fails; `xread` raises `fail_reads` times, then blocks."""

    def __init__(self, fail_reads: int = 0) -> None:
        self.fail_reads = fail_reads
        self.reads = 0

    def pipeline(self, transaction: bool = False) -> FailingPipe:
        return FailingPipe()

    async def xread(self, _streams: Any, block: int) -> Any:
        self.reads += 1
        if self.reads <= self.fail_reads:
            raise ConnectionError("store down")
        await asyncio.sleep(block / 1000)
        return []


async def test_en19_background_flush_failure_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    """EN-19: a failed timer flush was stored silently and lost if the call ended by error()."""
    writer = ChunkWriter(Client(), "k", "a", flush_ms=10, max_bytes=1_000_000, ttl_secs=60)  # type: ignore[arg-type]  # fake
    writer._seq = 1  # past the first event: later ones wait for the timer
    writer._content = True  # and past the first text (R3ST-12)
    writer._last = asyncio.get_running_loop().time() + 1e9
    with caplog.at_level(logging.WARNING):
        await writer.add({"x": 1})
        assert writer._timer is not None
        await asyncio.wait_for(asyncio.shield(writer._timer), 2)
    assert len(events(caplog, "chunk_flush_failed")) == 1


async def test_en7_stop_timer_does_not_swallow_the_callers_cancel() -> None:
    """EN-7: `suppress(CancelledError)` around the timer await ate the caller's own cancel."""
    writer = ChunkWriter(Client(), "k", "a", flush_ms=10, max_bytes=1, ttl_secs=60)  # type: ignore[arg-type]  # fake
    release = asyncio.Event()

    async def stubborn() -> None:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            await release.wait()  # takes a while to stop

    writer._timer = asyncio.create_task(stubborn())
    await asyncio.sleep(0)
    stopping = asyncio.create_task(writer._stop_timer())
    await asyncio.sleep(0.05)
    stopping.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await stopping


async def test_en18_reader_backs_off_and_logs_once(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """EN-18: a store outage logged a warning every 0.5 s; `aclose` left wake-up tasks pending."""
    pauses: list[float] = []
    real_sleep = asyncio.sleep

    async def sleep(secs: float) -> None:
        pauses.append(secs)
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", sleep)  # the module streaming uses
    client = Client(fail_reads=10)
    reader = ChunkReader(client, block_ms=1, wake_key="w")  # type: ignore[arg-type]  # fake
    with caplog.at_level(logging.INFO):
        reader.open("s1")
        while client.reads <= 11:
            await real_sleep(0.001)
        reader._reading = {"s1"}
        reader._woken = False
        reader.open("s2")  # starts a wake-up task
        assert reader._rings
        await reader.aclose()
    assert len(events(caplog, "chunk_reader_error")) == 1
    assert len(events(caplog, "chunk_reader_recovered")) == 1
    assert max(pauses) == 5.0
    assert not reader._rings
