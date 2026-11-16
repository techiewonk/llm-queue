"""ChunkWriter without a store (RESCAN S1, S3): failed flushes keep their events; `end` raises."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from llm_queue.engine.streaming import END_TTL_SECS, ChunkWriter


class FlakyStore:
    """A client whose pipelines fail `fail` times, then record each XADD and EXPIRE."""

    def __init__(self, fail: int = 0) -> None:
        self.fail = fail
        self.entries: list[dict[str, Any]] = []
        self.ttls: list[int] = []

    def pipeline(self, transaction: bool = False) -> FlakyStore.Pipe:
        return FlakyStore.Pipe(self)

    class Pipe:
        def __init__(self, store: FlakyStore) -> None:
            self.store = store
            self.ops: list[tuple[str, Any]] = []

        async def __aenter__(self) -> FlakyStore.Pipe:
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

        def xadd(self, key: str, fields: dict[str, Any]) -> None:
            self.ops.append(("xadd", fields))

        def pexpire(self, key: str, ms: int, **_: Any) -> None:
            self.expire(key, ms // 1000)

        def zrem(self, *_: Any) -> None:
            return None

        def persist(self, *_: Any) -> None:
            return None

        def zadd(self, *_: Any, **__: Any) -> None:
            return None

        def expire(self, key: str, ttl: int) -> None:
            self.ops.append(("expire", ttl))

        async def execute(self) -> list[Any]:
            if self.store.fail:
                self.store.fail -= 1
                raise ConnectionError("store blip")
            for op, value in self.ops:
                (self.store.entries if op == "xadd" else self.store.ttls).append(value)
            return []


def _writer(store: FlakyStore, flush_ms: int = 0) -> ChunkWriter:
    return ChunkWriter(store, "k", "att", flush_ms=flush_ms, max_bytes=1_000_000, ttl_secs=300)  # type: ignore[arg-type]


def _delta(text: str) -> dict[str, Any]:
    return {"contentBlockDelta": {"delta": {"text": text}}}


async def test_failed_flush_keeps_its_events() -> None:
    """S1: a failed write dropped its events; the stream then ended "ok" with a hole."""
    store = FlakyStore(fail=1)
    writer = _writer(store)
    with pytest.raises(ConnectionError):
        await writer.add(_delta("a"))
    await writer.add(_delta("b"))
    texts = [e["contentBlockDelta"]["delta"]["text"] for e in _events(store)]
    assert texts == ["a", "b"]
    # The failed write may have reached the store: sent again under its own seq (R3ST-7).
    assert [int(e["seq"]) for e in store.entries] == [1, 2]


async def test_end_raises_a_background_flush_failure() -> None:
    """S1: a flush that failed in the background was never reported; `end` wrote "ok"."""
    store = FlakyStore()
    writer = _writer(store, flush_ms=20)
    await writer.add(_delta("first"))  # written at once
    store.fail = 1
    await writer.add(_delta("second"))  # buffered; its timer's flush fails
    await asyncio.sleep(0.1)
    with pytest.raises(ConnectionError):
        await writer.end({"outputTokens": 1})
    assert all(e["t"] != "end" for e in store.entries)


async def test_end_shortens_the_streams_life() -> None:
    """S3: a stream read to its end stayed for the result TTL plus the timeout."""
    store = FlakyStore()
    writer = _writer(store)
    await writer.add(_delta("x"))
    await writer.end({"outputTokens": 1})
    assert store.ttls[-1] == END_TTL_SECS


def _events(store: FlakyStore) -> list[dict[str, Any]]:
    import orjson

    out: list[dict[str, Any]] = []
    for entry in store.entries:
        if entry["t"] == "chunk":
            out.extend(orjson.loads(entry["d"]))
    return out
