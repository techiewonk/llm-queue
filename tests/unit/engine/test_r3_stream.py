"""Chunk stream fixes from the round 3 review (FE-3, R3ST-7, R3ST-8, R3ST-12, R3ST-13).

A fake store client records each XADD and can hold a pipeline inside `execute`, so a write
can be cut (cancelled) after the store already took it. Runs without the Docker stack.
"""

from __future__ import annotations

import asyncio
from typing import Any

import orjson
import pytest

from llm_queue.engine.streaming import ChunkReader, ChunkWriter, StreamTooLarge


class Store:
    """Records XADDs at `execute`; while `gate` is clear, `execute` waits after recording."""

    def __init__(self) -> None:
        self.entries: list[dict[str, Any]] = []
        self.gate = asyncio.Event()
        self.gate.set()
        self.waiting = asyncio.Event()  # an execute is held at the gate

    def pipeline(self, transaction: bool = False) -> Store.Pipe:
        return Store.Pipe(self)

    class Pipe:
        def __init__(self, store: Store) -> None:
            self.store = store
            self.adds: list[dict[str, Any]] = []

        async def __aenter__(self) -> Store.Pipe:
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

        def xadd(self, key: str, fields: dict[str, Any], **_: Any) -> None:
            self.adds.append(fields)

        def expire(self, key: str, ttl: int) -> None:
            return None

        async def execute(self) -> list[Any]:
            self.store.entries.extend(self.adds)  # the store took it
            if not self.store.gate.is_set():
                self.store.waiting.set()
                await self.store.gate.wait()  # the reply is late
            return []


def _delta(text: str) -> dict[str, Any]:
    return {"contentBlockDelta": {"delta": {"text": text}, "contentBlockIndex": 0}}


def _writer(store: Store, *, flush_ms: int = 0, max_bytes: int = 1_000_000) -> ChunkWriter:
    return ChunkWriter(store, "k", "att", flush_ms=flush_ms, max_bytes=max_bytes, ttl_secs=60)  # type: ignore[arg-type]  # fake


def _read(entries: list[dict[str, Any]]) -> tuple[list[str], dict[str, Any]]:
    """What `Engine.stream` makes of the entries: the texts and the last entry, else fail."""
    expected, texts = 1, list[str]()
    for entry in entries:
        seq = int(entry["seq"])
        if seq < expected:
            continue  # the same entry read twice: skipped
        assert seq == expected, f"gap at {expected}"
        expected += 1
        if entry["t"] != "chunk":
            return texts, {"t": entry["t"], **orjson.loads(entry["d"])}
        for event in orjson.loads(entry["d"]):
            if "contentBlockDelta" in event:
                texts.append(event["contentBlockDelta"]["delta"]["text"])
    raise AssertionError("the stream never ended")


async def test_fe3_end_waits_for_the_timer_flush_in_flight() -> None:
    """FE-3: `end` cancelled the timer inside its XADD; the `end` reused that seq and was lost."""
    store = Store()
    writer = _writer(store, flush_ms=20)
    await writer.add(_delta("a"))  # the first content: written at once
    store.gate.clear()
    await writer.add(_delta("b"))  # buffered for the timer
    await asyncio.wait_for(store.waiting.wait(), 2)  # the timer's write reached the store
    ending = asyncio.create_task(writer.end({"outputTokens": 2}))
    await asyncio.sleep(0.05)
    store.gate.set()
    await asyncio.wait_for(ending, 2)
    texts, last = _read(store.entries)
    assert texts == ["a", "b"]
    assert last["t"] == "end"


async def test_r3st7_cancelled_add_neither_loses_a_chunk_nor_reuses_its_seq() -> None:
    """R3ST-7: an `add` cancelled mid-write lost its batch, and `error` reused its seq."""
    store = Store()
    writer = _writer(store)
    await writer.add(_delta("a"))
    store.gate.clear()
    adding = asyncio.create_task(writer.add(_delta("b")))
    await asyncio.wait_for(store.waiting.wait(), 2)
    adding.cancel()
    with pytest.raises(asyncio.CancelledError):
        await adding
    store.gate.set()
    await writer.error("cancelled", "the call was cancelled")
    texts, last = _read(store.entries)
    assert texts == ["a", "b"]
    assert last["t"] == "error"
    assert last["partial"] == "ab"


async def test_r3st7_a_write_cancelled_before_the_store_is_sent_again() -> None:
    """R3ST-7: a batch whose write was cancelled before the store took it was dropped."""

    class Slow(Store):
        class Pipe(Store.Pipe):
            async def execute(self) -> list[Any]:
                if not self.store.gate.is_set():
                    self.store.waiting.set()
                    await self.store.gate.wait()  # cancelled here: nothing recorded
                return await super().execute()

        def pipeline(self, transaction: bool = False) -> Store.Pipe:
            return Slow.Pipe(self)

    store = Slow()
    writer = _writer(store)
    await writer.add(_delta("a"))
    store.gate.clear()
    adding = asyncio.create_task(writer.add(_delta("b")))
    await asyncio.wait_for(store.waiting.wait(), 2)
    adding.cancel()
    with pytest.raises(asyncio.CancelledError):
        await adding
    store.gate.set()
    await writer.end({"outputTokens": 2})
    texts, last = _read(store.entries)
    assert texts == ["a", "b"]
    assert last["t"] == "end"


async def test_r3st12_events_up_to_the_first_text_are_not_held() -> None:
    """R3ST-12: the first text delta waited `stream_flush_ms` behind `messageStart`."""
    store = Store()
    writer = _writer(store, flush_ms=1000)
    await writer.add({"messageStart": {"role": "assistant"}})
    await writer.add(_delta("first"))
    assert len(store.entries) == 2  # both at once
    await writer.add(_delta("second"))
    assert len(store.entries) == 2  # later ones wait for the window
    await writer.end({})


async def test_r3st13_end_is_not_capped() -> None:
    """R3ST-13: an answer that fit failed with `StreamTooLarge` on its `end` entry."""
    store = Store()
    writer = _writer(store, max_bytes=90)
    await writer.add(_delta("x" * 10))  # about 70 bytes: fits
    await writer.end({"inputTokens": 1000, "outputTokens": 1000})
    assert store.entries[-1]["t"] == "end"
    with pytest.raises(StreamTooLarge):
        await _writer(store, max_bytes=10).add(_delta("y" * 50))


async def test_r3st8_a_second_open_of_one_stream_is_refused() -> None:
    """R3ST-8: a second open replaced the first's queue, and its close removed the second's."""
    store = Store()
    reader = ChunkReader(store)  # type: ignore[arg-type]  # fake: the read task is never run
    reader._task = asyncio.get_running_loop().create_future()  # type: ignore[assignment]
    first = reader.open("k")
    with pytest.raises(ValueError, match="already open"):
        reader.open("k")
    assert reader._open["k"][0] is first
    reader.close("k")
    assert "k" not in reader._open
