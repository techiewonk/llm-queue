"""Streaming to the caller (plan §3.13, F6, R19): chunk streams in the state store.

A worker writes a call's events with a `ChunkWriter`; each caller process runs one
`ChunkReader` that reads all its open chunk streams with one blocking read.
"""

# The stream is `P:{r:ID}:chunks`: the first event at once, later ones batched by
# `stream_flush_ms`, then one `end` or `error` entry.

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import orjson

from llm_queue._logging import get_logger
from llm_queue.engine._codec import text as _text
from llm_queue.engine.envelope import decode_bytes, json_default

if TYPE_CHECKING:
    import redis.asyncio

logger = get_logger(__name__)
END_TTL_SECS = 30  # a stream read to its `end` is kept this long more, not result TTL (S3)


class StreamTooLarge(Exception):
    """The chunk stream went above `stream_max_bytes` (plan §3.13: fail, never trim)."""


def text_of(event: Mapping[str, Any]) -> str:
    """The answer text an event adds: a Bedrock `contentBlockDelta` or an OpenAI chunk delta."""
    delta = event.get("contentBlockDelta", {}).get("delta", {})
    if "text" in delta:
        return str(delta["text"])
    choices = event.get("choices") or []
    if choices and isinstance(choices[0], Mapping):
        content = (choices[0].get("delta") or {}).get("content")
        return str(content) if content else ""
    return ""


class ChunkWriter:
    """Writes one attempt's events to a chunk stream; `sent` once any entry was written."""

    def __init__(
        self,
        client: redis.asyncio.Redis,
        key: str,
        attempt_id: str,
        *,
        flush_ms: int,
        max_bytes: int,
        ttl_secs: int,
    ) -> None:
        """Keep the target; nothing is written until the first event."""
        self._client = client
        self.key = key
        self.attempt_id = attempt_id
        self._flush_secs = flush_ms / 1000
        self._max_bytes = max_bytes
        self._ttl_secs = ttl_secs
        self._buffer: list[Any] = []
        self._seq = 0
        self._bytes = 0
        self._last = 0.0
        self._lock = asyncio.Lock()
        self._timer: asyncio.Task[None] | None = None
        self._failed: Exception | None = None
        self.partial: list[str] = []

    @property
    def sent(self) -> bool:
        """True once the caller may have seen part of the answer."""
        return self._seq > 0 or bool(self._buffer)

    @property
    def partial_text(self) -> str:
        """The answer text written so far."""
        return "".join(self.partial)

    async def add(self, event: Any) -> None:
        """One provider event: written now if it is the first or the flush window has passed."""
        if self._failed is not None:
            raise self._failed
        data = event.model_dump(exclude_none=True) if hasattr(event, "model_dump") else event
        self._buffer.append(data)
        if text := text_of(data):
            self.partial.append(text)
        if (
            self._seq == 0
            or self._flush_secs == 0
            or time.monotonic() - self._last >= self._flush_secs
        ):
            await self._flush()
        elif self._timer is None or self._timer.done():
            self._timer = asyncio.create_task(self._flush_later())

    async def _flush_later(self) -> None:
        await asyncio.sleep(self._flush_secs)
        try:
            await self._flush()
        except Exception as exc:  # StreamTooLarge or a store error: raised by the next add()/end()
            self._failed = exc
            # Logged here too: a call that ends through error() or shutdown never re-raised it,
            # so the failure went unseen (EN-19).
            logger.warning("chunk_flush_failed", key=self.key, error_type=type(exc).__name__)

    async def _write(self, kind: str, data: Any, *, ttl_secs: int | None = None) -> None:
        payload = orjson.dumps(data, default=_chunk_default)
        size = self._bytes + len(payload)  # counted only once written, like the sequence
        if size > self._max_bytes:
            raise StreamTooLarge(f"the stream is above {self._max_bytes} bytes")
        seq = self._seq + 1  # counted only once written: a failed write leaves no gap
        async with self._client.pipeline(transaction=False) as pipe:
            pipe.xadd(self.key, {"seq": seq, "att": self.attempt_id, "t": kind, "d": payload})
            pipe.expire(
                self.key, ttl_secs or self._ttl_secs
            )  # every write: the first may have failed
            await pipe.execute()
        self._seq = seq
        self._bytes = size
        self._last = time.monotonic()

    async def _flush(self) -> None:
        async with self._lock:
            if self._buffer:
                events, self._buffer = self._buffer, []
                try:
                    await self._write("chunk", events)
                except Exception:
                    # Kept for the next flush: dropped, the stream ended "ok" with chunks
                    # missing (RESCAN S1).
                    self._buffer = events + self._buffer
                    raise

    async def end(self, usage: Mapping[str, Any]) -> None:
        """Write what is left, then the `end` entry; a background flush's failure raises here.

        After `end` the stream lives `END_TTL_SECS` more: a read stream (up to 8 MB) stayed
        for the result TTL plus the timeout (RESCAN S3).
        """
        await self._stop_timer()
        if self._failed is not None:
            raise self._failed
        await self._flush()
        async with self._lock:
            await self._write("end", {"usage": dict(usage)}, ttl_secs=END_TTL_SECS)

    async def error(self, kind: str, message: str) -> None:
        """End the stream with an error that carries the partial text (best effort)."""
        await self._stop_timer()
        with contextlib.suppress(StreamTooLarge):
            await self._flush()
        async with self._lock:
            self._bytes = 0  # the error entry is always written
            await self._write(
                "error", {"kind": kind, "message": message, "partial": self.partial_text}
            )

    async def _stop_timer(self) -> None:
        if self._timer is not None and not self._timer.done():
            await _cancel_and_wait([self._timer])


class ChunkReader:
    """One per caller process: one blocking read over every open chunk stream."""

    def __init__(
        self, client: redis.asyncio.Redis, *, block_ms: int = 100, wake_key: str | None = None
    ) -> None:
        """Keep the client; the read task starts with the first open stream.

        `block_ms`: the longest single blocking read (`Tuning.engine.stream_read_block_ms`).
        `wake_key`: a stream of this reader's own; a stream opened while the shared read blocks
        adds an entry there, so the read returns and takes the new stream in at once (without
        it, the new stream joins when the read ends: up to `block_ms` later).
        """
        self._client = client
        self._block_ms = block_ms
        self._wake_key = wake_key
        self._open: dict[str, tuple[asyncio.Queue[dict[str, Any]], list[str]]] = {}
        self._task: asyncio.Task[None] | None = None
        self._reading: set[str] | None = None  # the keys of the blocking read in flight
        self._woken = False  # a wake-up was sent for the read in flight
        self._rings: set[asyncio.Task[None]] = set()

    def open(self, key: str) -> asyncio.Queue[dict[str, Any]]:
        """Start reading a chunk stream from its beginning; entries arrive on the queue."""
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._open[key] = (queue, ["0"])
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._read())
        elif (
            self._wake_key is not None
            and self._reading is not None
            and key not in self._reading
            and not self._woken
        ):
            self._woken = True
            ring = asyncio.create_task(self._ring())
            self._rings.add(ring)
            ring.add_done_callback(self._rings.discard)
        return queue

    async def _ring(self) -> None:
        """Wake the blocking read (one entry on the wake stream, which lapses after a minute)."""
        assert self._wake_key is not None
        try:
            async with self._client.pipeline(transaction=False) as pipe:
                pipe.xadd(self._wake_key, {"w": "1"}, maxlen=1, approximate=False)
                pipe.pexpire(self._wake_key, 60_000)
                await pipe.execute()
        except Exception as exc:  # the new stream still joins when the read ends
            logger.warning("chunk_reader_wake_failed", error=str(exc))

    def close(self, key: str) -> None:
        """Stop reading a chunk stream."""
        self._open.pop(key, None)

    async def aclose(self) -> None:
        """Stop the read task and any wake-up still being sent."""
        self._open.clear()
        # The wake-up tasks were left pending at loop close ("Task was destroyed") (EN-18).
        tasks = [*self._rings, *([self._task] if self._task is not None else [])]
        await _cancel_and_wait(tasks)
        self._rings.clear()

    async def _read(self) -> None:
        failures = 0
        while self._open:
            streams: dict[Any, Any] = {key: last[0] for key, (_, last) in self._open.items()}
            if self._wake_key is not None:
                streams[self._wake_key] = "$"  # only wake-ups sent while this read blocks
            self._reading, self._woken = set(streams), False
            try:
                found: Any = await self._client.xread(streams, block=self._block_ms)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # A store outage logged a warning every 0.5 s per process: log the first
                # failure and the recovery, and back off to 5 s between reads (EN-18).
                if failures == 0:
                    logger.warning("chunk_reader_error", error=str(exc))
                failures += 1
                await asyncio.sleep(min(5.0, 0.5 * 2 ** min(failures - 1, 4)))
                continue
            finally:
                self._reading = None
            if failures:
                logger.info("chunk_reader_recovered", failures=failures)
                failures = 0
            for raw_key, entries in found or []:
                key = _text(raw_key)
                if key not in self._open:
                    continue
                queue, last = self._open[key]
                for entry_id, fields in entries:
                    last[0] = _text(entry_id)
                    # One bad entry is skipped: it ended the reader shared by every stream of
                    # this process (RESCAN S11); the stream's caller then sees a gap.
                    try:
                        entry = {_text(k): v for k, v in fields.items()}
                        item = {
                            "seq": int(entry["seq"]),
                            "att": _text(entry.get("att", b"")),
                            "t": _text(entry["t"]),
                            "d": decode_bytes(orjson.loads(entry["d"])),
                        }
                    except Exception as exc:
                        logger.warning("chunk_entry_bad", error_type=type(exc).__name__)
                        continue
                    queue.put_nowait(item)


async def _cancel_and_wait(tasks: list[asyncio.Task[None]]) -> None:
    """Cancel tasks and wait for them; a cancel of the caller itself still propagates.

    `suppress(CancelledError)` around the await also ate the caller's own cancellation (a
    shutdown timeout), and the caller kept running (EN-7). Other errors are logged.
    """
    for task in tasks:
        task.cancel()
    for result in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(result, Exception):
            logger.warning("stream_task_failed", error_type=type(result).__name__)


def _chunk_default(value: Any) -> Any:
    """Bytes as the envelope encodes them (Bedrock's redacted reasoning), read back as bytes.

    `default=str` wrote bytes as their repr ("b'...'"): the caller got text, not the bytes.
    Any other unknown type is still written as its text.
    """
    if isinstance(value, bytes | bytearray | memoryview):
        return json_default(value)
    return str(value)
