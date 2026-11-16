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
from llm_queue.engine.expiry import Expiry, native

if TYPE_CHECKING:
    import redis.asyncio

logger = get_logger(__name__)
END_TTL_SECS = 30  # a stream read to its `end` is kept this long more, not result TTL (S3)


class StreamTooLarge(Exception):
    """The chunk stream went above `stream_max_bytes` (plan §3.13: fail, never trim)."""


def text_of(event: Mapping[str, Any]) -> str:
    """The answer text an event adds: a Bedrock `contentBlockDelta` or an OpenAI chunk delta."""
    # `delta: null` raised TypeError (`"text" in None`, R3ST-14); with `n` > 1 the text of
    # every choice was mixed into `partial_text` (R3ST-10): only choice 0 counts.
    delta = (event.get("contentBlockDelta") or {}).get("delta") or {}
    if "text" in delta:
        return str(delta["text"])
    for choice in event.get("choices") or ():
        if isinstance(choice, Mapping) and choice.get("index", 0) == 0:
            content = (choice.get("delta") or {}).get("content")
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
        expiry: Expiry | None = None,
    ) -> None:
        """Keep the target; nothing is written until the first event."""
        self._client = client
        self._expiry = expiry or native("")
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
        # A write whose outcome is unknown (a store error or a cancel mid-XADD): sent again, with
        # the same seq, before anything else. The reader skips the copy if the first landed.
        self._doubt: tuple[int, str, bytes, int | None] | None = None
        self._content = False  # an event with answer content (text, tool, reasoning) was sent
        self.closed = False  # `end` or `error` was written
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
        """One provider event: written now if it is early or the flush window has passed.

        Every event up to the first with answer content is written at once: the first text
        delta waited up to `stream_flush_ms` behind `messageStart`, and time to first token is
        the number users see (R3ST-12).
        """
        if self._failed is not None:
            raise self._failed
        # `exclude_unset`, not `exclude_none`: the wire shape, with the nulls the server sent;
        # `chunk["choices"][0]["finish_reason"]` raised KeyError on most chunks (R3ST-10).
        data = event.model_dump(exclude_unset=True) if hasattr(event, "model_dump") else event
        self._buffer.append(data)
        text = text_of(data)
        if text:
            self.partial.append(text)
        early = not self._content
        if early:
            self._content = bool(text) or _has_content(data)
        if early or self._flush_secs == 0 or time.monotonic() - self._last >= self._flush_secs:
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

    async def _write(
        self, kind: str, data: Any, *, ttl_secs: int | None = None, capped: bool = True
    ) -> None:
        """Write one entry under the next seq; the lock is held.

        The seq is taken before the write (R3ST-7, FE-3): a write cut by a cancel or a store
        error may have reached the store, and the next entry under the same seq was dropped by
        the reader as a re-read, so the caller never saw the `end` or `error` entry. The entry
        is kept in doubt and sent again, same seq, before the next one.
        """
        payload = orjson.dumps(data, default=_chunk_default)
        size = self._bytes + len(payload)
        # `end` is not capped (R3ST-13): an answer delivered in full failed on its last entry.
        if capped and size > self._max_bytes:
            raise StreamTooLarge(f"the stream is above {self._max_bytes} bytes")
        self._seq += 1
        self._bytes = size
        self._doubt = (self._seq, kind, payload, ttl_secs)
        await self._send()

    async def _send(self) -> None:
        """Send the entry in doubt; it stays in doubt until the store confirmed it."""
        assert self._doubt is not None
        seq, kind, payload, ttl_secs = self._doubt
        async with self._client.pipeline(transaction=False) as pipe:
            pipe.xadd(self.key, {"seq": seq, "att": self.attempt_id, "t": kind, "d": payload})
            # Every write: the first may have failed.
            self._expiry.expire(pipe, self.key, ttl_secs or self._ttl_secs)
            await pipe.execute()
        self._doubt = None
        self._last = time.monotonic()

    async def _flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        if self._doubt is not None:
            await self._send()
        if self._buffer:
            events, self._buffer = self._buffer, []
            try:
                await self._write("chunk", events)
            except StreamTooLarge:
                # Kept: `error` still carries their text (RESCAN S1). Any other failure left
                # them in doubt, to be sent again (R3ST-7).
                self._buffer = events + self._buffer
                raise

    async def end(self, usage: Mapping[str, Any], meta: Mapping[str, Any] | None = None) -> None:
        """Write what is left, then the `end` entry; a background flush's failure raises here.

        After `end` the stream lives `END_TTL_SECS` more: a read stream (up to 8 MB) stayed
        for the result TTL plus the timeout (RESCAN S3). `meta` (served model, deployment,
        fallback, attempts) is for stream callers, who never see the call's `Reply` (R3ST-9).
        """
        await self._stop_timer()
        if self._failed is not None:
            raise self._failed
        data: dict[str, Any] = {"usage": dict(usage)}
        if meta is not None:
            data["meta"] = dict(meta)
        async with self._lock:
            await self._flush_locked()
            await self._write("end", data, ttl_secs=END_TTL_SECS, capped=False)
            self.closed = True

    async def error(self, kind: str, message: str) -> None:
        """End the stream with an error that carries the partial text (best effort).

        A no-op once the stream ended: a cancel that came after `end` wrote an error entry
        nobody reads.
        """
        if self.closed:
            return
        await self._stop_timer()
        async with self._lock:
            with contextlib.suppress(StreamTooLarge):
                await self._flush_locked()
            await self._write(
                "error",
                {"kind": kind, "message": message, "partial": self.partial_text},
                capped=False,  # the error entry is always written
            )
            self.closed = True

    async def _stop_timer(self) -> None:
        """Stop the flush timer; a flush it has in flight finishes first.

        Cancelled inside its write, the timer lost the batch or left the seq behind, and the
        `end` written next under the same seq was dropped by the reader (FE-3).
        """
        timer = self._timer
        if timer is None or timer.done():
            return
        async with self._lock:  # the timer is asleep or waiting for the lock once this is held
            timer.cancel()
        await _cancel_and_wait([timer])


def _has_content(event: Mapping[str, Any]) -> bool:
    """True for an event that carries answer content: a text, tool or reasoning delta."""
    if "contentBlockDelta" in event:
        return True
    choices = event.get("choices") or []
    if choices and isinstance(choices[0], Mapping):
        delta = choices[0].get("delta") or {}
        return any(key != "role" and value for key, value in delta.items())
    return False


def replay_events(response: Mapping[str, Any] | None) -> list[dict[str, Any]] | None:
    """Stream events rebuilt from a call's kept reply; None for a shape this cannot rebuild.

    For an app id streamed again once its chunks expired (`END_TTL_SECS`) while its result is
    still kept: the stream ended with no event and no error (R3ST-6). Bedrock gets the
    ConverseStream events of the body, OpenAI-compatible one chunk plus a usage chunk.
    """
    if not isinstance(response, Mapping):
        return None
    if isinstance(response.get("output"), Mapping):
        return _converse_events(response)
    if isinstance(response.get("choices"), list):
        return _chat_events(response)
    return None


def _converse_events(body: Mapping[str, Any]) -> list[dict[str, Any]]:
    message = body["output"].get("message") or {}
    events: list[dict[str, Any]] = [{"messageStart": {"role": message.get("role", "assistant")}}]
    for index, block in enumerate(message.get("content") or []):
        deltas: list[dict[str, Any]] = []
        if "text" in block:
            deltas.append({"text": block["text"]})
        elif "toolUse" in block:
            tool = block["toolUse"]
            start = {k: tool[k] for k in ("toolUseId", "name") if k in tool}
            events.append(
                {"contentBlockStart": {"start": {"toolUse": start}, "contentBlockIndex": index}}
            )
            raw = tool.get("input", {})
            text = raw if isinstance(raw, str) else orjson.dumps(raw).decode()
            deltas.append({"toolUse": {"input": text}})
        elif "reasoningContent" in block:
            reasoning = block["reasoningContent"]
            if "redactedContent" in reasoning:
                deltas.append(
                    {"reasoningContent": {"redactedContent": reasoning["redactedContent"]}}
                )
            part = reasoning.get("reasoningText") or {}
            if "text" in part:
                deltas.append({"reasoningContent": {"text": part["text"]}})
            if "signature" in part:
                deltas.append({"reasoningContent": {"signature": part["signature"]}})
        elif "citationsContent" in block:
            cited = block["citationsContent"]
            text = "".join(str(c.get("text", "")) for c in cited.get("content") or [])
            deltas.append({"text": text})
            deltas.extend({"citation": c} for c in cited.get("citations") or [])
        events.extend(
            {"contentBlockDelta": {"delta": delta, "contentBlockIndex": index}} for delta in deltas
        )
        events.append({"contentBlockStop": {"contentBlockIndex": index}})
    stop: dict[str, Any] = {"stopReason": body.get("stopReason")}
    if "additionalModelResponseFields" in body:
        stop["additionalModelResponseFields"] = body["additionalModelResponseFields"]
    events.append({"messageStop": stop})
    metadata = {k: body[k] for k in ("usage", "metrics", "trace") if k in body}
    if metadata:
        events.append({"metadata": metadata})
    return events


def _chat_events(body: Mapping[str, Any]) -> list[dict[str, Any]]:
    head = {k: body[k] for k in ("id", "created", "model", "system_fingerprint") if k in body}
    head["object"] = "chat.completion.chunk"
    choices = []
    for choice in body["choices"]:
        delta = dict(choice.get("message") or {})
        if delta.get("tool_calls"):
            delta["tool_calls"] = [
                {"index": i, **call} for i, call in enumerate(delta["tool_calls"])
            ]
        choices.append(
            {
                "index": choice.get("index", 0),
                "delta": delta,
                "finish_reason": choice.get("finish_reason"),
            }
        )
    events: list[dict[str, Any]] = [{**head, "choices": choices}]
    if body.get("usage") is not None:
        events.append({**head, "choices": [], "usage": body["usage"]})
    return events


class ChunkReader:
    """One per caller process: one blocking read over every open chunk stream."""

    def __init__(
        self,
        client: redis.asyncio.Redis,
        *,
        block_ms: int = 100,
        wake_key: str | None = None,
        expiry: Expiry | None = None,
    ) -> None:
        """Keep the client; the read task starts with the first open stream.

        `block_ms`: the longest single blocking read (`Tuning.engine.stream_read_block_ms`).
        `wake_key`: a stream of this reader's own; a stream opened while the shared read blocks
        adds an entry there, so the read returns and takes the new stream in at once (without
        it, the new stream joins when the read ends: up to `block_ms` later).
        """
        self._client = client
        self._expiry = expiry or native("")
        self._block_ms = block_ms
        self._wake_key = wake_key
        # Per open stream: its queue, the last entry id read (bytes, in a list to update in
        # place) and its key as a packed RESP argument, built once (R3PF-2).
        self._open: dict[str, tuple[asyncio.Queue[dict[str, Any]], list[bytes], bytes]] = {}
        self._task: asyncio.Task[None] | None = None
        self._reading: set[str] | None = None  # the keys of the blocking read in flight
        self._woken = False  # a wake-up was sent for the read in flight
        self._rings: set[asyncio.Task[None]] = set()

    def open(self, key: str) -> asyncio.Queue[dict[str, Any]]:
        """Start reading a chunk stream from its beginning; entries arrive on the queue.

        `ValueError` when this process already reads it: a second open replaced the first's
        queue and the first's close then removed the second's, so both starved (R3ST-8).
        """
        if key in self._open:
            raise ValueError(f"the chunk stream {key} is already open in this process")
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._open[key] = (queue, [b"0"], _arg(key.encode()))
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
                self._expiry.expire(pipe, self._wake_key, 60)
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
            self._reading, self._woken = set(self._open), False
            try:
                found: Any = await self._xread()
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
            # A raw reply: a map under RESP3, a list of [key, entries] pairs under RESP2.
            for raw_key, entries in found.items() if isinstance(found, Mapping) else found or []:
                key = _text(raw_key)
                if key not in self._open:
                    continue
                queue, last, _ = self._open[key]
                for entry_id, fields in entries:
                    last[0] = entry_id if isinstance(entry_id, bytes) else str(entry_id).encode()
                    # One bad entry is skipped: it ended the reader shared by every stream of
                    # this process (RESCAN S11); the stream's caller then sees a gap.
                    try:
                        pairs = (
                            fields.items()
                            if isinstance(fields, Mapping)
                            else zip(fields[::2], fields[1::2], strict=True)
                        )
                        entry = {_text(k): v for k, v in pairs}
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

    async def _xread(self) -> Any:
        """One blocking XREAD over every open stream (and the wake stream).

        Packed here from each stream's cached key argument and sent on a pooled connection:
        redis-py's `xread` built a dict and packed every key and id in Python again on each
        turn, 3.6 ms per read at 1,000 open streams (R3PF-2). The reply is raw (a map under
        RESP3, a list of pairs under RESP2; fields as a flat list). A client without a pool (a
        test fake) gets the plain command.
        """
        pool = getattr(self._client, "connection_pool", None)
        if pool is None:
            streams: dict[Any, Any] = {key: last[0] for key, (_, last, _) in self._open.items()}
            if self._wake_key is not None:
                streams[self._wake_key] = "$"  # only wake-ups sent while this read blocks
            return await self._client.xread(streams, block=self._block_ms)
        command = self._command()
        conn = await pool.get_connection()
        try:
            await conn.send_packed_command(command)
            return await conn.read_response()
        except BaseException:
            # Cancelled or failed mid-read: the reply may still come on this connection, and
            # the next command would read it as its own.
            await asyncio.shield(conn.disconnect())
            raise
        finally:
            await pool.release(conn)

    def _command(self) -> bytes:
        """The packed `XREAD BLOCK ms STREAMS keys... ids...` of the open streams."""
        opened = list(self._open.values())
        keys = [packed for _, _, packed in opened]
        ids = [_arg(last[0]) for _, last, _ in opened]
        if self._wake_key is not None:
            keys.append(_arg(self._wake_key.encode()))
            ids.append(b"$1\r\n$\r\n")  # only wake-ups sent while this read blocks
        head = b"*%d\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n%s$7\r\nSTREAMS\r\n" % (
            4 + 2 * len(keys),
            _arg(b"%d" % self._block_ms),
        )
        return b"".join([head, *keys, *ids])


def _arg(value: bytes) -> bytes:
    """One RESP bulk-string argument."""
    return b"$%d\r\n%s\r\n" % (len(value), value)


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
