"""Streaming fixes from the round 3 review (R3ST-1/2/3/6/8/9/13) against fakellm and the store.

`admission.finish` is wrapped to record each settlement, so the breaker outcome and the
refund of a cut stream can be read without the admission keys.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

import pytest
from engine_testkit import CHAT, SMALL, Make

from llm_queue.engine.errors import CallFailed
from llm_queue.engine.runtime import Engine

pytestmark = pytest.mark.integration

KEY = f"{SMALL}@r1"


async def _drain(events: AsyncIterator[dict[str, Any]], seen: list[dict[str, Any]]) -> None:
    async for event in events:
        seen.append(event)


def _text(events: list[dict[str, Any]]) -> str:
    return "".join(
        e["contentBlockDelta"]["delta"].get("text", "") for e in events if "contentBlockDelta" in e
    )


def _settlements(engine: Engine, monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Record the keyword arguments of every `admission.finish`."""
    seen: list[dict[str, Any]] = []
    real = engine.admission.finish

    async def finish(*args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs)
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine.admission, "finish", finish)
    return seen


async def test_r3st1_a_stalled_stream_ends_with_the_workers_error(make: Make) -> None:
    """R3ST-1: the caller's idle timer raced the worker's; the kind flipped by flush phase."""
    engine, _ = await make(
        {"small": {"stream_flush_ms": 0, "stream_idle_secs": 1}},
        {"models": {KEY: {"output_tokens": 80, "cut_after_chunks": 2, "cut_with": "stall"}}},
    )
    for _ in range(4):
        seen: list[dict[str, Any]] = []
        with pytest.raises(CallFailed) as info:
            await _drain(engine.stream("small", CHAT), seen)
        assert info.value.kind == "provider_error"
        assert info.value.partial_text == _text(seen)
        assert info.value.partial_text


async def test_r3st2_deadline_mid_stream_is_deadline_exceeded(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R3ST-2: the call's deadline mid-stream charged the breaker, as `provider_error` ""."""
    engine, _ = await make(
        {"small": {"stream_flush_ms": 0}},
        {"models": {KEY: {"output_tokens": 400, "tokens_per_s": 20}}},
    )
    settled = _settlements(engine, monkeypatch)
    seen: list[dict[str, Any]] = []
    with pytest.raises(CallFailed) as info:
        await _drain(engine.stream("small", CHAT, timeout_secs=1), seen)
    assert info.value.kind == "deadline_exceeded"
    assert "deadline" in str(info.value)
    assert info.value.partial_text
    assert [s["outcome"] for s in settled] == ["none"]  # the breaker is not charged


async def test_r3st3_cancel_mid_stream_ends_at_once_with_partial_text(make: Make) -> None:
    """R3ST-3: a cancelled stream got no error entry; its caller waited out the idle timer."""
    engine, _ = await make(
        {"small": {"stream_flush_ms": 0}},
        {"models": {KEY: {"output_tokens": 400, "tokens_per_s": 40}}},
    )
    seen: list[dict[str, Any]] = []
    started = 0.0

    async def read_then_cancel() -> None:
        nonlocal started
        async for event in engine.stream("small", CHAT, request_id="r3st3"):
            seen.append(event)
            if _text(seen) and not started:
                await engine.cancel("r3st3")
                started = time.monotonic()

    with pytest.raises(CallFailed) as info:
        await read_then_cancel()
    assert info.value.kind == "cancelled"
    assert time.monotonic() - started < 3
    assert info.value.partial_text
    assert _text(seen).startswith(info.value.partial_text[:4])


async def test_r3st6_restream_after_the_chunks_expired_replays_the_reply(make: Make) -> None:
    """R3ST-6: an app id streamed again once its chunks expired ended with no event at all."""
    engine, _ = await make({"small": {"stream_flush_ms": 0}})
    first = [e async for e in engine.stream("small", CHAT, request_id="r3st6")]
    for _ in range(40):
        if (await engine.requests.status("r3st6")).get("state") == "done":
            break
        await asyncio.sleep(0.05)
    await engine.state.delete(engine.keys.request("r3st6", "chunks"))
    again = [e async for e in engine.stream("small", CHAT, request_id="r3st6")]
    assert _text(again) == _text(first)
    assert "messageStop" in again[-2]


async def test_r3st8_a_second_stream_of_one_id_is_refused(make: Make) -> None:
    """R3ST-8: two streams with one id in a process both starved."""
    engine, _ = await make(
        {"small": {"stream_flush_ms": 0}},
        {"models": {KEY: {"output_tokens": 40, "tokens_per_s": 100}}},
    )
    seen: list[dict[str, Any]] = []
    first = asyncio.create_task(_drain(engine.stream("small", CHAT, request_id="r3st8"), seen))
    await asyncio.sleep(0.2)
    with pytest.raises(CallFailed) as info:
        await _drain(engine.stream("small", CHAT, request_id="r3st8"), [])
    assert info.value.kind == "invalid_request"
    await asyncio.wait_for(first, 10)
    assert len(_text(seen)) == 160


async def test_r3st9_meta_event_tells_the_served_model_and_tokens(make: Make) -> None:
    """R3ST-9: a stream caller could not learn the served model, deployment or tokens."""
    engine, _ = await make({}, {"models": {KEY: {"output_tokens": 20}}})
    events = [e async for e in engine.stream("small", CHAT, meta_event=True)]
    last = events[-1]["llmq"]
    assert last["meta"]["served_model"] == "small"
    assert last["meta"]["deployment"] == "aws-a/r1"
    assert last["usage"]["outputTokens"] == 20
    assert "llmq" not in events[-2]
    plain = [e async for e in engine.stream("small", CHAT)]
    assert all("llmq" not in e for e in plain)


async def test_r3st13_a_cut_stream_refunds_the_unused_output(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R3ST-13: a stream cut at its first tokens kept the whole max_tokens reservation."""
    engine, _ = await make(
        {"small": {"stream_flush_ms": 0, "max_attempts": 1}},
        {"models": {KEY: {"output_tokens": 80, "cut_after_chunks": 1}}},
    )
    settled = _settlements(engine, monkeypatch)
    request = {**CHAT, "inferenceConfig": {"maxTokens": 2000}}
    with pytest.raises(CallFailed):
        await _drain(engine.stream("small", request), [])
    assert settled[-1]["refund_t"] > 1500  # most of the 2,000 output tokens back


async def test_r3pf11_no_task_per_chunk_entry(make: Make, monkeypatch: pytest.MonkeyPatch) -> None:
    """R3PF-11: each chunk entry cost a Task for `queue.get()` and an `asyncio.wait` (42 us)."""
    engine, _ = await make(
        {"small": {"stream_flush_ms": 0}},
        {"models": {KEY: {"output_tokens": 80, "tokens_per_s": 400}}},
    )
    getters = 0
    real = asyncio.ensure_future

    def counted(coro: Any, **kwargs: Any) -> Any:
        nonlocal getters
        if getattr(coro, "__qualname__", "") == "Queue.get":
            getters += 1
        return real(coro, **kwargs)

    monkeypatch.setattr(asyncio, "ensure_future", counted)
    events = [e async for e in engine.stream("small", CHAT)]
    assert len(_text(events)) == 320
    assert getters == 0
