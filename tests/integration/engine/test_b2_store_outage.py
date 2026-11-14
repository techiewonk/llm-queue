"""A short store outage mid-call (failover drills slowdns and restart): no call runs twice.

The provider answered while the store was out of reach: the result write failed, the message
was released (that failed too) and showed again only after its visibility timeout, so the
caller waited 61 s and the provider ran the call twice. A failover also ended the doorbell
wait with an `UNBLOCKED` error reply, which ended the model's receive loop.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from engine_testkit import CHAT, SMALL, Make, stats
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ResponseError

pytestmark = pytest.mark.integration


async def test_answer_waits_out_a_store_outage(make: Make, monkeypatch: pytest.MonkeyPatch) -> None:
    """Result write and message return fail for 1.5 s (< the 3 s lease): one run, prompt reply."""
    engine, fake = await make()
    real_put, real_return = engine.requests.put_result, engine.queues.return_
    outage = {"until": 0.0}

    def down() -> None:
        if outage["until"] == 0.0:
            outage["until"] = time.monotonic() + 1.5
        if time.monotonic() < outage["until"]:
            raise StoreConnectionError("Connection closed by server.")

    async def put_result(*args: Any, **kwargs: Any) -> Any:
        down()
        return await real_put(*args, **kwargs)

    async def return_(*args: Any, **kwargs: Any) -> Any:
        if outage["until"]:
            down()
        return await real_return(*args, **kwargs)

    monkeypatch.setattr(engine.requests, "put_result", put_result)
    monkeypatch.setattr(engine.queues, "return_", return_)
    started = time.monotonic()
    reply = await engine.call("small", CHAT, request_id="r-outage")
    assert reply.ok
    assert time.monotonic() - started < 5
    await asyncio.sleep(0.5)  # a second run, if any, would have reached the provider by now
    counts = stats(fake, f"{SMALL}@r1")
    assert counts["calls"] == 1
    assert counts.get("duplicates", 0) == 0
    assert not engine._held


async def test_model_loop_survives_an_unblocked_bell(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failover's `UNBLOCKED` reply to the doorbell BLPOP leaves the receive loop running."""
    engine, _ = await make()
    real = engine.queues.wait_bell
    hit = asyncio.Event()

    async def unblocked(*args: Any, **kwargs: Any) -> Any:
        if not hit.is_set():
            hit.set()
            raise ResponseError(
                "UNBLOCKED force unblock from blocking operation, instance state changed"
                " (master -> replica?)"
            )
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine.queues, "wait_bell", unblocked)
    await asyncio.wait_for(hit.wait(), 5)
    await asyncio.sleep(0.2)
    assert not any(task.done() for task in engine._model_tasks.values())
    reply = await asyncio.wait_for(engine.call("small", CHAT), 5)
    assert reply.ok
