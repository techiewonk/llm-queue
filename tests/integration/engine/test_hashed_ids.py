"""Messages carry only keyed hashes of the tenant and user ids (owner D1, 0.2.0).

The stream body, its dead letter and everything a worker reads hold the hashes; the raw ids
stay in the caller. Tenant fairness and the request index key by the same hashes.
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest
from engine_testkit import CHAT, Make

from llm_queue.engine.envelope import parse
from llm_queue.engine.queue import QueueSettings

pytestmark = pytest.mark.integration

TENANT, USER = "acme-raw-tenant", "ann-raw-user"


async def test_stream_body_and_dead_letter_hold_hashes_only(make: Make) -> None:
    engine, _ = await make(worker=False)
    task = asyncio.create_task(
        engine.call("small", CHAT, tenant_id=TENANT, user_id=USER, timeout_secs=20)
    )
    queue = engine._queue_name(engine.registry.current, "small", False)
    stream = engine.keys.queue(queue, "s")
    for _ in range(200):
        if await engine.state.xlen(stream):
            break
        await asyncio.sleep(0.02)
    entries = await engine.state.xrange(stream)
    body = entries[0][1][b"body"]
    assert TENANT.encode() not in body
    assert USER.encode() not in body
    env = parse(body)
    assert (env.tenant_id, env.user_id) == (engine.hash_id(TENANT), engine.hash_id(USER))
    # Dead-lettered: the letter is the same body (hashes only).
    messages, _ = await engine.queues.receive(queue, 1, QueueSettings())
    assert await engine.queues.fail(messages[0], QueueSettings(max_receives=1)) == "dead"
    for letter in await engine.queues.dead_letters(queue):
        assert TENANT.encode() not in letter["body"]
        assert USER.encode() not in letter["body"]
    # Nothing under the prefix holds the raw ids either (index, job, status).
    async for key in engine.state.scan_iter(match=f"{engine.keys.prefix}*", count=500):
        assert TENANT.encode() not in key
        kind = await engine.state.type(key)
        if kind == b"hash":
            flat = await engine.state.hgetall(key)
            text = b" ".join([*flat.keys(), *flat.values()])
            assert TENANT.encode() not in text
            assert USER.encode() not in text
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await task


async def test_worker_hooks_and_fairness_see_the_hash(make: Make) -> None:
    """The worker's fairness tenant is the hash the message carries (same keys as before)."""
    engine, _ = await make({"big": {"fairness": {"mode": "quota", "rpm": 100}}})
    assert (await engine.call("big", CHAT, tenant_id=TENANT)).ok
    h = engine.hash_id(TENANT)
    assert await engine.state.exists(engine.keys.model("big", f"tf:t:{h}"))
