"""Engine audit fixes that change store scripts or commands (EN-9, EN-12, EN-13, EN-23).

Run on both engines (Valkey and Redis), like every engine test.
"""

from __future__ import annotations

import pytest
import redis.asyncio

from llm_queue.engine.health import Health
from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import StreamQueues
from llm_queue.engine.status import Alert, AlertEvaluator

pytestmark = pytest.mark.integration


async def test_en12_redrive_skips_a_dead_letter_without_body(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """EN-12: a DLQ entry without `body` made REDRIVE fail part-way, moves not rolled back."""
    queues = StreamQueues(store, Keys(prefix))
    dlq = queues.queue_keys("q")[5]
    await store.xadd(dlq, {"body": b"a", "src": "1-0", "fails": 3})
    await store.xadd(dlq, {"src": "2-0", "fails": 3})  # no body
    await store.xadd(dlq, {"body": b"c", "src": "3-0", "fails": 3})
    await store.delete(queues.bell("q"))
    assert await queues.redrive("q") == 2
    assert await store.xlen(dlq) == 0
    assert await store.xlen(queues.queue_keys("q")[0]) == 2
    assert await store.llen(queues.bell("q")) == 1  # a waiting worker is woken
    assert await store.pttl(queues.bell("q")) > 0


async def test_en13_ring_again_keeps_a_ttl(store: redis.asyncio.Redis, prefix: str) -> None:
    """EN-13: `ring_again` on an expired bell made one with no TTL that outlived the model."""
    queues = StreamQueues(store, Keys(prefix))
    await store.delete(queues.bell("q"))
    for _ in range(70):
        await queues.ring_again("q")
    assert await store.pttl(queues.bell("q")) > 0
    assert await store.llen(queues.bell("q")) == 64


async def test_en23_failed_probe_doubles_from_probe_after(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """EN-23: a failed probe waited at least 60 s, whatever the model's `probe_after_secs`."""
    health = Health(store, Keys(prefix))
    await health.disable("m", "d", probe_after_secs=5)
    assert await health.probed("m", "d", ok=False, probe_max_secs=600) == "waiting"
    assert (await health.snapshot("m"))["d|wait"] == "10000"
    await health.disable("m", "e", probe_after_secs=5)
    await health.probed("m", "e", ok=False, probe_max_secs=600, probe_after_secs=30)
    assert (await health.snapshot("m"))["e|wait"] == "30000"


async def test_en9_alert_markers_in_one_pipeline(store: redis.asyncio.Redis, prefix: str) -> None:
    """EN-9: each alert cost GETSET then EXPIRE, two serial round trips."""
    evaluator = AlertEvaluator(store, Keys(prefix))
    alerts = [Alert("a", True, {"model": "m"}), Alert("b", False, {"model": "m"})]
    assert [a.name for a in await evaluator.changed(alerts)] == ["a"]
    assert await evaluator.changed(alerts) == []
    flipped = await evaluator.changed([Alert("a", False, {"model": "m"}), alerts[1]])
    assert [a.name for a in flipped] == ["a"]
    assert await store.ttl(f"{prefix}:alert:a:model=m") > 0
