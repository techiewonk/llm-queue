"""Tenant names never reach the store raw (0.2.0 review RQ-5, RQ-6, B-02; scenario G16).

A live edit of `fairness.weights.<name>` stores the name's hash and the weight applies to the
tenant's calls; publish and rollback keep hashed blocks as they are; a job's message carries
only the hashes of its sender's ids; the message format is version 3.
"""

from __future__ import annotations

import asyncio

import orjson
import pytest
from engine_testkit import Make

from llm_queue.context import request_context
from llm_queue.engine.admin import AdminServices
from llm_queue.engine.envelope import VERSION, parse
from llm_queue.engine.fairness import fairness_for
from llm_queue.engine.jobs import JobRunner
from llm_queue.errors import ConfigError

pytestmark = pytest.mark.integration

SHARE = {"small": {"fairness": {"mode": "share"}}}


async def test_g16_a_live_weight_edit_stores_the_hash_and_applies(make: Make) -> None:
    engine, _ = await make(SHARE, worker=False)
    admin = AdminServices.for_engine(engine)
    plan = await admin.registry.set({"fairness.weights.acme": 3}, model="small", actor="ops")
    assert "acme" not in orjson.dumps(plan).decode()  # the dry run shows the hash too
    await admin.registry.set(
        {"fairness.weights.acme": 3}, model="small", actor="ops", dry_run=False
    )
    stored = (await admin.registry.get("small"))["registry"]["fairness"]
    h = engine.hash_id("acme")
    assert stored["weights"] == {h: 3}
    assert stored["tenant_keys"] == "hashed"
    setting = engine.registry.current.value("small", "fairness")
    fair = fairness_for(setting, h)
    assert fair is not None
    assert fair.weight == 3.0
    with pytest.raises(ConfigError, match="looks like a tenant hash"):
        await admin.registry.set({"fairness.weights": {h: 2}}, model="small", actor="ops")
    with pytest.raises(ConfigError, match="looks like a tenant hash"):
        await admin.registry.set({f"fairness.weights.{h}": 2}, model="small", actor="ops")


async def test_g16_a_quota_path_hashes_the_name_not_the_field(make: Make) -> None:
    engine, _ = await make({"small": {"fairness": {"mode": "quota", "rpm": 10}}}, worker=False)
    admin = AdminServices.for_engine(engine)
    await admin.registry.set(
        {"fairness.quotas.acme.inc.rpm": 5}, model="small", actor="ops", dry_run=False
    )
    stored = (await admin.registry.get("small"))["registry"]["fairness"]
    assert stored["quotas"] == {engine.hash_id("acme.inc"): {"rpm": 5}}


async def test_g16_a_job_message_carries_only_hashes(make: Make) -> None:
    """RQ-6: job messages (and their dead letters, kept 7 days) held the raw ids."""
    engine, _ = await make(worker=False)
    runner = JobRunner(engine.queues, hash_id=engine.hash_id)

    async def job(_payload: object) -> None:
        return None

    runner.register("g16", job)
    with request_context(tenant_id="acme", user_id="user-1"):
        await runner.send("g16", {"x": 1})
    entries = await engine.state.xrange(engine.keys.queue(runner.queue("g16"), "s"))
    body = entries[0][1][b"body"]
    assert b"acme" not in body
    assert b"user-1" not in body
    ctx = orjson.loads(body)["ctx"]
    assert ctx == {"user_hash": engine.hash_id("user-1"), "tenant_hash": engine.hash_id("acme")}


async def test_b02_messages_are_version_3_and_a_0_1_1_message_is_refused(make: Make) -> None:
    """B-02: 0.1.1 messages carried raw ids under format 2; 0.2.0 refuses them."""
    engine, _ = await make(worker=False)
    assert VERSION == 3
    task = asyncio.create_task(engine.call("small", {"messages": []}, timeout_secs=5))
    try:
        for _ in range(100):
            queue = engine._queue_name(engine.registry.current, "small", False)
            entries = await engine.state.xrange(engine.keys.queue(queue, "s"))
            if entries:
                break
            await asyncio.sleep(0.02)
        assert orjson.loads(entries[0][1][b"body"])["v"] == 3
        old = orjson.loads(entries[0][1][b"body"])
        old["v"] = 2
        with pytest.raises(ValueError, match="not supported"):
            parse(orjson.dumps(old))
    finally:
        task.cancel()
        with pytest.raises(BaseException):  # noqa: B017 PT011  # cancelled or failed
            await task
