"""Operations views on Valkey and Redis (plan step A12; scenarios B13, B14, B36, B55, B79).

Status per model and per request, readiness, alerts (gauges and one log per change), metrics
and the per-call record, model drain, the HTTP handlers, the command line and the environment
settings. No real LLM is called.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from typing import Any

import pytest
import redis.asyncio
from engine_testkit import CHAT, Make, away_from_minute_end

from llm_queue.engine import cli
from llm_queue.engine.admin import AdminServices
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.http import status_router
from llm_queue.engine.metrics import EmfSink
from llm_queue.engine.settings import InfraSettings
from llm_queue.errors import AdminRefused, ConfigError, UnknownModel
from llm_queue.registry.store import LiveEditRefused

pytestmark = pytest.mark.integration


async def test_status_of_model_and_request(make: Make) -> None:
    engine, _ = await make(
        {"big": {"rpm": 50}},
        {"defaults": {"latency_ms": {"p50": 600, "p95": 600}}},
    )
    await away_from_minute_end(engine.clock.now)  # the counters are per calendar minute
    task = asyncio.create_task(engine.call("big", CHAT, request_id="r-status"))
    for _ in range(100):  # until it runs (not a fixed sleep: client start-up time varies)
        running = await engine.request_status("r-status")
        if running.get("state") == "in_progress":
            break
        await asyncio.sleep(0.03)
    assert running["state"] == "in_progress"
    assert running["deployment"] == "aws-a/r1"
    await task
    done = await engine.request_status("r-status")
    assert done["state"] == "done"
    status = await engine.status("big")
    dep = next(d for d in status["deployments"] if d["name"] == "aws-a/r1")
    assert dep["rpm"] == {"limit": 50, "used": 1, "left": 49, "quota": None}
    assert dep["healthy"]
    assert dep["breaker"] == "closed"
    assert dep["concurrency"]["in_use"] == 0
    assert status["waiting"] == {"priority": 0, "normal": 0}
    everything = await engine.status()
    assert {m["model"] for m in everything["models"]} == {"big", "small", "titan", "cohere"}
    assert await engine.request_status("nope") == {}


async def test_readiness_never_depends_on_providers(make: Make) -> None:
    engine, _ = await make(scenario={"defaults": {"force_error": "ServiceUnavailableException"}})
    health = await engine.health_check()
    policy = health.pop("eviction_policy")  # read at start (plan C4)
    assert policy != "unknown"
    stable = {key: health[key] for key in ("live", "ready", "stores")}
    assert stable == {"live": True, "ready": True, "stores": {"state": "ok", "queue": "ok"}}
    assert "not_ready_reason" not in health
    assert "live_reason" not in health
    # Fields for the operator (R3OB-20): present, whatever their values.
    assert {"worker", "version", "registry_version", "held", "heartbeat_age_secs"} <= set(health)
    await engine.stop(drain_secs=0)
    assert (await engine.health_check())["ready"] is False


async def test_alerts_log_once_per_change(make: Make) -> None:
    engine, _ = await make()
    await engine.health.disable("big", "aws-a/r1", probe_after_secs=600)
    view = engine.registry.current
    alerts = engine.alerts.evaluate(view, await engine.status())
    on = {(a.name, a.labels.get("deployment")) for a in alerts if a.on}
    assert ("deployment_disabled", "aws-a/r1") in on
    first = await engine.alerts.changed(alerts)
    assert any(a.name == "deployment_disabled" for a in first)
    assert await engine.alerts.changed(alerts) == []  # no change: no new log line


async def test_metrics_and_call_record(make: Make, caplog: pytest.LogCaptureFixture) -> None:
    lines: list[str] = []
    engine, _ = await make()
    engine.metrics = EmfSink(lines.append)
    caplog.set_level(logging.INFO, logger="llm_queue.engine.runtime")
    await engine.call("big", CHAT, user_id="ann", tenant_id="desk")
    # The sink adds points up for a second (R3OB-11, R3PF-14); stop writes what it holds.
    await engine.stop(drain_secs=0)
    logs = [getattr(r, "llmq", {}) for r in caplog.records]
    # One line per label set, with every metric of that set (its `_aws` Metrics list).
    points = [json.loads(line) for line in lines]
    named = {
        metric["Name"]: point
        for point in points
        for metric in point["_aws"]["CloudWatchMetrics"][0]["Metrics"]
    }
    assert "llmq_tokens_output_total" in named
    point = named["llmq_calls_total"]
    assert (point["model"], point["region"], point["outcome"]) == ("big", "r1", "ok")
    assert point["llmq_calls_total"] == 1
    record = [entry for entry in logs if entry.get("event") == "llmq_call_finished"]
    assert len(record) == 1
    assert record[0]["tenant"] != "desk"  # the tenant id is hashed too (RESCAN X10)
    assert record[0]["user"] != "ann"  # the user id is hashed
    assert record[0]["tokens_out"] is not None


async def test_model_drain(make: Make) -> None:
    engine, _ = await make()
    await engine.registry.set({"enabled": False}, model="small", actor="t")
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT)
    assert info.value.kind == "model_disabled"
    rows = await engine.state.hget(engine.keys.model("small", "cfg"), "numbers")
    assert json.loads(rows)["deployments"][0]["enabled"] is True  # waiting calls still served


async def test_admin_services_on_a_live_engine(make: Make) -> None:
    """Every non-request operation through `llmq.admin`, against the real stores."""
    engine, _ = await make()
    admin = AdminServices.for_engine(engine)
    assert (await admin.status.get("big"))["model"] == "big"
    with pytest.raises(UnknownModel):
        await admin.status.get("nope")
    got = await admin.registry.get()
    assert got["registry"]["models"]["big"]["model_id"]
    assert (await admin.registry.versions())["current"] >= 1
    assert "properties" in admin.registry.schema()
    plan = await admin.registry.set({"rpm": 5}, model="big", actor="ann")
    assert plan["dry_run"] is True
    assert plan["diff"] == [{"path": "models.big.rpm", "before": None, "after": 5}]
    assert engine.registry.current.value("big", "rpm") != 5  # a dry run writes nothing
    done = await admin.registry.set({"rpm": 5}, model="big", actor="ann", dry_run=False)
    assert done["diff"] == plan["diff"]
    assert engine.registry.current.value("big", "rpm") == 5
    with pytest.raises(LiveEditRefused):  # protected: refused by the dry run too
        await admin.registry.set({"deployments.aws-a.confirmed": ["r1"]}, model="big", actor="a")
    assert (await admin.registry.publish(actor="ann"))["action"] in ("kept", "not_bumped")
    models = {m["model"]: m for m in (await admin.models.list())["models"]}
    assert models["big"]["deployments"] >= 1
    assert (await admin.models.get("big"))["settings"]["rpm"]["value"] == 5
    off = await admin.models.disable("small", actor="ann", dry_run=False)
    assert off["diff"] == [{"path": "models.small.enabled", "before": None, "after": False}]
    drained = await admin.models.drain("small", wait_secs=2, actor="ann", dry_run=False)
    assert (drained["enabled"], drained["drained"]) == (False, True)
    await admin.models.enable("small", actor="ann", dry_run=False)
    deps = (await admin.deployments.list("big"))["deployments"]
    assert deps[0]["usable"] is True
    one = await admin.deployments.get("big", deps[0]["name"])
    assert one is not None
    assert "settings" in one
    assert await admin.deployments.get("big", "nope/none") is None
    disabled = await admin.deployments.disable("big", deps[0]["name"], actor="ann", dry_run=False)
    assert disabled["enabled"] is False
    await admin.deployments.enable("big", deps[0]["name"], actor="ann", dry_run=False)
    blocked = await admin.blocks.add(region="r2", actor="ann", dry_run=False)
    assert blocked["blocked_regions"] == ["r2"]
    listed = await admin.blocks.list()
    assert listed["regions"][0]["name"] == "r2"
    assert listed["regions"][0]["by"] == "ann"
    assert (await admin.blocks.remove(region="r2", actor="ann", dry_run=False))[
        "blocked_regions"
    ] == []
    assert (await admin.blocks.kill_switch(True, actor="ann"))["dry_run"] is True
    assert engine.registry.current.registry.routing.kill_switch is False
    entries = (await admin.audit.list(50, op="edit"))["entries"]
    assert entries
    assert all(e["op"] == "edit" for e in entries)
    queues = (await admin.queues.list())["queues"]
    assert any(q["queue"].endswith("llm-big-priority") for q in queues)
    assert (await admin.dlq.list(model="big"))["letters"] == []
    assert await admin.dlq.show("1-0", model="big") is None
    assert (await admin.dlq.redrive(model="big", actor="ann"))["moved"] == 0
    assert (await admin.dlq.purge(model="big", actor="ann", dry_run=False))["deleted"] == 0
    assert (await admin.jobs.list())["jobs"] == []
    assert (await admin.purge.scheduled())["scheduled"] == []
    assert (await admin.purge.unschedule("ghost", actor="ann"))["cancelled"] is False
    assert len(admin.tenants.hash("acme")["tenant_hash"]) == 16
    summary = await admin.ops.index()
    assert summary["registry"]["version"] >= 1
    assert {m["model"] for m in summary["models"]} >= {"big", "small"}
    assert summary["process"]["live"] is True
    with pytest.raises(AdminRefused, match="process-local"):
        admin.drain.get()  # no LLMQueue around this engine
    with pytest.raises(AdminRefused, match="requests service not available"):
        await admin.requests.list("big")


async def test_admin_cancel_and_request_by_id(make: Make) -> None:
    """`requests.get` / `requests.cancel` by id work without the request index."""
    engine, _ = await make()
    admin = AdminServices.for_engine(engine)
    reply = await engine.call("big", CHAT)
    found = await admin.requests.get(reply.request_id)
    assert found is not None
    assert found["final"] is True
    assert await admin.requests.get("nope") is None
    report = await admin.requests.cancel(request_ids=[reply.request_id], actor="ann")
    assert (report["matched"], report["skipped"]) == (0, {"final": 1})


async def test_admin_http_routes_on_a_live_engine(make: Make) -> None:
    import httpx
    from fastapi import FastAPI

    engine, _ = await make()
    app = FastAPI()
    admin = AdminServices.for_engine(engine)
    app.include_router(status_router(admin, admin_token="w", allow_writes=True))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://app") as http:
        token = {"X-LLMQ-Token": "w", "X-LLMQ-Actor": "bob"}
        assert (await http.get("/health/ready")).status_code == 200
        nope = await http.get("/status", params={"model": "nope"}, headers=token)
        assert nope.status_code == 404
        assert (await http.get("/requests/nope", headers=token)).status_code == 404
        body = (await http.get("/registry", headers=token)).json()
        assert body["registry"]["models"]["big"]["model_id"]
        edit = {"values": {"rpm": 5}, "model": "big"}
        assert (await http.post("/registry", json=edit, headers=token)).json()["dry_run"] is True
        done = await http.post("/registry", json={**edit, "dry_run": False}, headers=token)
        assert done.json()["diff"] == [{"path": "models.big.rpm", "before": None, "after": 5}]
        protected = {"values": {"deployments.aws-a.confirmed": ["r1"]}, "model": "big"}
        assert (await http.post("/registry", json=protected, headers=token)).status_code == 400
        block = {"region": "r2", "dry_run": False}
        blocked = await http.post("/blocks", json=block, headers=token)
        assert blocked.json()["blocked_regions"] == ["r2"]
        entry = (await admin.audit.list(1))["entries"][0]
        assert entry["actor"] == "token:bob"
        index = (await http.get("/", headers=token)).json()
    assert index["writes"] is True
    assert {"method": "POST", "path": "/blocks"}.items() <= next(
        r for r in index["routes"] if r["op"] == "blocks.add"
    ).items()


async def test_command_line(make: Make, store: redis.asyncio.Redis, prefix: str) -> None:
    engine, _ = await make()
    port = store.connection_pool.connection_kwargs["port"]
    env = {
        "LLMQ_STORE_URL": f"redis://:localdev@127.0.0.1:{port}/0",
        "LLMQ_ENVIRONMENT": "test",
        "LLMQ_KEY_PREFIX": prefix,
        "LLMQ_REGISTRY": json.dumps(dict(engine.registry.current.registry.raw)),
    }

    async def run(*argv: str) -> Any:
        return await cli.amain(["--actor", "tester", *argv], env)

    assert (await run("registry", "get", "--model", "big"))["registry"]["model_id"]
    dep = await run("deployments", "get", "big", "aws-a/r1")
    assert dep["settings"]["timeout_secs"] == {"value": 10, "level": "defaults"}
    plan = await run("registry", "set", "--model", "big", "rpm=7")
    assert plan["dry_run"] is True
    assert (await run("registry", "set", "--model", "big", "rpm=7", "--yes"))["dry_run"] is False
    log = await run("audit", "list", "--count", "1")
    assert log["entries"][0]["actor"] == "tester"
    assert (await run("blocks", "add", "--region", "r3", "--yes"))["blocked_regions"] == ["r3"]
    assert (await run("blocks", "remove", "--region", "r3", "--yes"))["blocked_regions"] == []
    assert "models.big.rpm" in [c["path"] for c in (await run("registry", "diff"))["live_edits"]]
    assert (await run("status", "--model", "big"))["model"] == "big"
    assert (await run("dlq", "list", "--model", "big"))["letters"] == []
    queues = (await run("queues", "list"))["queues"]
    assert any(q["queue"].endswith("llm-big-priority") for q in queues)
    assert (await run("describe"))["key_prefix"] == prefix
    assert (await run("ops"))["registry"]["version"] >= 1
    drained = await run("models", "drain", "small", "--wait-secs", "2", "--yes")
    assert (drained["enabled"], drained["drained"]) == (False, True)
    assert cli.exit_code(await run("requests", "get", "nope")) == 1


def test_environment_settings() -> None:
    ok = InfraSettings.from_env(
        {"LLMQ_STORE_URL": "redis://localhost:6379/0", "LLMQ_ENVIRONMENT": "dev"}
    )
    assert ok.registry_live_edit == "safe"
    with pytest.raises(ConfigError) as info:
        InfraSettings.from_env(
            {
                "LLMQ_STORE_URL": "redis://cache.example.com:6379",
                "LLMQ_ENVIRONMENT": "prod",
                "LLMQ_REGISTRY": "{}",
                "LLMQ_REGISTRY_FILE": "x.json",
            }
        )
    text = str(info.value)
    assert "must use TLS" in text
    assert "LLMQ_ID_HASH_SALT is required outside dev (prod)" in text
    assert "not both" in text


async def test_debug_capture(make: Make) -> None:
    engine, _ = await make({"big": {"debug_capture": True}})
    reply = await engine.call("big", CHAT)  # without the process opt-in: nothing is kept
    assert await engine.state.get(engine.keys.request(reply.request_id, "debug")) is None
    engine.settings = dataclasses.replace(engine.settings, debug_capture_allowed=True)
    reply = await engine.call("big", CHAT)
    captured = await engine.state.get(engine.keys.request(reply.request_id, "debug"))
    assert captured is not None
    assert json.loads(captured)["request"].startswith("{")
    assert 0 < await engine.state.ttl(engine.keys.request(reply.request_id, "debug")) <= 3600
