"""0.2.0 F2 on a real store: `status.health` on every surface follows the registry.

A model starts healthy; a live edit that disables its only deployment makes it critical
(`no_usable_deployment`) in `status()`, `llmq.admin.status.health()` and `GET /status/health`;
a live edit of `health_summary` moves the waiting marks. No real LLM is called.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from engine_testkit import Make
from fastapi import FastAPI

from llm_queue.engine.admin import AdminServices
from llm_queue.engine.http import status_router

pytestmark = pytest.mark.integration


async def _get(admin: AdminServices, path: str) -> tuple[int, Any]:
    app = FastAPI()
    app.include_router(status_router(admin, insecure_reads=True))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        response = await client.get(path)
    return response.status_code, response.json()


async def test_health_summary_follows_live_edits(make: Make) -> None:
    engine, _ = await make()
    admin = AdminServices.for_engine(engine)

    out = await admin.status.health("small")
    assert out["health"] == "healthy"
    assert out["models"][0]["usable_deployments"] == 1
    report = await admin.status.get()
    assert report["health"] in ("healthy", "warning")
    assert {m["model"]: m["health"]["state"] for m in report["models"]}["small"] == "healthy"

    await admin.deployments.disable("small", "aws-a/r1", actor="test", dry_run=False)
    out = await admin.status.health("small")
    assert out["health"] == "critical"
    assert out["models"][0]["reasons"] == ["no_usable_deployment"]
    code, body = await _get(admin, "/status/health?model=small")
    assert code == 200
    assert body["models"][0]["state"] == "critical"
    code, body = await _get(admin, "/")
    assert {m["model"]: m["health"] for m in body["models"]}["small"] == "critical"

    await admin.deployments.enable("small", "aws-a/r1", actor="test", dry_run=False)
    await admin.registry.set(
        {"health_summary.waiting_warning": 1}, model="small", actor="test", dry_run=False
    )
    model = (await admin.status.health("small"))["models"][0]
    assert model["state"] == "healthy"  # nothing waits: 0 is below the new mark of 1
