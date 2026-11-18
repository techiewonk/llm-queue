"""Wire the queue into a FastAPI app with `install()`: admin, status, health and drain routes.

`llm_queue.fastapi.install(app, llmq, ...)` starts and stops the queue with the app and mounts
`llmq.http_router()` (extra `http`): every admin operation (docs/reference/admin-api.md), with
`GET /` listing them and a live summary. `/health/live` and `/health/ready` take no auth
(Kubernetes probes); every other route runs the app's admin dependency (or an admin token).
Writes (registry edits, blocks, purges, drain) stay off unless `allow_writes=True`; they need
the `X-LLMQ-Admin` header (a browser cannot send it cross-site) and are a dry run unless the
body says `"dry_run": false`. With `dependencies` and `actor`, `install()` also mounts the drain
routes: `POST /drain` fails readiness and hands held calls back, `POST /resume` undoes it,
`GET /drain` shows the state. A pod with several processes drains through `LLMQ_DRAIN_FILE`
instead: its preStop hook runs `touch $LLMQ_DRAIN_FILE` and every process sees it (a route
reaches one process only). `metrics_route=True` adds an open `GET /metrics` (extra `metrics`).

Here the app is called in process (httpx's ASGI transport), so no port is opened; in the app
the same routes are served by uvicorn.

    python scripts/samples.py 09_http_router [--live]
"""

from __future__ import annotations

import asyncio
import hmac
import os

import httpx
from fastapi import FastAPI, Header, HTTPException

from llm_queue import LLMQueue, PrometheusSink
from llm_queue.fastapi import install

# The app's own admin secret (here from the environment; use the app's auth in a real app).
ADMIN_SECRET = os.environ.get("SAMPLE_ADMIN_SECRET", "sample-admin")


def admin_only(x_admin_token: str = Header(default="")) -> None:
    """The app's own admin check: compared in constant time, so timing cannot leak it."""
    if not hmac.compare_digest(x_admin_token.encode(), ADMIN_SECRET.encode()):
        raise HTTPException(status_code=401, detail="admin only")


app = FastAPI()
# Prometheus metrics for /metrics; in an app `LLMQ_METRICS_SINK_MODE=prometheus` does the same.
llmq = install(
    app,
    LLMQueue.from_env(metrics=PrometheusSink()),
    dependencies=[admin_only],
    allow_writes=True,
    actor=lambda request: "sample-admin",  # the authenticated user's name in an app
    metrics_route=True,
)


async def main() -> None:
    admin = {"x-admin-token": ADMIN_SECRET}
    writer = {**admin, "X-LLMQ-Admin": "1"}  # write routes need this header too
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://app") as client,
    ):
        ready = await client.get("/internal/llm-queue/health/ready")
        print(f"GET /health/ready -> {ready.status_code} {ready.json()}")
        # Strict mode keeps a pod not ready on a store that can evict its keys (owner decision
        # Q10). On a `volatile-*` store (ElastiCache's default `volatile-lru`) the default
        # LLMQ_KEY_EXPIRY_MODE=auto picks `swept` and the pod is ready; `native` there, or an
        # `allkeys-*` policy, shows here. Keep `swept` (auto), or set noeviction.
        reason = str(ready.json().get("not_ready_reason", ""))
        if ready.status_code == 503 and "maxmemory-policy" in reason:
            print(
                "    not ready because the store can evict keys: on volatile-* keep "
                "LLMQ_KEY_EXPIRY_MODE=auto (swept), or set maxmemory-policy noeviction"
            )
        else:
            assert ready.status_code == 200

        denied = await client.get("/internal/llm-queue/status")
        print(f"GET /status without the admin header -> {denied.status_code}")
        assert denied.status_code == 401

        reply = await llmq.converse(
            "gpt20b",
            {"messages": [{"role": "user", "content": [{"text": "Say hi."}]}],
             "inferenceConfig": {"maxTokens": 200}},
        )  # fmt: skip
        found = await client.get(f"/internal/llm-queue/requests/{reply.request_id}", headers=admin)
        print(f"GET /requests/{{id}} -> {found.json()['status']['state']}")
        assert found.json()["status"]["state"] == "done"

        index = (await client.get("/internal/llm-queue/", headers=admin)).json()
        print(f"GET / -> {len(index['routes'])} routes, registry v{index['registry']['version']}")

        metrics = await client.get(
            "/internal/llm-queue/metrics"
        )  # no admin header: a scraper's GET
        series = [line for line in metrics.text.splitlines() if line.startswith("llmq_")]
        print(f"GET /metrics -> {metrics.status_code}, {len(series)} llmq_ series")
        assert metrics.status_code == 200
        assert series

        status = (await client.get("/internal/llm-queue/status?model=gpt20b", headers=admin)).json()
        for dep in status["deployments"]:
            print(f"  {dep['name']}: rpm {dep['rpm']['used']}/{dep['rpm']['limit']}")

        edit = {"values": {"rpm": 50}, "model": "gpt20b"}
        no_header = await client.post("/internal/llm-queue/registry", json=edit, headers=admin)
        print(f"POST /registry without X-LLMQ-Admin -> {no_header.status_code}")
        assert no_header.status_code == 403

        plan = await client.post("/internal/llm-queue/registry", json=edit, headers=writer)
        print(f"POST /registry rpm=50 (dry run) -> {plan.json()['diff']}")
        assert plan.json()["dry_run"] is True
        done = await client.post(
            "/internal/llm-queue/registry", json={**edit, "dry_run": False}, headers=writer
        )
        print(f"POST /registry rpm=50 -> {done.json()['diff']}")
        assert done.status_code == 200
        back = {"values": {"rpm": 60}, "model": "gpt20b", "dry_run": False}
        await client.post(
            "/internal/llm-queue/registry", json=back, headers=writer
        )  # the sample value

        region = status["deployments"][0]["name"].split("/")[-1]
        for path in ("/blocks", "/blocks/remove"):  # an emergency block of a region, lifted
            block = await client.post(
                f"/internal/llm-queue{path}",
                json={"region": region, "dry_run": False},
                headers=writer,
            )
            print(f"POST {path} {region} -> {block.json()['blocked_regions']}")
            assert block.status_code == 200

        drained = await client.post(
            "/internal/llm-queue/drain", json={"dry_run": False}, headers=writer
        )
        print(f"POST /drain -> {drained.status_code}")
        assert drained.status_code == 200
        after = await client.get("/internal/llm-queue/health/ready")
        print(f"after POST /drain: GET /health/ready -> {after.status_code}")
        assert after.status_code == 503
        state = await client.get("/internal/llm-queue/drain", headers=admin)
        print(f"GET /drain -> {state.status_code}")
        assert state.status_code == 200
        resumed = await client.post(
            "/internal/llm-queue/resume", json={"dry_run": False}, headers=writer
        )
        print(f"POST /resume -> {resumed.status_code}")
        assert resumed.status_code == 200


if __name__ == "__main__":
    asyncio.run(main())
