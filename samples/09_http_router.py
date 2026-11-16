"""Mount the queue's status, registry and health routes on a FastAPI app (admin pages, probes).

`llmq.http_router()` returns an `APIRouter` (extra `http`). `/health/live` and `/health/ready`
take no auth (Kubernetes probes); every other route runs the app's admin dependencies. Writes
(live registry edits, region / account blocks) stay off unless `allow_writes=True`; they take a
JSON body and need the `X-LLMQ-Admin` header (a browser cannot send it cross-site). A preStop
hook calls `llmq.pause_receiving()`: readiness fails and no new queued work starts.
`metrics_route=True` adds `GET /metrics`, the Prometheus exposition of a Prometheus sink (extra
`metrics`), open like the probes so a scraper can read it.

Here the app is called in process (httpx's ASGI transport), so no port is opened; in the app
the same router is served by uvicorn.

    python scripts/samples.py 09_http_router [--live]
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import httpx
from fastapi import FastAPI, Header, HTTPException

from llm_queue import LLMQueue, PrometheusSink

# Prometheus metrics for /metrics; in an app `LLMQ_METRICS_SINK=prometheus` does the same.
llmq = LLMQueue.from_env(metrics=PrometheusSink())


def admin_only(x_admin_token: str = Header(default="")) -> None:
    """The app's own admin check (here a fixed header; use the app's auth)."""
    if x_admin_token != "sample-admin":
        raise HTTPException(status_code=401, detail="admin only")


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await llmq.start()
    yield
    await llmq.stop()


app = FastAPI(lifespan=lifespan)
app.include_router(
    llmq.http_router(
        dependencies=[admin_only],
        allow_writes=True,
        actor=lambda request: "sample-admin",  # the authenticated user's name in an app
        metrics_route=True,
    ),
    prefix="/llm-queue",
)


# GET for Kubernetes' `preStop: httpGet` (a POST-only route answered it 405 and nothing
# drained); POST for scripts. The route is the app's own: the router has no `/drain`.
@app.api_route("/drain", methods=["GET", "POST"])
async def drain() -> dict[str, bool]:
    """PreStop hook: fail readiness and take no new queued calls; running ones finish."""
    llmq.pause_receiving()
    return {"draining": True}


async def main() -> None:
    admin = {"x-admin-token": "sample-admin"}
    writer = {**admin, "X-LLMQ-Admin": "1"}  # write routes need this header too
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://app") as client,
    ):
        ready = await client.get("/llm-queue/health/ready")
        print(f"GET /health/ready -> {ready.status_code} {ready.json()}")
        # Strict mode keeps a pod not ready on a store that evicts keys (owner decision Q10):
        # an ElastiCache group left on its default `volatile-lru` shows it here. Set the
        # group's maxmemory-policy to noeviction before deploying.
        reason = str(ready.json().get("not_ready_reason", ""))
        if ready.status_code == 503 and "maxmemory-policy" in reason:
            print("    not ready because the store evicts keys: set maxmemory-policy noeviction")
        else:
            assert ready.status_code == 200

        denied = await client.get("/llm-queue/status")
        print(f"GET /status without the admin header -> {denied.status_code}")
        assert denied.status_code == 401

        reply = await llmq.converse(
            "gpt20b",
            {"messages": [{"role": "user", "content": [{"text": "Say hi."}]}],
             "inferenceConfig": {"maxTokens": 200}},
        )  # fmt: skip
        found = await client.get(f"/llm-queue/requests/{reply.request_id}", headers=admin)
        print(f"GET /requests/{{id}} -> {found.json()['state']}")
        assert found.json()["state"] == "done"

        metrics = await client.get("/llm-queue/metrics")  # no admin header: a scraper's GET
        series = [line for line in metrics.text.splitlines() if line.startswith("llmq_")]
        print(f"GET /metrics -> {metrics.status_code}, {len(series)} llmq_ series")
        assert metrics.status_code == 200
        assert series

        status = (await client.get("/llm-queue/status?model=gpt20b", headers=admin)).json()
        for dep in status["deployments"]:
            print(f"  {dep['name']}: rpm {dep['rpm']['used']}/{dep['rpm']['limit']}")

        no_header = await client.post(
            "/llm-queue/registry", params={"model": "gpt20b"}, json={"rpm": 50}, headers=admin
        )
        print(f"POST /registry without X-LLMQ-Admin -> {no_header.status_code}")
        assert no_header.status_code == 403

        edit = await client.post(
            "/llm-queue/registry",
            params={"model": "gpt20b"},
            json={"rpm": 50},
            headers=writer,
        )
        print(f"POST /registry rpm=50 -> {edit.json()['diff']}")
        assert edit.status_code == 200
        await client.post(
            "/llm-queue/registry",
            params={"model": "gpt20b"},
            json={"rpm": 60},
            headers=writer,
        )  # put the sample value back

        region = status["deployments"][0]["name"].split("/")[-1]
        for on in (True, False):  # an emergency block of a region, then lifted
            block = await client.post(
                "/llm-queue/registry/block", json={"region": region, "on": on}, headers=writer
            )
            print(f"POST /registry/block {region} on={on} -> {block.status_code}")
            assert block.status_code == 200

        await client.get("/drain")  # what the preStop httpGet sends
        after = await client.get("/llm-queue/health/ready")
        print(f"after /drain: GET /health/ready -> {after.status_code}")
        assert after.status_code == 503


if __name__ == "__main__":
    asyncio.run(main())
