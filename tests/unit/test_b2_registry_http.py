"""0.1.0b2 HTTP router write guards (RESCAN X2), without a store.

0.1.0b1 mounted write routes with no auth when the app forgot `dependencies`, and took
`/registry/block` from the query string: one cross-site POST (or an <img> to a GET-turned-POST
proxy) from a page an admin opened blocked a region with the admin's cookies.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from llm_queue.engine.http import ADMIN_HEADER, status_router
from llm_queue.errors import ConfigError


class _Registry:
    def __init__(self) -> None:
        self.blocks: list[dict[str, Any]] = []
        routing = SimpleNamespace(blocked_regions=(), blocked_accounts=())
        self.current = SimpleNamespace(registry=SimpleNamespace(routing=routing, raw={}))

    async def block(self, **kw: Any) -> None:
        self.blocks.append(kw)


def _allow(_: Any = None) -> None:
    return None


def test_writes_need_dependencies() -> None:
    engine: Any = SimpleNamespace(registry=_Registry())
    with pytest.raises(ConfigError, match="dependencies"):
        status_router(engine, allow_writes=True, actor=lambda r: "ann")


async def test_block_takes_a_json_body_and_the_admin_header() -> None:
    registry = _Registry()
    engine: Any = SimpleNamespace(registry=registry)
    app = FastAPI()
    app.include_router(
        status_router(engine, dependencies=[_allow], allow_writes=True, actor=lambda r: "ann")
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://app") as client:
        no_header = await client.post("/registry/block", json={"region": "r1"})
        assert no_header.status_code == 403
        query = await client.post(
            "/registry/block", params={"region": "r1"}, headers={ADMIN_HEADER: "1"}
        )
        assert query.status_code == 422  # no body: a query string no longer blocks
        bad = await client.post(
            "/registry/block", json={"region": "r1", "on": "yes"}, headers={ADMIN_HEADER: "1"}
        )
        assert bad.status_code == 400
        assert registry.blocks == []
        done = await client.post(
            "/registry/block", json={"region": "r1", "on": False}, headers={ADMIN_HEADER: "1"}
        )
        assert done.status_code == 200
        assert registry.blocks == [
            {"region": "r1", "account": None, "on": False, "actor": "http:ann"}
        ]
        edit = await client.post("/registry", json={"rpm": 5})
        assert edit.status_code == 403
