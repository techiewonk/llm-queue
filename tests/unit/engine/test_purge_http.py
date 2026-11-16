"""Purge surfaces without a store: HTTP routes and handlers, settings, the report (0.1.0b3).

A fake purge service stands in for `engine.purge`; the purges themselves are tested against
the real stores in tests/integration/engine/test_purge.py.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as StoreConnectionError

from llm_queue.engine.http import PurgeHandlers, status_router
from llm_queue.engine.purge import PurgeReport, PurgeSettings
from llm_queue.engine.settings import InfraSettings
from llm_queue.errors import ConfigError, PurgeRefused

DEV = {"LLMQ_STORE_URL": "redis://localhost:6379", "LLMQ_ENVIRONMENT": "dev"}


class _Purge:
    """Records each call; `error` is raised instead when set."""

    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.error = error

    def _report(self, name: str, *args: Any, **kw: Any) -> PurgeReport:
        self.calls.append((name, args, kw))
        if self.error:
            raise self.error
        return PurgeReport(
            target=str(args[0]) if args else "*", kind="model", dry_run=kw["dry_run"]
        )

    async def model(self, *args: Any, **kw: Any) -> PurgeReport:
        return self._report("model", *args, **kw)

    async def deployment(self, *args: Any, **kw: Any) -> PurgeReport:
        return self._report("deployment", *args, **kw)

    async def tenant(self, *args: Any, **kw: Any) -> PurgeReport:
        return self._report("tenant", *args, **kw)

    async def removed(self, *args: Any, **kw: Any) -> PurgeReport:
        return self._report("removed", *args, **kw)

    async def scheduled(self) -> list[dict[str, Any]]:
        return [{"target": "m", "kind": "model", "due_at": 1.0}]

    async def cancel_scheduled(self, target: str) -> bool:
        return target == "m"


class _Engine:
    def __init__(self, purge: _Purge) -> None:
        self.purge = purge


def _client(purge: _Purge, *, allow_writes: bool = True) -> TestClient:
    app = FastAPI()
    router = status_router(
        _Engine(purge),  # type: ignore[arg-type]  # fake
        dependencies=[lambda: None],
        allow_writes=allow_writes,
        actor=lambda _request: "ann",
    )
    app.include_router(router)
    return TestClient(app)


ADMIN = {"X-LLMQ-Admin": "1"}


def test_purge_routes_need_the_admin_header_and_default_to_a_dry_run() -> None:
    purge = _Purge()
    client = _client(purge)
    for path, body in (
        ("/purge/model", {"model": "m"}),
        ("/purge/deployment", {"model": "m", "deployment": "a/r"}),
        ("/purge/tenant", {"tenant": "acme"}),
        ("/registry/gc", {}),
    ):
        assert client.post(path, json=body).status_code == 403
        reply = client.post(path, json=body, headers=ADMIN)
        assert reply.status_code == 200, (path, reply.json())
        assert reply.json()["dry_run"] is True
    assert [c[0] for c in purge.calls] == ["model", "deployment", "tenant", "removed"]
    assert all(c[2]["actor"] == "http:ann" for c in purge.calls)
    done = client.post("/purge/model", json={"model": "m", "dry_run": False}, headers=ADMIN)
    assert done.json()["dry_run"] is False
    assert client.get("/purge/scheduled").status_code == 403
    assert client.get("/purge/scheduled", headers=ADMIN).json()["scheduled"][0]["target"] == "m"
    cancel = client.post("/purge/scheduled/cancel", json={"target": "m"}, headers=ADMIN)
    assert cancel.json() == {"target": "m", "cancelled": True}


def test_purge_routes_exist_only_with_writes() -> None:
    client = _client(_Purge(), allow_writes=False)
    assert client.post("/purge/model", json={"model": "m"}, headers=ADMIN).status_code in (
        404,
        405,
    )


@pytest.mark.parametrize(
    ("body", "code"),
    [
        ({}, 400),  # model missing
        ({"model": 1}, 400),
        ({"model": "m", "extra": True}, 400),
        ({"model": "m" * 10_000}, 400),
    ],
)
async def test_bad_purge_bodies_are_400(body: dict[str, Any], code: int) -> None:
    handlers = PurgeHandlers(_Engine(_Purge()), allow_writes=True)  # type: ignore[arg-type]
    assert (await handlers.purge("model", body, actor="ann"))[0] == code


async def test_refusals_errors_and_outages() -> None:
    def handlers(error: Exception) -> PurgeHandlers:
        return PurgeHandlers(_Engine(_Purge(error)), allow_writes=True)  # type: ignore[arg-type]

    assert (await handlers(PurgeRefused("no")).purge("model", {"model": "m"}, actor="a"))[0] == 409
    assert (await handlers(ConfigError("bad")).purge("tenant", {"tenant": "t"}, actor="a"))[
        0
    ] == 400
    down = await handlers(StoreConnectionError("x")).purge("gc", {}, actor="a")
    assert down == (503, {"error": "state store unavailable"})
    off = PurgeHandlers(_Engine(_Purge()))  # type: ignore[arg-type]
    assert (await off.purge("gc", {}, actor="a"))[0] == 403
    assert (await off.scheduled())[0] == 403


def test_purge_settings_defaults_and_validation() -> None:
    settings = InfraSettings.from_env(DEV)
    purge = PurgeSettings.from_infra(settings)
    assert purge == PurgeSettings()  # one set of defaults, env and code
    custom = InfraSettings.from_env(
        {
            **DEV,
            "LLMQ_AUTO_PURGE": "off",
            "LLMQ_TENANT_INDEX": "on",
            "LLMQ_TENANT_PURGE_DEFAULT": "running, keys",
            "LLMQ_PURGE_BATCH": "50",
        }
    )
    got = PurgeSettings.from_infra(custom)
    assert (got.auto, got.tenant_index, got.batch) == (False, True, 50)
    assert got.tenant_default == frozenset({"keys", "running"})
    for name, value in (
        ("LLMQ_AUTO_PURGE", "maybe"),
        ("LLMQ_TENANT_PURGE_DEFAULT", "keys,everything"),
        ("LLMQ_PURGE_BATCH", "0"),
    ):
        with pytest.raises(ConfigError, match=name):
            InfraSettings.from_env({**DEV, name: value})


def test_report_as_dict() -> None:
    report = PurgeReport("m", "model", 3, ("q",), (("q2", "busy"),), True, "ann", 1.0)
    assert report.as_dict() == {
        "target": "m",
        "kind": "model",
        "deleted_keys": 3,
        "queues": ["q"],
        "skipped": [{"thing": "q2", "why": "busy"}],
        "dry_run": True,
        "actor": "ann",
        "at": 1.0,
        "deleted_fields": 0,
        "cancelled": 0,
    }
