"""Fakes for the admin surfaces: an `AdminServices` stand-in that records each call."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from llm_queue.engine.admin.ops import Op
from llm_queue.engine.http import status_router


class FakeEngine:
    """What the open routes read: liveness, health, metrics."""

    worker_id = "pod-1"
    metrics: Any = None

    def __init__(self) -> None:
        self.health_calls = 0

    def _live(self) -> bool:
        return True

    def _not_live_reason(self) -> str:
        return ""

    async def health_check(self) -> dict[str, Any]:
        self.health_calls += 1
        return {"live": True, "ready": True, "stores": {"state": "ok"}}


class FakeAdmin:
    """Records `(op id, kwargs)` per call; answers `results[op id]`, or raises `error`."""

    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.error = error
        self.results: dict[str, Any] = {}
        self.context = SimpleNamespace(engine=FakeEngine())
        self.ops = SimpleNamespace(index=self._index)
        self.index_calls = 0

    async def _index(self, routes: Any = None) -> dict[str, Any]:
        self.index_calls += 1
        return {"library": {"version": "x"}, "routes": list(routes or ())}

    def method(self, op: Op) -> Any:
        async def run(**kwargs: Any) -> Any:
            self.calls.append((op.id, kwargs))
            if self.error is not None:
                raise self.error
            return self.results.get(op.id, {"ok": True})

        return run


def allow(_: Any = None) -> None:
    """An app dependency that lets everyone in."""


def client(admin: Any, **kw: Any) -> TestClient:
    """A test client of `status_router(admin, **kw)`."""
    app = FastAPI()
    router = status_router(admin, **kw)
    app.include_router(router)
    http = TestClient(app)
    http.admin_router = router  # type: ignore[attr-defined]
    return http


def mounted(http: TestClient) -> set[tuple[str, str]]:
    """The (method, path) pairs the admin router mounted (HEAD excluded)."""
    router = http.admin_router  # type: ignore[attr-defined]
    return {(m, r.path) for r in router.routes for m in r.methods if m != "HEAD"}
