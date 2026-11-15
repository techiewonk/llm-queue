"""HTTP admin handlers: status codes, bounds and auth warnings (FO-6, -7, -9, -14 to -17).

Fakes stand in for the engine and the registry store, so these run without the Docker stack.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

import pytest

from llm_queue.engine.http import MAX_EDIT_PATHS, Handlers, redacted, status_router
from llm_queue.errors import ConfigError, UnknownModel
from llm_queue.registry.store import RegistryConflict


class _Store:
    """A registry store fake: loaded or not, and a `set` / `block` that raise `error`."""

    def __init__(self, *, loaded: bool = True, error: Exception | None = None) -> None:
        self.loaded = loaded
        self.error = error
        self.changes: list[Any] = []
        routing = SimpleNamespace(blocked_regions=(), blocked_accounts=())
        self._view = SimpleNamespace(registry=SimpleNamespace(raw={}, routing=routing))

    @property
    def current(self) -> Any:
        if not self.loaded:
            raise ConfigError("the registry has not been loaded yet")
        return self._view

    async def diff_with_config(self) -> Any:
        _ = self.current
        return {}

    async def set(self, values: Any, **_: Any) -> Any:
        if self.error:
            raise self.error
        return self._view

    async def block(self, **_: Any) -> None:
        if self.error:
            raise self.error


class _Engine:
    def __init__(self, store: _Store, *, status_error: Exception | None = None) -> None:
        self.registry = store
        self.status_error = status_error
        self.health_calls = 0

    async def status(self, _model: str | None = None) -> Any:
        _ = self.registry.current
        if self.status_error:
            raise self.status_error
        return {}

    async def request_status(self, _rid: str) -> Any:
        raise AssertionError("an over-long id must not reach the store")

    async def health_check(self) -> Any:
        self.health_calls += 1
        raise AssertionError("liveness must not probe the store")

    def _live(self) -> bool:
        return True


def _handlers(store: _Store, **kw: Any) -> Handlers:
    return Handlers(_Engine(store, **kw), allow_writes=True)  # type: ignore[arg-type]  # fake


async def test_fo6_lost_compare_and_set_is_409() -> None:
    """FO-6: a RegistryConflict (a ConfigError) answered 400, though the request is retryable."""
    handlers = _handlers(_Store(error=RegistryConflict("lost the race")))
    code, body = await handlers.set({"models.m.rpm": 5}, actor="ann")
    assert code == 409
    assert body["retry_after_secs"] >= 1
    code, _ = await handlers.block(actor="ann", region="r1")
    assert code == 409


async def test_fo7_admin_routes_before_load_are_503() -> None:
    """FO-7: reading the registry before the first load raised ConfigError: a 500."""
    handlers = _handlers(_Store(loaded=False))
    expected = (503, {"error": "registry not loaded"})
    assert await handlers.status() == expected
    assert await handlers.registry() == expected
    assert await handlers.set({"rpm": 5}, actor="ann") == expected
    assert await handlers.block(actor="ann", region="r1") == expected


async def test_fo9_liveness_does_not_touch_the_store() -> None:
    """FO-9: /health/live ran the full health check, which probed the store inline."""
    handlers = _handlers(_Store())
    assert await handlers.live() == (200, {"live": True})


async def test_fo15_bounds_on_ids_and_edits() -> None:
    """FO-15: request ids, models and edit bodies had no size bound before the store read."""
    handlers = _handlers(_Store())
    code, _ = await handlers.request("x" * 10_000)
    assert code == 404
    code, _ = await handlers.status("m" * 10_000)
    assert code == 404
    many = {f"models.m{i}.rpm": 1 for i in range(MAX_EDIT_PATHS + 1)}
    code, _ = await handlers.set(many, actor="ann")
    assert code == 400
    code, _ = await handlers.block(actor="ann", region="r" * 10_000)
    assert code == 400


def test_fo16_redaction_shares_the_store_set_and_walks_lists() -> None:
    """FO-16: two copies of the secret-field set; a new field had to be added in both."""
    from llm_queue.registry.store import _SECRET_FIELDS

    doc = {"accounts": [dict.fromkeys(_SECRET_FIELDS, "secret")]}
    assert all(v == "[redacted]" for v in redacted(doc)["accounts"][0].values())
    from llm_queue.registry.store import audit_diff

    (entry,) = audit_diff({"a": []}, {"a": [{"api_key_ref": "LLMQ_ACCOUNT_K"}]})
    assert entry["after"] == [{"api_key_ref": "[redacted]"}]  # the audit walks lists too


async def test_fo17_only_an_unknown_model_is_404() -> None:
    """FO-17: any KeyError inside status reading answered 404 'unknown model'."""
    unknown = _handlers(_Store(), status_error=UnknownModel("nope"))
    code, _ = await unknown.status("nope")
    assert code == 404
    bug = _handlers(_Store(), status_error=KeyError("internal"))
    with pytest.raises(KeyError):
        await bug.status("m")


def test_fo14_open_admin_routes_log_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    """FO-14: with no dependencies, /registry was mounted with no auth and no warning."""
    engine: Any = _Engine(_Store())
    with caplog.at_level(logging.WARNING):
        status_router(engine)
    events = [getattr(r, "llmq", {}).get("event") for r in caplog.records]
    assert "admin_routes_unauthenticated" in events
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        status_router(engine, dependencies=[lambda: None])
    events = [getattr(r, "llmq", {}).get("event") for r in caplog.records]
    assert "admin_routes_unauthenticated" not in events
