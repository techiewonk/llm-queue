"""Bug-pinning tests for admin paths from the engine audit (EN-10, EN-22, EN-30, EN-31).

Fakes stand in for the engine and the store, so they run without the Docker stack.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import pytest
from redis.exceptions import ConnectionError as StoreConnectionError


def events(caplog: pytest.LogCaptureFixture, name: str) -> list[logging.LogRecord]:
    """The captured records of one structured event."""
    return [r for r in caplog.records if getattr(r, "llmq", {}).get("event") == name]


async def test_en10_http_reports_a_store_outage_as_503() -> None:
    """EN-10: redis-py's ConnectionError reached FastAPI as a 500 with a traceback."""
    from llm_queue.engine.admin import BY_ID
    from llm_queue.engine.http import call

    class Down:
        def method(self, _op: Any) -> Any:
            async def run(**_: Any) -> Any:
                raise StoreConnectionError("refused")

            return run

    for op in ("status.get", "requests.get", "registry.get"):
        response = await call(Down(), BY_ID[op], {})  # type: ignore[arg-type]  # fake
        assert response == (503, {"error": "state store unavailable"})


async def test_en22_store_check_failures_leave_a_trace(caplog: pytest.LogCaptureFixture) -> None:
    """EN-22: the eviction and cluster checks swallowed store errors with no trace."""
    from llm_queue.engine import store_checks

    class Down:
        async def config_get(self, _name: str) -> Any:
            raise StoreConnectionError("refused")

        async def info(self, _section: str) -> Any:
            raise StoreConnectionError("refused")

    with caplog.at_level(logging.DEBUG, logger="llm_queue.engine.store_checks"):
        assert await store_checks.eviction_policy(Down()) == store_checks.UNKNOWN  # type: ignore[arg-type]  # fake
        assert await store_checks.cluster_enabled(Down()) is False  # type: ignore[arg-type]  # fake
    assert len(events(caplog, "store_check_skipped")) == 3


def test_en30_gc_scan_pattern_escapes_the_prefix() -> None:
    """EN-30: `[`, `*` or `?` in LLMQ_KEY_PREFIX widened `registry gc`'s SCAN match."""
    from llm_queue.engine.purge import _glob_escape

    assert _glob_escape("app[1]*?") == "app\\[1\\]\\*\\?"
    assert _glob_escape("plain:prefix") == "plain:prefix"


async def test_en30_queue_client_closed_when_the_first_close_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EN-30: if the state client's `aclose` raised, the queue client was never closed."""
    from llm_queue.engine import cli

    closed: list[str] = []

    class Client:
        def __init__(self, name: str) -> None:
            self.name = name

        def register_script(self, _script: str) -> Any:
            return None

        async def aclose(self) -> None:
            closed.append(self.name)
            if self.name == "state":
                raise RuntimeError("close failed")

    def make(url: str, *_: Any) -> Client:
        return Client("queue" if "6380" in url else "state")

    async def run(*_: Any) -> Any:
        return {"ok": True}

    monkeypatch.setattr("llm_queue.engine.store_client.secret_store_client", make)
    monkeypatch.setattr(cli, "run", run)
    env = {
        "LLMQ_STORE_URL": "redis://127.0.0.1:6379/0",
        "LLMQ_QUEUE_STORE_URL": "redis://127.0.0.1:6380/0",
        "LLMQ_ENVIRONMENT": "test",
    }
    with pytest.raises(RuntimeError):
        await cli.amain(["--actor", "t", "status"], env)
    assert sorted(closed) == ["queue", "state"]


def test_en30_main_maps_a_store_error_to_one_line(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """EN-30: a store outage printed redis-py's traceback instead of one line and exit 1."""
    from llm_queue.engine import cli

    async def down(*_: Any) -> Any:
        raise StoreConnectionError("Connection refused")

    monkeypatch.setattr(cli, "amain", down)
    assert cli.main(["status"], {}) == 1
    assert "Connection refused" in capsys.readouterr().err


async def test_en31_fresh_password_does_not_block_the_loop() -> None:
    """EN-31: a slow `secrets` lookup ran on the event loop on every new connection."""
    from llm_queue.engine.store_client import FreshPassword

    def slow(_name: str) -> str:
        time.sleep(0.2)
        return "pw"

    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    task = asyncio.create_task(ticker())
    try:
        assert await FreshPassword(slow, "s").get_credentials_async() == ("pw",)
    finally:
        task.cancel()
    assert ticks >= 5
