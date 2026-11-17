"""0.2.0 FastAPI integration (plan B2, B3, B4, H2, H10) without a store.

`install()` / `serve()` / `mount()` against a recording queue double, the request context
middleware on HTTP and websocket, the background helpers and `LLMQueue.ready`. The same
wiring on a real store is in tests/integration/engine/test_020_fastapi_store.py.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from fastapi import APIRouter, FastAPI, WebSocket
from fastapi.testclient import TestClient

import llm_queue
from llm_queue import (
    ConfigError,
    LLMQueue,
    LLMQueueError,
    RequestContextMiddleware,
    background,
    background_priority,
    create_background_task,
    current_context,
    queue_ready,
    request_context,
    run_in_thread,
)
from llm_queue.context import _BACKGROUND_TASKS
from llm_queue.fastapi import install, mount, serve
from llm_queue.langchain import set_default_queue

DEV = {
    "LLMQ_STORE_URL": "redis://localhost:6379/0",
    "LLMQ_ENVIRONMENT": "dev",
    "LLMQ_REGISTRY": '{"registry_version": 1, "models": {}}',
}


class FakeQueue:
    """Records start / stop into a shared event list and the options `http_router` got."""

    def __init__(
        self, events: list[Any], *, fail_start: bool = False, token: str | None = None
    ) -> None:
        self.events = events
        self.fail_start = fail_start
        self.router_options: dict[str, Any] = {}
        self._admin_token = lambda: token
        self.ready = False

    async def start(self) -> None:
        self.events.append("queue_start")
        if self.fail_start:
            raise LLMQueueError("store down")
        self.ready = True

    async def stop(self, drain_secs: float | None = None) -> None:
        self.events.append(("queue_stop", drain_secs))
        self.ready = False

    def http_router(self, **options: Any) -> APIRouter:
        self.router_options = options
        router = APIRouter()

        @router.get("/status")
        async def status() -> dict[str, bool]:
            return {"ok": True}

        return router


def _app(events: list[Any]) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        events.append("app_start")
        yield
        events.append("app_stop")

    return FastAPI(lifespan=lifespan)


# ---------------------------------------------------------------- H2: install()


def test_install_starts_the_queue_before_the_app_and_stops_it_after() -> None:
    events: list[Any] = []
    app = _app(events)
    queue: Any = FakeQueue(events)
    assert install(app, queue, drain_secs=3) is queue
    with TestClient(app):
        assert events == ["queue_start", "app_start"]
        assert app.state.llm_queue is queue
    assert events == ["queue_start", "app_start", "app_stop", ("queue_stop", 3)]


def test_install_keeps_the_lifespan_state_of_the_app() -> None:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[dict[str, str]]:
        yield {"db": "conn"}

    app = FastAPI(lifespan=lifespan)
    install(app, FakeQueue([]))  # type: ignore[arg-type]
    seen: list[Any] = []

    async def run() -> None:
        async with app.router.lifespan_context(app) as state:
            seen.append(state)

    asyncio.run(run())
    assert seen == [{"db": "conn"}]


def test_on_start_error_log_keeps_the_app_up_and_still_stops() -> None:
    events: list[Any] = []
    app = _app(events)
    queue: Any = FakeQueue(events, fail_start=True)
    install(app, queue, on_start_error="log")
    with TestClient(app) as client:
        assert client.get("/internal/llm-queue/status").status_code == 200
        assert queue.ready is False
    assert events == ["queue_start", "app_start", "app_stop", ("queue_stop", None)]


def test_on_start_error_raise_fails_the_startup_before_the_app_starts() -> None:
    events: list[Any] = []
    app = _app(events)
    install(app, FakeQueue(events, fail_start=True))  # type: ignore[arg-type]
    with pytest.raises(LLMQueueError, match="store down"), TestClient(app):
        pass
    assert "app_start" not in events


def test_bad_on_start_error_and_a_second_install_are_refused() -> None:
    app = FastAPI()
    with pytest.raises(ConfigError, match="on_start_error"):
        install(app, FakeQueue([]), on_start_error="ignore")  # type: ignore[arg-type]
    install(app, FakeQueue([]))  # type: ignore[arg-type]
    with pytest.raises(ConfigError, match="already ran"):
        install(app, FakeQueue([]))  # type: ignore[arg-type]


def test_routes_mount_under_the_prefix_and_options_pass_through() -> None:
    app = FastAPI()
    queue: Any = FakeQueue([])
    install(app, queue, prefix="/ops/llmq", allow_writes=False, admin_header="X-T")
    with TestClient(app) as client:
        assert client.get("/ops/llmq/status").json() == {"ok": True}
        assert client.get("/internal/llm-queue/status").status_code == 404
    assert queue.router_options == {
        "allow_writes": False,
        "admin_header": "X-T",
        "drain_routes": False,  # no token, no dependencies: drain by file only
    }


@pytest.mark.parametrize(
    ("token", "options", "drain"),
    [
        (None, {}, False),
        ("s3cret", {}, True),  # LLMQ_ADMIN_TOKEN set
        (None, {"admin_token": "t"}, True),
        (None, {"dependencies": [object()]}, False),  # no actor for the audit
        (None, {"dependencies": [object()], "actor": lambda r: "ann"}, True),
        (None, {"admin_read_token": "r"}, False),  # the read token never writes
        ("s3cret", {"drain_routes": False}, False),  # an explicit choice wins
    ],
)
def test_drain_routes_are_mounted_when_they_can_be_guarded(
    token: str | None, options: dict[str, Any], drain: bool
) -> None:
    queue: Any = FakeQueue([], token=token)
    mount(FastAPI(), queue, **options)
    assert queue.router_options["drain_routes"] is drain


def test_mount_admin_false_mounts_nothing() -> None:
    app = FastAPI()
    queue: Any = FakeQueue([])
    install(app, queue, mount_admin=False)
    with TestClient(app) as client:
        assert client.get("/internal/llm-queue/status").status_code == 404
    assert queue.router_options == {}


def test_install_without_a_queue_builds_one_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built: Any = FakeQueue([])
    monkeypatch.setattr(LLMQueue, "from_env", classmethod(lambda cls: built))
    assert install(FastAPI()) is built


async def test_serve_starts_and_stops_around_the_block() -> None:
    events: list[Any] = []
    app = FastAPI()
    queue: Any = FakeQueue(events)
    async with serve(app, queue, drain_secs=0) as served:
        assert served is queue
        events.append("app")
    assert events == ["queue_start", "app", ("queue_stop", 0)]


# ---------------------------------------------------------------- B3: middleware


def _context_app(**values: Any) -> FastAPI:
    app = FastAPI()

    def fields() -> dict[str, Any]:
        ctx = current_context()
        return {
            "priority": ctx.priority,
            "tenant_id": ctx.tenant_id,
            "user_id": ctx.user_id,
            "session_id": ctx.session_id,
        }

    @app.get("/ctx")
    async def http_ctx() -> dict[str, Any]:
        return fields()

    @app.websocket("/ws")
    async def ws_ctx(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.send_json(fields())
        await websocket.close()

    app.add_middleware(RequestContextMiddleware, **values)
    return app


def _header(name: str) -> Any:
    def read(scope: Any) -> str | None:
        for key, value in scope["headers"]:
            if key == name.encode():
                return str(value.decode())
        return None

    return read


def test_middleware_sets_the_context_for_http_requests() -> None:
    async def session(scope: Any) -> str:  # an async callable is awaited
        return "s-" + str(scope["path"]).strip("/")

    app = _context_app(
        tenant_id="acme", user_id=_header("x-user"), session_id=session, priority=True
    )
    with TestClient(app) as client:
        assert client.get("/ctx", headers={"x-user": "u1"}).json() == {
            "priority": True,
            "tenant_id": "acme",
            "user_id": "u1",
            "session_id": "s-ctx",
        }
        # A callable returning None leaves the field inherited (unset here).
        assert client.get("/ctx").json()["user_id"] is None


def test_middleware_sets_the_context_for_websockets() -> None:
    app = _context_app(tenant_id="acme", user_id=_header("x-user"))
    with TestClient(app) as client, client.websocket_connect("/ws", headers={"x-user": "u2"}) as ws:
        assert ws.receive_json() == {
            "priority": False,
            "tenant_id": "acme",
            "user_id": "u2",
            "session_id": None,
        }


def test_install_adds_the_middleware_with_its_values() -> None:
    app = _context_app()
    install(app, FakeQueue([]), context={"tenant_id": "t1"}, mount_admin=False)  # type: ignore[arg-type]
    with TestClient(app) as client:
        assert client.get("/ctx").json()["tenant_id"] == "t1"


# ---------------------------------------------------------------- B3 / H10: background helpers


def test_background_keeps_ids_and_drops_priority() -> None:
    with request_context(priority=True, user_id="u", tenant_id="t"):
        with background() as ctx:
            assert (ctx.priority, ctx.user_id, ctx.tenant_id) == (False, "u", "t")
        assert current_context().priority is True


def test_background_priority_on_a_sync_function() -> None:
    @background_priority
    def work(x: int) -> tuple[int, bool, str | None]:
        """Doc kept."""
        ctx = current_context()
        return x, ctx.priority, ctx.user_id

    with request_context(priority=True, user_id="u"):
        assert work(2) == (2, False, "u")
        assert current_context().priority is True
    assert work.__doc__ == "Doc kept."


async def test_background_priority_on_a_coroutine_function() -> None:
    @background_priority
    async def work() -> tuple[bool, str | None]:
        await asyncio.sleep(0)  # the context holds across awaits
        ctx = current_context()
        return ctx.priority, ctx.session_id

    with request_context(priority=True, session_id="s"):
        assert await work() == (False, "s")
        assert current_context().priority is True


def test_background_priority_refuses_generator_functions() -> None:
    def gen() -> Any:
        yield 1

    async def agen() -> Any:
        yield 1

    for fn in (gen, agen):
        with pytest.raises(TypeError, match="generator"):
            background_priority(fn)


async def test_create_background_task_keeps_a_reference_and_runs_at_background_priority() -> None:
    release = asyncio.Event()

    async def work() -> tuple[bool, str | None]:
        await release.wait()
        ctx = current_context()
        return ctx.priority, ctx.tenant_id

    with request_context(priority=True, tenant_id="t"):
        create_background_task(work(), name="bg-work")  # not held by the caller
    gc.collect()
    held = [t for t in _BACKGROUND_TASKS if t.get_name() == "bg-work"]
    assert len(held) == 1
    assert current_context().priority is False  # the caller's context is untouched
    release.set()
    assert await held[0] == (False, "t")
    await asyncio.sleep(0)
    assert held[0] not in _BACKGROUND_TASKS


async def test_a_failing_background_task_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    async def boom() -> None:
        raise RuntimeError("bad")

    with caplog.at_level(logging.WARNING, logger="llm_queue.context"):
        task = create_background_task(boom(), name="bg-boom")
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)
    assert any("background_task_failed" in r.getMessage() for r in caplog.records)


def test_run_in_thread_copies_the_ids_at_background_priority(
    caplog: pytest.LogCaptureFixture,
) -> None:
    seen: list[Any] = []

    def work(tag: str, *, n: int) -> None:
        ctx = current_context()
        seen.append(
            (tag, n, ctx.priority, ctx.user_id, ctx.tenant_id, threading.current_thread().daemon)
        )

    with request_context(priority=True, user_id="u", tenant_id="t"):
        run_in_thread(work, "a", n=1).join(5)
    assert seen == [("a", 1, False, "u", "t", True)]

    def boom() -> None:
        raise RuntimeError("bad")

    with caplog.at_level(logging.WARNING, logger="llm_queue.context"):
        run_in_thread(boom).join(5)
    assert any("background_thread_failed" in r.getMessage() for r in caplog.records)


def test_helpers_are_exported_from_the_package() -> None:
    for name in (
        "background",
        "background_priority",
        "create_background_task",
        "run_in_thread",
        "queue_ready",
        "RequestContextMiddleware",
    ):
        assert name in llm_queue.__all__
        assert getattr(llm_queue, name) is not None


# ---------------------------------------------------------------- B4: ready


def test_ready_needs_start_registry_no_drain_and_no_evicting_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    llmq = LLMQueue.from_env(DEV)
    engine = llmq._engine

    def ready() -> bool:  # read anew each time (mypy would narrow the property)
        return llmq.ready

    assert ready() is False  # not started
    llmq.started = True
    engine.ready = True
    monkeypatch.setattr(engine.registry, "_current", None)
    assert ready() is False  # registry not loaded
    monkeypatch.setattr(engine.registry, "_current", object())
    assert ready() is True
    monkeypatch.setattr(engine, "_paused", True)
    assert ready() is False  # draining
    monkeypatch.setattr(engine, "_paused", False)
    monkeypatch.setattr(engine, "_evicting", lambda: "state store: allkeys-lru")
    assert ready() is False  # strict mode and an evicting store
    monkeypatch.setattr(engine, "_evicting", lambda: None)
    llmq._closing = True
    assert ready() is False  # stopped
    llmq._closing, llmq.started = False, False


def test_queue_ready() -> None:
    set_default_queue(None)
    assert queue_ready() is False  # nothing started
    queue: Any = FakeQueue([])
    assert queue_ready(queue) is False
    queue.ready = True
    assert queue_ready(queue) is True
    set_default_queue(queue)
    try:
        assert queue_ready() is True
    finally:
        set_default_queue(None)
