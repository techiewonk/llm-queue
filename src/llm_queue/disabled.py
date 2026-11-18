"""The queue `LLMQueue.from_env()` returns with `LLMQ_ENABLED=false` (0.2.0 H1, FU1).

The off switch, the last step of the switch ladder (docs/how-to/modes.md): no store client,
no engine, no provider client. The app runs its own direct path, because the factories
(`chat_model`, `embeddings`) return None. What still works: `start()` / `stop()` (the drain file
watcher only), `pause_receiving()` / `resume_receiving()` / `drain()` (readiness), `health()`
and `ready` (`enabled: false`), `describe()`, and `http_router()` (the two probes only). Every
call and store operation raises `QueueDisabled` (category `config_error`, not retryable).
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, NoReturn, cast

from llm_queue._logging import get_logger
from llm_queue.core import Health, LLMQueue
from llm_queue.drain import DrainWatcher
from llm_queue.errors import LLMQueueError, MissingExtra, QueueDisabled

if TYPE_CHECKING:
    from llm_queue.engine.admin import AdminServices
    from llm_queue.engine.envelope import Reply
    from llm_queue.engine.settings import InfraSettings

logger = get_logger(__name__)


class DisabledQueue(LLMQueue):
    """An `LLMQueue` switched off: probes, drain and `describe()`; calls raise `QueueDisabled`."""

    def __init__(self, settings: InfraSettings) -> None:  # noqa: D107 - see the class
        # No engine: nothing of LLMQueue.__init__ applies. The attributes below are what the
        # fork hook and `queue_ready()` read.
        self.settings = settings
        self.started = False
        self._stopped = False
        self._closing = False
        self._inherited = False
        self._sync = None
        self._created = time.time()
        self._drain_watcher: DrainWatcher | None = None
        self._paused_since: float | None = None
        self._admin: AdminServices | None = None

    @property
    def enabled(self) -> bool:
        """False: `LLMQ_ENABLED=false` switched the library off."""
        return False

    @property
    def execution_mode(self) -> str:
        """`disabled`."""
        return "disabled"

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        """Log `llmq_disabled` and watch `LLMQ_DRAIN_FILE` (when set); nothing else starts."""
        if self.started:
            return
        if self._stopped:
            raise LLMQueueError("this LLMQueue was stopped; build a new one to start again")
        from llm_queue.langchain import get_default_queue, set_default_queue

        try:
            get_default_queue()
        except LLMQueueError:
            set_default_queue(self)  # so `queue_ready()` finds it
        settings = self.settings
        if settings is not None and settings.drain_file:  # readiness still fails on preStop
            self._drain_watcher = DrainWatcher(
                settings.drain_file,
                self.pause_receiving,
                poll_secs=settings.drain_poll_secs,
                since=self._created,
            )
            self._drain_watcher.start()
        self.started = True
        logger.info("llmq_disabled", **self.describe())

    async def stop(self, drain_secs: float | None = None) -> None:
        """Stop the drain file watcher; a second call does nothing."""
        from llm_queue.langchain import get_default_queue, set_default_queue

        if self._stopped:
            return
        self._stopped = self._closing = True
        self.started = False
        try:
            if get_default_queue() is self:
                set_default_queue(None)
        except LLMQueueError:
            pass
        watcher, self._drain_watcher = self._drain_watcher, None
        if watcher is not None:
            await watcher.stop()

    def pause_receiving(self) -> None:
        """Fail readiness (the drain file or a preStop hook); nothing else to pause."""
        if self._paused_since is None:
            self._paused_since = time.time()

    def resume_receiving(self) -> None:
        """Ready again; `LLMQueueError` once `stop()` began."""
        if self._stopped:
            raise LLMQueueError("this LLMQueue is stopped; build a new one to resume receiving")
        self._paused_since = None

    @property
    def draining(self) -> bool:
        """True between `pause_receiving()` (or the drain file) and `resume_receiving()`."""
        return self._paused_since is not None

    @property
    def draining_since(self) -> float | None:
        """When the pause began (Unix seconds), None when not draining."""
        return self._paused_since

    @property
    def ready(self) -> bool:
        """True unless draining: the app's own direct path serves the calls."""
        return not self.draining and not self._closing

    def drain_status(self) -> dict[str, Any]:
        """`draining`, `since`, and no held or running work."""
        return {"draining": self.draining, "since": self._paused_since, "held": 0, "running": 0}

    async def drain(self, drain_secs: float | None = None) -> bool:
        """Pause; nothing runs here, so True at once."""
        self.pause_receiving()
        return True

    async def health(self) -> Health:
        """`enabled: false`, live, ready unless draining; no stores."""
        out: dict[str, Any] = {
            "enabled": False,
            "live": True,
            "ready": self.ready,
            "mode": "disabled",
            "draining": self.draining,
            "draining_since": self._paused_since,
            "stores": {},
            "eviction_policy": "none",
            "key_expiry_mode": "none",
        }
        if self.draining:
            out["not_ready_reason"] = "draining"
        return cast("Health", out)

    def describe(self) -> dict[str, Any]:
        """`enabled: false`, the version, environment and whether a drain file is watched."""
        from llm_queue.engine.admin.process import describe_settings

        return describe_settings(self.settings, None)

    # ---------------------------------------------------------------- refused

    def _refuse(self, operation: str) -> NoReturn:
        raise QueueDisabled(operation)

    async def call(self, model: str, params: Mapping[str, Any], **kw: Any) -> Reply:
        """Refused: `QueueDisabled`."""
        self._refuse("call")

    async def converse(self, model: str, request: Mapping[str, Any], **kw: Any) -> Reply:
        """Refused: `QueueDisabled`."""
        self._refuse("converse")

    async def chat(self, model: str, request: Mapping[str, Any], **kw: Any) -> Reply:
        """Refused: `QueueDisabled`."""
        self._refuse("chat")

    def converse_stream(
        self, model: str, request: Mapping[str, Any], **kw: Any
    ) -> AsyncIterator[dict[str, Any]]:
        """Refused: `QueueDisabled` (on the first `async for` step)."""
        return _refused_events("converse_stream")

    def chat_stream(
        self, model: str, request: Mapping[str, Any], **kw: Any
    ) -> AsyncIterator[dict[str, Any]]:
        """Refused: `QueueDisabled` (on the first `async for` step)."""
        return _refused_events("chat_stream")

    async def embed(self, model: str, texts: Sequence[str], **kw: Any) -> Any:
        """Refused: `QueueDisabled`."""
        self._refuse("embed")

    async def embed_one(self, model: str, text: str, **kw: Any) -> list[float]:
        """Refused: `QueueDisabled`."""
        self._refuse("embed_one")

    def converse_sync(self, model: str, request: Mapping[str, Any], **kw: Any) -> Reply:
        """Refused: `QueueDisabled`."""
        self._refuse("converse_sync")

    def chat_sync(self, model: str, request: Mapping[str, Any], **kw: Any) -> Reply:
        """Refused: `QueueDisabled`."""
        self._refuse("chat_sync")

    def embed_sync(self, model: str, texts: Sequence[str], **kw: Any) -> Any:
        """Refused: `QueueDisabled`."""
        self._refuse("embed_sync")

    def embed_one_sync(self, model: str, text: str, **kw: Any) -> list[float]:
        """Refused: `QueueDisabled`."""
        self._refuse("embed_one_sync")

    def close_sync(self) -> None:
        """Nothing to close."""

    def register_job(self, name: str, fn: Any, **kw: Any) -> None:
        """Refused: `QueueDisabled` (jobs live in the store)."""
        self._refuse("register_job")

    async def send_job(self, name: str, payload: Mapping[str, Any], **kw: Any) -> str:
        """Refused: `QueueDisabled`."""
        self._refuse("send_job")

    async def status(self, model: str | None = None) -> Any:
        """Refused: `QueueDisabled`."""
        self._refuse("status")

    async def request_status(self, request_id: str) -> Any:
        """Refused: `QueueDisabled`."""
        self._refuse("request_status")

    @property
    def admin(self) -> AdminServices:
        """Admin services of a disabled queue: `describe`, `ops`, drain; the rest refuse."""
        if self._admin is None:
            from llm_queue.engine.admin import AdminServices

            self._admin = AdminServices.for_disabled(settings=self.settings, process=self)
        return self._admin

    def http_router(self, **_options: Any) -> Any:
        """Probes only (owner Q13): `GET /health/live` and `/health/ready` with `enabled: false`.

        Readiness is 503 while draining (the drain file still works). Router options (tokens,
        `dependencies`, `drain_routes`...) are accepted and ignored: no admin route is mounted.
        """
        try:
            from fastapi import APIRouter
            from fastapi.responses import JSONResponse
        except ImportError as exc:  # pragma: no cover - exercised only without the extra
            raise MissingExtra("http_router", "http") from exc

        router = APIRouter()

        async def live() -> Any:
            return JSONResponse({"live": True, "enabled": False})

        async def ready() -> Any:
            health = dict(await self.health())
            return JSONResponse(health, status_code=200 if health["ready"] else 503)

        router.add_api_route("/health/live", live, methods=["GET"], name="health.live")
        router.add_api_route("/health/ready", ready, methods=["GET"], name="health.ready")
        return router


async def _refused_events(operation: str) -> AsyncIterator[dict[str, Any]]:
    """A stream that raises `QueueDisabled` when iterated (as a queued stream starts then)."""
    if operation:
        raise QueueDisabled(operation)
    yield {}  # pragma: no cover - makes this an async generator
