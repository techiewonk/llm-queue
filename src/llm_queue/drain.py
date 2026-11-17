"""A drain file watcher: a file that appears pauses every process of a pod (plan 0.2.0 A4).

A route reaches one process; a pod with several (gunicorn / uvicorn workers) needs a signal
they all see. A preStop hook `touch`es a file on a volume every process shares, and each
process's `DrainWatcher` calls its `on_drain` (`LLMQueue.pause_receiving`, or the app's own
readiness flag) within `poll_secs`.

Standalone on purpose: it imports nothing heavy (no store client, no engine), so an app can use
it to fail readiness even with its queue switched off. `LLMQueue.start()` runs one when
`LLMQ_DRAIN_FILE` is set.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import math
import os
import time
from collections.abc import Callable
from pathlib import Path

from llm_queue._logging import get_logger
from llm_queue.errors import ConfigError

logger = get_logger(__name__)

# A file touched up to this long before the watcher was made still counts: a preStop hook can
# race a process that is just starting (and file systems keep coarse mtimes).
SINCE_MARGIN_SECS = 1.0
MIN_POLL_SECS, MAX_POLL_SECS = 0.1, 60.0


class DrainWatcher:
    """Call `on_drain` once when `path` appears (or is touched) after the watcher was made.

    A file older than `since` (default: now) minus 1 s is a leftover and ignored. After it
    fired, only a newer mtime fires again, so a resumed process is not paused by the same file.
    `on_drain` may be sync or async; an error in it is logged and the watcher goes on.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        on_drain: Callable[[], object],
        *,
        poll_secs: float = 1.0,
        since: float | None = None,
    ) -> None:
        """Keep the file, the callback and the poll interval (0.1 to 60 s)."""
        if not (math.isfinite(poll_secs) and MIN_POLL_SECS <= poll_secs <= MAX_POLL_SECS):
            raise ConfigError(
                f"DrainWatcher poll_secs must be {MIN_POLL_SECS:g} to {MAX_POLL_SECS:g} s"
            )
        self.path = Path(path)
        self.on_drain = on_drain
        self.poll_secs = poll_secs
        self.since = time.time() if since is None else since
        self.fired_mtime: float | None = None  # the mtime that fired last
        self._task: asyncio.Task[None] | None = None
        self._callbacks: set[asyncio.Future[object]] = set()  # async `on_drain` runs

    def _callback_done(self, task: asyncio.Future[object]) -> None:
        self._callbacks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.warning(
                "drain_callback_failed", path=str(self.path), error=repr(task.exception())
            )

    @property
    def running(self) -> bool:
        """True while the poll task runs."""
        return self._task is not None and not self._task.done()

    def check(self) -> bool:
        """One poll: True when the file is new (or newer than the last one) and `on_drain` ran.

        A coroutine `on_drain` returns is scheduled on the running loop (`start()` runs one).
        """
        try:
            mtime = self.path.stat().st_mtime
        except OSError:  # no file (the usual case), or not readable: nothing to do
            return False
        if mtime < self.since - SINCE_MARGIN_SECS:
            return False
        if self.fired_mtime is not None and mtime <= self.fired_mtime:
            return False
        self.fired_mtime = mtime
        logger.info("drain_file_seen", path=str(self.path))
        try:
            result = self.on_drain()
            if inspect.isawaitable(result):
                task = asyncio.ensure_future(result)
                self._callbacks.add(task)  # kept until done: the loop holds only a weak one
                task.add_done_callback(self._callback_done)
        except Exception as exc:
            logger.warning("drain_callback_failed", path=str(self.path), error=repr(exc))
        return True

    def start(self) -> None:
        """Poll on the running event loop until `stop()`; a second call does nothing."""
        if self.running:
            return
        self._task = asyncio.create_task(self._poll(), name="llmq:drain_watcher")

    async def stop(self) -> None:
        """Stop polling (idempotent)."""
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _poll(self) -> None:
        while True:
            # On the loop: one stat of a local file (an emptyDir) is microseconds, and the
            # callback then runs where an app's readiness flag or queue lives.
            self.check()
            await asyncio.sleep(self.poll_secs)
