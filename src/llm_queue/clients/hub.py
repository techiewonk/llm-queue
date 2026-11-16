"""Process-level client hub (plan E1-E4): one SDK client per (loop, credentials, region, options).

Every `ClientPool` (each `LLMQueue`, and the engine of sync calls) borrows its clients here, so
two queues in one process share one client and one connection pool per account and region
instead of building their own. Entries are reference-counted: the last `release` closes the
client. Keys carry the event loop, because an aiohttp / httpx connection pool belongs to the
loop it was opened on. Never a client per model: same endpoint and credentials would only split
the connection pool and add TLS handshakes (isolation comes from admission slots).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import threading
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any

from llm_queue._logging import get_logger

logger = get_logger(__name__)

# Pool size per client from the concurrency slots routed to its account and region (E5): a
# floor so a small model still has spare connections. No cap: the slots already bound this
# process's calls on the client, and a cap below them (200) made callers queue for a socket
# inside the first-response timer, so a local wait charged the breaker as a provider timeout
# (CL-14).
POOL_FLOOR = 10


def pool_size_for(slots: int) -> int:
    """Connections for one client: the slots routed to it, at least 10."""
    return max(POOL_FLOOR, int(slots))


def identity_hash(*parts: Any) -> str:
    """A short hash of credential identity (account fields, key ids): a key, never the secret."""
    return hashlib.sha256(repr(parts).encode()).hexdigest()[:24]


@dataclass(eq=False)
class Lease:
    """One hub entry: the client, how to close it, how many pools hold it, calls using it.

    `inflight` counts calls running on the client (`ClientPool.borrow`), so a retired client
    is closed when its calls end, not after a fixed grace that cut long answers (CL-3).
    """

    key: tuple[Any, ...]
    client: Any
    loop: asyncio.AbstractEventLoop
    close: Callable[[], Awaitable[Any]]
    refs: int = 0
    tasks: list[asyncio.Task[Any]] = field(default_factory=list)
    inflight: int = 0
    _idle: asyncio.Event | None = None
    # Connections of the client's pool, and a cap on the calls borrowing it at once: a call
    # beyond the pool waits here, before its provider timers, not for a socket inside them
    # (R3PF-9: caller-only engines have no slots to bound their direct runs).
    size: int = 0
    limit: asyncio.Semaphore | None = None

    def cap(self, size: int) -> None:
        """Set the pool size and its borrow cap once (the first pool to build it decides)."""
        if self.limit is None:
            self.size = size
            self.limit = asyncio.Semaphore(size)

    def enter(self) -> None:
        """One more call on the client."""
        self.inflight += 1
        if self._idle is not None:
            self._idle.clear()

    def leave(self) -> None:
        """One call fewer; wakes `wait_idle` at zero."""
        self.inflight -= 1
        if self.inflight <= 0 and self._idle is not None:
            self._idle.set()

    async def wait_idle(self, limit_secs: float) -> bool:
        """Wait until no call uses the client, at most `limit_secs`; True when idle."""
        if self.inflight <= 0:
            return True
        if self._idle is None:
            self._idle = asyncio.Event()
        try:
            async with asyncio.timeout(limit_secs):
                await self._idle.wait()
        except TimeoutError:
            return False
        return True


class ClientHub:
    """Reference-counted clients shared by every pool in the process.

    `acquire` builds a missing client once under a lock of its own key (builds for different
    keys run concurrently); `release` closes it when no pool holds it any more; `evict` makes the
    next `acquire` build a fresh one (credentials failed) while current holders keep theirs.
    """

    def __init__(self) -> None:
        """Empty; the dicts are guarded by a thread lock (sync calls run on another loop)."""
        self._entries: dict[tuple[Any, ...], Lease] = {}
        self._locks: dict[tuple[Any, ...], asyncio.Lock] = {}
        self._guard = threading.Lock()

    def __len__(self) -> int:
        return len(self._entries)

    def _live(self, key: tuple[Any, ...]) -> Lease | None:
        lease = self._entries.get(key)
        if lease is not None and lease.loop.is_closed():  # its loop is gone: never reusable
            with self._guard:
                if self._entries.get(key) is lease:
                    del self._entries[key]
                # Its lock too: keys use id(loop), and a new loop with the same id met a lock
                # bound to the dead one ("bound to a different event loop", FC-14).
                self._locks.pop(key, None)
            return None
        return lease

    async def acquire(
        self,
        key: tuple[Any, ...],
        build: Callable[[], Awaitable[tuple[Any, Callable[[], Awaitable[Any]]]]],
    ) -> Lease:
        """The entry of `key` with one more holder; `build()` -> (client, close) when missing."""
        _register_fork_hook()
        loop = asyncio.get_running_loop()
        full = (id(loop), *key)
        if (lease := self._live(full)) is None:
            with self._guard:
                lock = self._locks.setdefault(full, asyncio.Lock())
            async with lock:
                if (lease := self._live(full)) is None:
                    client, close = await build()
                    lease = Lease(full, client, loop, close)
                    with self._guard:
                        self._entries[full] = lease
        with self._guard:
            lease.refs += 1
        return lease

    def acquire_now(
        self, key: tuple[Any, ...], build: Callable[[], tuple[Any, Callable[[], Awaitable[Any]]]]
    ) -> Lease:
        """`acquire` for clients built without awaiting (OpenAI); must run on an event loop."""
        _register_fork_hook()
        loop = asyncio.get_running_loop()
        full = (id(loop), *key)
        with self._guard:
            lease = self._entries.get(full)
            if lease is None or lease.loop.is_closed():
                client, close = build()
                lease = self._entries[full] = Lease(full, client, loop, close)
            lease.refs += 1
        return lease

    def evict(self, lease: Lease) -> None:
        """Stop handing out `lease`; it is closed when its last holder releases it."""
        with self._guard:
            if self._entries.get(lease.key) is lease:
                del self._entries[lease.key]
                self._locks.pop(lease.key, None)

    def spawn(self, lease: Lease, coro: Coroutine[Any, Any, Any]) -> None:
        """Run a background task tied to the entry (warm-up, refresher); cancelled at close."""
        task = lease.loop.create_task(coro)
        lease.tasks.append(task)
        task.add_done_callback(lambda done: _forget_task(lease, done))

    async def release(self, lease: Lease) -> None:
        """One holder fewer; the last one closes the client (on the loop it belongs to)."""
        with self._guard:
            lease.refs -= 1
            last = lease.refs <= 0
            if last and self._entries.get(lease.key) is lease:
                del self._entries[lease.key]
                self._locks.pop(lease.key, None)
        if not last:
            return
        tasks = list(lease.tasks)
        for task in tasks:
            task.cancel()
        try:
            # gather hands back the tasks' own CancelledError as values but re-raises a
            # cancellation of this task: suppressing it lost the caller's cancel (CL-11).
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            try:
                await lease.close()
            except Exception as exc:  # logged, not raised: the caller is closing too (FC-15)
                logger.debug("client_close_failed", error=repr(exc))

    def reset_after_fork(self) -> None:
        """Forget every entry in a forked child: they belong to the parent's loops and sockets."""
        self._entries = {}
        self._locks = {}
        self._guard = threading.Lock()


def _forget_task(lease: Lease, task: asyncio.Task[Any]) -> None:
    with contextlib.suppress(ValueError):
        lease.tasks.remove(task)


_HUB = ClientHub()
_fork_hook = False  # registered on the first acquire, not at import (CL-18)


def _register_fork_hook() -> None:
    """Reset the process hub in a forked child; registered once, when a client is first made."""
    global _fork_hook
    if not _fork_hook:
        _fork_hook = True
        if hasattr(os, "register_at_fork"):  # not on Windows
            os.register_at_fork(after_in_child=_HUB.reset_after_fork)


def client_hub() -> ClientHub:
    """The process's hub (a fresh, empty one in a forked child)."""
    return _HUB


def run[T](main: Coroutine[Any, Any, T], *, use_uvloop: bool | None = None) -> T:
    """Run `main` like `asyncio.run`, on uvloop when it is installed (extra `uvloop`, not Windows).

    `use_uvloop=None` uses it when importable; `True` raises `MissingExtra` when it is not;
    `False` never does. uvloop speeds up the event loop itself (sockets, timers), not the SDKs.
    """
    if use_uvloop is not False:
        try:
            import uvloop
        except ImportError as exc:
            if use_uvloop:
                from llm_queue.errors import MissingExtra

                raise MissingExtra("run(use_uvloop=True)", "uvloop") from exc
        else:
            result: T = uvloop.run(main)
            return result
    return asyncio.run(main)
