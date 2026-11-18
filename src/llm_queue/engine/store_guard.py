"""`store_op` (admin errors as `StoreUnavailable`) and `ReplicaWait` (the replica wait).

Callers catch one family (`LLMQueueError`). Calls already turn store trouble into `CallFailed`;
admin operations (cancel, status, registry edits) let redis-py's `ConnectionError` reach the
app when the store was down (scenario X2: a live edit during a store block).
"""

from __future__ import annotations

import functools
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import OutOfMemoryError, ReadOnlyError
from redis.exceptions import TimeoutError as StoreTimeoutError

from llm_queue._logging import get_logger
from llm_queue.errors import StoreUnavailable

logger = get_logger(__name__)

# Store trouble an admin path reports as "unavailable" (http.py and the CLI catch it too).
STORE_DOWN = (StoreConnectionError, StoreTimeoutError, ReadOnlyError, OutOfMemoryError, OSError)


def store_op[**P, R](fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """Wrap an async admin method: store trouble becomes `StoreUnavailable`."""

    @functools.wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await fn(*args, **kwargs)
        except OutOfMemoryError as exc:
            raise StoreUnavailable("the state store is out of memory") from exc
        except STORE_DOWN as exc:
            if isinstance(exc, StoreUnavailable):
                raise
            raise StoreUnavailable(f"the state store is unreachable: {exc}") from exc

    return wrapper


# What `LLMQ_REPLICA_WAIT_COUNT` covers (owner Q8 / Q51): call sends, job sends and registry
# live edits, each switchable, all on by default when replicas are asked for.
WAIT_SCOPES = ("calls", "jobs", "registry")
_SHORT_WARN_EVERY_SECS = 60.0


@dataclass
class ReplicaWait:
    """The replica wait of one process: how many replicas a write of a scope waits for.

    Shared by the call path, the job runner and the registry store. Too few replicas never
    fail a write (owner Q9): it was sent anyway, so it is warned about at most once a minute
    per scope and counted (`llmq_send_not_replicated_total`).
    """

    replicas: int = 0
    wait_ms: int = 100
    scopes: frozenset[str] = frozenset(WAIT_SCOPES)
    connected: int | None = None  # replicas the store reports (None: unknown); 0 skips the WAIT
    _warned: dict[str, float] = field(default_factory=dict, repr=False)

    def replicas_for(self, scope: str) -> int:
        """Replicas a write of `scope` waits for: 0 when off, or when the store has none.

        A WAIT with no replica connected cannot succeed: it only cost `wait_ms` per write
        (R3FM-11). Unknown (not read yet) still waits.
        """
        if scope not in WAIT_SCOPES:
            raise ValueError(f"unknown replica wait scope {scope!r}")
        if not self.replicas or scope not in self.scopes or self.connected == 0:
            return 0
        return self.replicas

    def short(self, scope: str, metrics: Any, detail: str = "") -> None:
        """A write of `scope` reached fewer replicas than asked: count it, warn once a minute."""
        metrics.count("llmq_send_not_replicated_total", scope=scope)
        now = time.monotonic()
        if now - self._warned.get(scope, -1e9) >= _SHORT_WARN_EVERY_SECS:
            self._warned[scope] = now
            logger.warning(
                "replicas_short",
                scope=scope,
                asked=self.replicas,
                connected=self.connected,
                detail=detail,
            )


def parse_scopes(raw: str) -> tuple[frozenset[str], list[str]]:
    """`LLMQ_REPLICA_WAIT_SCOPES` (comma-separated; `all` or `none`): the scopes and problems."""
    value = raw.strip().lower()
    if value in ("", "all"):
        return frozenset(WAIT_SCOPES), []
    if value == "none":
        return frozenset(), []
    names = {part.strip() for part in value.split(",") if part.strip()}
    problems = [
        f"LLMQ_REPLICA_WAIT_SCOPES: unknown scope {name!r}"
        for name in sorted(names - set(WAIT_SCOPES))
    ]
    return frozenset(names & set(WAIT_SCOPES)), problems
