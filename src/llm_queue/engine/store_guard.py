"""`store_op`: admin operations raise `StoreUnavailable`, never redis-py's own errors.

Callers catch one family (`LLMQueueError`). Calls already turn store trouble into `CallFailed`;
admin operations (cancel, status, registry edits) let redis-py's `ConnectionError` reach the
app when the store was down (scenario X2: a live edit during a store block).
"""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeVar

from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import OutOfMemoryError, ReadOnlyError
from redis.exceptions import TimeoutError as StoreTimeoutError

from llm_queue.errors import StoreUnavailable

P = ParamSpec("P")
R = TypeVar("R")

# Store trouble an admin path reports as "unavailable" (http.py and the CLI catch it too).
STORE_DOWN = (StoreConnectionError, StoreTimeoutError, ReadOnlyError, OutOfMemoryError, OSError)


def store_op(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
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
