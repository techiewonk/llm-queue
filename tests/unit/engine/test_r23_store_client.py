"""The store client: an ACL user with the rotating password, and a saturated pool (R3SE-2, R3PF-6).

No store is needed: connections are built but never opened, and the pool's availability is
faked.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from llm_queue.engine import store_client as store_module
from llm_queue.engine.store_client import secret_store_client
from llm_queue.tuning import StoreOptions


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("rediss://app@cache.example:6380/0", ("app", "fresh")),
        ("rediss://app:old@cache.example:6380/0", ("app", "fresh")),
        ("rediss://:old@cache.example:6380/0", ("fresh",)),
        ("rediss://cache.example:6380/0", ("fresh",)),
    ],
)
async def test_an_acl_user_works_with_the_rotating_password(
    url: str, expected: tuple[str, ...]
) -> None:
    """R3SE-2: a URL user or password with `LLMQ_STORE_PASSWORD` failed every connection.

    redis-py raised `DataError` ("username and password cannot be passed along with
    credential_provider"), so an ACL user limited to the app's prefix could not be combined
    with the rotating secret. The URL's user is kept and the fresh password is used.
    """
    secrets = {"LLMQ_STORE_PASSWORD": "fresh"}.get
    client = secret_store_client(url, secrets, "LLMQ_STORE_PASSWORD", 4, StoreOptions())
    try:
        pool: Any = client.connection_pool
        connection: Any = pool.make_connection()
        provider = connection.credential_provider
        assert provider.get_credentials() == expected
        assert connection.username is None
        assert connection.password is None
    finally:
        await client.aclose()


def _busy_pool(free_after_secs: float | None) -> Any:
    options = StoreOptions(pool_wait_secs=0.05, read_timeout_secs=0.4)
    client = store_module.store_client("redis://127.0.0.1:1/0", max_connections=1, options=options)
    pool: Any = client.connection_pool
    started = time.monotonic()

    def can_get() -> bool:
        return free_after_secs is not None and time.monotonic() - started >= free_after_secs

    async def connected(_: Any) -> None:
        return None

    pool.can_get_connection = can_get
    pool.ensure_connection = connected
    return client, pool


async def test_a_busy_pool_waits_for_a_connection() -> None:
    """R3PF-6: a pool wait that ran out raised `ConnectionError`, read as the store being down.

    A connection freed within one read timeout is now taken: the caller never sees an error.
    """
    client, pool = _busy_pool(free_after_secs=0.2)
    try:
        connection = await pool.get_connection()
        await pool.release(connection)
    finally:
        await client.aclose()


async def test_a_pool_busy_for_too_long_names_the_cause() -> None:
    """R3PF-6: after the extra wait the error is `StorePoolBusy`, naming the pool size."""
    client, pool = _busy_pool(free_after_secs=None)
    try:
        with pytest.raises(store_module.StorePoolBusy, match="all 1 store connections"):
            await pool.get_connection()
    finally:
        await client.aclose()
