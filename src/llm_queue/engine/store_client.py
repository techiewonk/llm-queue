"""The store client (Valkey / Redis) every process builds: one place for its failover options.

`READONLY`, like a dropped connection, closes the connection and retries on a new one.
Used by `LLMQueue.from_env` and the `llm-queue` CLI.
"""

# A managed store (ElastiCache) fails over by promoting the replica and moving the endpoint's
# DNS name. Pooled connections still reach the old primary, now a replica, and every write gets
# `READONLY`; redis-py keeps such a connection, so without these options the queue stayed down
# after a failover until the process restarted (measured, CHANGELOG 0.3.0). A new connection
# resolves the name again.

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import unquote, urlsplit, urlunsplit

import redis.asyncio
from redis.asyncio.retry import Retry
from redis.backoff import EqualJitterBackoff  # in every redis-py the bounds allow (>=5)
from redis.credentials import CredentialProvider
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ReadOnlyError

from llm_queue._logging import get_logger
from llm_queue.tuning import StoreOptions

logger = get_logger(__name__)

Secrets = Callable[[str], "str | None"]  # a secret's value by name

MAX_CONNECTIONS = 200  # LLMQ_STORE_MAX_CONNECTIONS; about worker slots + callers + 3 readers


def store_client(
    url: str,
    credential_provider: CredentialProvider | None = None,
    *,
    max_connections: int = MAX_CONNECTIONS,
    options: StoreOptions | None = None,
) -> redis.asyncio.Redis:
    """An async client (bytes replies) for `url` that reconnects on drops and on `READONLY`.

    The pool blocks when every connection is in use: redis-py's default pool raised at once,
    and 150 concurrent calls on 100 connections were reported as the store being down
    (measured in the 0.3.0 review). `valkey://` / `valkeys://` are read as `redis(s)://`.
    """
    opts = options or StoreOptions()  # timeouts, retries: `Tuning.store`
    if url.startswith("valkey"):
        url = "redis" + url[len("valkey") :]
    kwargs: dict[str, Any] = {
        # Not on a read timeout: the command may have run (a Lua write would run twice), and
        # each retry waited the whole timeout again: 4 x 15 s, a 61 s call in the slow-DNS
        # failover drill. redis-py's Retry retries TimeoutError by default and
        # `retry_on_error` only adds to that list, so the list is given to Retry itself
        # (measured: a half-open store hung a command 60 s with the default, 15 s without).
        # A connection reset mid-reply is retried too, and the script may already have run
        # (EN-5). Scripts with a receipt or owner check are safe to replay (ACK, RETURN,
        # EXTEND, FAIL, which ends its receipt, the admission scripts, REGISTER_SEND). The
        # worker's RECEIVE_FIRST is too: it records its batch under a nonce in a per-loop claim,
        # a replay hands back the same batch, and a receive whose reply never came is reclaimed
        # (S11). Still not: a plain RECEIVE (jobs, tools) takes a second batch on replay, and a
        # plain SEND appends twice (jobs run at least once; a call's duplicate finds its request
        # id claimed); both rare and bounded.
        "retry": Retry(
            EqualJitterBackoff(cap=1.0, base=0.05),
            opts.retries,
            supported_errors=(StoreConnectionError, ReadOnlyError),
        ),
        "retry_on_error": [StoreConnectionError, ReadOnlyError],
        "socket_connect_timeout": opts.connect_timeout_secs,
        "socket_timeout": opts.read_timeout_secs,
        "socket_keepalive": True,
        "health_check_interval": opts.health_check_secs,
    }
    if credential_provider is not None:
        kwargs["credential_provider"] = credential_provider
    pool = BusyPool.from_url(
        url, max_connections=max_connections, timeout=opts.pool_wait_secs, **kwargs
    )
    # A connection in use frees within one read timeout (its command answers or times out).
    pool.busy_secs = opts.read_timeout_secs
    return redis.asyncio.Redis.from_pool(pool)  # owns the pool: aclose() closes it


class StorePoolBusy(StoreConnectionError):
    """Every pooled connection stayed in use past the wait: the pool is too small for the load.

    Still a `ConnectionError` for the callers that handle one, but named so a saturated pool
    (a slow store, too few `LLMQ_STORE_MAX_CONNECTIONS`) is not read as a store that is down
    (R3PF-6). The message names the pool size.
    """


class BusyPool(redis.asyncio.BlockingConnectionPool):
    """A blocking pool that waits again, up to `busy_secs` more, while every connection is busy.

    redis-py raised `ConnectionError("No connection available.")` after `timeout`: under a
    300 ms store latency 3,612 of 10,000 concurrent commands failed that way and took the
    `store_down` path (R3PF-6). In-use connections free within one read timeout, so the wait is
    bounded; after it `StorePoolBusy` says what happened.
    """

    busy_secs: float = 0.0

    async def get_connection(self, *args: Any, **kwargs: Any) -> Any:  # noqa: D102  # as redis-py
        deadline = time.monotonic() + self.busy_secs
        while True:
            try:
                return await super().get_connection(*args, **kwargs)  # type: ignore[no-untyped-call]  # redis-py has no annotation here
            except StoreConnectionError as exc:
                if not isinstance(exc.__cause__, TimeoutError):
                    raise  # a real connection failure
                if time.monotonic() < deadline:
                    _log_busy(self.max_connections)
                    continue
                raise StorePoolBusy(
                    f"all {self.max_connections} store connections stayed in use; raise "
                    "LLMQ_STORE_MAX_CONNECTIONS or lower worker_slots"
                ) from exc


_busy_logged = 0.0


def _log_busy(size: int) -> None:
    """Warn at most once a minute per process that the store pool is saturated."""
    global _busy_logged
    now = time.monotonic()
    if now - _busy_logged >= 60:
        _busy_logged = now
        logger.warning("store_pool_busy", max_connections=size)


class FreshPassword(CredentialProvider):
    """Reads the store password from the secret on every new connection (rotation, R42)."""

    def __init__(self, secrets: Secrets, name: str, username: str | None = None) -> None:
        """Read secret `name` through `secrets` (with `username`, for ACL users)."""
        self._secrets = secrets
        self._name = name
        self._username = username

    def get_credentials(self) -> tuple[str] | tuple[str, str]:
        """The password as the secret holds it now (redis-py asks per new connection)."""
        password = self._secrets(self._name) or ""
        return (self._username, password) if self._username else (password,)

    async def get_credentials_async(self) -> tuple[str] | tuple[str, str]:
        """`get_credentials` in a worker thread: the asyncio client asks this one.

        A network-backed `secrets` lookup ran on the event loop on every reconnect, stalling
        every call during a failover (EN-31). `secrets` should still cache its values.
        """
        return await asyncio.to_thread(self.get_credentials)


def secret_store_client(
    url: str, secrets: Secrets | None, name: str, max_connections: int, options: StoreOptions
) -> redis.asyncio.Redis:
    """`store_client` for a URL whose password secret `name`, when set, wins (read fresh).

    Used by `LLMQueue.from_env` and the CLI. The URL's user (an ACL user limited to the app's
    key prefix) is kept and its password dropped: redis-py refuses a URL user or password
    together with a credential provider, so every connection failed (R3SE-2).
    """
    if secrets is None or not secrets(name):
        return store_client(url, None, max_connections=max_connections, options=options)
    bare, username = split_user(url)
    provider = FreshPassword(secrets, name, username)
    return store_client(bare, provider, max_connections=max_connections, options=options)


def split_user(url: str) -> tuple[str, str | None]:
    """`url` without its user and password, and the user name (None when it has none)."""
    parts = urlsplit(url)
    if "@" not in parts.netloc:
        return url, None
    userinfo, host = parts.netloc.rsplit("@", 1)
    user = unquote(userinfo.split(":", 1)[0]) or None
    return urlunsplit(parts._replace(netloc=host)), user
