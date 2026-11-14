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
from collections.abc import Callable
from typing import Any

import redis.asyncio
from redis.asyncio.retry import Retry
from redis.backoff import EqualJitterBackoff  # in every redis-py the bounds allow (>=5)
from redis.credentials import CredentialProvider
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ReadOnlyError

from llm_queue.tuning import StoreOptions

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
        # EXTEND, FAIL, which ends its receipt, the admission scripts, REGISTER_SEND). Two are
        # not: a replayed RECEIVE takes a second batch (the first stays hidden for the
        # visibility time and is charged one failure when it lapses, within max_receives), and
        # a replayed plain SEND appends twice (jobs run at least once; a call's duplicate finds
        # its request id claimed). Both are rare and bounded; removing them needs a retry-free
        # connection or a per-call token write on every RECEIVE (not done: unmeasured).
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
    pool = redis.asyncio.BlockingConnectionPool.from_url(
        url, max_connections=max_connections, timeout=opts.pool_wait_secs, **kwargs
    )
    return redis.asyncio.Redis.from_pool(pool)  # owns the pool: aclose() closes it


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

    Used by `LLMQueue.from_env` and the CLI.
    """
    provider = FreshPassword(secrets, name) if secrets is not None and secrets(name) else None
    return store_client(url, provider, max_connections=max_connections, options=options)
