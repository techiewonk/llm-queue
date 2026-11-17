"""Large results in object storage (plan E137): the state store keeps only a reference.

A result above `offload_bytes` is written to an S3 bucket (encrypted, with a one-day
lifecycle rule set on the bucket by the owner) and the stored reply is a small marker. Readers
fetch it transparently and the object is deleted once read. Any large result is offloaded
(chat answers too), most often bulk embedding batches. See docs/how-to/large-results.md.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from typing import TYPE_CHECKING, Any, Protocol

from llm_queue._logging import get_logger
from llm_queue.errors import ConfigError, LLMQueueError
from llm_queue.ids import new_id

if TYPE_CHECKING:
    from aiobotocore.session import AioSession

logger = get_logger(__name__)
MARKER = b"llmq-offload:"
# botocore's defaults are 60 s connect and 60 s read, with retries on top: the upload runs on
# the worker's success path, so a slow S3 held a finished call for minutes (R3JE-30).
CLIENT_CONFIG: dict[str, Any] = {
    "connect_timeout": 3,
    "read_timeout": 10,
    "retries": {"max_attempts": 2, "mode": "standard"},
}


def _prefix(prefix: str, key_prefix: str) -> str:
    """`prefix` and `key_prefix` as one key prefix ending in exactly one `/`.

    `prefix="llmq"` gave keys `llmq<hash>` and let a reference into `llmq-other/` pass the
    check; `key_prefix="/"` gave `llmq-results//` (R3JE-31).
    """
    parts = [part for value in (prefix, key_prefix) for part in value.split("/") if part]
    if not parts or any(part in (".", "..") for part in parts):
        raise ConfigError(f"S3Offload prefix {prefix!r} / key_prefix {key_prefix!r} is not valid")
    return "/".join(parts) + "/"


def _is_gone(exc: Exception) -> bool:
    """True only for S3's own "no such object" error code.

    It matched "404" anywhere in the text (a request id, a host), so a real outage could be
    reported as an expired result instead of a retryable error (EN-11).
    """
    response = getattr(exc, "response", None)  # botocore ClientError
    if not isinstance(response, dict):
        return False
    code = (response.get("Error") or {}).get("Code")
    return code in {"NoSuchKey", "404"}


class ResultOffload(Protocol):
    """Somewhere to keep a large result body."""

    async def put(self, key: str, data: bytes) -> str:
        """Store `data`; return a reference."""
        ...

    async def get(self, ref: str) -> bytes | None:
        """The data of a reference (None when it expired)."""
        ...


class S3Offload:
    """`ResultOffload` on an S3 bucket (server-side encryption; expiry by the bucket's rule).

    Object names are `{prefix}/{key_prefix}/{random id}` (no caller text or request id,
    R3JE-33); `sse`, `kms_key_id`, `bucket_key` and `expected_bucket_owner` as in S3.
    """

    # A reference outside that prefix is refused before any read. `sse=None` leaves the
    # header out (the bucket's default encryption applies). The client gets short timeouts
    # (`CLIENT_CONFIG`) unless `client_kwargs` has a `config`; one client per event loop.

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "llmq-results/",
        key_prefix: str = "",
        session: AioSession | None = None,
        client_kwargs: dict[str, Any] | None = None,
        sse: str | None = "AES256",
        kms_key_id: str | None = None,
        bucket_key: bool = False,
        expected_bucket_owner: str | None = None,
        delete_after_read: bool = True,
    ) -> None:
        """Keep settings; a client is created on first use on each loop."""
        if sse is not None and sse not in ("AES256", "aws:kms", "aws:kms:dsse"):
            raise ConfigError(f"S3Offload sse {sse!r}: use AES256, aws:kms, aws:kms:dsse or None")
        if kms_key_id and sse not in ("aws:kms", "aws:kms:dsse"):
            raise ConfigError('S3Offload kms_key_id needs sse="aws:kms"')
        if sse in ("aws:kms", "aws:kms:dsse") and not kms_key_id:
            logger.warning(
                "offload_kms_default_key",
                hint="sse=aws:kms without kms_key_id uses the account's AWS-managed key",
            )
        self.bucket = bucket
        self.prefix = _prefix(prefix, key_prefix)
        self._session = session
        self._kwargs = dict(client_kwargs or {})
        self._sse = sse
        self._kms_key_id = kms_key_id
        self._bucket_key = bucket_key
        self._owner = (
            {"ExpectedBucketOwner": expected_bucket_owner} if expected_bucket_owner else {}
        )
        self.delete_after_read = delete_after_read
        self._clients: dict[asyncio.AbstractEventLoop, tuple[Any, Any]] = {}
        self._locks: dict[asyncio.AbstractEventLoop, asyncio.Lock] = {}
        self._guard = threading.Lock()

    async def _s3(self) -> Any:
        loop = asyncio.get_running_loop()
        if (held := self._clients.get(loop)) is not None:
            return held[1]
        with self._guard:
            lock = self._locks.setdefault(loop, asyncio.Lock())
        async with lock:  # concurrent first calls built (and leaked) one client each
            if (held := self._clients.get(loop)) is not None:
                return held[1]
            from aiobotocore.session import get_session

            session = self._session or get_session()
            kwargs = dict(self._kwargs)
            if "config" not in kwargs:
                from botocore.config import Config

                kwargs["config"] = Config(**CLIENT_CONFIG)
            context = session.create_client("s3", **kwargs)
            client = await context.__aenter__()
            with self._guard:
                self._clients[loop] = (context, client)
            return client

    def _check_ref(self, ref: str) -> None:
        if not ref.startswith(self.prefix) or ".." in ref:
            raise LLMQueueError("offload reference outside this queue's prefix")

    async def put(self, key: str, data: bytes) -> str:
        """Write the object under a new random name; returns that name (`key` is not used)."""
        s3 = await self._s3()
        name = f"{self.prefix}{new_id()}"
        extra: dict[str, Any] = dict(self._owner)
        if self._sse is not None:
            extra["ServerSideEncryption"] = self._sse
        if self._kms_key_id:
            extra["SSEKMSKeyId"] = self._kms_key_id
        if self._bucket_key:
            extra["BucketKeyEnabled"] = True
        await s3.put_object(Bucket=self.bucket, Key=name, Body=data, **extra)
        return name

    async def delete(self, ref: str) -> None:
        """Delete one object of this prefix (one no stored marker points to)."""
        self._check_ref(ref)
        s3 = await self._s3()
        await s3.delete_object(Bucket=self.bucket, Key=ref, **self._owner)

    async def check_lifecycle(self) -> bool:
        """True when an enabled bucket rule expires objects under this prefix; else a warning.

        Objects nobody reads (a caller that timed out, a delete that failed) are removed only
        by that rule (Q23). Needs `s3:GetLifecycleConfiguration`; access denied skips quietly.
        """
        s3 = await self._s3()
        try:
            config = await s3.get_bucket_lifecycle_configuration(Bucket=self.bucket, **self._owner)
        except Exception as exc:
            code = ((getattr(exc, "response", None) or {}).get("Error") or {}).get("Code")
            if code == "NoSuchLifecycleConfiguration":
                config = {"Rules": []}
            else:
                logger.info("offload_lifecycle_not_checked", code=code or type(exc).__name__)
                return False
        for rule in config.get("Rules", []):
            if rule.get("Status") != "Enabled" or "Expiration" not in rule:
                continue
            filt = rule.get("Filter") or {}
            scope = filt.get("Prefix", (filt.get("And") or {}).get("Prefix", rule.get("Prefix")))
            if scope is None or self.prefix.startswith(scope):
                return True
        logger.warning(
            "offload_no_lifecycle_rule",
            bucket=self.bucket,
            prefix=self.prefix,
            hint="add an expiry rule for this prefix: unread objects stay until it removes them",
        )
        return False

    async def get(self, ref: str) -> bytes | None:
        """Read the object (None when it is gone), then delete it; refuses a foreign reference."""
        self._check_ref(ref)
        s3 = await self._s3()
        try:
            reply = await s3.get_object(Bucket=self.bucket, Key=ref, **self._owner)
        except Exception as exc:
            if _is_gone(exc):
                return None
            raise
        async with reply["Body"] as body:
            data: bytes = await body.read()
        if self.delete_after_read:  # after a successful read only: a failed read can be tried again
            try:
                await s3.delete_object(Bucket=self.bucket, Key=ref, **self._owner)
            except Exception as exc:  # the bucket's lifecycle rule removes it later anyway
                logger.warning("offload_delete_failed", error=type(exc).__name__)
        return data

    async def aclose(self) -> None:
        """Close this loop's client (each loop closes its own); safe to repeat."""
        loop = asyncio.get_running_loop()
        with self._guard:
            held = self._clients.pop(loop, None)
            self._locks.pop(loop, None)
        if held is not None:
            with contextlib.suppress(Exception):
                await held[0].__aexit__(None, None, None)
