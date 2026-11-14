"""Large results in object storage (plan E137): the state store keeps only a reference.

A result above `result_offload_bytes` is written to an S3 bucket (encrypted, with a one-day
lifecycle rule set on the bucket by the owner) and the stored reply is a small marker. Readers
fetch it transparently and the object is deleted once read. Used for bulk embedding batches,
whose vectors are large.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import threading
import uuid
from typing import TYPE_CHECKING, Any, Protocol

from llm_queue._logging import get_logger
from llm_queue.errors import LLMQueueError

if TYPE_CHECKING:
    from aiobotocore.session import AioSession

logger = get_logger(__name__)
MARKER = b"llmq-offload:"


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

    Object names are `{prefix}{key_prefix}/{sha256(request id)}-{random}`: no caller text in a
    key, and no two writes on one name. A reference outside that prefix is refused before any
    read. `sse="aws:kms"` with `kms_key_id` uses a customer key; `expected_bucket_owner` makes
    every request fail when the bucket changed owner. One client per event loop (never shared).
    """

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "llmq-results/",
        key_prefix: str = "",
        session: AioSession | None = None,
        client_kwargs: dict[str, Any] | None = None,
        sse: str = "AES256",
        kms_key_id: str | None = None,
        expected_bucket_owner: str | None = None,
        delete_after_read: bool = True,
    ) -> None:
        """Keep settings; a client is created on first use on each loop."""
        self.bucket = bucket
        self.prefix = prefix + (f"{key_prefix.strip('/')}/" if key_prefix else "")
        self._session = session
        self._kwargs = dict(client_kwargs or {})
        self._sse = sse
        self._kms_key_id = kms_key_id
        self._owner = (
            {"ExpectedBucketOwner": expected_bucket_owner} if expected_bucket_owner else {}
        )
        self._delete = delete_after_read
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
            context = session.create_client("s3", **self._kwargs)
            client = await context.__aenter__()
            with self._guard:
                self._clients[loop] = (context, client)
            return client

    async def put(self, key: str, data: bytes) -> str:
        """Write the object under a hashed, unique name; returns that name."""
        s3 = await self._s3()
        digest = hashlib.sha256(key.encode()).hexdigest()
        name = f"{self.prefix}{digest}-{uuid.uuid4().hex}"
        extra: dict[str, Any] = {"ServerSideEncryption": self._sse, **self._owner}
        if self._kms_key_id:
            extra["SSEKMSKeyId"] = self._kms_key_id
        await s3.put_object(Bucket=self.bucket, Key=name, Body=data, **extra)
        return name

    async def get(self, ref: str) -> bytes | None:
        """Read the object (None when it is gone), then delete it; refuses a foreign reference."""
        if not ref.startswith(self.prefix) or ".." in ref:
            raise LLMQueueError("offload reference outside this queue's prefix")
        s3 = await self._s3()
        try:
            reply = await s3.get_object(Bucket=self.bucket, Key=ref, **self._owner)
        except Exception as exc:
            if "NoSuchKey" in str(exc) or "404" in str(exc):
                return None
            raise
        async with reply["Body"] as body:
            data: bytes = await body.read()
        if self._delete:  # after a successful read only: a failed read can be tried again
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
