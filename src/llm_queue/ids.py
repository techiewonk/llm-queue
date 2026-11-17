"""Ids for requests and jobs: UUIDv7 (RFC 9562) as 32 hex characters, from `uuid-utils`.

Time-ordered, so request ids sort by creation in logs, keys and stores, which random UUIDv4 do
not. `uuid-utils` (Rust) makes one in 0.49 us against 2.4 us for `uuid.uuid4().hex` (Python
3.12, measured 2026-11-13); the stdlib's own `uuid.uuid7` is 3.14+ only and took 3.1 us.
A v7 id starts with the time: short ids take its random tail, never its head.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Callable

from uuid_utils import uuid7


def new_id() -> str:
    """A UUIDv7: 48-bit Unix milliseconds, version 7, 74 random bits, RFC variant."""
    return uuid7().hex


class HashedId(str):
    """A user or tenant id that is already its keyed hash: `call()` passes it on as is.

    A job carries only the hashes of its sender's ids (D1, review RQ-6) and runs its calls under
    them; hashing them again would make another tenant.
    """

    __slots__ = ()


def hash_once(value: str, hash_id: Callable[[str], str]) -> str:
    """`hash_id(value)`, or `value` itself when it is a `HashedId` already."""
    return str(value) if isinstance(value, HashedId) else hash_id(value)


def keyed_hash(salt: str, value: str) -> str:
    """HMAC-SHA256 of `value` keyed by `salt` (`LLMQ_ID_HASH_SALT`), 16 hex characters.

    The one hash of tenant, user and session ids: messages, store keys, logs, the registry's
    fairness names and the admin views all use it (two copies drifting would split a tenant).
    """
    return hmac.new(salt.encode(), value.encode(), hashlib.sha256).hexdigest()[:16]
