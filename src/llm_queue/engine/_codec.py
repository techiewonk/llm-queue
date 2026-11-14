"""Store reply decoding shared by the engine modules: bytes or str in, str or bytes out.

The store client returns bytes; these helpers were copied into six modules (EN-26).
"""

from __future__ import annotations

from typing import Any


def text(value: Any) -> str:
    """A store reply value as text (bytes decoded as UTF-8)."""
    return value.decode() if isinstance(value, bytes) else str(value)


def as_bytes(value: Any) -> bytes:
    """A store reply value as bytes (text encoded as UTF-8)."""
    return value if isinstance(value, bytes) else str(value).encode()
