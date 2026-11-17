"""Error kinds a caller can receive (plan §3.19), as one exception type with a `kind`.

Callers catch `CallFailed` (an `LLMQueueError`) and branch on `kind`; `retry_after_secs` is set
where the plan says a later retry can work, `partial_text` for a stream cut mid-answer.
`category` and `retryable` come from `KIND_CATEGORIES`.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Literal, cast, get_args

from llm_queue.errors import Category, LLMQueueError

# Every kind a caller can receive (owner Q38): `match exc.kind` and `exc.kind == "..."` are
# checked by a type checker. `request_too_large` is every "too big" call: above the token budget
# or above `max_message_bytes`.
Kind = Literal[
    "rate_limited",
    "deadline_exceeded",
    "request_too_large",
    "no_deployment",
    "model_unavailable",
    "model_disabled",
    "queue_full",
    "queue_unavailable",
    "credentials_unavailable",
    "retry_budget_exhausted",
    "request_id_conflict",
    "result_expired",
    "dead_lettered",
    "worker_lost",
    "cancelled",
    "provider_error",
    "rejected",
    "invalid_request",
    "context_length_exceeded",
    "worker_shutdown",
]
KINDS: frozenset[str] = frozenset(get_args(Kind))

# Every kind's (category, retryable). `retryable` means a later retry by the caller can succeed:
# the queue already retried inside the call, so it is False for the request's own faults and
# for states only an operator changes (a disabled model, filters that match no deployment).
KIND_CATEGORIES: MappingProxyType[str, tuple[Category, bool]] = MappingProxyType(
    {
        "rate_limited": ("throttled", True),
        "queue_full": ("throttled", True),
        "deadline_exceeded": ("timeout", True),
        "invalid_request": ("bad_request", False),
        "context_length_exceeded": ("bad_request", False),
        "request_too_large": ("bad_request", False),
        "request_id_conflict": ("bad_request", False),
        "result_expired": ("bad_request", False),
        "rejected": ("bad_request", False),
        "model_unavailable": ("unavailable", True),
        "queue_unavailable": ("unavailable", True),
        "retry_budget_exhausted": ("unavailable", True),
        "worker_lost": ("unavailable", True),
        "worker_shutdown": ("unavailable", True),
        "model_disabled": ("unavailable", False),
        "provider_error": ("unavailable", False),
        "no_deployment": ("config_error", False),
        "credentials_unavailable": ("config_error", True),
        "cancelled": ("cancelled", False),
        "dead_lettered": ("internal", False),
    }
)
_UNKNOWN_KIND: tuple[Category, bool] = ("internal", False)


class CallFailed(LLMQueueError):
    """A call ended without a reply; `kind` is one of `KINDS`."""

    def __init__(
        self,
        kind: Kind | str,
        message: str = "",
        *,
        retry_after_secs: float | None = None,
        partial_text: str | None = None,
        request_id: str | None = None,
    ) -> None:
        """Keep the kind and its details."""
        super().__init__(f"{kind}: {message}" if message else kind)
        # Typed as `Kind` for readers; a kind read back from the store arrives as text.
        self.kind: Kind = cast("Kind", kind)
        self.retry_after_secs = retry_after_secs
        self.partial_text = partial_text
        self.request_id = request_id

    @property
    def category(self) -> Category:
        """The kind's category (`KIND_CATEGORIES`); `internal` for a kind it does not know."""
        return KIND_CATEGORIES.get(self.kind, _UNKNOWN_KIND)[0]

    @property
    def retryable(self) -> bool:
        """True when a later retry by the caller can succeed (`KIND_CATEGORIES`)."""
        return KIND_CATEGORIES.get(self.kind, _UNKNOWN_KIND)[1]
