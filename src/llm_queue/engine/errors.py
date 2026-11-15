"""Error kinds a caller can receive (plan §3.19), as one exception type with a `kind`.

Callers catch `CallFailed` (an `LLMQueueError`) and branch on `kind`; `retry_after_secs` is set
where the plan says a later retry can work, `partial_text` for a stream cut mid-answer.
"""

from __future__ import annotations

from typing import Literal, cast, get_args

from llm_queue.errors import LLMQueueError

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
