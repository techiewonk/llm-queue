"""Exception base classes and provider-error classification shared by every layer.

Callers catch one type, `LLMQueueError` (calls raise `CallFailed`, a subclass with a `kind`);
workers turn provider exceptions into `ErrorInfo` so retry, budgets and health never depend on
SDK-specific exception classes.

Every `LLMQueueError` has a `category` (one of `CATEGORIES`, for logs and dashboards) and
`retryable` (a later retry by the caller can succeed: the queue already retried what it could);
`error_category` / `error_retryable` answer for any exception.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar, Literal, get_args

Category = Literal[
    "throttled",
    "timeout",
    "bad_request",
    "unavailable",
    "config_error",
    "cancelled",
    "internal",
]
CATEGORIES: frozenset[str] = frozenset(get_args(Category))


class LLMQueueError(Exception):
    """Base class for every error this package raises to callers.

    `category` and `retryable` are computed from the class (and a `CallFailed`'s kind), never
    stored on the instance, so pickling and the constructor are unchanged.
    """

    _category: ClassVar[Category] = "internal"
    _retryable: ClassVar[bool] = False

    @property
    def category(self) -> Category:
        """What kind of failure this is (one of `CATEGORIES`)."""
        return self._category

    @property
    def retryable(self) -> bool:
        """True when a later retry by the caller can succeed (the queue already retried)."""
        return self._retryable


def error_category(exc: BaseException) -> Category:
    """The `category` of an llm-queue error; `"internal"` for any other exception."""
    return exc.category if isinstance(exc, LLMQueueError) else "internal"


def error_retryable(exc: BaseException) -> bool:
    """The `retryable` of an llm-queue error; False for any other exception."""
    return exc.retryable if isinstance(exc, LLMQueueError) else False


class ConfigError(LLMQueueError):
    """Invalid configuration, raised at startup or on a refused edit (never mid-call)."""

    _category = "config_error"


class UnknownModel(ConfigError, KeyError):  # named for what it is
    """A model name that is neither an alias nor a model id in the registry.

    Also a `KeyError`, so code that looks models up by key keeps working (HTTP 404, CLI).
    Category `config_error` (from `ConfigError`): the name comes from the app's code or
    configuration, and retrying it unchanged cannot succeed.
    """

    def __str__(self) -> str:
        return f"unknown model {self.args[0]!r}"


class StoreUnavailable(LLMQueueError, ConnectionError):  # named for what it is
    """The state store could not serve an admin operation (unreachable, read-only, full).

    Raised by `cancel`, `status`, `send_job` and the registry's edits and reads; a call
    raises `CallFailed` instead. Also a `ConnectionError`, so existing handlers keep working.
    """

    _category = "unavailable"
    _retryable = True


class LocalModeRefused(ConfigError):
    """An operation that needs the store, asked of a queue in local mode (`LLMQ_EXECUTION_MODE`).

    Request records and status by id, cancel by id, purges, registry live edits and publishes,
    jobs and dead letters live in the store, which local mode never contacts. Category
    `config_error`: the process is set up without a store; HTTP routes answer 503.
    """

    def __init__(self, operation: str) -> None:
        """`operation` names what was asked (`cancel`, `request status`, `registry edit`...)."""
        super().__init__(f"{operation} is not available in local mode (LLMQ_EXECUTION_MODE=local)")
        self.operation = operation


class QueueDisabled(ConfigError):
    """A call or store operation asked of a queue switched off with `LLMQ_ENABLED=false`.

    The off switch keeps the app on its own direct path: the factories return None, so only
    code that calls the queue directly gets this. Category `config_error`, not retryable;
    HTTP routes answer 503 with `reason: disabled`.
    """

    def __init__(self, operation: str) -> None:
        """`operation` names what was asked (`converse`, `status`, `purge.model`...)."""
        super().__init__(f"llm-queue is disabled (LLMQ_ENABLED=false): {operation} refused")
        self.operation = operation


class AdminRefused(LLMQueueError):
    """An admin operation that was refused; nothing was changed.

    E.g. a purge of a model still in the registry without `force`, or a read past
    `LLMQ_ADMIN_SCAN_MAX`. Category `bad_request` (HTTP 409): a repeat gets the same answer.
    """

    _category = "bad_request"


class MissingExtra(ConfigError, ImportError):  # named for what it is
    """An optional feature's dependencies are not installed; names the extra to install.

    Also an `ImportError`, so `except ImportError` around optional imports keeps working.
    """

    def __init__(self, feature: str, extra: str) -> None:
        """`feature` needs `pip install 'llm-queue[extra]'`."""
        super().__init__(f"{feature} needs: pip install 'llm-queue[{extra}]'")


class UnsupportedEnvelopeError(LLMQueueError):
    """A message is newer than this worker understands; it is returned, never dropped."""


class ErrorKind(StrEnum):
    """How the worker reacts to a provider error: give back and retry, retry, or give up."""

    THROTTLE = "throttle"
    RETRYABLE = "retryable"
    FATAL = "fatal"


@dataclass(frozen=True, slots=True)
class ErrorInfo:
    """Provider-neutral classification of one failed attempt.

    `code` keeps the provider's own spelling for logs only; decisions use `kind`, because
    fakes and real services spell the same failure differently.
    """

    kind: ErrorKind
    code: str = ""
    retry_after_secs: float | None = None
