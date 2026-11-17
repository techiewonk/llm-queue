"""C1: every error has a `category` and `retryable`, and the helpers answer for any exception."""

from __future__ import annotations

import pickle

import pytest

import llm_queue
from llm_queue import (
    CATEGORIES,
    KIND_CATEGORIES,
    KINDS,
    AdminRefused,
    CallFailed,
    ConfigError,
    LLMQueueError,
    MissingExtra,
    StoreUnavailable,
    UnknownModel,
    error_category,
    error_retryable,
)
from llm_queue.errors import UnsupportedEnvelopeError
from llm_queue.registry import RegistryError
from llm_queue.registry.store import LiveEditRefused, RegistryConflict


def test_the_table_covers_every_kind_with_a_known_category() -> None:
    assert set(KIND_CATEGORIES) == KINDS
    assert {category for category, _ in KIND_CATEGORIES.values()} <= CATEGORIES
    assert all(isinstance(retryable, bool) for _, retryable in KIND_CATEGORIES.values())


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_call_failed_reads_its_kind(kind: str) -> None:
    exc = CallFailed(kind, "x")
    assert (exc.category, exc.retryable) == KIND_CATEGORIES[kind]
    assert (error_category(exc), error_retryable(exc)) == KIND_CATEGORIES[kind]


@pytest.mark.parametrize(
    ("kind", "category", "retryable"),
    [
        ("rate_limited", "throttled", True),
        ("deadline_exceeded", "timeout", True),
        ("invalid_request", "bad_request", False),
        ("queue_unavailable", "unavailable", True),
        ("model_disabled", "unavailable", False),
        ("no_deployment", "config_error", False),
        ("cancelled", "cancelled", False),
        ("dead_lettered", "internal", False),
    ],
)
def test_call_failed_examples(kind: str, category: str, retryable: bool) -> None:
    exc = CallFailed(kind)
    assert (exc.category, exc.retryable) == (category, retryable)


def test_an_unknown_kind_is_internal() -> None:
    """A kind read back from a newer worker's reply."""
    exc = CallFailed("from_the_future")
    assert (exc.category, exc.retryable) == ("internal", False)


@pytest.mark.parametrize(
    ("exc", "category", "retryable"),
    [
        (LLMQueueError("x"), "internal", False),
        (ConfigError("x"), "config_error", False),
        (UnknownModel("m"), "config_error", False),
        (MissingExtra("feature", "extra"), "config_error", False),
        (RegistryError(["bad"]), "config_error", False),
        (RegistryConflict("x"), "config_error", True),
        (LiveEditRefused("x"), "config_error", False),
        (StoreUnavailable("x"), "unavailable", True),
        (AdminRefused("x"), "bad_request", False),
        (UnsupportedEnvelopeError("x"), "internal", False),
    ],
)
def test_every_error_class(exc: LLMQueueError, category: str, retryable: bool) -> None:
    assert (exc.category, exc.retryable) == (category, retryable)
    assert (error_category(exc), error_retryable(exc)) == (category, retryable)


@pytest.mark.parametrize("exc", [ValueError("x"), TimeoutError(), KeyboardInterrupt()])
def test_other_exceptions_are_internal(exc: BaseException) -> None:
    assert error_category(exc) == "internal"
    assert error_retryable(exc) is False


def test_computed_not_stored_so_pickling_is_unchanged() -> None:
    exc = CallFailed("rate_limited", "busy", retry_after_secs=2.0, request_id="r1")
    assert "category" not in vars(exc)
    assert "retryable" not in vars(exc)
    back = pickle.loads(pickle.dumps(exc))
    assert (back.kind, back.category, back.retryable) == ("rate_limited", "throttled", True)
    assert pickle.loads(pickle.dumps(StoreUnavailable("down"))).category == "unavailable"


def test_exported() -> None:
    for name in ("CATEGORIES", "Category", "KIND_CATEGORIES", "error_category", "error_retryable"):
        assert name in llm_queue.__all__
