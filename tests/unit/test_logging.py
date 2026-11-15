"""The library's loggers: stdlib levels decide before any work, and one call is one line.

Pins CO-22: disabled calls still ran the processor that builds the message, and a field
value holding a newline could forge a second log line.
"""

from __future__ import annotations

import logging

import pytest

from llm_queue import _logging


def test_a_disabled_call_builds_no_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """CO-22: with no `filter_by_level`, an `info` call under WARNING still built its line."""
    built: list[str] = []
    real = _logging._to_stdlib

    def spy(*args: object) -> object:
        built.append("x")
        return real(*args)  # type: ignore[arg-type]

    monkeypatch.setattr(_logging, "_to_stdlib", spy)
    name = "llm_queue.tests.disabled"
    logging.getLogger(name).setLevel(logging.WARNING)
    logger = _logging.get_logger(name)
    logger.info("quiet", n=1)
    assert built == []
    logger.warning("loud", n=1)
    assert built == ["x"]


def test_a_newline_in_a_value_stays_on_one_line(caplog: pytest.LogCaptureFixture) -> None:
    """CO-22: `error="a\\nb"` printed as two lines, so a value could forge a log entry."""
    logger = _logging.get_logger("llm_queue.tests.oneline")
    with caplog.at_level(logging.INFO, logger="llm_queue.tests.oneline"):
        logger.info("event", error="a\nfake_event k=v", plain="two words")
    message = caplog.records[-1].getMessage()
    assert "\n" not in message
    assert message == "event error='a\\nfake_event k=v' plain=two words"
    assert caplog.records[-1].llmq["error"] == "a\nfake_event k=v"  # type: ignore[attr-defined]


@pytest.mark.parametrize("value", ["a\nb", "a\x85b", "plain text", "\x7f", "\x9f", "\xa0é", 42])
def test_value_output_is_unchanged_by_the_regex(value: object) -> None:
    """R3PF-5: the precompiled search gives the old per-character loop's output exactly."""
    text = str(value)
    old = repr(text) if any(ord(c) < 32 or 127 <= ord(c) < 160 for c in text) else text
    assert _logging._value(value) == old


def test_log_limiter_keeps_one_line_per_interval_with_a_count() -> None:
    """R3OB-7: 1,000 store failures in 1 s logged 1,000 lines; now 2, the second counted."""
    now = [0.0]
    lines: list[tuple[str, dict[str, object]]] = []
    limited = _logging.LogLimiter(every_secs=10, clock=lambda: now[0])

    def log(event: str, **fields: object) -> None:
        lines.append((event, fields))

    for i in range(1000):
        now[0] = i / 1000
        limited(log, "receive_failed", key="m", model="m")
    limited(log, "receive_failed", key="other", model="other")  # its own key: logged at once
    now[0] = 11.0
    limited(log, "receive_failed", key="m", model="m")
    assert lines == [
        ("receive_failed", {"model": "m"}),
        ("receive_failed", {"model": "other"}),
        ("receive_failed", {"model": "m", "suppressed": 999}),
    ]
