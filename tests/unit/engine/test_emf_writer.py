"""EN-20: EmfSink's default writer never blocks the event loop on a slow stdout."""

from __future__ import annotations

import io
import sys
import time

import pytest

from llm_queue.engine.metrics import EmfSink, _StdoutWriter


class _SlowStdout(io.StringIO):
    """A stdout that takes 20 ms per write (a backed-up container log pipe)."""

    def write(self, s: str) -> int:
        time.sleep(0.02)
        return super().write(s)


def test_a_slow_stdout_does_not_block_a_metric_point(monkeypatch: pytest.MonkeyPatch) -> None:
    """EN-20: a point used to wait for stdout (20 ms here); it is now only queued."""
    slow = _SlowStdout()
    monkeypatch.setattr(sys, "stdout", slow)
    writer = _StdoutWriter()
    sink = EmfSink(writer)
    started = time.perf_counter()
    for _ in range(10):
        sink.count("llmq_calls_total", model="m")
    assert time.perf_counter() - started < 0.15  # inline: 10 x 20 ms = 200 ms
    assert writer.flush(timeout_secs=5)
    assert len(slow.getvalue().splitlines()) == 10  # every point written, in order


def test_a_full_queue_drops_and_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    """EN-20: when stdout cannot keep up, lines past MAX_LINES are dropped, never blocking."""
    monkeypatch.setattr(sys, "stdout", _SlowStdout())
    monkeypatch.setattr(_StdoutWriter, "MAX_LINES", 3)
    writer = _StdoutWriter()
    for i in range(10):
        writer(f"line {i}")
    assert writer.dropped >= 6


def test_no_thread_until_the_first_line() -> None:
    """Building a sink starts nothing (no side effects before use)."""
    writer = _StdoutWriter()
    EmfSink(writer)
    assert writer._thread is None


def test_queued_lines_are_written_before_the_process_exits() -> None:
    """EN-20 (verify pass): lines still queued at exit were lost with the daemon thread."""
    import subprocess

    code = (
        "from llm_queue.engine.metrics import EmfSink\n"
        "sink = EmfSink()\n"
        "for _ in range(500): sink.count('llmq_calls_total', model='m')\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True, timeout=60
    ).stdout
    assert len(out.splitlines()) == 500


def test_a_writer_whose_thread_died_starts_a_new_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """FC-9: after a fork `_thread` was set but dead; the child's lines piled up, then dropped."""
    import threading

    out = io.StringIO()
    monkeypatch.setattr(sys, "stdout", out)
    writer = _StdoutWriter()
    dead = threading.Thread(target=lambda: None)
    dead.start()
    dead.join()
    writer._thread = dead  # what a forked child inherits
    writer("line")
    assert writer.flush(timeout_secs=5)
    assert out.getvalue() == "line\n"
