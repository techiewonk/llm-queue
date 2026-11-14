"""Self-tests of the drill harness (scripts/): teardown after a fault, the store proxy (G8).

A stuck worker used to leave workers, fakellm and the run's keys behind (CO-3); a fault that
raised in a pair left the store proxy refusing (CO-16); the proxy dropped frozen bytes
(CO-14). No store or fakellm is needed: processes and servers here are local fakes.
"""

from __future__ import annotations

import asyncio
import importlib
import subprocess
import sys
import types
from pathlib import Path
from typing import Any

import pytest

SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import gates  # noqa: E402  # needs the sys.path entry above
import gates_fleet  # noqa: E402


class StuckProc:
    """A worker process that never stops on its own (`wait` times out until killed)."""

    def __init__(self) -> None:
        """Alive, with a stdin that takes the `stop` line."""
        self.killed = False
        self.stdin = types.SimpleNamespace(write=lambda data: None, flush=lambda: None)

    def poll(self) -> int | None:
        return 0 if self.killed else None

    def wait(self, timeout: float | None = None) -> int:
        if not self.killed:
            raise subprocess.TimeoutExpired("worker", timeout or 0)
        return 0

    def kill(self) -> None:
        self.killed = True


def stuck_worker() -> Any:
    worker = object.__new__(gates_fleet.Worker)
    worker.proc = StuckProc()
    worker._lock = asyncio.Lock()
    return worker


async def test_worker_stop_kills_a_stuck_worker() -> None:
    """The bug: `proc.wait(60)` raised TimeoutExpired and the worker stayed alive."""
    worker = stuck_worker()
    await worker.stop()
    assert worker.proc.killed


async def test_stop_all_kills_workers_whose_stop_raised() -> None:
    worker = stuck_worker()

    async def broken_stop() -> None:
        raise RuntimeError("stop failed")

    worker.stop = broken_stop
    await gates_fleet.stop_all([worker])
    assert worker.proc.killed


async def test_gate_teardown_runs_every_step_when_one_raises() -> None:
    """The bug: a raising step skipped fakellm's stop, the key cleanup and the store close."""
    done: list[str] = []
    worker = stuck_worker()

    def fake_stop() -> None:
        done.append("fake")
        raise RuntimeError("fakellm stop failed")

    gate = object.__new__(gates.Gate)
    gate.prefix = "llmqgate:test"
    gate.queues = []
    gate.fleet = [worker]
    gate.fake = types.SimpleNamespace(stop=fake_stop)
    gate.store = types.SimpleNamespace(
        cleanup=lambda prefix: done.append("cleanup"), close=lambda: done.append("close")
    )
    with pytest.raises(RuntimeError, match="fakellm stop failed"):
        await gate.__aexit__(None, None, None)
    assert worker.proc.killed
    assert done == ["fake", "cleanup", "close"]


class FakeProxy:
    """A `FaultProxy` stand-in: `set(refuse=...)` and `drop_all()` only record."""

    def __init__(self) -> None:
        """Not refusing."""
        self.refuse = False

    async def set(self, *, refuse: bool) -> None:
        self.refuse = refuse

    async def drop_all(self) -> None:
        return None


async def test_pair_turns_the_store_block_off_when_the_other_fault_raises() -> None:
    """The bug: `gather` left store_block running, the proxy refusing during teardown."""
    scenarios: Any = importlib.import_module("scenarios")
    proxy = FakeProxy()
    ctx = types.SimpleNamespace(proxy=proxy, profile=types.SimpleNamespace(fault_secs=30))

    async def boom(ctx: Any) -> None:
        await asyncio.sleep(0.05)
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await asyncio.wait_for(scenarios.pair(scenarios.store_block, boom)(ctx), 5)
    assert proxy.refuse is False


async def _echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while data := await reader.read(1024):
        writer.write(data)
        await writer.drain()
    writer.close()


async def test_proxy_holds_frozen_bytes_and_switches_faults(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Frozen bytes arrive after the thaw (they were dropped); refuse and drop_all work."""
    monkeypatch.setenv("LLMQ_DRILL_LOG_DIR", str(tmp_path))
    upstream = await asyncio.start_server(_echo, "127.0.0.1", 0)
    port = upstream.sockets[0].getsockname()[1]
    proxy = gates_fleet.FaultProxy(upstream=port)
    await asyncio.to_thread(proxy.start)
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy.port)
        writer.write(b"a")
        assert await asyncio.wait_for(reader.read(10), 5) == b"a"
        await proxy.set(freeze=True)
        writer.write(b"b")
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(reader.read(10), 0.3)
        await proxy.set(freeze=False)
        assert await asyncio.wait_for(reader.read(10), 5) == b"b"
        await proxy.set(refuse=True)
        await proxy.drop_all()
        assert await asyncio.wait_for(reader.read(10), 5) == b""
        writer.close()
        refused_r, refused_w = await asyncio.open_connection("127.0.0.1", proxy.port)
        assert await asyncio.wait_for(refused_r.read(10), 5) == b""
        refused_w.close()
    finally:
        await asyncio.to_thread(proxy.stop)
        upstream.close()
        await upstream.wait_closed()
    assert proxy._proc.poll() is not None  # no proxy process left behind
