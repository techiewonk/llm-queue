"""Self-tests of the drill harness for the verify-pass gaps (G8: CO-15, CO-16, CO-17, N5, N7).

Each test names its finding. Faults run against local fakes (no store, no Docker); the
CO-15 and N5 tests start small Python processes, killed at the end.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import os
import socket
import subprocess
import sys
import textwrap
import time
import types
from pathlib import Path
from typing import Any

import pytest

SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import gates_fleet  # noqa: E402  # needs the sys.path entry above

scenarios: Any = importlib.import_module("scenarios")
failover: Any = importlib.import_module("failover")
load: Any = importlib.import_module("load")

QUICK = types.SimpleNamespace(fault_secs=0)


def _accepts(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except OSError:
        return False


def test_fakellm_ends_when_its_drill_is_killed_hard(tmp_path: Path) -> None:
    """CO-15: a scenario child killed hard (no teardown) left its fakellm on its port.

    The child here starts a `FakeLLM` and is killed; fakellm sees its stdin pipe end and
    stops, so the port is free again.
    """
    child = textwrap.dedent(
        f"""
        import sys, time
        sys.path.insert(0, {str(SCRIPTS)!r})
        import gates_fleet
        fake = gates_fleet.FakeLLM({{}})
        fake.start()
        print(gates_fleet.FAKE_PORT, flush=True)
        time.sleep(120)
        """
    )
    env = {
        **os.environ,
        "LLMQ_DRILL_LOG_DIR": str(tmp_path / "logs"),
        "TMP": str(tmp_path),
        "TEMP": str(tmp_path),
        "TMPDIR": str(tmp_path),
    }
    env.pop("LLMQ_FAKE_PORT", None)
    proc = subprocess.Popen(
        [sys.executable, "-c", child], env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline()
        assert line.strip().isdigit(), f"the drill did not start fakellm: {line!r}"
        port = int(line)
        assert _accepts(port)
        proc.kill()  # hard: no finally, no FakeLLM.stop
        proc.wait(10)
        deadline = time.monotonic() + 15
        while _accepts(port) and time.monotonic() < deadline:
            time.sleep(0.1)
        assert not _accepts(port), "fakellm outlived the drill that started it"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(10)
        if proc.stdout is not None:
            proc.stdout.close()


async def test_rpm_cut_restores_the_budget_when_the_sampler_raised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CO-16: `samples = await sampler` raised in the finally and skipped the rpm restore."""
    edits: list[dict[str, Any]] = []

    async def edit(change: dict[str, Any], **_: Any) -> None:
        edits.append(change)

    async def broken_sampler(ctx: Any, stop: asyncio.Event) -> list[Any]:
        raise RuntimeError("fakellm stats failed")

    monkeypatch.setattr(scenarios, "_budget_samples", broken_sampler)
    ctx = types.SimpleNamespace(edit=edit, profile=QUICK, checks={}, notes=[])
    with pytest.raises(RuntimeError, match="stats failed"):
        await scenarios.rpm_cut(ctx)
    assert edits == [{"rpm": scenarios.RPM // 4}, {"rpm": scenarios.RPM}]


async def test_provider_both_down_resets_every_region_when_one_reset_fails() -> None:
    """CO-16: the first region whose reset raised left the other regions failing."""
    resets: list[str] = []

    async def provider(region: str, **behaviour: Any) -> None:
        if behaviour.get("force_error") is None:
            resets.append(region)
            if region == scenarios.REGIONS[0]:
                raise RuntimeError("reset failed")

    ctx = types.SimpleNamespace(provider=provider, profile=QUICK)
    with pytest.raises(RuntimeError, match="reset failed"):
        await scenarios.provider_both_down(ctx)
    assert resets == list(scenarios.REGIONS)


class _Pod:
    """A client pod stand-in: `done()` fails after `fail_after` seconds when set."""

    def __init__(self, log: list[str], fail_after: float | None) -> None:
        """Ready (no start needed)."""
        self.ready = {"version": 1}
        self.spec = {"model": "chat", "tag": "t"}
        self.report: dict[str, Any] = {"results": []}
        self._log = log
        self._fail_after = fail_after

    def go(self) -> None:
        return None

    async def done(self) -> dict[str, Any]:
        if self._fail_after is None:
            await asyncio.sleep(30)
        else:
            await asyncio.sleep(self._fail_after)
            raise RuntimeError("pod failed")
        return self.report

    async def kill(self) -> None:
        self._log.append("pod killed")


async def test_clients_cancel_the_fault_when_a_pod_fails() -> None:
    """CO-16: `gather` left the fault running in the background once a pod failed.

    The fault is cancelled (its finally turns it off) before the pods are killed.
    """
    log: list[str] = []

    async def fault() -> None:
        try:
            await asyncio.sleep(30)
        finally:
            log.append("fault off")

    pods = [_Pod(log, None), _Pod(log, 0.05)]
    with pytest.raises(RuntimeError, match="pod failed"):
        await asyncio.wait_for(scenarios.clients(types.SimpleNamespace(), pods, during=fault), 5)
    assert log == ["fault off", "pod killed", "pod killed"]


def _mute_proxy(tmp_path: Path) -> Any:
    """A `FaultProxy` whose process reads commands and never answers (a wedged proxy)."""
    proxy = gates_fleet.FaultProxy(port=1)
    mute = "import sys, time\nfor _ in sys.stdin: time.sleep(60)\n"
    proxy._proc = subprocess.Popen(
        [sys.executable, "-c", mute], stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    return proxy


async def test_a_wedged_proxy_does_not_freeze_the_driver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """N5: a setter read the proxy's answer on the driver's loop with no timeout.

    A proxy that never answers froze every in-process caller. The answer is read in a
    thread now, for at most `ANSWER_SECS`; then the proxy is killed and the switch raises.
    """
    monkeypatch.setattr(gates_fleet.FaultProxy, "ANSWER_SECS", 0.5)
    proxy = _mute_proxy(tmp_path)
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.02)
            ticks += 1

    task = asyncio.create_task(ticker())
    try:
        with pytest.raises(RuntimeError, match="no answer"):
            await proxy.set(refuse=True)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        proxy.stop()
    assert ticks >= 10  # the loop ran while the answer was awaited (0.5 s)
    assert proxy._proc.poll() is not None  # the wedged proxy was killed


class _SlowReap:
    """A process whose `wait` blocks 0.3 s (the OS reaping it)."""

    def __init__(self) -> None:
        """Alive until killed."""
        self.killed = False

    def poll(self) -> int | None:
        return None if not self.killed else 0

    def kill(self) -> None:
        self.killed = True

    def wait(self, timeout: float | None = None) -> int:
        time.sleep(0.3)
        return 0


async def test_worker_kill_waits_off_the_loop() -> None:
    """N7: `Worker.kill` called `proc.wait(10)` on the loop, stalling every task meanwhile."""
    worker = object.__new__(gates_fleet.Worker)
    worker.proc = _SlowReap()
    gaps: list[float] = []

    async def ticker() -> None:
        last = time.monotonic()
        while True:
            await asyncio.sleep(0.01)
            now = time.monotonic()
            gaps.append(now - last)
            last = now

    task = asyncio.create_task(ticker())
    try:
        await worker.kill()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    assert worker.proc.killed
    assert max(gaps) < 0.25


async def test_caller_dies_kills_and_reaps_a_caller_that_never_got_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CO-17: the dying caller was left running when its ready line never came.

    A caller that exits at start (no `ready`) now fails the scenario, killed, reaped and
    its pipes closed.
    """
    procs: list[Any] = []

    class Proc:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.stdin = types.SimpleNamespace(
                write=lambda data: None, flush=lambda: None, close=lambda: setattr(self, "i", 1)
            )
            self.stdout = types.SimpleNamespace(
                readline=lambda: b"Traceback ...\n", close=lambda: setattr(self, "o", 1)
            )
            self.killed = self.reaped = False
            procs.append(self)

        def poll(self) -> int | None:
            return 0 if self.killed else None

        def kill(self) -> None:
            self.killed = True

        def wait(self, timeout: float | None = None) -> int:
            self.reaped = True
            return 0

    monkeypatch.setattr(scenarios.subprocess, "Popen", Proc)
    ctx = types.SimpleNamespace(gate=types.SimpleNamespace(env={}), profile=QUICK)
    with pytest.raises(RuntimeError, match="did not start"):
        await scenarios.caller_dies(ctx)
    (proc,) = procs
    assert proc.killed
    assert proc.reaped
    assert (getattr(proc, "i", 0), getattr(proc, "o", 0)) == (1, 1)  # both pipes closed


async def test_store_oom_restores_both_settings_when_the_fault_raised() -> None:
    """CO-17: a failing `config_set` left the shared store at noeviction / a tiny maxmemory."""
    config = {"maxmemory": "0", "maxmemory-policy": "allkeys-lru"}

    class Client:
        def config_get(self, name: str) -> dict[str, str]:
            return {name: config[name]}

        def info(self, section: str) -> dict[str, int]:
            return {"used_memory": 5_000_000}

        def config_set(self, name: str, value: str) -> None:
            config[name] = value
            if name == "maxmemory" and value != "0":
                raise RuntimeError("config set failed")

    store = types.SimpleNamespace(client=Client())
    ctx = types.SimpleNamespace(gate=types.SimpleNamespace(store=store), profile=QUICK)
    with pytest.raises(RuntimeError, match="config set failed"):
        await scenarios.store_oom(ctx)
    assert config == {"maxmemory": "0", "maxmemory-policy": "allkeys-lru"}


async def test_load_stop_all_stops_every_queue_when_one_raises() -> None:
    """CO-17: a raising `queue.stop` skipped the other stops and the key cleanup after them."""
    stopped: list[int] = []

    def queue(n: int, fails: bool) -> Any:
        async def stop(drain_secs: float) -> None:
            stopped.append(n)
            if fails:
                raise RuntimeError(f"stop {n} failed")

        return types.SimpleNamespace(stop=stop)

    with pytest.raises(RuntimeError, match="stop 0 failed"):
        await load.stop_all([queue(0, True), queue(1, False), queue(2, True)])
    assert stopped == [0, 1, 2]


async def test_failover_undoes_proxy_and_fakellm_when_the_queue_fails_to_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CO-17: the proxy and fakellm leaked when `llmq.start()` raised in the failover drill."""
    undone: list[str] = []

    class Proxy:
        def start(self) -> None:
            return None

        def stop(self) -> None:
            undone.append("proxy")

    class Fake:
        url = "http://127.0.0.1:1"

        def __init__(self, scenario: Any) -> None:
            """No process."""

        def start(self) -> None:
            return None

        def stop(self) -> None:
            undone.append("fakellm")

    class Queue:
        @classmethod
        def from_env(cls, env: Any) -> Queue:
            return cls()

        async def start(self) -> None:
            raise RuntimeError("store down")

        async def stop(self, drain_secs: float) -> None:
            undone.append("queue")

    monkeypatch.setattr(failover, "Proxy", Proxy)
    monkeypatch.setattr(failover, "FakeLLM", Fake)
    monkeypatch.setattr(failover, "LLMQueue", Queue)
    with pytest.raises(RuntimeError, match="store down"):
        async with contextlib.AsyncExitStack() as stack:
            await failover._drill("restart", stack)
    assert undone == ["queue", "fakellm", "proxy"]  # each undone, in reverse
