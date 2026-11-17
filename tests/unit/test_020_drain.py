"""0.2.0 drain API (plan A1-A5) without a store: the drain file watcher, settings and routes.

The behaviour on a real store (hand-back, resume, jobs, the file pausing two queues) is in
tests/integration/engine/test_020_drain_store.py.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from llm_queue import ConfigError, LLMQueue
from llm_queue.drain import DrainWatcher
from llm_queue.engine.settings import InfraSettings

DEV = {
    "LLMQ_STORE_URL": "redis://localhost:6379/0",
    "LLMQ_ENVIRONMENT": "dev",
    "LLMQ_REGISTRY": '{"registry_version": 1, "models": {}}',
}


def _touch(path: Path, mtime: float) -> None:
    path.write_text("drain", encoding="utf-8")
    os.utime(path, (mtime, mtime))


# ---------------------------------------------------------------- A4: the watcher


def test_a_new_file_triggers_once(tmp_path: Path) -> None:
    path = tmp_path / "drain"
    fired: list[int] = []
    watcher = DrainWatcher(path, lambda: fired.append(1), poll_secs=0.1)
    assert watcher.check() is False  # no file: nothing
    _touch(path, time.time())
    assert watcher.check() is True
    assert watcher.check() is False  # the same file again: once only
    assert fired == [1]


def test_a_file_older_than_the_watcher_is_ignored(tmp_path: Path) -> None:
    """A drain file left from the last pod (an emptyDir survives a container restart)."""
    path = tmp_path / "drain"
    _touch(path, time.time() - 30)
    fired: list[int] = []
    watcher = DrainWatcher(path, lambda: fired.append(1), poll_secs=0.1)
    assert watcher.check() is False
    # Within the 1 s margin of the construction time: counted (a touch at start races it).
    _touch(path, time.time() - 0.5)
    assert watcher.check() is True
    assert fired == [1]


def test_the_since_argument_sets_the_cut_off(tmp_path: Path) -> None:
    path = tmp_path / "drain"
    _touch(path, 1000.0)
    assert DrainWatcher(path, lambda: None, since=1000.5).check() is True
    assert DrainWatcher(path, lambda: None, since=1002.0).check() is False


def test_it_re_arms_only_on_a_newer_mtime(tmp_path: Path) -> None:
    """After a resume the old file must not pause again; a fresh touch does."""
    path = tmp_path / "drain"
    fired: list[int] = []
    watcher = DrainWatcher(path, lambda: fired.append(1))
    now = time.time()
    _touch(path, now)
    assert watcher.check() is True
    _touch(path, now)  # rewritten, same mtime
    assert watcher.check() is False
    _touch(path, now + 5)
    assert watcher.check() is True
    assert fired == [1, 1]


def test_a_failing_callback_is_logged_and_the_watcher_goes_on(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    path = tmp_path / "drain"

    def boom() -> None:
        raise RuntimeError("no")

    watcher = DrainWatcher(path, boom)
    _touch(path, time.time())
    assert watcher.check() is True
    assert any("drain_callback_failed" in r.getMessage() for r in caplog.records)


async def test_the_poll_task_fires_and_stops(tmp_path: Path) -> None:
    path = tmp_path / "drain"
    fired = asyncio.Event()
    watcher = DrainWatcher(path, fired.set, poll_secs=0.1)
    watcher.start()
    watcher.start()  # a second start does nothing
    _touch(path, time.time())
    await asyncio.wait_for(fired.wait(), 2)
    await watcher.stop()
    await watcher.stop()  # idempotent
    assert watcher.running is False


def test_poll_secs_is_bounded(tmp_path: Path) -> None:
    for bad in (0.05, 61, float("nan")):
        with pytest.raises(ConfigError, match="poll_secs"):
            DrainWatcher(tmp_path / "drain", lambda: None, poll_secs=bad)


def test_the_module_imports_nothing_heavy() -> None:
    """Usable by an app with its queue off: no redis, botocore or the engine on import."""
    code = (
        "import sys, llm_queue.drain\n"
        "heavy = {'redis', 'botocore', 'aiobotocore', 'llm_queue.core',\n"
        "         'llm_queue.engine.runtime'}\n"
        "print(sorted(heavy & set(sys.modules)))\n"
    )
    src = str(Path(__file__).resolve().parents[2] / "src")
    env = {**os.environ, "PYTHONPATH": src + os.pathsep + os.environ.get("PYTHONPATH", "")}
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True, env=env
    ).stdout
    assert out.strip() == "[]"


# ---------------------------------------------------------------- settings


def test_drain_settings_defaults_and_validation() -> None:
    settings = InfraSettings.from_env(DEV)
    assert settings.drain_handback_enabled is True
    assert settings.drain_file == ""
    assert settings.drain_poll_secs == 1.0
    set_ = InfraSettings.from_env(
        {**DEV, "LLMQ_DRAIN_HANDBACK_ENABLED": "off", "LLMQ_DRAIN_FILE": "/tmp/drain",
         "LLMQ_DRAIN_POLL_SECS": "0.5"}
    )  # fmt: skip
    assert (set_.drain_handback_enabled, set_.drain_file, set_.drain_poll_secs) == (
        False,
        "/tmp/drain",
        0.5,
    )
    for name, value in (
        ("LLMQ_DRAIN_HANDBACK_ENABLED", "maybe"),
        ("LLMQ_DRAIN_POLL_SECS", "0.01"),
        ("LLMQ_DRAIN_POLL_SECS", "120"),
    ):
        with pytest.raises(ConfigError, match=name):
            InfraSettings.from_env({**DEV, name: value})


def test_the_engine_gets_the_hand_back_setting() -> None:
    assert LLMQueue.from_env(DEV)._engine.settings.drain_handback_enabled is True
    off = LLMQueue.from_env({**DEV, "LLMQ_DRAIN_HANDBACK_ENABLED": "false"})
    assert off._engine.settings.drain_handback_enabled is False


# ---------------------------------------------------------------- A1-A3 on an unstarted queue


def test_pause_and_resume_on_an_unstarted_queue() -> None:
    llmq = LLMQueue.from_env(DEV)
    assert llmq.draining is False
    assert llmq.draining_since is None
    before = time.time()
    llmq.pause_receiving()
    assert llmq.draining is True
    assert llmq.draining_since is not None
    assert llmq.draining_since >= before
    first = llmq.draining_since
    llmq.pause_receiving()  # a second pause keeps the first time
    assert llmq.draining_since == first
    llmq.resume_receiving()
    assert llmq.draining is False
    assert llmq.draining_since is None
    assert llmq._engine.ready is False  # never started: not ready after a resume


# ---------------------------------------------------------------- A5: routes


def test_drain_routes_are_off_by_default() -> None:
    pytest.importorskip("fastapi")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()
    app.include_router(LLMQueue.from_env(DEV).http_router(admin_token="t"))
    client = TestClient(app)
    assert client.post("/drain", headers={"X-LLMQ-Token": "t"}).status_code in (404, 405)
    assert client.get("/drain").status_code in (404, 405)


def test_drain_routes_need_a_guard() -> None:
    """An open POST /drain would let anyone take a pod out of service."""
    pytest.importorskip("fastapi")
    with pytest.raises(ConfigError, match="dependencies"):
        LLMQueue.from_env(DEV).http_router(drain_routes=True)


def test_drain_routes_are_post_guarded_and_get_is_status_only() -> None:
    pytest.importorskip("fastapi")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    llmq = LLMQueue.from_env(DEV)
    app = FastAPI()
    app.include_router(llmq.http_router(admin_token="t", drain_routes=True))
    client = TestClient(app)
    token = {"X-LLMQ-Token": "t"}
    both = {**token, "X-LLMQ-Admin": "1"}
    assert client.get("/drain").status_code == 403  # a read: the token
    status = client.get("/drain", headers=token)  # changes nothing
    assert status.status_code == 200
    assert status.json() == {"draining": False, "since": None, "held": 0, "running": 0}
    assert client.post("/drain").status_code == 403  # no token
    assert llmq.draining is False
    paused = client.post("/drain", headers=both)
    assert paused.status_code == 200
    assert paused.json()["draining"] is True
    assert llmq.draining is True
    assert client.get("/drain", headers=token).json()["since"] == llmq.draining_since
    resumed = client.post("/resume", headers=both)
    assert resumed.status_code == 200
    assert resumed.json()["draining"] is False
    assert llmq.draining is False
