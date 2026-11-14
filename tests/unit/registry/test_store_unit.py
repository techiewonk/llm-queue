"""The registry store without a store: its pure helpers and its failure paths on a fake client.

The helpers (`refused_edits`, `split_path`, `carry_routing`, `audit_diff`, `check_actor`) were
tested only through the Docker store tests, which the Windows and lowest-direct jobs skip
(audit G6). The failure paths (lost races, a flushed store, a broken reload) need a client
that misbehaves on cue, so they use the fake below. Tests that pin an audit finding name it.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import orjson
import pytest

from llm_queue.errors import ConfigError, StoreUnavailable
from llm_queue.registry.settings import SETTINGS
from llm_queue.registry.store import (
    _PROTECTED_WHY,
    PublishResult,
    RegistryConflict,
    RegistryStore,
    _reload_wait,
    audit_diff,
    carry_routing,
    check_actor,
    refused_edits,
    split_path,
)

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def full() -> dict[str, Any]:
    """The plan's full example registry."""
    data: dict[str, Any] = json.loads((FIXTURES / "full.json").read_text(encoding="utf-8"))
    return data


# ---------------------------------------------------------------- a fake store client


class _Pipe:
    """The transactional GET + HGETALL that `RegistryStore._read` sends."""

    def __init__(self, client: _Client) -> None:
        self.client = client

    async def __aenter__(self) -> _Pipe:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    def get(self, key: str) -> None:
        return None

    def hgetall(self, key: str) -> None:
        return None

    async def execute(self) -> list[Any]:
        return [self.client.doc, self.client.meta]


class _Script:
    """A registered Lua script that answers each call with `reply()` and records its args."""

    def __init__(self, reply: Callable[[], list[Any]]) -> None:
        self.reply = reply
        self.calls: list[list[Any]] = []

    async def __call__(self, keys: list[str], args: list[Any]) -> list[Any]:
        self.calls.append(args)
        return self.reply()


class _Client:
    """Just enough of `redis.asyncio.Redis` for the registry store: one stored doc and meta."""

    def __init__(self, doc: dict[str, Any] | None, meta: dict[bytes, bytes]) -> None:
        self.doc: bytes | None = orjson.dumps(doc) if doc is not None else None
        self.meta = meta
        self.audit: list[tuple[bytes, dict[bytes, bytes]]] = []
        self.history: dict[str, bytes] = {}

    def register_script(self, source: str) -> _Script:
        return _Script(lambda: [b"ok", 1])

    def pipeline(self, *, transaction: bool) -> _Pipe:
        return _Pipe(self)

    async def xrevrange(self, key: str, count: int) -> list[tuple[bytes, dict[bytes, bytes]]]:
        return self.audit[:count]

    async def hget(self, key: str, field: str) -> bytes | None:
        return self.history.get(field)


def _store(
    client: _Client, *, sleeps: list[float] | None = None, **kw: Any
) -> RegistryStore:
    async def sleep(secs: float) -> None:
        if sleeps is not None:
            sleeps.append(secs)

    return RegistryStore(client, "t", sleep=sleep, **kw)  # type: ignore[arg-type]  # a fake


def _meta(changes: int = 1, version: int = 1) -> dict[bytes, bytes]:
    return {b"changes": str(changes).encode(), b"version": str(version).encode()}


@pytest.fixture
def top_of_range(monkeypatch: pytest.MonkeyPatch) -> None:
    """Jitter at its top, so a backoff's growth can be asserted exactly."""
    monkeypatch.setattr("llm_queue.registry.store.random.uniform", lambda lo, hi: hi)


# ---------------------------------------------------------------- pure helpers (G6)


def test_split_path_keeps_names_with_dots_whole() -> None:
    """A deployment named after a dotted region stays one segment."""
    doc = {"deployments": {"aws-main": {"overrides": {"us.east.1": {"rpm": 1}}}}}
    assert split_path(doc, "deployments.aws-main.overrides.us.east.1.rpm") == [
        "deployments",
        "aws-main",
        "overrides",
        "us.east.1",
        "rpm",
    ]
    assert split_path({}, "a.b") == ["a", "b"]


def test_carry_routing_keeps_live_blocks_and_the_kill_switch() -> None:
    """R5: a publish never undoes an emergency block set live; malformed stored values skip."""
    stored = {"routing": {"blocked_regions": ["r1"], "kill_switch": True}}
    doc = {"routing": {"blocked_regions": ["r2", "r1"]}, "x": 1}
    out = carry_routing(stored, doc)
    assert out["routing"] == {"blocked_regions": ["r1", "r2"], "kill_switch": True}
    assert carry_routing({"routing": {"blocked_regions": "r1"}}, doc)["routing"] == {
        "blocked_regions": ["r2", "r1"]
    }
    assert carry_routing(None, doc) == doc


def test_audit_diff_redacts_secrets() -> None:
    """Secret references never reach the audit stream, even inside a whole new account."""
    before = {"accounts": {"a": {"role_arn": "arn:old"}}}
    after = {"accounts": {"a": {"role_arn": "arn:new"}, "b": {"api_key_ref": "K", "kind": "x"}}}
    rows = {row["path"]: row for row in audit_diff(before, after)}
    assert rows["accounts.a.role_arn"]["before"] == "[redacted]"
    assert rows["accounts.a.role_arn"]["after"] == "[redacted]"
    assert rows["accounts.b.api_key_ref"]["after"] == "[redacted]"
    assert rows["accounts.b.kind"]["after"] == "x"


@pytest.mark.parametrize("actor", ["op\nfake entry", "op\x1b[2J", "", " ", "x" * 201])
def test_check_actor_refuses_bad_actors(actor: str) -> None:
    """An empty, overlong or control-character actor could forge or hide audit rows."""
    with pytest.raises(ConfigError, match="actor"):
        check_actor(actor)
    assert check_actor("ops@host") == "ops@host"


def test_refused_edits_by_mode(full: dict[str, Any]) -> None:
    """Numbers edit live in `safe`; protected fields only in `all`; nothing in `off`."""
    after = copy.deepcopy(full)
    after["models"]["gpt20b"]["rpm"] = 7
    assert refused_edits(full, after, "safe") == []
    after["residency"] = {"allowed_regions": ["us-east-1"]}
    assert any("residency" in r for r in refused_edits(full, after, "safe"))
    assert refused_edits(full, after, "all") == []
    assert refused_edits(full, after, "off") == ["live edits are off (registry_live_edit=off)"]
    removed = copy.deepcopy(full)
    del removed["models"]["gpt20b"]
    assert refused_edits(full, removed, "all") == [
        "models.gpt20b: adding or removing a model needs a new registry_version"
    ]


def test_refused_edits_unknown_mode_is_a_config_error() -> None:
    """CO-24: an unknown mode raised `ValueError`, outside the one error family."""
    with pytest.raises(ConfigError, match="registry_live_edit"):
        refused_edits({}, {}, "bogus")


def test_deploy_only_and_protected_come_from_the_settings_table(full: dict[str, Any]) -> None:
    """CO-11: `Spec.deploy_only` / `protected` were never read; the store named the rules.

    A setting newly marked in the table would still have been live-editable.
    """
    deploy_only = [n for n, s in SETTINGS.items() if s.deploy_only]
    protected = [n for n, s in SETTINGS.items() if s.protected]
    assert deploy_only and protected
    assert set(protected) == set(_PROTECTED_WHY)  # every protected setting says why
    for name in deploy_only:
        for path in (("defaults", name), ("models", "gpt20b", name)):
            after = copy.deepcopy(full)
            node = after
            for key in path[:-1]:
                node = node[key]
            node[path[-1]] = 7
            reasons = refused_edits(full, after, "all")
            assert reasons == [f"{'.'.join(path)}: `{name}` changes only with a deploy"]
    after = copy.deepcopy(full)
    after["models"]["gpt20b"]["debug_capture"] = True
    after["models"]["gpt20b"]["model_fallback"] = {"on": ["throttle"], "models": ["gpt120b"]}
    override = after["models"]["gpt120b"]["deployments"]["aws-main"].setdefault("overrides", {})
    override.setdefault("ap-south-1", {})["params"] = {"x": 1}
    reasons = refused_edits(full, after, "safe")
    assert any(r.startswith("models.gpt20b.debug_capture: `debug_capture`") for r in reasons)
    assert any("model_fallback" in r for r in reasons)
    assert any("deployment params" in r for r in reasons)
    assert refused_edits(full, after, "all") == []


def test_reload_wait_doubles_to_a_cap() -> None:
    """CO-4: a failing reload backs off (and a long interval is never shortened)."""
    assert [_reload_wait(1, n) for n in range(7)] == [1, 2, 4, 8, 16, 30, 30]
    assert _reload_wait(60, 3) == 60


# ---------------------------------------------------------------- failure paths


async def test_edit_conflict_retries_with_backoff_then_raises(
    full: dict[str, Any], top_of_range: None
) -> None:
    """CO-10: a lost compare-and-set was retried at once, and the out-of-tries path untested.

    Concurrent editors collided again on every retry and used up their tries together.
    """
    client = _Client(full, _meta())
    sleeps: list[float] = []
    store = _store(client, sleeps=sleeps, edit_retries=4)
    script = _Script(lambda: [b"conflict", 1])
    store._edit = script
    with pytest.raises(RegistryConflict, match="4 tries"):
        await store.set({"rpm": 5}, model="gpt20b", actor="op")
    assert len(script.calls) == 4
    assert sleeps == [0.04, 0.08, 0.16]


async def test_publish_conflict_retries_with_backoff_then_raises(
    full: dict[str, Any], top_of_range: None
) -> None:
    """CO-10: `publish` retried a lost race at once, as `edit` did."""
    client = _Client(None, {})
    sleeps: list[float] = []
    store = _store(client, sleeps=sleeps, edit_retries=1)
    script = _Script(lambda: [b"conflict", 1])
    store._publish = script
    with pytest.raises(RegistryConflict, match="3 tries"):
        await store.publish(full, actor="op")
    assert len(script.calls) == 3
    assert sleeps == [0.04, 0.08]


async def test_edit_then_flushed_store_is_store_unavailable(full: dict[str, Any]) -> None:
    """CO-12: a store flushed between the edit and its read-back raised `AssertionError`."""
    client = _Client(full, _meta())
    store = _store(client)

    def flush() -> list[Any]:
        client.doc, client.meta = None, {}
        return [b"ok", 2]

    store._edit = _Script(flush)
    with pytest.raises(StoreUnavailable, match="vanished"):
        await store.set({"rpm": 5}, model="gpt20b", actor="op")


async def test_reload_loop_survives_any_error(caplog: pytest.LogCaptureFixture) -> None:
    """CO-4: an error outside (Redis, OS, Config) ended the reload task silently, for good.

    Here a corrupt change counter (`int("x")`) raises `ValueError` on every reload.
    """
    client = _Client({"registry_version": 1}, {b"changes": b"x"})
    waits: list[float] = []

    async def sleep(secs: float) -> None:
        waits.append(secs)
        if len(waits) == 3:
            raise asyncio.CancelledError

    store = RegistryStore(client, "t", sleep=sleep)  # type: ignore[arg-type]  # a fake
    with caplog.at_level(logging.ERROR), pytest.raises(asyncio.CancelledError):
        await store.run(1)
    assert waits == [2, 4, 8]  # three reloads ran, each backing off further
    assert caplog.text.count("registry_reload_failed") == 3
    assert "ValueError" in caplog.text


async def test_refresh_restore_failure_is_logged_not_raised(
    full: dict[str, Any], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """CO-9: a failing restore publish raised a config error into the call path.

    `refresh()` runs from admission on `no_config`; the next reload retries the restore.
    """
    store = _store(_Client(None, {}), config=full)

    async def publish(doc: Any, *, actor: str) -> PublishResult:
        raise RegistryConflict("the registry kept changing")

    monkeypatch.setattr(store, "publish", publish)
    with caplog.at_level(logging.ERROR):
        assert await store.refresh() is None
    assert "registry_restore_failed" in caplog.text


async def test_rollback_retries_when_a_deploy_publishes_in_between(
    full: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """CO-18: a deploy between the version read and the publish made the rollback do nothing.

    It came back `ignored` and the CLI printed it as if it had worked. A long actor also went
    past the actor limit once " (rollback to N)" was added.
    """
    client = _Client(full, _meta(version=2))
    client.history["1"] = orjson.dumps(full)
    store = _store(client)
    published: list[tuple[int, str]] = []

    async def publish(doc: dict[str, Any], *, actor: str) -> PublishResult:
        published.append((doc["registry_version"], actor))
        if len(published) == 1:  # a deploy got version 3 in first
            client.meta = _meta(version=3)
            return PublishResult("ignored", 3)
        return PublishResult("published", doc["registry_version"])

    monkeypatch.setattr(store, "publish", publish)
    result = await store.rollback(1, actor="x" * 200)
    assert result == PublishResult("published", 4)
    assert [v for v, _ in published] == [3, 4]
    assert all(len(actor) <= 200 and actor.endswith("(rollback to 1)") for _, actor in published)


async def test_rollback_of_a_corrupt_history_entry_is_a_config_error(
    full: dict[str, Any],
) -> None:
    """CO-18: a corrupt history entry raised `JSONDecodeError`, outside the error family."""
    client = _Client(full, _meta())
    client.history["1"] = b"not json"
    with pytest.raises(ConfigError, match="unreadable"):
        await _store(client).rollback(1, actor="op")


async def test_log_skips_malformed_audit_entries(caplog: pytest.LogCaptureFixture) -> None:
    """CO-19: one malformed audit entry (a manual XADD) failed the whole `log()` call."""
    client = _Client(None, {})
    good = orjson.dumps([{"path": "models.m.rpm", "before": 1, "after": 2}])
    client.audit = [
        (b"3-0", {b"actor": b"op", b"diff": good}),
        (b"2-0", {b"actor": b"op", b"diff": b"nope"}),
        (b"1-0", {b"actor": b"op", b"diff": b'[{"no_path": 1}]'}),
    ]
    with caplog.at_level(logging.WARNING):
        rows = await _store(client).log(count=5, path_prefix="models")
    assert [row["id"] for row in rows] == ["3-0"]
    assert caplog.text.count("audit_entry_unreadable") == 2


async def test_block_of_an_unknown_name_lists_the_known_ones(full: dict[str, Any]) -> None:
    """CO-27: `unknown account 'x'` gave no hint of the names the registry does use."""
    client = _Client(full, _meta())
    store = _store(client)
    with pytest.raises(ConfigError, match="call LLMQueue.start"):
        _ = store.current
    await store.load()
    with pytest.raises(ConfigError, match="did you mean 'aws-main'.*known: aws-dr, aws-main"):
        await store.block(account="aws-mian", on=True, actor="op")
    with pytest.raises(ConfigError, match="known: .*ap-south-1"):
        await store.block(region="mars-1", on=True, actor="op")
