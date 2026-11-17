"""CLI operator fixes (FO-5, FO-8, owner Q5), with fakes: no store is needed.

Each test pins a behaviour an operator's script relies on: who the audit names, the exit code,
and an audited cancel.
"""

from __future__ import annotations

import argparse
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest

from llm_queue.engine import cli
from llm_queue.engine.settings import InfraSettings
from llm_queue.errors import ConfigError


def test_fo5_actor_falls_back_without_a_passwd_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    """FO-5: `getpass.getuser()` raised for a UID with no passwd entry, failing every command."""

    def no_user() -> str:
        raise OSError("No username set in the environment")

    monkeypatch.setattr("getpass.getuser", no_user)
    monkeypatch.setattr("os.getuid", lambda: 12345, raising=False)
    assert cli.default_actor() == "uid:12345"
    monkeypatch.delattr("os.getuid")
    with pytest.raises(ConfigError, match="--actor"):
        cli.default_actor()


def test_fo5_actor_is_looked_up_only_when_a_command_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FO-5: read-only commands looked the user up too."""
    calls: list[int] = []

    def lookup() -> str:
        calls.append(1)
        return "ann"

    monkeypatch.setattr(cli, "default_actor", lookup)
    tool = cli.Cli(InfraSettings(store_url="redis://x"), MagicMock(), None)
    assert calls == []
    assert tool.actor == "ann"
    assert calls == [1]


@pytest.mark.parametrize(
    ("result", "code"),
    [
        ({"error": "unknown or expired request"}, 1),
        ({"model": "m", "enabled": False, "drained": False}, cli.EXIT_DRAIN_TIMEOUT),
        ({"model": "m", "enabled": False, "drained": True}, 0),
        ([{"id": "1"}], 0),
    ],
)
def test_fo8_exit_codes(result: Any, code: int) -> None:
    """FO-8: an unknown request and a timed-out `drain --wait` both exited 0."""
    assert cli.exit_code(result) == code


def test_fo8_main_returns_the_exit_code(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FO-8: `main` printed the error document and still returned 0."""

    async def unknown(*_: Any) -> Any:
        return {"error": "unknown or expired request"}

    monkeypatch.setattr(cli, "amain", unknown)
    assert cli.main(["request", "nope"], {}) == 1
    assert "unknown or expired" in capsys.readouterr().out


async def test_q5_cancel_is_confirmed_logged_and_audited() -> None:
    """Q5: an operator could not cancel a call; the CLI cancel is audited with the actor."""
    client = MagicMock()
    script = AsyncMock(return_value=[2, 0])  # CANCEL: marked, skipped (0.2.0)
    client.register_script.return_value = script
    client.xadd = AsyncMock()
    tool = cli.Cli(InfraSettings(store_url="redis://x"), client, "ann")
    parser = cli._parser()

    dry = parser.parse_args(["cancel", "r1", "r2", "--dry-run"])
    assert await tool.run(dry) == {"would_cancel": ["r1", "r2"]}
    script.assert_not_awaited()
    with pytest.raises(ConfigError, match="--yes"):
        await tool.run(parser.parse_args(["cancel", "r1"]))

    done = await tool.run(parser.parse_args(["cancel", "r1", "r2", "r1", "--yes"]))
    assert done == {"cancelled": 2, "requested": 2, "actor": "ann"}
    script.assert_awaited_once()
    (key, fields), _ = client.xadd.await_args
    assert key == tool.registry.audit_key
    assert (fields["actor"], fields["op"]) == ("ann", "cancel")
    assert [d["path"] for d in orjson.loads(fields["diff"])] == ["requests.r1", "requests.r2"]


async def test_q5_cancel_bounds_its_ids() -> None:
    """Q5: one command takes a bounded list of ids of bounded length."""
    args = argparse.Namespace(request_ids=["x" * 300], yes=True, dry_run=False)
    tool = cli.Cli(InfraSettings(store_url="redis://x"), MagicMock(), "ann")
    with pytest.raises(ConfigError, match="request ids"):
        await tool._cancel(args)


def test_fo25_caller_data_shaped_like_the_bytes_marker_survives() -> None:
    """FO-25: a caller's own `{"__b64__": "..."}` was decoded, and bad base64 failed parse."""
    from llm_queue.engine import envelope

    params = {
        "messages": [{"content": [{"image": {"source": {"bytes": b"\x89PNG"}}}]}],
        "data": {"__b64__": "not base64 !!"},
        "odd": {envelope.B64: "%%%"},
    }
    sent = envelope.Envelope(
        request_id="r", op="converse", model="m", params=params, caller="c", timeout_secs=5
    )
    got = envelope.parse(sent.dumps()).params
    assert got["messages"][0]["content"][0]["image"]["source"]["bytes"] == b"\x89PNG"
    assert got["data"] == {"__b64__": "not base64 !!"}
    assert got["odd"] == {envelope.B64: "%%%"}
