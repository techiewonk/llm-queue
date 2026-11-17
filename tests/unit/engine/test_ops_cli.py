"""The generated CLI with fakes (FO-5, FO-8, D3): no store is needed.

Each test pins a behaviour an operator's script relies on: who the audit names, the exit code,
and that a change is only a report until `--yes`.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest

from llm_queue.engine import cli
from llm_queue.engine.admin import AdminContext, AdminServices
from llm_queue.engine.admin.ops import Op
from llm_queue.engine.audit import Audit
from llm_queue.engine.keys import Keys
from llm_queue.errors import ConfigError


class _Admin:
    """Records each call: `(op id, kwargs)`."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def method(self, op: Op) -> Any:
        async def run(**kw: Any) -> Any:
            self.calls.append((op.id, kw))
            return {"ok": True}

        return run


async def _run(argv: list[str], actor: str | None = "ann") -> tuple[str, dict[str, Any]]:
    admin = _Admin()
    args = cli._parser().parse_args(argv)
    await cli.run(admin, args.admin_op, args, actor)
    return admin.calls[-1]


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


async def test_fo5_actor_is_looked_up_only_when_a_command_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FO-5: read-only commands looked the user up too."""
    calls: list[int] = []

    def lookup() -> str:
        calls.append(1)
        return "ann"

    monkeypatch.setattr(cli, "default_actor", lookup)
    await _run(["status"], actor=None)
    assert calls == []
    _, kw = await _run(["purge", "model", "m"], actor=None)
    assert (calls, kw["actor"]) == ([1], "ann")


async def test_d3_a_change_is_a_dry_run_until_yes() -> None:
    """D3: `--dry-run` is gone; without `--yes` every change only reports."""
    assert (await _run(["dlq", "purge", "--model", "m"]))[1]["dry_run"] is True
    assert (await _run(["dlq", "purge", "--model", "m", "--yes"]))[1]["dry_run"] is False
    with pytest.raises(SystemExit):
        cli._parser().parse_args(["dlq", "purge", "--model", "m", "--dry-run"])


async def test_arguments_take_the_table_forms() -> None:
    op, kw = await _run(["registry", "set", "--model", "m", "rpm=400", "enabled=true", "x=a b"])
    assert (op, kw["values"], kw["model"]) == (
        "registry.set",
        {"rpm": 400, "enabled": True, "x": "a b"},
        "m",
    )
    _, kw = await _run(["requests", "cancel", "--id", "a", "--id", "b", "--state", "all"])
    assert (kw["request_ids"], kw["state"]) == (["a", "b"], "all")
    _, kw = await _run(["purge", "tenant", "acme", "--remove", "keys, queued"])
    assert kw["remove"] == ["keys", "queued"]
    _, kw = await _run(["purge", "deployment", "m", "a/r", "--no-wait"])
    assert kw["wait"] is False
    _, kw = await _run(["blocks", "kill-switch", "on"])
    assert kw["on"] is True
    _, kw = await _run(["deployments", "get", "m", "acct/us-east-1"])
    assert kw == {"model": "m", "deployment": "acct/us-east-1"}
    with pytest.raises(ConfigError, match="path=value"):
        await _run(["registry", "set", "rpm"])


@pytest.mark.parametrize(
    ("result", "code"),
    [
        ({"error": "not found"}, 1),
        ({"model": "m", "enabled": False, "drained": False}, cli.EXIT_DRAIN_TIMEOUT),
        ({"model": "m", "enabled": False, "drained": True}, 0),
        ({"model": "m", "enabled": False, "drained": None}, 0),
        ([{"id": "1"}], 0),
    ],
)
def test_fo8_exit_codes(result: Any, code: int) -> None:
    """FO-8: an unknown request and a timed-out drain both exited 0."""
    assert cli.exit_code(result) == code


def test_fo8_main_returns_the_exit_code(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FO-8: `main` printed the error document and still returned 0."""

    async def unknown(*_: Any) -> Any:
        return {"error": "not found (unknown or expired)"}

    monkeypatch.setattr(cli, "amain", unknown)
    assert cli.main(["requests", "get", "nope"], {}) == 1
    assert "unknown or expired" in capsys.readouterr().out


def _admin_over(client: Any) -> AdminServices:
    keys = Keys("llmq")
    ctx = AdminContext(
        state=client, queue_client=None, keys=keys, registry=MagicMock(), queues=MagicMock(),
        status=MagicMock(), purge=MagicMock(), audit=Audit(client, "llmq:cfg:audit"),
        hash_id=lambda v: v, queue_prefix="", status_ttl_secs=60,
    )  # fmt: skip
    return AdminServices(ctx)


class _Pipe:
    """A pipeline whose replies are given: one (job state, cancel mark) pair per id."""

    def __init__(self, replies: list[Any]) -> None:
        self.replies = replies

    async def __aenter__(self) -> _Pipe:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def hget(self, *_: Any) -> None:
        return None

    def exists(self, *_: Any) -> None:
        return None

    async def execute(self) -> list[Any]:
        return self.replies


async def test_cancel_by_ids_is_dry_by_default_filters_and_is_audited() -> None:
    """Q5 / D9: an operator cancel is audited; `state` waiting leaves running calls alone."""
    client = MagicMock()
    script = AsyncMock(return_value=[2, 0])  # CANCEL: [marked, started]
    client.register_script.return_value = script
    client.xadd = AsyncMock()
    replies = [b"queued", 0, b"running", 0, b"done", 0, b"queued", 1, None, 0]
    client.pipeline.return_value = _Pipe(replies)
    admin = _admin_over(client)
    ids = ["q", "r", "d", "c", "gone"]

    dry = await admin.requests.cancel(request_ids=ids, actor="ann")
    assert (dry["dry_run"], dry["matched"], dry["changed"]) == (True, 1, 0)
    assert dry["skipped"] == {"started": 1, "final": 1, "already_cancelling": 1, "unknown": 1}
    script.assert_not_awaited()
    client.xadd.assert_not_awaited()

    done = await admin.requests.cancel(request_ids=ids, state="all", actor="ann", dry_run=False)
    assert (done["matched"], done["by_state"]) == (2, {"waiting": 1, "running": 1})
    script.assert_awaited_once()
    (key, fields), _ = client.xadd.await_args
    assert (key, fields["actor"], fields["op"]) == ("llmq:cfg:audit", "ann", "requests_cancel")
    assert orjson.loads(fields["diff"])[0]["after"]["matched"] == 2


async def test_cancel_bounds_its_ids_and_needs_the_service_for_the_rest() -> None:
    from llm_queue.errors import AdminRefused

    admin = _admin_over(MagicMock())
    with pytest.raises(ConfigError, match="request ids"):
        await admin.requests.cancel(request_ids=["x" * 300], actor="ann")
    with pytest.raises(AdminRefused, match="requests service not available"):
        await admin.requests.cancel("m", actor="ann")


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
