"""RQ-8 without a store: moves `MOVE` marked but nobody recorded are written by the next read.

A separate queue store cannot take the record write into `MOVE`, so `MOVE` marks each moved call
and the admin writes the records from the marks; a process that stopped in between leaves them
for the next admin read of the model, which writes, clears and audits them.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import MOVE
from llm_queue.engine.requests_admin import RequestsAdmin


class _Pipe:
    def __init__(self, replies: dict[str, dict[bytes, bytes]]) -> None:
        self.replies = replies
        self.asked: list[str] = []

    async def __aenter__(self) -> _Pipe:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def hgetall(self, key: str) -> None:
        self.asked.append(key)

    async def execute(self) -> list[dict[bytes, bytes]]:
        return [self.replies.get(key, {}) for key in self.asked]


def _admin(marks: dict[str, dict[bytes, bytes]], *, shared: bool = False) -> RequestsAdmin:
    state = MagicMock()
    queues = state if shared else MagicMock()
    queues.pipeline = lambda **_: _Pipe(marks)
    return RequestsAdmin(
        state,
        queue_client=queues,
        keys=Keys("p"),
        registry=MagicMock(audit_key="p:audit"),
        hash_id=lambda v: v,
    )


def _args(mock: AsyncMock) -> tuple[Any, ...]:
    assert mock.await_args is not None
    return tuple(mock.await_args.args)


def _view() -> Any:
    return SimpleNamespace(
        registry=SimpleNamespace(models={"small": {}}), queue_name=lambda alias: alias
    )


async def test_marked_moves_are_recorded_cleared_and_audited() -> None:
    probe = _admin({})
    marks_key = probe._queues.moved_key("small-priority")
    admin = _admin({marks_key: {b"r2": b"20-0", b"r1": b"10-0"}})
    admin._write_moved_script = write = AsyncMock(return_value=[1, 2])
    admin._clear_moved = clear = AsyncMock(return_value=2)
    admin._audit = audit = AsyncMock()  # type: ignore[method-assign]

    fixed = await admin._finish_moves(_view(), "small")

    assert fixed == {"r1", "r2"}
    keys, args = _args(write)
    assert keys == [
        admin.keys.request("r1", "job"),
        admin.keys.status("r1"),
        admin.keys.request("r2", "job"),
        admin.keys.status("r2"),
    ]
    assert args == ["p", "10-0", "20-0"]  # the priority queue's marks: class `p`
    clear.assert_awaited_once_with([marks_key], ["r1", "10-0", "r2", "20-0"])
    report = _args(audit)[0]
    assert (report.op, report.changed, report.dry_run) == ("requests_reprioritize_repair", 2, False)
    assert report.actor.startswith("repair@")


async def test_nothing_marked_writes_nothing() -> None:
    admin = _admin({})
    admin._write_moved_script = write = AsyncMock()
    admin._audit = audit = AsyncMock()  # type: ignore[method-assign]
    assert await admin._finish_moves(_view(), "small") == set()
    write.assert_not_awaited()
    audit.assert_not_awaited()


async def test_records_already_written_are_not_audited_again() -> None:
    probe = _admin({})
    admin = _admin({probe._queues.moved_key("small"): {b"r1": b"10-0"}})
    admin._write_moved_script = write = AsyncMock(return_value=[])
    admin._clear_moved = clear = AsyncMock(return_value=1)
    admin._audit = audit = AsyncMock()  # type: ignore[method-assign]
    assert await admin._finish_moves(_view(), "small") == set()
    write.assert_awaited_once()
    clear.assert_awaited_once()  # the mark goes all the same
    audit.assert_not_awaited()


async def test_one_store_moves_records_in_the_script() -> None:
    admin = _admin({}, shared=True)
    assert admin.queues_shared
    assert await admin._finish_moves(_view(), "small") == set()
    # MOVE marks only when the records live elsewhere, and the marks expire with the queue.
    assert "redis.call('HSET', KEYS[15], rid, new)" in MOVE
    assert "if prefix == '' then X.expire(KEYS[15], retention) end" in MOVE
