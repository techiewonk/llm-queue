"""0.1.0b2 speed work (plan B3-B5): round trips folded into the scripts, on Valkey and Redis.

PICK_TAKE checks callers, counts a first attempt and reports a stream's length; waiter expiry
lives in a sorted set; REGISTER_SEND registers, sends and confirms; RESULT_PUT writes the
status; and one converse costs the store round trips pinned at the end.
"""

from __future__ import annotations

import asyncio
import collections
import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest
import redis.asyncio
from engine_testkit import CHAT, Make
from test_admission import rig  # type: ignore[import-not-found]  # sibling test module

from llm_queue.engine.admission import Candidate
from llm_queue.engine.envelope import Meta, Reply
from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import QueueFull, QueueSettings, StreamQueues
from llm_queue.engine.requests import RequestStore

pytestmark = pytest.mark.integration
R1 = [Candidate("aws-a/r1")]


def _m(prefix: str, name: str) -> str:
    return f"{prefix}:{{m:m}}:{name}"


async def test_pick_take_drops_a_call_whose_callers_are_gone(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """B4 (104): CALLERS_ALIVE was a script call of its own before every admission."""
    r = await rig(store, prefix)
    keys = Keys(prefix)
    await store.set(keys.epoch, "1000")  # an old store: lapsed caller keys prove something
    await store.sadd(keys.request("gone", "callers"), "c1")
    assert (await r.ask("gone", candidates=R1, callers_ttl=30)).kind == "gone"
    await store.set(keys.caller("c1"), "1")
    assert (await r.ask("gone", candidates=R1, callers_ttl=30)).kind == "granted"
    # A store that came back less than a caller TTL ago: lapsed keys prove nothing.
    await store.sadd(keys.request("young", "callers"), "c2")
    secs, _ = await store.time()
    await store.set(keys.epoch, str(secs - 5))
    assert (await r.ask("young", candidates=R1, callers_ttl=30)).kind == "granted"


async def test_pick_take_counts_a_first_attempt_and_reports_the_stream(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """B4 (104): the retry budget's `first` and the stream's XLEN were round trips of their own."""
    r = await rig(store, prefix)
    await store.xadd(Keys(prefix).request("s", "chunks"), {"seq": 1})
    granted = await r.ask("s", candidates=R1, count_first=123, chunks=True)
    assert granted.kind == "granted"
    assert granted.extra["streamed"] == 1
    key = _m(prefix, "retries:123")
    assert await store.hget(key, "first") == b"1"
    assert 0 < await store.ttl(key) <= 180
    again = await r.ask("s", candidates=R1, count_first=123, chunks=True)  # the same owner
    assert again.extra["streamed"] == 1
    assert await store.hget(key, "first") == b"1"  # counted once, on the grant
    plain = await r.ask("p", candidates=R1)
    assert "streamed" not in plain.extra


async def test_expired_waiters_are_found_by_their_expiry(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """B5 (109): PICK_TAKE decoded the first 50 waiters of each class on every call."""
    r = await rig(store, prefix, model={"rpm": 1}, waiter_secs=0.2)
    assert (await r.ask("a", candidates=R1)).kind == "granted"
    assert (await r.ask("w", candidates=R1)).kind == "wait"
    assert await store.zscore(_m(prefix, "waitexp"), "n|w") is not None
    await asyncio.sleep(0.3)
    assert (await r.ask("x", candidates=R1)).kind == "wait"  # cleans the expired one
    assert await store.zscore(_m(prefix, "wait:n"), "w") is None
    assert not await store.hexists(_m(prefix, "waitinfo"), "w")
    assert await store.zscore(_m(prefix, "waitexp"), "n|w") is None
    assert await store.zscore(_m(prefix, "wait:n"), "x") is not None  # the live one stays


async def test_sweep_drops_expired_waiters_without_an_expiry_entry(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """B5 (109): a waiter with no `waitexp` member (older library) is still cleaned up."""
    r = await rig(store, prefix)
    await store.zadd(_m(prefix, "wait:p"), {"old": 1})
    await store.hset(_m(prefix, "waitinfo"), "old", '{"exp": 0, "deps": ["aws-a/r1"]}')
    await r.admission.sweep("m")
    assert await store.zscore(_m(prefix, "wait:p"), "old") is None
    assert not await store.hexists(_m(prefix, "waitinfo"), "old")


def _record(**kw: Any) -> dict[str, Any]:
    return {
        "caller": "c1",
        "user_hash": "u",
        "params_hash": "p",
        "model": "m",
        "priority": False,
        "app_id": False,
        **kw,
    }


async def test_register_send_registers_sends_and_confirms(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """B4 (105): REGISTER, the app flag, SEND and CONFIRM were four round trips."""
    keys = Keys(prefix)
    requests = RequestStore(store, keys)
    queues = StreamQueues(store, keys)

    async def send(rid: str, **kw: Any) -> str:
        found = await requests.register_send(
            rid,
            **_record(**kw),
            queue="q",
            queue_keys=queues.queue_keys("q"),
            bell=queues.bell("q"),
            body=rid.encode(),
            max_waiting=1,
            retention_secs=3600,
            keep_secs=10,
        )
        return found.outcome

    assert await send("a", app_id=True) == "new"
    job = await requests.job("a")
    assert (job["confirmed"], job["app"]) == ("1", "1")
    assert await store.ttl(keys.request("a", "job")) > 30  # kept for the call, not unconfirmed
    (message,), _ = await queues.receive("q", 5, QueueSettings())
    assert message.body == b"a"
    assert await store.llen(queues.bell("q")) == 1
    assert await send("a", caller="c2") == "attached"  # no second send
    assert await store.xlen(keys.queue("q", "s")) == 1
    with pytest.raises(QueueFull):
        await send("b")  # max_waiting 1: the record it made is dropped
    assert await requests.job("b") == {}
    assert not await store.exists(keys.status("b"))


async def test_register_takes_the_app_flag(store: redis.asyncio.Redis, prefix: str) -> None:
    """B4 (105): the app flag was an HSET after REGISTER."""
    requests = RequestStore(store, Keys(prefix))
    assert (await requests.register("a", **_record(app_id=True))).outcome == "new"
    assert (await requests.register("l", **_record())).outcome == "new"
    assert (await requests.job("a")).get("app") == "1"
    assert "app" not in await requests.job("l")


async def test_result_put_writes_the_final_status(store: redis.asyncio.Redis, prefix: str) -> None:
    """B3 (103): the final status was a second script after the result."""
    requests = RequestStore(store, Keys(prefix))
    reply = Reply(request_id="r", ok=True, meta=Meta())
    assert await requests.put_result(reply, final_state="done", status={"attempts": 2, "x": None})
    status = await requests.status("r")
    assert (status["state"], status["attempts"]) == ("done", "2")
    assert "x" not in status
    assert "done_at" in status
    failed = Reply(request_id="r", ok=False, error="e", meta=Meta())
    assert not await requests.put_result(failed, final_state="failed", status={"error": "e"})
    assert (await requests.status("r"))["state"] == "done"  # a final status is never replaced


def _scripts() -> dict[str, str]:
    """SHA1 -> name of every engine script, to tell the hot path from background loops."""
    from llm_queue.engine import _admission_lua, queue, requests, runtime

    out = {}
    for module in (_admission_lua, queue, requests, runtime):
        for name, value in vars(module).items():
            if name.lstrip("_").isupper() and isinstance(value, str) and "redis.call" in value:
                out[hashlib.sha1(value.encode()).hexdigest()] = name.lstrip("_")
    return out


SCRIPTS = _scripts()
# Per call, caller and worker: the only scripts of a plain converse.
HOT_PATH = {"REGISTER_SEND", "RECEIVE_FIRST", "PICK_TAKE", "RESULT_PUT", "FINISH", "ACK"}


class _Sends:
    """Counts store round trips (packed sends) and their commands, like scripts/bench.py."""

    def __init__(self) -> None:
        self.on = False
        self.sends = 0
        self.commands: collections.Counter[str] = collections.Counter()

    @contextmanager
    def window(self) -> Iterator[None]:
        self.sends, self.commands, self.on = 0, collections.Counter(), True
        try:
            yield
        finally:
            self.on = False


@pytest.fixture
def sends(monkeypatch: pytest.MonkeyPatch) -> _Sends:
    from redis.asyncio.connection import AbstractConnection

    counter = _Sends()
    real = AbstractConnection.send_packed_command

    async def send(conn: Any, command: Any, check_health: bool = True) -> None:
        if counter.on:
            counter.sends += 1
            raw = command if isinstance(command, bytes) else b"".join(command)
            parts = raw.split(b"\r\n")
            name = parts[2].decode().upper() if raw.startswith(b"*") else "?"
            if name == "EVALSHA":  # named by its script
                name = SCRIPTS.get(parts[4].decode(), "EVALSHA?")
            counter.commands[name] += 1
        await real(conn, command, check_health)

    monkeypatch.setattr(AbstractConnection, "send_packed_command", send)
    return counter


async def test_one_converse_costs_few_round_trips(make: Make, sends: _Sends) -> None:
    """B3-B5: one converse (caller and worker in one process) took ~19 store round trips."""
    engine, _ = await make()
    await engine.call("small", CHAT)  # warm: scripts loaded, clients built
    await asyncio.sleep(1.5)  # the loop idles on the doorbell (1 s pauses after 20 empty polls)
    with sends.window():
        await engine.call("small", CHAT, priority=True)
        await asyncio.sleep(0.2)  # the worker's settle after the reply
    hot = {name: n for name, n in sends.commands.items() if name in HOT_PATH}
    # One of each (the other models' idle loops receive too: RECEIVE_FIRST is not counted).
    assert hot.keys() == HOT_PATH, sends.commands
    assert all(n == 1 for name, n in hot.items() if name != "RECEIVE_FIRST"), sends.commands
    # Folded away: no REGISTER / SEND / CONFIRM, CALLERS_ALIVE, STATUS_SET, single RECEIVE,
    # nor the retry budget's HINCRBY pipeline.
    for gone in ("REGISTER", "SEND", "CONFIRM", "STATUS_SET", "RECEIVE", "HINCRBY", "HSET"):
        assert gone not in sends.commands, sends.commands
    assert sends.commands["RESULT_TAKE"] == 1, sends.commands
