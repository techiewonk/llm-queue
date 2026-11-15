"""Stream queue on Valkey and Redis (plan step A5; scenarios B42, B43, B76, B80 queue parts).

Order by stream id, return in place without a failure, a lapsed hold counts one failure, dead
letters reported to the receiver, stale receipts refused, the waiting limit, redrive.
"""

from __future__ import annotations

import asyncio

import pytest
import redis.asyncio

from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import DeadLetter, QueueFull, QueueSettings, StreamQueues

pytestmark = pytest.mark.integration
FAST = QueueSettings(visibility_secs=0.3, max_receives=3, max_waiting=0)


async def _filled(store: redis.asyncio.Redis, prefix: str, n: int) -> StreamQueues:
    queues = StreamQueues(store, Keys(prefix))
    for i in range(n):
        await queues.send("q", f"m{i}".encode(), FAST)
    return queues


async def test_order_and_ack(store: redis.asyncio.Redis, prefix: str) -> None:
    queues = await _filled(store, prefix, 5)
    first, _ = await queues.receive("q", 2, FAST)
    second, _ = await queues.receive("q", 10, FAST)
    assert [m.body for m in first] == [b"m0", b"m1"]
    assert [m.body for m in second] == [b"m2", b"m3", b"m4"]
    assert await queues.depth("q") == (5, 5, 0)
    for message in first + second:
        assert await queues.ack(message)
    assert await queues.depth("q") == (0, 0, 0)
    assert first[0].position_ms > 0


async def test_return_keeps_place_and_no_failure(store: redis.asyncio.Redis, prefix: str) -> None:
    queues = await _filled(store, prefix, 3)
    batch, _ = await queues.receive("q", 3, FAST)
    for message in batch:  # e.g. a held batch returned at shutdown
        await queues.return_(message)
    again, _ = await queues.receive("q", 3, FAST)
    assert [m.body for m in again] == [b"m0", b"m1", b"m2"]
    assert all(m.generation == 3 for m in again)  # received, returned (receipt ended), received
    for _ in range(10):  # returned many times: never dead
        for message in again:
            await queues.return_(message)
        again, dead = await queues.receive("q", 3, FAST)
        assert not dead
    assert len(again) == 3


async def test_a_returned_receipt_cannot_hide_the_message_again(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """A heartbeat extend in flight while the message was given back hid it for the whole
    visibility timeout: no pod took it and its caller hit its deadline (drain test, 1 in 12).
    """
    queues = await _filled(store, prefix, 1)
    (held,), _ = await queues.receive("q", 1, FAST)
    await queues.return_(held)
    assert not await queues.extend(held, 60)
    assert await queues.extend_many([(held, 60)]) == [False]
    assert not await queues.ack(held)
    again, _ = await queues.receive("q", 1, FAST)
    assert [m.body for m in again] == [b"m0"]


async def test_returned_message_goes_before_newer_ones(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    queues = await _filled(store, prefix, 1)
    (held,), _ = await queues.receive("q", 1, FAST)
    await queues.send("q", b"later", FAST)
    await queues.return_(held)
    order, _ = await queues.receive("q", 2, FAST)
    assert [m.body for m in order] == [b"m0", b"later"]


async def test_delayed_return(store: redis.asyncio.Redis, prefix: str) -> None:
    queues = await _filled(store, prefix, 1)
    (held,), _ = await queues.receive("q", 1, FAST)
    await queues.return_(held, delay_secs=0.4)
    assert (await queues.receive("q", 1, FAST))[0] == []
    await asyncio.sleep(0.5)
    assert len((await queues.receive("q", 1, FAST))[0]) == 1


async def test_lapsed_hold_counts_until_dead(store: redis.asyncio.Redis, prefix: str) -> None:
    queues = await _filled(store, prefix, 1)
    received, _ = await queues.receive("q", 1, FAST)
    assert len(received) == 1
    dead: list[DeadLetter] = []
    for _ in range(5):  # the worker dies every time: the hold lapses
        await asyncio.sleep(0.35)
        _, dead = await queues.receive("q", 1, FAST)
        if dead:
            break
    assert [d.body for d in dead] == [b"m0"]
    assert await queues.depth("q") == (0, 0, 1)
    letters = await queues.dead_letters("q")
    assert letters[0]["fails"] == 3
    assert await queues.redrive("q") == 1
    assert len((await queues.receive("q", 1, FAST))[0]) == 1


async def test_stale_receipt_refused(store: redis.asyncio.Redis, prefix: str) -> None:
    queues = await _filled(store, prefix, 1)
    (old,), _ = await queues.receive("q", 1, FAST)
    await asyncio.sleep(0.35)  # the hold lapses; another worker receives it
    (new,), _ = await queues.receive("q", 1, FAST)
    assert new.generation == old.generation + 1
    assert not await queues.ack(old)
    assert not await queues.extend(old, 10)
    assert not await queues.return_(old)
    assert await queues.extend(new, 10)
    assert await queues.ack(new)


async def test_failures_and_waiting_limit(store: redis.asyncio.Redis, prefix: str) -> None:
    queues = await _filled(store, prefix, 1)
    (message,), _ = await queues.receive("q", 1, FAST)
    assert await queues.fail(message, FAST) == "retry"
    (message,), _ = await queues.receive("q", 1, FAST)
    assert await queues.fail(message, FAST) == "retry"
    (message,), _ = await queues.receive("q", 1, FAST)
    assert await queues.fail(message, FAST) == "dead"
    limited = QueueSettings(max_waiting=2)
    await queues.send("q", b"a", limited)
    await queues.send("q", b"b", limited)
    with pytest.raises(QueueFull) as info:
        await queues.send("q", b"c", limited)
    assert info.value.depth == 2


async def test_receive_wait_returns_when_a_message_arrives(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    queues = StreamQueues(store, Keys(prefix))

    async def later() -> None:
        await asyncio.sleep(0.2)
        await queues.send("q", b"x", FAST)

    task = asyncio.create_task(later())
    messages, _ = await queues.receive_wait("q", 1, FAST, wait_secs=2)
    await task
    assert [m.body for m in messages] == [b"x"]
    empty, _ = await queues.receive_wait("q", 1, FAST, wait_secs=0.1)
    assert empty == []


SLOW_VIS = QueueSettings(visibility_secs=60, max_receives=3, max_waiting=0)


async def test_s11_a_lost_receive_is_reclaimed(store: redis.asyncio.Redis, prefix: str) -> None:
    """Scenario S11: a receive whose reply was lost left its messages hidden for 60 s.

    Its receiver now returns them to the queue at once (receipt-checked), and a message taken
    by another receive since is left alone.
    """
    queues = await _filled(store, prefix, 3)
    key = queues.claim_key("w1:model")
    lost, _ = await queues.receive_first(["q"], 2, SLOW_VIS, claim=(key, "n1"))  # reply "lost"
    assert [m.body for m in lost] == [b"m0", b"m1"]
    assert await queues.reclaim(key, "other") == 0  # another receive's nonce: nothing
    assert await queues.reclaim(key, "n1") == 2
    again, _ = await queues.receive_first(["q"], 3, SLOW_VIS)
    assert [m.body for m in again] == [b"m0", b"m1", b"m2"]  # visible now, in place
    for message in lost:
        assert not await queues.ack(message)  # the lost receipts are stale
    assert await queues.reclaim(key, "n1") == 0  # once


async def test_s11_a_retried_receive_hands_back_the_same_messages(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """Scenario S11: redis-py re-ran a receive whose reply was cut; the first batch was lost.

    Run again with the same nonce, the receive returns what it took, and takes nothing new.
    """
    queues = await _filled(store, prefix, 4)
    key = queues.claim_key("w1:model")
    first, _ = await queues.receive_first(["q"], 2, SLOW_VIS, claim=(key, "n1"))
    retry, _ = await queues.receive_first(["q"], 2, SLOW_VIS, claim=(key, "n1"))
    assert [(m.id, m.generation) for m in retry] == [(m.id, m.generation) for m in first]
    fresh, _ = await queues.receive_first(["q"], 5, SLOW_VIS, claim=(key, "n2"))
    assert [m.body for m in fresh] == [b"m2", b"m3"]
    for message in retry + fresh:
        assert await queues.ack(message)


async def test_fl4_a_replayed_receive_hands_back_its_dead_letters(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """FL-4: dead letters were not in the claim; a receive replayed after a lost reply (or one
    that took nothing but dead-lettered) returned no dead letter, so the caller was never told.
    """
    once = QueueSettings(visibility_secs=0.05, max_receives=1, max_waiting=0)
    queues = await _filled(store, prefix, 1)
    await queues.receive("q", 1, once)  # held; its hold lapses: the next receive counts a failure
    await asyncio.sleep(0.1)  # time is what is under test: past the visibility
    key = queues.claim_key("w1:model")
    taken, dead = await queues.receive_first(["q"], 1, once, claim=(key, "n1"))
    assert (taken, [d.body for d in dead]) == ([], [b"m0"])
    taken, dead = await queues.receive_first(["q"], 1, once, claim=(key, "n1"))  # the replay
    assert (taken, [d.body for d in dead]) == ([], [b"m0"])  # ([], []) before the fix
    assert await store.type(key) == b"string"  # FL-16: one SET, not a list of RPUSHes


async def test_fl5_a_returned_or_reclaimed_message_rings_the_doorbell(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """FL-5: RETURN and RECLAIM made a message visible without a doorbell token, so idle
    workers of other pods slept in BLPOP for their poll pause before they took it.
    """
    queues = await _filled(store, prefix, 2)
    await store.delete(queues.bell("q"))  # the sends' tokens are taken
    (held,), _ = await queues.receive("q", 1, SLOW_VIS)
    await queues.return_(held, delay_secs=5)
    assert await store.llen(queues.bell("q")) == 0  # visible later: no token yet
    (held,), _ = await queues.receive("q", 1, SLOW_VIS)
    await queues.return_(held)
    assert await queues.wait_bell(["q"], 0.5) == "q"
    key = queues.claim_key("w1:model")
    await queues.receive_first(["q"], 1, SLOW_VIS, claim=(key, "n1"))
    assert await queues.reclaim(key, "n1") == 1
    assert await queues.wait_bell(["q"], 0.5) == "q"


async def test_rv_t1_a_reclaim_leaves_a_re_received_message_alone(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """RV-T1 (S11): a message another receiver took after the lost receive's hold lapsed is
    left alone by the reclaim: its receipt stays good and it is not made visible again.
    """
    short = QueueSettings(visibility_secs=0.05, max_receives=0, max_waiting=0)
    queues = await _filled(store, prefix, 1)
    key = queues.claim_key("w1:model")
    (lost,), _ = await queues.receive_first(["q"], 1, short, claim=(key, "n1"))
    await asyncio.sleep(0.1)  # time is what is under test: the lost hold lapses
    (other,), _ = await queues.receive_first(["q"], 1, SLOW_VIS, claim=(key + "2", "m1"))
    assert other.id == lost.id
    assert await queues.reclaim(key, "n1") == 0
    assert await queues.receive("q", 1, SLOW_VIS) == ([], [])  # still hidden under `other`
    assert await queues.ack(other)


async def test_q27_a_send_keeps_messages_one_day_by_default(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """Owner decision Q27 (R3SE-4): a send with the default settings trimmed the stream at
    4 days; the default retention applied on every send is now 1 day.
    """
    assert QueueSettings().retention_secs == 86_400
    queues = StreamQueues(store, Keys(prefix))
    await queues.send("q", b"x", QueueSettings())
    assert await store.xlen(f"{prefix}:{{q:q}}:s") == 1
