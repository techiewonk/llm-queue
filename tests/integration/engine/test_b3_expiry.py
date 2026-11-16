"""Key expiry modes (0.1.0b3, owner K4): the Lua and Python helpers in native and swept mode.

Swept mode must leave no native TTL on what it writes (a `volatile-*` store cannot evict it) and
`TTL_SWEEP` must delete exactly the due keys.
"""

from __future__ import annotations

import asyncio
import time

import redis.asyncio
from engine_testkit import Make

from llm_queue.engine.expiry import NATIVE, SWEPT, TTL_SWEEP, Expiry, decide
from llm_queue.engine.store_checks import eviction_warning

_PROBE = """
X.expire(KEYS[1], tonumber(ARGV[1]))
local set = X.set(KEYS[2], 'v', tonumber(ARGV[1]), true)
local again = X.set(KEYS[2], 'w', tonumber(ARGV[1]), true)
return {X.pttl(KEYS[1]), set and 1 or 0, again and 1 or 0, X.pttl(KEYS[3])}
"""


async def _run(store: redis.asyncio.Redis, prefix: str, mode: str) -> tuple[Expiry, list[int]]:
    expiry = Expiry(prefix, time.time)
    await expiry.publish_mode(store, mode)
    await store.set(f"{prefix}:a", "1")
    probe = store.register_script(expiry.lua(_PROBE))
    keys = [f"{prefix}:a", f"{prefix}:b", f"{prefix}:missing"]
    reply = await probe(keys=keys, args=[60_000])
    return expiry, [int(v) for v in reply]


async def test_native_mode_uses_native_ttls(store: redis.asyncio.Redis, prefix: str) -> None:
    """Native: PEXPIRE / SET PX as before 0.1.0b3; nothing goes to the index."""
    _, (pttl, set_ok, again, missing) = await _run(store, prefix, NATIVE)
    assert 0 < pttl <= 60_000
    assert (set_ok, again, missing) == (1, 0, -2)
    assert await store.pttl(f"{prefix}:b") > 0
    assert await store.zcard(f"{prefix}:ttl") == 0


async def test_swept_mode_leaves_no_native_ttl(store: redis.asyncio.Redis, prefix: str) -> None:
    """Swept: the keys have no TTL (not evictable); the index holds their expire-at."""
    _, (pttl, set_ok, again, missing) = await _run(store, prefix, SWEPT)
    assert 0 < pttl <= 60_000  # X.pttl reads the index
    assert (set_ok, again, missing) == (1, 0, -2)
    for key in ("a", "b"):
        assert await store.pttl(f"{prefix}:{key}") == -1
    assert await store.zcard(f"{prefix}:ttl") == 2


async def test_missing_mode_key_means_native(store: redis.asyncio.Redis, prefix: str) -> None:
    """No `P:expiry` (no engine started yet): native TTLs, so keys expire without a sweeper."""
    expiry = Expiry(prefix, time.time)
    probe = store.register_script(expiry.lua("X.expire(KEYS[1], 60000) return 1"))
    await store.set(f"{prefix}:a", "1")
    await probe(keys=[f"{prefix}:a"])
    assert await store.pttl(f"{prefix}:a") > 0


async def test_sweep_deletes_only_due_keys(store: redis.asyncio.Redis, prefix: str) -> None:
    """Due keys go with their index entries; later ones stay; batches repeat while full."""
    expiry = Expiry(prefix, time.time, SWEPT)
    now_ms = int(time.time() * 1000)
    due = {f"{prefix}:due{i}": now_ms - 1_000 for i in range(7)}
    await store.mset(dict.fromkeys(due, "1"))
    await store.set(f"{prefix}:later", "1")
    await store.zadd(expiry.index, {**due, f"{prefix}:later": now_ms + 600_000})
    script = store.register_script(TTL_SWEEP)
    assert await expiry.sweep(script, batch=3) == 7
    assert await store.exists(*due) == 0
    assert await store.exists(f"{prefix}:later") == 1
    assert await store.zcard(expiry.index) == 1


async def test_python_helpers_follow_the_mode(store: redis.asyncio.Redis, prefix: str) -> None:
    """`Expiry.set` / `Expiry.expire`: native TTL or index entry by mode."""
    expiry = Expiry(prefix, time.time, NATIVE)
    assert await expiry.set(store, f"{prefix}:n", "1", 30, nx=True)
    assert await store.pttl(f"{prefix}:n") > 0
    await expiry.publish_mode(store, SWEPT)  # `set` runs X.set: the store's mode
    assert await expiry.set(store, f"{prefix}:s", "1", 30, nx=True)
    assert not await expiry.set(store, f"{prefix}:s", "2", 30, nx=True)
    async with store.pipeline(transaction=False) as pipe:
        expiry.expire(pipe, f"{prefix}:n", 30)
        await pipe.execute()
    assert await store.pttl(f"{prefix}:s") == -1
    assert await store.zscore(expiry.index, f"{prefix}:s") is not None
    assert await store.zscore(expiry.index, f"{prefix}:n") is not None


async def test_publish_mode_reports_a_change(store: redis.asyncio.Redis, prefix: str) -> None:
    """The first publish and a repeat report nothing; another mode reports the old one."""
    expiry = Expiry(prefix, time.time)
    assert await expiry.publish_mode(store, NATIVE) is None
    assert await expiry.publish_mode(store, NATIVE) is None
    assert await expiry.publish_mode(store, SWEPT) == NATIVE


def test_decide_and_readiness_rule() -> None:
    """`auto` follows the policy; `volatile-*` is safe only when swept; `allkeys-*` never."""
    assert decide("auto", "noeviction") == NATIVE
    assert decide("auto", "volatile-lru") == SWEPT
    assert decide("auto", "unknown") == SWEPT
    assert decide("native", "volatile-lru") == NATIVE
    assert eviction_warning("volatile-lru", SWEPT) is None
    assert eviction_warning("volatile-lru", NATIVE)
    assert eviction_warning("allkeys-lru", SWEPT)
    assert eviction_warning("noeviction", NATIVE) is None


async def test_sweep_unpacks_large_batches_in_chunks(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """Review B3-3: one batch of 2,500 due keys (Lua unpack refuses about 8,000 at once)."""
    expiry = Expiry(prefix, time.time, SWEPT)
    past = int(time.time() * 1000) - 1_000
    keys = [f"{prefix}:big{i}" for i in range(2_500)]
    await store.mset(dict.fromkeys(keys, "1"))
    await store.zadd(expiry.index, dict.fromkeys(keys, past))
    script = store.register_script(TTL_SWEEP)
    assert await expiry.sweep(script, batch=2_500) == 2_500
    assert await store.zcard(expiry.index) == 0
    assert await store.exists(*keys[:100]) == 0


async def test_swept_expire_skips_a_missing_key(store: redis.asyncio.Redis, prefix: str) -> None:
    """As PEXPIRE: no index entry for a key that is not there (it would expire a later one)."""
    expiry = Expiry(prefix, time.time)
    await expiry.publish_mode(store, SWEPT)
    probe = store.register_script(expiry.lua("X.expire(KEYS[1], 60000) return 1"))
    await probe(keys=[f"{prefix}:gone"])
    assert await store.zscore(expiry.index, f"{prefix}:gone") is None


async def test_switching_modes_moves_a_key_to_the_new_expiry(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """Rescan B3-R1: after a switch a key kept the old mode's expiry (fired early, evictable)."""
    expiry = Expiry(prefix, time.time)
    key = f"{prefix}:k"
    probe = store.register_script(expiry.lua("X.expire(KEYS[1], 60000) return X.pttl(KEYS[1])"))
    await store.set(key, "1")
    await expiry.publish_mode(store, NATIVE)
    await probe(keys=[key])
    assert await store.pttl(key) > 0
    await expiry.publish_mode(store, SWEPT)  # native -> swept: the TTL goes, the index has it
    assert 0 < int(await probe(keys=[key])) <= 60_000
    assert await store.pttl(key) == -1
    assert await store.zscore(expiry.index, key) is not None
    await expiry.publish_mode(store, NATIVE)  # swept -> native: the entry goes
    await probe(keys=[key])
    assert await store.pttl(key) > 0
    assert await store.zscore(expiry.index, key) is None


async def test_native_pttl_reads_an_indexed_expiry(store: redis.asyncio.Redis, prefix: str) -> None:
    """A key indexed while swept still shows its time left in native mode (claim refresh)."""
    expiry = Expiry(prefix, time.time)
    await expiry.publish_mode(store, NATIVE)
    await store.set(f"{prefix}:j", "1")
    await store.zadd(expiry.index, {f"{prefix}:j": int(time.time() * 1000) + 30_000})
    probe = store.register_script(expiry.lua("return X.pttl(KEYS[1])"))
    assert 0 < int(await probe(keys=[f"{prefix}:j"])) <= 30_000


async def test_a_caller_only_process_sweeps_with_its_own_lock(make: Make) -> None:
    """Callers write indexed keys too, and must not take the worker sweep's lock (B3-R2)."""
    engine, _ = await make(worker=False)
    key = f"{engine.keys.prefix}:due"
    await engine.state.set(key, "1")
    await engine.state.zadd(engine.expiry.index, {key: int(time.time() * 1000) - 1_000})
    for _ in range(40):
        if not await engine.state.exists(key):
            break
        await asyncio.sleep(0.1)
    assert not await engine.state.exists(key)
    assert not await engine.state.exists(f"{engine.keys.prefix}:lock:sweep")


async def test_a_worker_only_process_reads_its_results_at_once(make: Make) -> None:
    """0.1.1: with no result reader a worker-only process waited for the poll safety net."""
    import dataclasses

    from engine_testkit import CHAT

    engine, _ = await make(caller=False)
    engine.settings = dataclasses.replace(engine.settings, poll_secs=30)
    started = time.monotonic()
    reply = await engine.call("small", CHAT)
    assert reply.ok
    assert time.monotonic() - started < 5
