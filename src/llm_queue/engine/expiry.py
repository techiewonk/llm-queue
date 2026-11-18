"""How llm-queue's store keys expire: native TTLs, or the expiry index (`LLMQ_KEY_EXPIRY_MODE`).

`volatile-*` eviction policies evict only keys that have a TTL. In `swept` mode no key the
library needs has one: each expiry is a member of the sorted set `P:ttl` (score = expire-at,
store time in ms) and `TTL_SWEEP` deletes the due keys. The same keys go at the same times as
with native TTLs (late by at most one sweep), and none can be evicted (owner decisions K1-K5).

The mode is the store's, not a process's: `P:expiry` holds `native` or `swept` (no TTL) and
every script reads it. A missing key means `native` (a part used without a started engine,
which runs no sweeper). The first process to start sets it; after that only `swept` may
replace `native`, and a process whose setting differs follows the store (owner Q-6).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

KEY_EXPIRY_MODES = ("auto", "native", "swept")
NATIVE, SWEPT = "native", "swept"
SWEEP_BATCH = 500  # keys per TTL_SWEEP run (LLMQ_KEY_EXPIRY_SWEEP_BATCH)
SWEEP_ROUNDS = 100  # full batches per sweep, at most (LLMQ_KEY_EXPIRY_SWEEP_ROUNDS)
# Policies that evict only keys with a TTL: safe in `swept` mode.
VOLATILE_POLICIES = ("volatile-lru", "volatile-lfu", "volatile-ttl", "volatile-random")

# The Lua helpers every script that sets an expiry starts with. One table (`X`), not locals: a
# chunk has at most 200 locals and PICK_TAKE is close to it. `{P}` is filled in per prefix (the
# prefix rule keeps quotes and backslashes out of it).
_PRELUDE = """
local X = {idx = '{P}:ttl', mode = '{P}:expiry'}
function X.swept()
  if X.s == nil then X.s = redis.call('GET', X.mode) == 'swept' end
  return X.s
end
-- Whether the index exists, read once per script: a native-only store has none, so the
-- per-key clean-up below costs one EXISTS a script instead of a ZREM per key (gate B91).
function X.indexed()
  if X.i == nil then X.i = redis.call('EXISTS', X.idx) == 1 end
  return X.i
end
function X.now()
  if X.t == nil then
    local t = redis.call('TIME')
    X.t = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  end
  return X.t
end
-- PEXPIRE, or the key's expire-at in the index (swept). Each mode also drops the other's
-- expiry: a key written before a mode switch kept its native TTL (it fired mid-call, and stayed
-- evictable) or its old index entry (the sweep deleted it early) (rescan B3-R1).
function X.expire(key, ms)
  ms = math.floor(tonumber(ms))
  if X.swept() then
    -- As PEXPIRE: nothing for a missing key (an entry would expire a later key of that name).
    if redis.call('EXISTS', key) == 1 then
      redis.call('PERSIST', key)
      redis.call('ZADD', X.idx, X.now() + ms, key)
    end
  else
    redis.call('PEXPIRE', key, ms)
    if X.indexed() then redis.call('ZREM', X.idx, key) end
  end
end
-- PTTL: ms left, -1 with no expiry, -2 when the key is missing; either kind of expiry counts.
function X.pttl(key)
  local p = redis.call('PTTL', key)
  if p ~= -1 or not X.indexed() then return p end
  local at = redis.call('ZSCORE', X.idx, key)
  if not at then return -1 end
  return math.max(0, tonumber(at) - X.now())
end
-- SET key value [NX] with an expiry of `ms`; returns what SET returned.
function X.set(key, value, ms, nx)
  local r
  if X.swept() then
    if nx then r = redis.call('SET', key, value, 'NX') else r = redis.call('SET', key, value) end
    if r then X.expire(key, ms) end
  else
    if nx then
      r = redis.call('SET', key, value, 'NX', 'PX', math.floor(tonumber(ms)))
    else
      r = redis.call('SET', key, value, 'PX', math.floor(tonumber(ms)))
    end
    if r and X.indexed() then redis.call('ZREM', X.idx, key) end
  end
  return r
end
"""

# KEYS[1]: the key. ARGV: value, ms, '1' for NX. `X.set` in one script: a SET then a separate
# ZADD left a key with no expiry at all when the ZADD failed (review B3).
_SET = "return X.set(KEYS[1], ARGV[1], tonumber(ARGV[2]), ARGV[3] == '1') and 1 or 0"

# KEYS: the mode key. ARGV: the mode a process wants. The store's mode wins once set (owner
# Q-6): only `swept` replaces `native` (always safe), nothing else moves it. Processes with
# different explicit settings flipped it every minute, and each `native` minute on a
# `volatile-*` store left evictable keys (review A-12). Reply: {mode now, mode before ('' none)}.
_ADOPT = """
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then return {current, current} end
if (current ~= 'native' and current ~= 'swept') or ARGV[1] == 'swept' then
  redis.call('SET', KEYS[1], ARGV[1])
  return {ARGV[1], current or ''}
end
return {current, current}
"""

# KEYS: the index. ARGV: batch. Deletes the due keys of one batch; returns how many.
TTL_SWEEP = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
-- In chunks: Lua's unpack refuses about 8000 values (review B3-3).
for i = 1, #due, 1000 do
  local part = {unpack(due, i, math.min(i + 999, #due))}
  redis.call('UNLINK', unpack(part))
  redis.call('ZREM', KEYS[1], unpack(part))
end
return #due
"""


def prelude(prefix: str) -> str:
    """The Lua helpers (`X.expire`, `X.pttl`, `X.set`) for keys under `prefix`."""
    return _PRELUDE.replace("{P}", prefix)


def decide(setting: str, policy: str) -> str:
    """The mode a process asks for: the setting, or for `auto` the store's eviction policy.

    `noeviction` gets native TTLs; any other policy (`volatile-*`, `allkeys-*`, unknown) gets
    the index: safe everywhere, and `allkeys-*` stays not ready anyway (`eviction_warning`).
    """
    if setting in (NATIVE, SWEPT):
        return setting
    return NATIVE if policy == "noeviction" else SWEPT


def native(prefix: str) -> Expiry:
    """Native TTLs: what an engine part uses until the runtime gives it the store's mode."""
    return Expiry(prefix, time.time, NATIVE)


class Expiry:
    """The expiry mode of one process and the Python side of the helpers.

    Shared by every engine part that sets an expiry outside Lua (pipelines, `SET ... EX`).
    `mode` follows the store's `P:expiry`, set at `start()` and re-read each minute.
    """

    def __init__(self, prefix: str, clock: Callable[[], float], mode: str = SWEPT) -> None:
        """Keys under `prefix`; `clock` is the store time in seconds (`StoreClock.now`)."""
        self.prefix = prefix
        self.index = f"{prefix}:ttl"
        self.mode_key = f"{prefix}:expiry"
        self.mode = mode
        self._clock = clock
        self._set_scripts: dict[int, Any] = {}  # per client: `X.set` as one script
        self._adopt_scripts: dict[int, Any] = {}  # per client: `_ADOPT`

    @property
    def swept(self) -> bool:
        """True when expiries go to the index instead of native TTLs."""
        return self.mode != NATIVE

    def lua(self, script: str) -> str:
        """`script` with the helpers in front (for `register_script`)."""
        return prelude(self.prefix) + script

    def expire(self, pipe: Any, key: str, secs: float, *, grow: bool = False) -> int:
        """Queue an expiry of `secs` for `key` on a pipeline; how many commands it queued.

        Two (three with `grow`, never shorten): each mode also drops the other's expiry, as
        `X.expire` does (rescan B3-R1). Callers that read the pipeline's replies skip them.
        """
        ms = max(1, int(secs * 1000))
        if self.swept:
            pipe.persist(key)
            pipe.zadd(self.index, {key: int(self._clock() * 1000) + ms}, gt=grow)
            return 2
        if grow:  # GT alone never sets a first TTL (a key without one counts as endless)
            pipe.pexpire(key, ms, nx=True)
            pipe.pexpire(key, ms, gt=True)
            pipe.zrem(self.index, key)
            return 3
        pipe.pexpire(key, ms)
        pipe.zrem(self.index, key)
        return 2

    async def set(
        self, client: Any, key: str, value: Any, secs: float, *, nx: bool = False
    ) -> bool:
        """`SET key value [NX]` that expires in `secs`; True when written."""
        ms = max(1, int(secs * 1000))
        if not self.swept:
            written = bool(await client.set(key, value, px=ms, nx=nx))
            if written:  # an entry from a swept period would delete it early (rescan B3-R1)
                await client.zrem(self.index, key)
            return written
        script = self._set_scripts.get(id(client))
        if script is None:
            script = self._set_scripts[id(client)] = client.register_script(self.lua(_SET))
        return bool(await script(keys=[key], args=[value, ms, "1" if nx else "0"]))

    async def adopt(self, client: Any, wanted: str) -> tuple[str, str | None]:
        """Ask for `wanted`, follow the store's mode; returns (the mode now, the one replaced).

        The store's mode wins once set; only `swept` replaces `native` (owner Q-6). The mode
        replaced is None when nothing changed.
        """
        script = self._adopt_scripts.get(id(client))
        if script is None:
            script = self._adopt_scripts[id(client)] = client.register_script(_ADOPT)
        reply = await script(keys=[self.mode_key], args=[wanted])
        now, before = (v.decode() if isinstance(v, bytes) else str(v) for v in reply)
        self.mode = now
        return now, (before if before not in ("", now) else None)

    async def publish_mode(self, client: Any, mode: str) -> str | None:
        """Force `mode` as the store's (tests, an operator); the mode it replaced, if another.

        Processes follow it at their next `adopt` (each minute).
        """
        old = await client.set(self.mode_key, mode, get=True)
        self.mode = mode
        before = old.decode() if isinstance(old, bytes) else old
        return before if before not in (None, mode) else None

    async def sweep(
        self, script: Any, *, batch: int = SWEEP_BATCH, rounds: int = SWEEP_ROUNDS
    ) -> int:
        """Delete due keys, batch after batch while batches come back full; returns the count."""
        total = 0
        for _ in range(rounds):
            done = int(await script(keys=[self.index], args=[batch]))
            total += done
            if done < batch:
                break
        return total
