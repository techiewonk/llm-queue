"""How llm-queue's store keys expire: native TTLs, or the expiry index (`LLMQ_KEY_EXPIRY`).

`volatile-*` eviction policies evict only keys that have a TTL. In `swept` mode no key the
library needs has one: each expiry is a member of the sorted set `P:ttl` (score = expire-at,
store time in ms) and `TTL_SWEEP` deletes the due keys. The same keys go at the same times as
with native TTLs (late by at most one sweep), and none can be evicted (owner decisions K1-K5).

The mode is the store's, not a process's: `P:expiry` holds `native` or `swept` (no TTL) and
every script reads it. A missing key means `native` (a part used without a started engine,
which runs no sweeper); the engine publishes the mode at start, each minute and after a reset.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

KEY_EXPIRY_MODES = ("auto", "native", "swept")
NATIVE, SWEPT = "native", "swept"
SWEEP_BATCH = 500  # keys per TTL_SWEEP run (LLMQ_TTL_SWEEP_BATCH)
SWEEP_ROUNDS = 20  # full batches per sweep, at most (LLMQ_TTL_SWEEP_ROUNDS)
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
function X.now()
  if X.t == nil then
    local t = redis.call('TIME')
    X.t = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  end
  return X.t
end
-- PEXPIRE, or the key's expire-at in the index (swept).
function X.expire(key, ms)
  ms = math.floor(tonumber(ms))
  if X.swept() then redis.call('ZADD', X.idx, X.now() + ms, key)
  else redis.call('PEXPIRE', key, ms) end
end
-- PTTL: ms left, -1 with no expiry, -2 when the key is missing.
function X.pttl(key)
  if not X.swept() then return redis.call('PTTL', key) end
  if redis.call('EXISTS', key) == 0 then return -2 end
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
  elseif nx then
    r = redis.call('SET', key, value, 'NX', 'PX', math.floor(tonumber(ms)))
  else
    r = redis.call('SET', key, value, 'PX', math.floor(tonumber(ms)))
  end
  return r
end
"""

# KEYS: the index. ARGV: batch. Deletes the due keys of one batch; returns how many.
TTL_SWEEP = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
if #due == 0 then return 0 end
redis.call('UNLINK', unpack(due))
redis.call('ZREM', KEYS[1], unpack(due))
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

    @property
    def swept(self) -> bool:
        """True when expiries go to the index instead of native TTLs."""
        return self.mode != NATIVE

    def lua(self, script: str) -> str:
        """`script` with the helpers in front (for `register_script`)."""
        return prelude(self.prefix) + script

    def expire(self, pipe: Any, key: str, secs: float, *, grow: bool = False) -> None:
        """Queue an expiry of `secs` for `key` on a pipeline; `grow`: never shorten it."""
        ms = max(1, int(secs * 1000))
        if self.swept:
            pipe.zadd(self.index, {key: int(self._clock() * 1000) + ms}, gt=grow)
        elif grow:  # GT alone never sets a first TTL (a key without one counts as endless)
            pipe.pexpire(key, ms, nx=True)
            pipe.pexpire(key, ms, gt=True)
        else:
            pipe.pexpire(key, ms)

    async def set(
        self, client: Any, key: str, value: Any, secs: float, *, nx: bool = False
    ) -> bool:
        """`SET key value [NX]` that expires in `secs`; True when written."""
        ms = max(1, int(secs * 1000))
        if not self.swept:
            return bool(await client.set(key, value, px=ms, nx=nx))
        if not await client.set(key, value, nx=nx):
            return False
        await client.zadd(self.index, {key: int(self._clock() * 1000) + ms})
        return True

    async def publish_mode(self, client: Any, mode: str) -> str | None:
        """Make `mode` the store's; returns the mode it replaced when it was another one."""
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
