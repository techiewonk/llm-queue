"""Per-request records: the duplicate-id claim, the result and the request status.

Results are written once and announced to the caller in the same script; a final status is
never replaced. Used by the runtime on both the caller and the worker side.
"""

# A call's record is written only if absent when it is sent and confirmed only after the send
# succeeded, so a failed send never leaves a phantom (plan §3.18 R23). A library id's result goes
# seconds after its first read (FL-2), an app id's is kept `result_ttl_secs`. Scripts use the
# store's clock.

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NamedTuple

import orjson
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import OutOfMemoryError, ReadOnlyError, ResponseError
from redis.exceptions import TimeoutError as StoreTimeoutError

from llm_queue._logging import get_logger
from llm_queue.engine._admission_lua import _COMMON, _SETTLE
from llm_queue.engine._codec import text as _text
from llm_queue.engine.envelope import Reply
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.expiry import native, prelude
from llm_queue.engine.keys import Keys
from llm_queue.engine.offload import MARKER, ResultOffload
from llm_queue.engine.queue import SEND_FN, QueueFull
from llm_queue.errors import ConfigError

if TYPE_CHECKING:
    import redis.asyncio

logger = get_logger(__name__)
FINAL_STATES = ("done", "failed", "expired", "cancelled")
_STORE_ERRORS = (StoreConnectionError, StoreTimeoutError, ReadOnlyError, OSError)
# A result whose offload write failed is kept inline up to this size (the store takes 512 MB;
# a result lives `result_ttl_secs`). Above it the caller gets `result_expired`.
_INLINE_MAX_BYTES = 64 * 1024 * 1024
# An offload write slower than this falls back inline: a slow S3 held a finished call past
# its caller's deadline (R3JE-30).
OFFLOAD_PUT_SECS = 15.0
# A failed offload read is tried this many times, these seconds apart, before the caller gets
# a retryable failure (R3JE-28).
_GET_RETRY_SECS = (0.2, 0.5)
# How long the store remembers that this fleet offloads results (refreshed on every start
# and offloaded write): a pod without the offload then refuses to start (Q24).
OFFLOAD_FLAG_SECS = 7 * 86_400
# Prepared bodies of results whose store write failed, kept for the retry (oldest dropped).
_UNSENT_MAX = 64

_NOW_MS = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
"""

# KEYS: job, callers, status, result. ARGV: user hash, params hash, caller, unconfirmed ttl,
# status ttl, model, priority, takeover grace secs, app id ('1': kept after its first read);
# `register(caller_ttl, lease_ms)` takes the caller key's TTL and the worker lease (their ARGV
# places differ per script).
# Returns new | attached | takeover | done | expired | conflict | cancelled. Only a success is
# replayed (owner decision Q2): an id whose last run ended failed, expired or cancelled runs
# again (`new`), its old result, status and cancel mark dropped in the same script. `nonce`:
# one registration's random token, kept with a `new` / `takeover` (`rn`, `ro`); the same nonce
# again is redis-py replaying the script after a lost reply and gets the first outcome (A-5:
# it answered `attached` to the caller's own record, which then sent nothing for ~6 s).
_REGISTER_FN = """
local function register(caller_ttl, lease_ms, nonce)
local job = redis.call('HGETALL', KEYS[1])
local clock = redis.call('TIME')
local now = tonumber(clock[1])
local cancel_key = string.sub(KEYS[1], 1, -4) .. 'cancel'
-- Every send and attach refreshes the caller's liveness key (owner Q4): a live caller whose
-- heartbeat was late was taken for gone and its queued calls dropped (R3FM-1, R3RC-3).
if tonumber(caller_ttl or '') and tonumber(caller_ttl) > 0 then
  local prefix = string.match(KEYS[1], '^(.*):{r:')
  X.set(prefix .. ':caller:' .. ARGV[3], '1', tonumber(caller_ttl) * 1000)
end
-- The callers set lives as long as the record: an attach made it a set with no TTL (S9).
local function keep_callers()
  local ttl = X.pttl(KEYS[1])
  if ttl > 0 then X.expire(KEYS[2], ttl) end
end
local function create()
  redis.call('HSET', KEYS[1], 'state', 'queued', 'user', ARGV[1], 'params', ARGV[2],
    'caller', ARGV[3], 'confirmed', '0', 'created', now, 'rn', nonce or '', 'ro', 'new')
  if ARGV[9] == '1' then redis.call('HSET', KEYS[1], 'app', '1') end
  X.expire(KEYS[1], tonumber(ARGV[4]) * 1000)
  redis.call('SADD', KEYS[2], ARGV[3])
  X.expire(KEYS[2], tonumber(ARGV[4]) * 1000)
  if redis.call('HGET', KEYS[3], 'state') == false then
    local t = redis.call('TIME')
    redis.call('HSET', KEYS[3], 'state', 'waiting', 'model', ARGV[6], 'priority', ARGV[7],
      'enqueued_at', t[1] .. '.' .. string.format('%06d', tonumber(t[2])))
    X.expire(KEYS[3], tonumber(ARGV[5]) * 1000)
  end
  return 'new'
end
if #job == 0 then
  redis.call('DEL', cancel_key)  -- a cancel left by an earlier run of this id
  return create()
end
local fields = {}
for i = 1, #job, 2 do fields[job[i]] = job[i + 1] end
if nonce and nonce ~= '' and fields['rn'] == nonce then return fields['ro'] end
if fields['params'] == nil then
  -- A record a worker's claim made (its caller's record was gone): the call is running;
  -- attach to it rather than call it a conflict.
  redis.call('HSET', KEYS[1], 'user', ARGV[1], 'params', ARGV[2], 'caller', ARGV[3],
    'confirmed', '1', 'created', now)
  redis.call('SADD', KEYS[2], ARGV[3])
  keep_callers()
  return 'attached'
end
if fields['user'] ~= ARGV[1] or fields['params'] ~= ARGV[2] then return 'conflict' end
local state = fields['state']
if state == 'done' then
  if redis.call('EXISTS', KEYS[4]) == 1 then return 'done' end
  return 'expired'
end
if state == 'failed' or state == 'expired' or state == 'cancelled' then
  -- Q2: run again, from a clean slate (a stream's old chunks too: a new run would resume
  -- after them). The key names of one request share the job key's base.
  local base = string.sub(KEYS[1], 1, -4)
  redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4], base .. 'cancel', base .. 'chunks')
  return create()
end
-- A cancel is pending: it wins for every caller of the id (owner Q3), an attach after it too.
if redis.call('EXISTS', cancel_key) == 1 then return 'cancelled' end
-- An unconfirmed record older than the grace: its send failed or its sender died; take over.
if fields['confirmed'] == '0' and now - tonumber(fields['created'] or '0') >= tonumber(ARGV[8]) then
  redis.call('HSET', KEYS[1], 'caller', ARGV[3], 'created', now, 'rn', nonce or '',
    'ro', 'takeover')
  redis.call('SADD', KEYS[2], ARGV[3])
  keep_callers()
  return 'takeover'
end
-- A direct run whose process died (AMEND FE-5: killed, OOM, node lost): its record stayed
-- confirmed with no result, and every caller of the id waited to its deadline for up to the
-- claim's TTL (1 h). Taken over when the runner's liveness key is gone and its claim had no
-- heartbeat for a lease (or it never started running). A live runner renews both.
if fields['direct'] == '1' and fields['caller'] ~= ARGV[3] and tonumber(lease_ms or '') then
  local prefix = string.match(KEYS[1], '^(.*):{r:')
  if redis.call('EXISTS', prefix .. ':caller:' .. fields['caller']) == 0 then
    local now_ms = now * 1000 + math.floor(tonumber(clock[2]) / 1000)
    if state ~= 'running' or now_ms - tonumber(fields['hb'] or '0') >= tonumber(lease_ms) then
      redis.call('HSET', KEYS[1], 'caller', ARGV[3], 'created', now, 'confirmed', '0',
        'state', 'queued', 'rn', nonce or '', 'ro', 'takeover')
      redis.call('HDEL', KEYS[1], 'direct', 'owner', 'hb')
      redis.call('SADD', KEYS[2], ARGV[3])
      keep_callers()
      return 'takeover'
    end
  end
end
redis.call('SADD', KEYS[2], ARGV[3])
keep_callers()
return 'attached'
end
"""

REGISTER = _REGISTER_FN + "return register(ARGV[10], ARGV[11], ARGV[12])\n"

# The request index per model and tenant (0.2.0 T1): `P:{m:ALIAS}:tq:H` (request id -> the
# call's deadline, store ms) and `P:{m:ALIAS}:tq` (tenant hash -> its latest deadline). Written
# in the script that confirms a call (no extra round trip), removed by the script that writes
# its final state. Lapsed entries are pruned on each add; the keys live 60 s past their latest
# deadline (grow-only). The job record keeps where its entry is: `ix` (the `tq:H` key), `ixh`
# (H), `mid` (the stream message id, '' for a direct run) and `q` (class `p` / `n`). `names`
# (optional): `P:{m:ALIAS}:tqn`, the tenant's name beside its hash (`LLMQ_TENANT_NAMES_ENABLED`).
_INDEX_FN = """
local function index_add(job, ix, ixs, rid, h, keep_ms, mid, cls, names, name)
  local now = X.now()
  local dl = now + keep_ms
  -- A takeover by a caller of another tenant: the old tenant's entry goes (RQ-9), or the call
  -- was listed twice and the old tenant's cancel reached it.
  local old = redis.call('HGET', job, 'ix')
  if old and old ~= ix then redis.call('ZREM', old, rid) end
  redis.call('HSET', job, 'ix', ix, 'ixh', h, 'mid', mid, 'q', cls)
  redis.call('ZADD', ix, dl, rid)
  redis.call('ZREMRANGEBYSCORE', ix, '-inf', '(' .. now)
  redis.call('ZADD', ixs, 'GT', dl, h)
  local has_names = names and names ~= ''
  if has_names then
    -- Tenants whose last call lapsed leave the names hash with the index (RQ-14): it only
    -- grew while traffic went on.
    local lapsed = redis.call('ZRANGEBYSCORE', ixs, '-inf', '(' .. now)
    if #lapsed > 0 then redis.call('HDEL', names, unpack(lapsed)) end
  end
  redis.call('ZREMRANGEBYSCORE', ixs, '-inf', '(' .. now)
  local ms = keep_ms + 60000
  if X.pttl(ix) < ms then X.expire(ix, ms) end
  if X.pttl(ixs) < ms then X.expire(ixs, ms) end
  if has_names and name and name ~= '' then
    redis.call('HSET', names, h, name)
    if X.pttl(names) < ms then X.expire(names, ms) end
  end
end
local function index_drop(job, rid)
  local ix = redis.call('HGET', job, 'ix')
  if ix then redis.call('ZREM', ix, rid) end
end
"""

# KEYS: REGISTER's, then the queue's keys (stream first, `_PARTS` order), then (index on) the
# request index `tq:H`, the tenant index `tq` and (names on) `tqn`. ARGV: REGISTER's, then body,
# max waiting, retention ms, doorbell key, keep secs, caller ttl, lease ms, the registration
# nonce, then (index on) the tenant hash, the index keep ms, the class (`p` / `n`) and the
# tenant name ('' for none). Register, send a new call and confirm it in one round trip when
# the queues share the state store (three before). A full queue drops the record it just made
# and indexes nothing. A replay (same nonce) answers the first outcome and its message id
# (`rm`), sending nothing again. Reply: {outcome}, {outcome, message id} or {'full', depth}.
REGISTER_SEND = (
    _NOW_MS
    + SEND_FN
    + _REGISTER_FN
    + _INDEX_FN
    + """
if ARGV[17] ~= '' and redis.call('HGET', KEYS[1], 'rn') == ARGV[17] then
  local first = redis.call('HMGET', KEYS[1], 'ro', 'rm')
  if first[2] then return {first[1], first[2]} end
end
local outcome = register(ARGV[15], ARGV[16], ARGV[17])
if outcome ~= 'new' and outcome ~= 'takeover' then return {outcome} end
local sent = send(KEYS[5], ARGV[10], tonumber(ARGV[11]), tonumber(ARGV[12]), ARGV[13])
if sent[1] == 'full' then
  -- Only a record this call just made goes (FL-25): on a takeover the first sender's message
  -- may be in the stream, and dropping its record left that call running for nobody.
  if outcome == 'new' and redis.call('HGET', KEYS[1], 'confirmed') == '0'
    and redis.call('HGET', KEYS[1], 'caller') == ARGV[3] then
    redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
  end
  return sent
end
redis.call('HSET', KEYS[1], 'confirmed', '1', 'rm', sent[2])
X.expire(KEYS[1], tonumber(ARGV[14]) * 1000)
X.expire(KEYS[2], tonumber(ARGV[14]) * 1000)
if #KEYS >= 13 then
  local rid = string.match(KEYS[1], ':{r:(.*)}:job$')
  index_add(KEYS[1], KEYS[12], KEYS[13], rid, ARGV[18], tonumber(ARGV[19]), sent[2], ARGV[20],
    KEYS[14], ARGV[21])
end
return {outcome, sent[2]}
"""
)

# KEYS: job, callers, then (index on) `tq:H`, `tq` and (names on) `tqn`. ARGV: keep secs, '1'
# for a direct run, then (index on) the tenant hash, index keep ms, message id ('' direct),
# class, request id, tenant name. The send succeeded: the record lives until the deadline plus
# the status TTL, and the call is indexed. A direct run is marked: if its process dies,
# REGISTER takes the record over (AMEND FE-5), and its confirm indexes it again.
CONFIRM = (
    _INDEX_FN
    + """
redis.call('HSET', KEYS[1], 'confirmed', '1')
if ARGV[2] == '1' then redis.call('HSET', KEYS[1], 'direct', '1') end
X.expire(KEYS[1], tonumber(ARGV[1]) * 1000)
X.expire(KEYS[2], tonumber(ARGV[1]) * 1000)
if #KEYS >= 4 then
  index_add(KEYS[1], KEYS[3], KEYS[4], ARGV[7], ARGV[3], tonumber(ARGV[4]), ARGV[5], ARGV[6],
    KEYS[5], ARGV[8])
end
return 1
"""
)

# KEYS: job, callers, status. ARGV: caller. The send failed: drop the unconfirmed record.
ABANDON = """
local confirmed = redis.call('HGET', KEYS[1], 'confirmed')
if confirmed == '0' and redis.call('HGET', KEYS[1], 'caller') == ARGV[1] then
  redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
  return 1
end
return 0
"""

# A status write: state, ttl, field / value pairs from ARGV[from]. A final state is never
# replaced; the TTL only grows; every write stamps the store's time as `<state>_at`.
_STATUS_FN = """
local function set_status(key, state, ttl, from)
  local current = redis.call('HGET', key, 'state')
  if current == 'done' or current == 'failed' or current == 'expired'
    or current == 'cancelled' then
    return 0
  end
  local t = redis.call('TIME')
  local stamp = t[1] .. '.' .. string.format('%06d', tonumber(t[2]))
  local args = {'HSET', key, 'state', state, state .. '_at', stamp}
  for i = from, #ARGV, 2 do
    table.insert(args, ARGV[i])
    table.insert(args, ARGV[i + 1])
  end
  redis.call(unpack(args))
  if X.pttl(key) < ttl * 1000 then X.expire(key, ttl * 1000) end
  return 1
end
"""

# The final write of a call (A-1, owner Q-1): a `done` claim is never replaced (a late reply
# after a success writes nothing), the result is set only if absent, and the claim's state
# moves only with a result written or from a state not yet final; a late failure or cancel
# once set `cancelled` over a stored success and the next registration ran it again. Returns
# 1 written, 2 this same body was already stored (a replay of this write), else 0.
_FINAL_FN = """
local function final_write(res, job, body, ttl_ms, state, keep_ms)
  local current = redis.call('HGET', job, 'state')
  local written = false
  if current ~= 'done' then written = X.set(res, body, ttl_ms, true) end
  if redis.call('EXISTS', job) == 1 then
    local final = current == 'done' or current == 'failed' or current == 'expired'
      or current == 'cancelled'
    if written or not final then redis.call('HSET', job, 'state', state) end
    X.expire(job, keep_ms)
  end
  if written then return 1 end
  if redis.call('GET', res) == body then return 2 end
  return 0
end
"""

# KEYS: result, job, callers, status. ARGV: reply JSON, result ttl, final state, keep secs,
# channel prefix (`<prefix>:done:`), request id, then '1' to write the status too (its TTL and
# field / value pairs follow). Writes the result only if none exists, marks the claim final,
# writes the final status and wakes every caller waiting on this id (one round trip; the
# status was a second). The call leaves the request index (T1). Reply: 1 written, 2 this body
# was already stored (a replayed write), 0 another result already existed.
RESULT_PUT = (
    _STATUS_FN
    + _INDEX_FN
    + _FINAL_FN
    + """
index_drop(KEYS[2], ARGV[6])
local written = final_write(KEYS[1], KEYS[2], ARGV[1], tonumber(ARGV[2]) * 1000, ARGV[3],
  tonumber(ARGV[4]) * 1000)
for _, caller in ipairs(redis.call('SMEMBERS', KEYS[3])) do
  redis.call('PUBLISH', ARGV[5] .. caller, ARGV[6])
end
X.expire(KEYS[3], tonumber(ARGV[4]) * 1000)  -- (S9) no-op when the set is gone
if ARGV[7] == '1' then set_status(KEYS[4], ARGV[3], tonumber(ARGV[8]), 9) end
return written
"""
)

# The end of a call that succeeded, when the queues share the state store (FL-22): RESULT_PUT,
# then the message's ACK, then FINISH of the grant, in one round trip (three scripts in two
# round trips before, and a window where the result was stored while the slot waited). KEYS:
# result, job, callers, status, then the queue's keys (`_PARTS` order). ARGV: prefix, FINISH's
# JSON, clock ('' : the store's), reply JSON, result ttl, final state, keep secs, channel
# prefix, request id, status ttl, message id, generation, then status field / value pairs.
# FINISH runs under `pcall`: a failing settle never undoes the stored result or the ack (they
# were separate scripts, and a refused FINISH only logged). Reply: {written, acked, settled},
# settled -1 when FINISH raised (its error is the 4th element). written is `final_write`'s
# answer: 2 when this same body was stored already, a replay of this script (A-7).
SETTLE_DONE = (
    _COMMON
    + _SETTLE
    + _STATUS_FN
    + _INDEX_FN
    + _FINAL_FN
    + """
index_drop(KEYS[2], ARGV[9])
local written = final_write(KEYS[1], KEYS[2], ARGV[4], tonumber(ARGV[5]) * 1000, ARGV[6],
  tonumber(ARGV[7]) * 1000)
for _, caller in ipairs(redis.call('SMEMBERS', KEYS[3])) do
  redis.call('PUBLISH', ARGV[8] .. caller, ARGV[9])
end
X.expire(KEYS[3], tonumber(ARGV[7]) * 1000)
set_status(KEYS[4], ARGV[6], tonumber(ARGV[10]), 13)
local acked = 0
if redis.call('HGET', KEYS[7], ARGV[11]) == ARGV[12] then
  redis.call('XDEL', KEYS[5], ARGV[11])
  redis.call('ZREM', KEYS[6], ARGV[11])
  redis.call('HDEL', KEYS[7], ARGV[11])
  redis.call('HDEL', KEYS[8], ARGV[11])
  redis.call('HDEL', KEYS[9], ARGV[11])
  acked = 1
end
local ok, settled = pcall(settle, req.alias, req.owner, req.request_id, num(req.refund_r, 0),
  num(req.refund_t, 0), true, req.outcome, num(req.latency_ms, -1), num(req.retry_after_ms, 0))
if not ok then return {written, acked, -1, tostring(settled)} end
return {written, acked, settled}
"""
)

# KEYS: result, job. ARGV: dedupe secs. After its first read a result of a library-made id lives
# only `dedupe secs` more (an app-supplied id keeps its TTL).
_DROP_READ = """
if redis.call('HGET', KEYS[2], 'app') ~= '1' then
  -- A library id has one reader, and the job record still says `done` to any redelivery:
  -- with no dedupe window the body goes at once (bulk embeddings kept ~2 MB a call: B97).
  if tonumber(ARGV[1]) <= 0 then
    redis.call('DEL', KEYS[1])
  else
    local ttl = X.pttl(KEYS[1])
    if ttl > tonumber(ARGV[1]) * 1000 then X.expire(KEYS[1], tonumber(ARGV[1]) * 1000) end
  end
end
"""

# KEYS: result, job. ARGV: dedupe secs, offload marker, grace ms. Read a result (and drop it as
# above, but never at once); an offloaded one is only read: it is dropped once the offload read
# worked (RESULT_DROP).
#
# Replay-safe (FL-2): redis-py runs a script again when the connection drops after the server
# ran it. A take that deleted the body made that replay return nil, and the caller waited out
# its deadline for a call that had succeeded. With no dedupe window the body now lives `grace
# ms` more (longer than the client's retry span, about 1.5 s), so a replay reads it again.
# 2 s, not 10: every read body stayed 10 s and a burst of large embedding results held
# 215 MB of store memory (gate B97's budget is +64 MB).
TAKE_GRACE_MS = 2_000
RESULT_TAKE = """
local value = redis.call('GET', KEYS[1])
if not value then return false end
if ARGV[2] ~= '' and string.sub(value, 1, #ARGV[2]) == ARGV[2] then return value end
if redis.call('HGET', KEYS[2], 'app') ~= '1' then
  local keep_ms = tonumber(ARGV[3])
  if tonumber(ARGV[1]) > 0 then keep_ms = tonumber(ARGV[1]) * 1000 end
  local ttl = X.pttl(KEYS[1])
  if ttl < 0 or ttl > keep_ms then X.expire(KEYS[1], keep_ms) end
end
return value
"""
RESULT_DROP = _DROP_READ + "return 1\n"

# KEYS: job, cancel of each call. ARGV: cancel ttl secs, cancel channel, only state ('' any
# call not final; `queued`: only while its job is still `queued`, 0.2.0 T2), then the request
# ids. Cancel wins (owner decision Q3): any caller's cancel stops the call for every caller on
# its id, with no read of the callers set in a round trip of its own (R3RC-1: that
# read-then-write never cancelled a shared id, or cancelled a caller that attached in between).
# A call already final is left alone, so a later run of its id (Q2) is not cancelled by a stale
# mark. An attach after the mark waits on the same call and gets its `cancelled` reply. Reply:
# {the ids marked, how many were skipped because they started (only state `queued`)}.
CANCEL = """
local marked, started = {}, 0
for i = 1, #KEYS / 2 do
  local state = redis.call('HGET', KEYS[2 * i - 1], 'state')
  if state ~= 'done' and state ~= 'failed' and state ~= 'expired' and state ~= 'cancelled' then
    if ARGV[3] ~= '' and state ~= ARGV[3] then
      started = started + 1
    else
      X.set(KEYS[2 * i], '1', tonumber(ARGV[1]) * 1000)
      redis.call('PUBLISH', ARGV[2], ARGV[3 + i])
      table.insert(marked, ARGV[3 + i])
    end
  end
end
return {marked, started}
"""

# Extend a call's deadline (0.2.0 T2, owner D6). KEYS: job, callers, status. ARGV: the new
# deadline (store ms), how long the records outlive it (ms: caller margin + status TTL), the
# caller margin (ms, the index scores a deadline plus it), the done-channel prefix
# (`<prefix>:done:`), the cancel channel, the request id. The deadline lives in three places
# and all three move here: the records (job `dl`, grown TTLs of job / callers / status and the
# request index score), each worker holding the call (the notice on the cancel channel, every
# worker reads it; a worker that receives it later reads `dl` before it expires the call) and
# each waiting caller (the notice on its done channel: its timer is rescheduled). Only grows:
# the current deadline is the later of `dl` and the first one (the index score less the caller
# margin), so an extend shorter than the call's own timeout is `already` (RQ-4).
# Reply: {'ok'} or {'final' | 'gone' | 'already' | 'past'}.
EXTEND = """
local state = redis.call('HGET', KEYS[1], 'state')
if not state then return {'gone'} end
if state == 'done' or state == 'failed' or state == 'expired' or state == 'cancelled' then
  return {'final'}
end
local dl = tonumber(ARGV[1])
local current = tonumber(redis.call('HGET', KEYS[1], 'dl') or '0')
local ix = redis.call('HGET', KEYS[1], 'ix')
local h = redis.call('HGET', KEYS[1], 'ixh')
if ix then
  local first = tonumber(redis.call('ZSCORE', ix, ARGV[6]) or '')
  if first then current = math.max(current, first - tonumber(ARGV[3])) end
end
if dl <= current then return {'already'} end
local now = X.now()
if dl <= now then return {'past'} end
redis.call('HSET', KEYS[1], 'dl', ARGV[1])
local keep = dl - now + tonumber(ARGV[2])
for i = 1, 3 do
  if redis.call('EXISTS', KEYS[i]) == 1 and X.pttl(KEYS[i]) < keep then X.expire(KEYS[i], keep) end
end
if ix and h then
  local score = dl + tonumber(ARGV[3])
  redis.call('ZADD', ix, 'XX', 'GT', score, ARGV[6])
  local ixs = string.sub(ix, 1, #ix - #h - 1)
  redis.call('ZADD', ixs, 'GT', score, h)
  local ms = score - now + 60000
  if X.pttl(ix) < ms then X.expire(ix, ms) end
  if X.pttl(ixs) < ms then X.expire(ixs, ms) end
end
local notice = '\\0x\\0' .. ARGV[1] .. '\\0' .. ARGV[6]
for _, caller in ipairs(redis.call('SMEMBERS', KEYS[2])) do
  redis.call('PUBLISH', ARGV[4] .. caller, notice)
end
redis.call('PUBLISH', ARGV[5], notice)
return {'ok'}
"""
# The prefix of an extend notice on a done or cancel channel (a request id never starts with a
# NUL): `\0x\0<deadline ms>\0<request id>`.
EXTEND_NOTICE = "\0x\0"


def extend_notice(data: str) -> tuple[str, int] | None:
    """(request id, new deadline ms) of an extend notice; None for a plain request id."""
    if not data.startswith(EXTEND_NOTICE):
        return None
    deadline, _, rid = data[len(EXTEND_NOTICE) :].partition("\0")
    try:
        return rid, int(deadline)
    except ValueError:
        return None


# KEYS: status. ARGV: state, ttl, then field / value pairs (see `_STATUS_FN`).
STATUS_SET = _STATUS_FN + "return set_status(KEYS[1], ARGV[1], tonumber(ARGV[2]), 3)\n"


@dataclass(frozen=True, slots=True)
class Registration:
    """What `register` found: new, attached, takeover, done, expired, conflict or cancelled."""

    outcome: str
    message_id: str = ""  # `register_send`: the id of the message it sent ('' when none)


class Settled(NamedTuple):
    """What `put_result_settled` did: result written, message acked, grant settled (-1 raised).

    `replayed`: the result held this very body already, so the script ran before (a replay
    after a lost reply, A-7): its ack and settle happened then, not a second run.
    """

    written: bool
    acked: bool
    settled: int
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class IndexEntry:
    """Where a call goes in the request index (T1): its model's keys and its tenant.

    `keys`: the request index `tq:H`, the tenant index `tq` and, with tenant names on, `tqn`.
    `tenant_hash` is H (`-` for no tenant), `keep_ms` how long the entry counts (the call's
    deadline from now), `cls` its class (`p` / `n`), `name` the tenant name ('' unless names
    are on).
    """

    keys: tuple[str, ...]
    tenant_hash: str
    keep_ms: int
    cls: str
    name: str = ""


class RequestStore:
    """Claim records, results and request status of one environment."""

    def __init__(
        self,
        client: redis.asyncio.Redis,
        keys: Keys,
        *,
        result_ttl_secs: int = 300,
        result_dedupe_secs: int = 0,
        status_ttl_secs: int = 1800,
        unconfirmed_secs: int = 30,
        takeover_grace_secs: int = 5,
        caller_ttl_secs: int = 0,
        lease_secs: float = 60.0,
        offload: ResultOffload | None = None,
        offload_bytes: int | None = None,
    ) -> None:
        """Register the scripts; no I/O until the first call.

        With `result_dedupe_secs`, a result is read again by later callers of the same id, so
        an offload that deletes its object on the first read is refused (`ConfigError`). An
        offload needs `offload_bytes` (1 or more) and the other way round: one without the
        other turned nothing on, with no log (R3JE-27).
        """
        if (offload is None) != (offload_bytes is None):
            raise ConfigError(
                "offload and offload_bytes go together: give both (offload_bytes is the result "
                "size above which a result goes to the offload) or neither"
            )
        if offload_bytes is not None and offload_bytes < 1:
            raise ConfigError("offload_bytes must be 1 or more")
        if result_dedupe_secs > 0 and getattr(offload, "delete_after_read", False):
            raise ConfigError(
                "result_dedupe_secs > 0 reads a result more than once: build the offload with "
                "S3Offload(..., delete_after_read=False) (the bucket's expiry rule removes it)"
            )
        self._client = client
        self.keys = keys
        self.result_ttl_secs = result_ttl_secs
        self.result_dedupe_secs = result_dedupe_secs
        self.status_ttl_secs = status_ttl_secs
        self.unconfirmed_secs = unconfirmed_secs
        self.takeover_grace_secs = takeover_grace_secs
        self.caller_ttl_secs = caller_ttl_secs  # 0: registration leaves the caller key alone
        self.lease_ms = int(lease_secs * 1000)  # a dead direct run's claim lapses after it
        self.offload = offload
        self.offload_bytes = offload_bytes
        self._unsent: dict[str, tuple[Reply, bytes]] = {}
        self.expiry = native(keys.prefix)  # the store's mode, set by the runtime at start
        pre = prelude(keys.prefix)  # X.expire / X.pttl / X.set (engine/expiry.py)
        self._register = client.register_script(pre + REGISTER)
        self._register_send = client.register_script(pre + REGISTER_SEND)
        self._confirm = client.register_script(pre + CONFIRM)
        self._abandon = client.register_script(pre + ABANDON)
        self._put = client.register_script(pre + RESULT_PUT)
        self._take = client.register_script(pre + RESULT_TAKE)
        self._drop_read = client.register_script(pre + RESULT_DROP)
        self._status = client.register_script(pre + STATUS_SET)
        self._cancel = client.register_script(pre + CANCEL)
        self._settle_done = client.register_script(pre + SETTLE_DONE)
        self._extend = client.register_script(pre + EXTEND)

    def _req(self, request_id: str) -> tuple[str, str, str, str]:
        k = self.keys
        return (
            k.request(request_id, "job"),
            k.request(request_id, "callers"),
            k.status(request_id),
            k.request(request_id, "res"),
        )

    async def register(
        self,
        request_id: str,
        *,
        caller: str,
        user_hash: str,
        params_hash: str,
        model: str,
        priority: bool,
        app_id: bool,
    ) -> Registration:
        """Register a call before it is sent (duplicate ids attach to the first run)."""
        outcome = await self._register(
            list(self._req(request_id)),
            [
                user_hash,
                params_hash,
                caller,
                self.unconfirmed_secs,
                self.status_ttl_secs,
                model,
                "1" if priority else "0",
                self.takeover_grace_secs,
                "1" if app_id else "0",
                self.caller_ttl_secs,
                self.lease_ms,
                os.urandom(8).hex(),  # a replay of this script re-sends it: the first outcome
            ],
        )
        return Registration(_text(outcome))

    async def register_send(
        self,
        request_id: str,
        *,
        caller: str,
        user_hash: str,
        params_hash: str,
        model: str,
        priority: bool,
        app_id: bool,
        queue: str,
        queue_keys: Sequence[str],
        bell: str,
        body: bytes,
        max_waiting: int,
        retention_secs: int,
        keep_secs: float,
        index: IndexEntry | None = None,
    ) -> Registration:
        """`register`, and for a new (or taken over) call its send and `confirm`, in one call.

        Only when the queue lives in this store. `QueueFull` above `max_waiting` (the record
        just made is dropped, as `abandon` did). With `index`, a sent call is indexed in the
        same script (T1).
        """
        index_keys: list[str] = list(index.keys) if index else []
        index_args: list[Any] = (
            [index.tenant_hash, index.keep_ms, index.cls, index.name] if index else []
        )
        reply = await self._register_send(
            [*self._req(request_id), *queue_keys, *index_keys],
            [
                user_hash,
                params_hash,
                caller,
                self.unconfirmed_secs,
                self.status_ttl_secs,
                model,
                "1" if priority else "0",
                self.takeover_grace_secs,
                "1" if app_id else "0",
                body,
                max_waiting,
                retention_secs * 1000,
                bell,
                int(keep_secs) + self.status_ttl_secs,
                self.caller_ttl_secs,
                self.lease_ms,
                os.urandom(8).hex(),  # the registration nonce (A-5)
                *index_args,
            ],
        )
        outcome = _text(reply[0])
        if outcome == "full":
            raise QueueFull(queue, int(reply[1]))
        return Registration(outcome, _text(reply[1]) if len(reply) > 1 else "")

    async def confirm(
        self,
        request_id: str,
        *,
        keep_secs: float,
        direct: bool = False,
        index: IndexEntry | None = None,
        message_id: str = "",
    ) -> None:
        """The send succeeded: keep the record until the deadline plus the status TTL.

        `direct`: the call runs in this process, not from a queue; marked so that a caller of
        the id takes the record over if this process dies (AMEND FE-5). With `index`, the call
        is indexed in the same script (T1); `message_id` is its stream message ('' direct).
        """
        job, callers, _, _ = self._req(request_id)
        args: list[Any] = [int(keep_secs) + self.status_ttl_secs, "1" if direct else "0"]
        keys = [job, callers]
        if index is not None:
            keys += list(index.keys)
            args += [index.tenant_hash, index.keep_ms, message_id, index.cls, request_id]
            args.append(index.name)
        await self._confirm(keys, args)

    async def abandon(self, request_id: str, *, caller: str) -> None:
        """The send failed: remove this caller's unconfirmed record."""
        job, callers, status, _ = self._req(request_id)
        await self._abandon([job, callers, status], [caller])

    async def put_result(
        self, reply: Reply, *, final_state: str, status: Mapping[str, Any] | None = None
    ) -> bool:
        """Store a result once and wake its callers; False when one was already stored.

        With `status`, the request status is set to `final_state` with these fields in the
        same script (as `set_status` would; None values are left out). Safe to call again
        with the same `reply` after a store error: the body is offloaded only once.
        """
        job, callers, status_key, result = self._req(reply.request_id)
        data = await self._prepared(reply)
        args: list[Any] = [
            data,
            self.result_ttl_secs,
            final_state,
            self.status_ttl_secs,
            f"{self.keys.prefix}:done:",
            reply.request_id,
        ]
        if status is not None:
            args += ["1", self.status_ttl_secs, *_pairs(status)]
        try:
            written = await self._put([result, job, callers, status_key], args)
        except BaseException:
            # The runtime writes a result through a store outage by calling again every 0.5 s:
            # keep the prepared body, or each try uploaded the result again (EN-3).
            self._unsent[reply.request_id] = (reply, data)
            while len(self._unsent) > _UNSENT_MAX:
                self._unsent.pop(next(iter(self._unsent)))
            raise
        self._unsent.pop(reply.request_id, None)
        if int(written) == 0 and data.startswith(MARKER):
            # Another result was already stored (a duplicate run): the object just uploaded
            # would stay until the bucket rule removed it (R3JE-29). A replay of this write
            # (2) stored this very marker: its object stays.
            await self._delete_object(data[len(MARKER) :].decode())
        return int(written) != 0

    async def put_results(
        self, replies: Sequence[Reply], *, final_state: str, status: Mapping[str, Any]
    ) -> int:
        """`put_result` of many small replies in one pipelined round trip; how many written.

        For final replies the store makes up itself (an admin cancel answering waiting calls):
        one script per id, one round trip for all (a 50k cancel made 50k serial round trips).
        """
        if not replies:
            return 0
        async with self._client.pipeline(transaction=False) as pipe:
            for reply in replies:
                job, callers, status_key, result = self._req(reply.request_id)
                await self._put(
                    [result, job, callers, status_key],
                    [
                        reply.dumps(),
                        self.result_ttl_secs,
                        final_state,
                        self.status_ttl_secs,
                        f"{self.keys.prefix}:done:",
                        reply.request_id,
                        "1",
                        self.status_ttl_secs,
                        *_pairs(status),
                    ],
                    client=pipe,
                )
            written = await pipe.execute()
        return sum(1 for w in written if w)

    async def put_result_settled(
        self,
        reply: Reply,
        *,
        final_state: str,
        status: Mapping[str, Any],
        queue_keys: Sequence[str],
        message_id: str,
        generation: int | str,
        finish: Sequence[Any],
    ) -> Settled:
        """`put_result`, the message's ack and the grant's FINISH in one round trip (FL-22).

        Only when the queue lives in this store. `finish` is `Admission.finish_args(...)`. Safe
        to call again after a store error, as each part is (result set once, ack checks its
        receipt, FINISH its owner); a replay that found this body stored says `replayed`.
        """
        job, callers, status_key, result = self._req(reply.request_id)
        data = await self._prepared(reply)
        prefix, finish_json, clock = finish
        args: list[Any] = [
            prefix,
            finish_json,
            clock,
            data,
            self.result_ttl_secs,
            final_state,
            self.status_ttl_secs,
            f"{self.keys.prefix}:done:",
            reply.request_id,
            self.status_ttl_secs,
            message_id,
            generation,
            *_pairs(status),
        ]
        try:
            out = await self._settle_done([result, job, callers, status_key, *queue_keys], args)
        except BaseException:
            self._unsent[reply.request_id] = (reply, data)  # as `put_result` (EN-3)
            while len(self._unsent) > _UNSENT_MAX:
                self._unsent.pop(next(iter(self._unsent)))
            raise
        self._unsent.pop(reply.request_id, None)
        written, acked, settled = int(out[0]), bool(out[1]), int(out[2])
        if settled < 0:
            logger.warning("finish_failed", request_id=reply.request_id, error=_text(out[3]))
        if written == 0 and data.startswith(MARKER):
            await self._delete_object(data[len(MARKER) :].decode())
        return Settled(written == 1, acked, settled, written == 2)

    async def _delete_object(self, ref: str) -> None:
        """Delete an offloaded object no marker points to; a failure is only logged."""
        delete = getattr(self.offload, "delete", None)
        if delete is None:
            return
        try:
            await delete(ref)
        except Exception as exc:
            logger.warning("offload_delete_failed", error_type=type(exc).__name__)

    def _flag_key(self) -> str:
        return f"{self.keys.prefix}:offload"

    async def check_offload(self) -> None:
        """At start: record that this fleet offloads, or refuse to start without the offload.

        A process without the offload read an offloaded result as JSON, failed with a raw
        error and deleted the result (R3JE-26): owner decision Q24 makes it refuse to start
        while the store says the fleet offloads (the flag lapses `OFFLOAD_FLAG_SECS` after the
        last offloading process wrote).
        """
        if self.offload is not None:
            await self.expiry.set(self._client, self._flag_key(), b"1", OFFLOAD_FLAG_SECS)
            return
        if await self._client.exists(self._flag_key()):
            raise ConfigError(
                "this fleet stores large results in a result offload (the store's "
                f"`{self._flag_key()}` key says so), but this process has none: build it with "
                "the same LLMQueue.from_env(offload=..., offload_bytes=...) as the others"
            )

    async def check_lifecycle(self) -> None:
        """At start: warn when the bucket has no expiry rule for the offload's prefix (Q23).

        Objects of callers that never read them (timed out, cancelled, gone) are removed only
        by that rule. Never raises: a check that cannot run is logged and skipped.
        """
        check = getattr(self.offload, "check_lifecycle", None)
        if check is None:
            return
        try:
            await check()
        except Exception as exc:
            logger.info("offload_lifecycle_not_checked", error_type=type(exc).__name__)

    async def _prepared(self, reply: Reply) -> bytes:
        """The stored form of a reply: its JSON, or an offload marker when it is large.

        An offload failure never raises: a botocore error was not a store error, so the call
        the provider already answered was released and run again (EN-3). The body is kept
        inline instead, up to `_INLINE_MAX_BYTES`; above that the caller gets a failure.
        """
        held = self._unsent.get(reply.request_id)
        if held is not None and held[0] is reply:
            return held[1]
        data = reply.dumps()
        if self.offload is None or not self.offload_bytes or len(data) <= self.offload_bytes:
            return data
        try:
            async with asyncio.timeout(OFFLOAD_PUT_SECS):
                ref = await self.offload.put(reply.request_id, data)
        except Exception as exc:  # TimeoutError included: kept inline below
            logger.warning(
                "offload_put_failed",
                request_id=reply.request_id,
                size_bytes=len(data),
                error_type=type(exc).__name__,
            )
        else:
            with contextlib.suppress(Exception):  # only keeps the fleet flag fresh (Q24)
                await self.expiry.set(self._client, self._flag_key(), b"1", OFFLOAD_FLAG_SECS)
            return MARKER + ref.encode()
        if len(data) <= _INLINE_MAX_BYTES:
            return data
        failed = Reply(
            request_id=reply.request_id,
            ok=False,
            error="result_expired",
            message="the result could not be stored (offload failed)",
            meta=reply.meta,
        )
        return failed.dumps()

    async def take_result(self, request_id: str) -> Reply | None:
        """A stored result; a library id's then expires in seconds (or `result_dedupe_secs`).

        An offloaded result is deleted only after its offload read worked (RESCAN S4): it was
        deleted first, and a blip in that read lost it. A gone object raises
        `CallFailed("result_expired")`; a read that keeps failing (access denied, throttling,
        a timeout) raises `queue_unavailable` and leaves the result to read again (R3JE-28).
        A process without the offload never deletes an offloaded result (R3JE-26).
        """
        job, _, _, result = self._req(request_id)
        # The marker always: without it the script took the marker for a result and dropped
        # it, and the JSON parse of the marker raised a raw error (R3JE-26).
        try:
            raw = await self._take([result, job], [self.result_dedupe_secs, MARKER, TAKE_GRACE_MS])
        except ResponseError as exc:
            # RESULT_TAKE writes (it drops a read result): a store refusing writes (NOREPLICAS,
            # OOM, MISCONF) made a stored answer unreadable (R3FM-7). Read it plainly; the drop
            # is skipped and the result expires with its TTL.
            if not _refuses_writes(exc):
                raise
            raw = await self._client.get(result)
        if raw is None:
            return None
        if raw.startswith(MARKER):
            if self.offload is None:
                logger.error("offload_missing", request_id=request_id)
                raise CallFailed(
                    "result_expired",
                    "the result is in a result offload and this process has none configured",
                    request_id=request_id,
                )
            body = await self._read_offloaded(self.offload, request_id, raw[len(MARKER) :].decode())
            if body is None:
                raise CallFailed(
                    "result_expired", "the offloaded result is gone", request_id=request_id
                )
            try:
                await self._drop_read([result, job], [self.result_dedupe_secs])
            except (*_STORE_ERRORS, ResponseError) as exc:
                # The body is in hand and the offload may have deleted it on read: raising
                # here made the caller read again, find it gone and get `result_expired`
                # (EN-4). The marker expires with the result TTL.
                logger.warning(
                    "result_drop_skipped", request_id=request_id, error_type=type(exc).__name__
                )
            raw = body
        return Reply.model_validate(orjson.loads(raw))

    @staticmethod
    async def _read_offloaded(offload: ResultOffload, request_id: str, ref: str) -> bytes | None:
        """The offloaded body (None when gone), tried again on other errors, then refused."""
        for pause in (*_GET_RETRY_SECS, None):
            try:
                return await offload.get(ref)
            except Exception as exc:
                code = (getattr(exc, "response", None) or {}).get("Error", {}).get("Code")
                logger.warning(
                    "offload_get_failed",
                    request_id=request_id,
                    error_type=type(exc).__name__,
                    code=code,
                    last=pause is None,
                )
                if pause is None:
                    raise CallFailed(
                        "queue_unavailable",
                        "the offloaded result could not be read now; it is kept, try again",
                        retry_after_secs=1,
                        request_id=request_id,
                    ) from exc
                await asyncio.sleep(pause)
        return None  # not reached: the last pass returns or raises

    async def cancel(
        self,
        request_ids: Sequence[str],
        *,
        ttl_secs: int,
        channel: str,
        only_state: str | None = None,
    ) -> int:
        """Mark calls cancelled and announce each on `channel`, in one script; how many.

        Cancel wins for a shared id (Q3, R3RC-1): every caller of it gets `cancelled`. A call
        already final is skipped. `only_state="queued"`: only calls still waiting are marked
        (a call granted meanwhile is left running; `cancel_counts` says how many).
        """
        marked, _ = await self.cancel_counts(
            request_ids, ttl_secs=ttl_secs, channel=channel, only_state=only_state
        )
        return marked

    async def cancel_counts(
        self,
        request_ids: Sequence[str],
        *,
        ttl_secs: int,
        channel: str,
        only_state: str | None = None,
    ) -> tuple[int, int]:
        """`cancel`, returning (marked, skipped because they were no longer in `only_state`)."""
        marked, started = await self.cancel_ids(
            request_ids, ttl_secs=ttl_secs, channel=channel, only_state=only_state
        )
        return len(marked), started

    async def cancel_ids(
        self,
        request_ids: Sequence[str],
        *,
        ttl_secs: int,
        channel: str,
        only_state: str | None = None,
    ) -> tuple[list[str], int]:
        """`cancel`, returning (the ids it marked, how many were no longer in `only_state`)."""
        if not request_ids:
            return [], 0
        keys = [
            k
            for rid in request_ids
            for k in (self.keys.request(rid, "job"), self.keys.request(rid, "cancel"))
        ]
        reply = await self._cancel(keys, [ttl_secs, channel, only_state or "", *request_ids])
        return [_text(rid) for rid in reply[0]], int(reply[1])

    async def extend(self, request_id: str, *, deadline_ms: int, margin_secs: float) -> str:
        """Move a call's deadline to `deadline_ms` (store ms) everywhere it lives (`EXTEND`).

        `margin_secs` is the callers' margin (the index scores a deadline plus it). Returns
        `ok`, `final`, `gone`, `already` (not later than its current one) or `past`.
        """
        job, callers, status, _ = self._req(request_id)
        reply = await self._extend(
            [job, callers, status],
            [
                deadline_ms,
                int((margin_secs + self.status_ttl_secs) * 1000),
                int(margin_secs * 1000),
                f"{self.keys.prefix}:done:",
                self.keys.cancel_channel,
                request_id,
            ],
        )
        return _text(reply[0])

    async def set_status(self, request_id: str, state: str, **fields: Any) -> bool:
        """Write a status (False when a final state was already there)."""
        done = await self._status(
            [self.keys.status(request_id)], [state, self.status_ttl_secs, *_pairs(fields)]
        )
        return bool(done)

    async def status(self, request_id: str) -> dict[str, str]:
        """The status hash of a request (empty when unknown or expired)."""
        raw = await self._client.hgetall(self.keys.status(request_id))
        return {_text(k): _text(v) for k, v in (raw or {}).items()}

    async def callers(self, request_id: str) -> Sequence[str]:
        """Callers waiting on a request id."""
        return [_text(v) for v in await self._client.smembers(self._req(request_id)[1])]

    async def job(self, request_id: str) -> Mapping[str, str]:
        """The claim record (for tests and status views)."""
        raw = await self._client.hgetall(self._req(request_id)[0])
        return {_text(k): _text(v) for k, v in (raw or {}).items()}


def _refuses_writes(exc: ResponseError) -> bool:
    """True for a refusal of writes only (the store still answers reads)."""
    words = str(exc).split(maxsplit=1)
    return isinstance(exc, OutOfMemoryError) or (
        bool(words) and words[0].upper() in ("OOM", "MISCONF", "NOREPLICAS", "READONLY")
    )


def _pairs(fields: Mapping[str, Any]) -> list[str]:
    """Field / value pairs of a status write, None values left out."""
    return [
        part for name, value in fields.items() if value is not None for part in (name, str(value))
    ]
