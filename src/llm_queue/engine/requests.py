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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import orjson
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import OutOfMemoryError, ReadOnlyError, ResponseError
from redis.exceptions import TimeoutError as StoreTimeoutError

from llm_queue._logging import get_logger
from llm_queue.engine._codec import text as _text
from llm_queue.engine.envelope import Reply
from llm_queue.engine.errors import CallFailed
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
# again (`new`), its old result, status and cancel mark dropped in the same script.
_REGISTER_FN = """
local function register(caller_ttl, lease_ms)
local job = redis.call('HGETALL', KEYS[1])
local clock = redis.call('TIME')
local now = tonumber(clock[1])
local cancel_key = string.sub(KEYS[1], 1, -4) .. 'cancel'
-- Every send and attach refreshes the caller's liveness key (owner Q4): a live caller whose
-- heartbeat was late was taken for gone and its queued calls dropped (R3FM-1, R3RC-3).
if tonumber(caller_ttl or '') and tonumber(caller_ttl) > 0 then
  local prefix = string.match(KEYS[1], '^(.*):{r:')
  redis.call('SET', prefix .. ':caller:' .. ARGV[3], '1', 'EX', tonumber(caller_ttl))
end
-- The callers set lives as long as the record: an attach made it a set with no TTL (S9).
local function keep_callers()
  local ttl = redis.call('TTL', KEYS[1])
  if ttl > 0 then redis.call('EXPIRE', KEYS[2], ttl) end
end
local function create()
  redis.call('HSET', KEYS[1], 'state', 'queued', 'user', ARGV[1], 'params', ARGV[2],
    'caller', ARGV[3], 'confirmed', '0', 'created', now)
  if ARGV[9] == '1' then redis.call('HSET', KEYS[1], 'app', '1') end
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
  redis.call('SADD', KEYS[2], ARGV[3])
  redis.call('EXPIRE', KEYS[2], tonumber(ARGV[4]))
  if redis.call('HGET', KEYS[3], 'state') == false then
    local t = redis.call('TIME')
    redis.call('HSET', KEYS[3], 'state', 'waiting', 'model', ARGV[6], 'priority', ARGV[7],
      'enqueued_at', t[1] .. '.' .. string.format('%06d', tonumber(t[2])))
    redis.call('EXPIRE', KEYS[3], tonumber(ARGV[5]))
  end
  return 'new'
end
if #job == 0 then
  redis.call('DEL', cancel_key)  -- a cancel left by an earlier run of this id
  return create()
end
local fields = {}
for i = 1, #job, 2 do fields[job[i]] = job[i + 1] end
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
  redis.call('HSET', KEYS[1], 'caller', ARGV[3], 'created', now)
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
        'state', 'queued')
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

REGISTER = _REGISTER_FN + "return register(ARGV[10], ARGV[11])\n"

# KEYS: REGISTER's, then the queue's keys (stream first, `_PARTS` order). ARGV: REGISTER's, then
# body, max waiting, retention ms, doorbell key, keep secs, caller ttl, lease ms. Register, send
# a new call and confirm it in one round trip when the queues share the state store (three
# before). A full queue drops the record it just made. Reply: {outcome}, {outcome, message id}
# or {'full', depth}.
REGISTER_SEND = (
    _NOW_MS
    + SEND_FN
    + _REGISTER_FN
    + """
local outcome = register(ARGV[15], ARGV[16])
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
redis.call('HSET', KEYS[1], 'confirmed', '1')
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[14]))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[14]))
return {outcome, sent[2]}
"""
)

# KEYS: job, callers. ARGV: keep secs, '1' for a direct run. The send succeeded: the record
# lives until the deadline plus the status TTL. A direct run is marked: if its process dies,
# REGISTER takes the record over (AMEND FE-5).
CONFIRM = """
redis.call('HSET', KEYS[1], 'confirmed', '1')
if ARGV[2] == '1' then redis.call('HSET', KEYS[1], 'direct', '1') end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1]))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[1]))
return 1
"""

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
  if redis.call('TTL', key) < ttl then redis.call('EXPIRE', key, ttl) end
  return 1
end
"""

# KEYS: result, job, callers, status. ARGV: reply JSON, result ttl, final state, keep secs,
# channel prefix (`<prefix>:done:`), request id, then '1' to write the status too (its TTL and
# field / value pairs follow). Writes the result only if none exists, marks the claim final,
# writes the final status and wakes every caller waiting on this id (one round trip; the
# status was a second). Reply: 1 written, 0 a result already existed.
RESULT_PUT = (
    _STATUS_FN
    + """
local written = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2]))
if redis.call('EXISTS', KEYS[2]) == 1 then
  redis.call('HSET', KEYS[2], 'state', ARGV[3])
  redis.call('EXPIRE', KEYS[2], tonumber(ARGV[4]))
end
for _, caller in ipairs(redis.call('SMEMBERS', KEYS[3])) do
  redis.call('PUBLISH', ARGV[5] .. caller, ARGV[6])
end
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[4]))  -- (S9) no-op when the set is gone
if ARGV[7] == '1' then set_status(KEYS[4], ARGV[3], tonumber(ARGV[8]), 9) end
if written then return 1 end
return 0
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
    local ttl = redis.call('TTL', KEYS[1])
    if ttl > tonumber(ARGV[1]) then redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1])) end
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
# ms` more (longer than the client's retry span), so a replay reads it again.
TAKE_GRACE_MS = 10_000
RESULT_TAKE = """
local value = redis.call('GET', KEYS[1])
if not value then return false end
if ARGV[2] ~= '' and string.sub(value, 1, #ARGV[2]) == ARGV[2] then return value end
if redis.call('HGET', KEYS[2], 'app') ~= '1' then
  local keep_ms = tonumber(ARGV[3])
  if tonumber(ARGV[1]) > 0 then keep_ms = tonumber(ARGV[1]) * 1000 end
  local ttl = redis.call('PTTL', KEYS[1])
  if ttl < 0 or ttl > keep_ms then redis.call('PEXPIRE', KEYS[1], keep_ms) end
end
return value
"""
RESULT_DROP = _DROP_READ + "return 1\n"

# KEYS: job, cancel of each call. ARGV: cancel ttl secs, cancel channel, then the request ids.
# Cancel wins (owner decision Q3): any caller's cancel stops the call for every caller on its id,
# with no read of the callers set in a round trip of its own (R3RC-1: that read-then-write never
# cancelled a shared id, or cancelled a caller that attached in between). A call already final
# is left alone, so a later run of its id (Q2) is not cancelled by a stale mark. An attach after
# the mark waits on the same call and gets its `cancelled` reply. Reply: how many were marked.
CANCEL = """
local n = 0
for i = 1, #KEYS / 2 do
  local state = redis.call('HGET', KEYS[2 * i - 1], 'state')
  if state ~= 'done' and state ~= 'failed' and state ~= 'expired' and state ~= 'cancelled' then
    redis.call('SET', KEYS[2 * i], '1', 'EX', tonumber(ARGV[1]))
    redis.call('PUBLISH', ARGV[2], ARGV[2 + i])
    n = n + 1
  end
end
return n
"""

# KEYS: status. ARGV: state, ttl, then field / value pairs (see `_STATUS_FN`).
STATUS_SET = _STATUS_FN + "return set_status(KEYS[1], ARGV[1], tonumber(ARGV[2]), 3)\n"


@dataclass(frozen=True, slots=True)
class Registration:
    """What `register` found: new, attached, takeover, done, expired, conflict or cancelled."""

    outcome: str


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
        self._register = client.register_script(REGISTER)
        self._register_send = client.register_script(REGISTER_SEND)
        self._confirm = client.register_script(CONFIRM)
        self._abandon = client.register_script(ABANDON)
        self._put = client.register_script(RESULT_PUT)
        self._take = client.register_script(RESULT_TAKE)
        self._drop_read = client.register_script(RESULT_DROP)
        self._status = client.register_script(STATUS_SET)
        self._cancel = client.register_script(CANCEL)

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
    ) -> Registration:
        """`register`, and for a new (or taken over) call its send and `confirm`, in one call.

        Only when the queue lives in this store. `QueueFull` above `max_waiting` (the record
        just made is dropped, as `abandon` did).
        """
        reply = await self._register_send(
            [*self._req(request_id), *queue_keys],
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
            ],
        )
        outcome = _text(reply[0])
        if outcome == "full":
            raise QueueFull(queue, int(reply[1]))
        return Registration(outcome)

    async def confirm(self, request_id: str, *, keep_secs: float, direct: bool = False) -> None:
        """The send succeeded: keep the record until the deadline plus the status TTL.

        `direct`: the call runs in this process, not from a queue; marked so that a caller of
        the id takes the record over if this process dies (AMEND FE-5).
        """
        job, callers, _, _ = self._req(request_id)
        await self._confirm(
            [job, callers], [int(keep_secs) + self.status_ttl_secs, "1" if direct else "0"]
        )

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
        if not written and data.startswith(MARKER):
            # A result was already stored (a duplicate run): the object just uploaded would
            # stay until the bucket rule removed it (R3JE-29).
            await self._delete_object(data[len(MARKER) :].decode())
        return bool(written)

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
            await self._client.set(self._flag_key(), b"1", ex=OFFLOAD_FLAG_SECS)
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
                await self._client.set(self._flag_key(), b"1", ex=OFFLOAD_FLAG_SECS)
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

    async def cancel(self, request_ids: Sequence[str], *, ttl_secs: int, channel: str) -> int:
        """Mark calls cancelled and announce each on `channel`, in one script; how many.

        Cancel wins for a shared id (Q3, R3RC-1): every caller of it gets `cancelled`. A call
        already final is skipped.
        """
        if not request_ids:
            return 0
        keys = [
            k
            for rid in request_ids
            for k in (self.keys.request(rid, "job"), self.keys.request(rid, "cancel"))
        ]
        return int(await self._cancel(keys, [ttl_secs, channel, *request_ids]))

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
