"""Per-request records: the duplicate-id claim, the result and the request status.

Results are written once and announced to the caller in the same script; a final status is
never replaced. Used by the runtime on both the caller and the worker side.
"""

# A call's record is written only if absent when it is sent and confirmed only after the send
# succeeded, so a failed send never leaves a phantom (plan §3.18 R23). A library id's result goes
# seconds after its first read (FL-2), an app id's is kept `result_ttl_secs`. Scripts use the
# store's clock.

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import orjson
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ReadOnlyError
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
# Prepared bodies of results whose store write failed, kept for the retry (oldest dropped).
_UNSENT_MAX = 64

_NOW_MS = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
"""

# KEYS: job, callers, status, result. ARGV: user hash, params hash, caller, unconfirmed ttl,
# status ttl, model, priority, takeover grace secs, app id ('1': kept after its first read).
# Returns new | attached | takeover | done | expired | conflict.
_REGISTER_FN = """
local function register()
local job = redis.call('HGETALL', KEYS[1])
local clock = redis.call('TIME')
local now = tonumber(clock[1])
-- The callers set lives as long as the record: an attach made it a set with no TTL (S9).
local function keep_callers()
  local ttl = redis.call('TTL', KEYS[1])
  if ttl > 0 then redis.call('EXPIRE', KEYS[2], ttl) end
end
if #job == 0 then
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
if state == 'done' or state == 'failed' or state == 'expired' or state == 'cancelled' then
  if redis.call('EXISTS', KEYS[4]) == 1 then return 'done' end
  return 'expired'
end
-- An unconfirmed record older than the grace: its send failed or its sender died; take over.
if fields['confirmed'] == '0' and now - tonumber(fields['created'] or '0') >= tonumber(ARGV[8]) then
  redis.call('HSET', KEYS[1], 'caller', ARGV[3], 'created', now)
  redis.call('SADD', KEYS[2], ARGV[3])
  keep_callers()
  return 'takeover'
end
redis.call('SADD', KEYS[2], ARGV[3])
keep_callers()
return 'attached'
end
"""

REGISTER = _REGISTER_FN + "return register()\n"

# KEYS: REGISTER's, then the queue's keys (stream first, `_PARTS` order). ARGV: REGISTER's, then
# body, max waiting, retention ms, doorbell key, keep secs. Register, send a new call and
# confirm it in one round trip when the queues share the state store (three before). A full
# queue drops the record it just made. Reply: {outcome}, {outcome, message id} or {'full', depth}.
REGISTER_SEND = (
    _NOW_MS
    + SEND_FN
    + _REGISTER_FN
    + """
local outcome = register()
if outcome ~= 'new' and outcome ~= 'takeover' then return {outcome} end
local sent = send(KEYS[5], ARGV[10], tonumber(ARGV[11]), tonumber(ARGV[12]), ARGV[13])
if sent[1] == 'full' then
  if redis.call('HGET', KEYS[1], 'confirmed') == '0'
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

# KEYS: job, callers. ARGV: keep secs. The send succeeded: the record lives until the deadline
# plus the status TTL.
CONFIRM = """
redis.call('HSET', KEYS[1], 'confirmed', '1')
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

# KEYS: status. ARGV: state, ttl, then field / value pairs (see `_STATUS_FN`).
STATUS_SET = _STATUS_FN + "return set_status(KEYS[1], ARGV[1], tonumber(ARGV[2]), 3)\n"


@dataclass(frozen=True, slots=True)
class Registration:
    """What `register` found: `new`, `attached`, `takeover`, `done`, `expired` or `conflict`."""

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
        offload: ResultOffload | None = None,
        offload_bytes: int | None = None,
    ) -> None:
        """Register the scripts; no I/O until the first call.

        With `result_dedupe_secs`, a result is read again by later callers of the same id, so
        an offload that deletes its object on the first read is refused (`ConfigError`).
        """
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
            ],
        )
        outcome = _text(reply[0])
        if outcome == "full":
            raise QueueFull(queue, int(reply[1]))
        return Registration(outcome)

    async def confirm(self, request_id: str, *, keep_secs: float) -> None:
        """The send succeeded: keep the record until the deadline plus the status TTL."""
        job, callers, _, _ = self._req(request_id)
        await self._confirm([job, callers], [int(keep_secs) + self.status_ttl_secs])

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
        return bool(written)

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
            return MARKER + (await self.offload.put(reply.request_id, data)).encode()
        except Exception as exc:
            logger.warning(
                "offload_put_failed",
                request_id=reply.request_id,
                size_bytes=len(data),
                error_type=type(exc).__name__,
            )
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
        deleted first, and a blip in that read lost it. A failed or empty read raises
        `CallFailed("result_expired")`, not the offload's raw error.
        """
        job, _, _, result = self._req(request_id)
        marker = MARKER if self.offload is not None else b""
        raw = await self._take([result, job], [self.result_dedupe_secs, marker, TAKE_GRACE_MS])
        if raw is None:
            return None
        if self.offload is not None and raw.startswith(MARKER):
            try:
                body = await self.offload.get(raw[len(MARKER) :].decode())
            except Exception as exc:
                raise CallFailed(
                    "result_expired",
                    "the offloaded result could not be read",
                    request_id=request_id,
                ) from exc
            if body is None:
                raise CallFailed(
                    "result_expired", "the offloaded result is gone", request_id=request_id
                )
            try:
                await self._drop_read([result, job], [self.result_dedupe_secs])
            except _STORE_ERRORS as exc:
                # The body is in hand and the offload may have deleted it on read: raising
                # here made the caller read again, find it gone and get `result_expired`
                # (EN-4). The marker expires with the result TTL.
                logger.warning(
                    "result_drop_skipped", request_id=request_id, error_type=type(exc).__name__
                )
            raw = body
        return Reply.model_validate(orjson.loads(raw))

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


def _pairs(fields: Mapping[str, Any]) -> list[str]:
    """Field / value pairs of a status write, None values left out."""
    return [
        part for name, value in fields.items() if value is not None for part in (name, str(value))
    ]
