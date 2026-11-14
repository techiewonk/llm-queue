"""A queue on any Redis-protocol store: a stream plus visibility, generation and failure records.

Order is the stream id (the store's clock at send). A received message is held (invisible) for
the visibility time; `return_` makes it visible again in its place without a failure;
a lapsed hold (the worker died) counts one failure, and `max_receives` failures move it to the
dead-letter stream, reported to the receiver so the caller can be told (plan §3.18). Every
operation is one Lua call. A model has two queues: `<name>` and `<name>-priority` (E92).
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from redis.exceptions import NoScriptError

from llm_queue._logging import get_logger
from llm_queue.engine.keys import Keys

if TYPE_CHECKING:
    import redis.asyncio

logger = get_logger(__name__)
RETENTION_SECS = 345_600  # how long a message may wait (4 days), unless the model sets it

_NOW = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
"""

# Keys of one queue, in this order for every script.
_PARTS = ("s", "vis", "gen", "fails", "st", "dlq", "meta")

# Append to a stream: body, max waiting (0 = no limit), retention ms, doorbell key. Reply:
# {'ok', id} or {'full', depth}. One doorbell token per send wakes exactly one idle worker
# blocked on it (no receive-poll delay, B34; pub/sub woke every process and cost grew, B22).
# Also the tail of the caller's REGISTER_SEND (requests.py); both define `now` first.
SEND_FN = """
local function send(stream, body, limit, retention_ms, bell)
  local depth = redis.call('XLEN', stream)
  if limit > 0 and depth >= limit then return {'full', depth} end
  local id = redis.call('XADD', stream, 'MINID', '~', tostring(now - retention_ms), '*',
    'body', body)
  redis.call('RPUSH', bell, '1')
  redis.call('LTRIM', bell, -64, -1)
  redis.call('PEXPIRE', bell, 60000)
  return {'ok', id}
end
"""

# ARGV: body, max waiting (0 = no limit), retention ms, doorbell key.
SEND = (
    _NOW
    + SEND_FN
    + """
return send(KEYS[1], ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4])
"""
)

# K: the keys of one queue, in `_PARTS` order.
_DLQ = """
local function to_dlq(K, id, body, fails, keep_ms)
  redis.call('XADD', K[6], 'MINID', '~', tostring(now - keep_ms), '*',
    'body', body, 'src', id, 'fails', fails)
  redis.call('XDEL', K[1], id)
  redis.call('ZREM', K[2], id)
  redis.call('HDEL', K[3], id)
  redis.call('HDEL', K[4], id)
  redis.call('HDEL', K[5], id)
end
local function older(a, b)
  local am, as = string.match(a, '(%d+)-(%d+)')
  local bm, bs = string.match(b, '(%d+)-(%d+)')
  if tonumber(am) ~= tonumber(bm) then return tonumber(am) < tonumber(bm) end
  return tonumber(as) < tonumber(bs)
end
"""

# Up to `want` messages of one queue (keys K), oldest first, whether new or visible again; a
# lapsed hold counts one failure. Returns taken {id, generation, body, ...}, dead {id, body, ...}.
_RECEIVE_FN = """
local function receive(K, want, vis_ms, maxr, keep)
local due = redis.call('ZRANGEBYSCORE', K[2], '-inf', now, 'LIMIT', 0, 1000)
table.sort(due, older)
local cursor = redis.call('HGET', K[7], 'cursor')
local start = cursor and ('(' .. cursor) or '-'
local fresh = redis.call('XRANGE', K[1], start, '+', 'COUNT', want)
local taken, gone = {}, {}
local i, j = 1, 1
local last_new = nil
while #taken < want * 3 and (i <= #due or j <= #fresh) do
  local pick_due
  if i > #due then pick_due = false
  elseif j > #fresh then pick_due = true
  else pick_due = older(due[i], fresh[j][1]) end
  local id, body
  if pick_due then
    id = due[i]
    i = i + 1
    local entry = redis.call('XRANGE', K[1], id, id)
    if #entry == 0 then
      redis.call('ZREM', K[2], id)
      redis.call('HDEL', K[3], id)
      redis.call('HDEL', K[4], id)
      redis.call('HDEL', K[5], id)
    else
      body = entry[1][2][2]
      if redis.call('HGET', K[5], id) == 'h' then
        local fails = redis.call('HINCRBY', K[4], id, 1)
        if maxr > 0 and fails >= maxr then
          to_dlq(K, id, body, fails, keep)
          table.insert(gone, id)
          table.insert(gone, body)
          body = nil
        end
      end
    end
  else
    id = fresh[j][1]
    body = fresh[j][2][2]
    last_new = id
    j = j + 1
  end
  if body then
    local gen = redis.call('HINCRBY', K[3], id, 1)
    redis.call('HSET', K[5], id, 'h')
    redis.call('ZADD', K[2], now + vis_ms, id)
    table.insert(taken, id)
    table.insert(taken, tostring(gen))
    table.insert(taken, body)
    if #taken >= want * 3 then break end
  end
end
if last_new then redis.call('HSET', K[7], 'cursor', last_new) end
return taken, gone
end
"""

# ARGV: max messages, visibility ms, max receives, dead-letter retention ms.
# Reply: {taken, dead, the store's time in ms}.
RECEIVE = (
    _NOW
    + _DLQ
    + _RECEIVE_FN
    + """
local taken, gone = receive(KEYS, tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]),
  tonumber(ARGV[4]))
return {taken, gone, now}
"""
)

# KEYS: the keys of several queues (7 each), in order of preference. ARGV as RECEIVE. Receives
# from the first queue that has messages (a model's priority queue, then its normal one: one
# round trip, not two). Reply: {queue index from 1 (0: none), taken, dead {index, id, body,
# ...} of every queue read, the store's time in ms}.
RECEIVE_FIRST = (
    _NOW
    + _DLQ
    + _RECEIVE_FN
    + """
local dead = {}
for q = 1, #KEYS / 7 do
  local K = {}
  for i = 1, 7 do K[i] = KEYS[(q - 1) * 7 + i] end
  local taken, gone = receive(K, tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]),
    tonumber(ARGV[4]))
  for i = 1, #gone, 2 do
    table.insert(dead, q)
    table.insert(dead, gone[i])
    table.insert(dead, gone[i + 1])
  end
  if #taken > 0 then return {q, taken, dead, now} end
end
return {0, {}, dead, now}
"""
)

# ARGV: id, generation. Reply: 1 deleted, 0 the receipt is stale (another holder has it).
ACK = """
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('XDEL', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
return 1
"""

# ARGV: id, generation, delay ms. Visible again in its place after the delay; no failure.
# The receipt ends here (generation + 1): a heartbeat extend already in flight hid the given-back
# message again for its whole visibility timeout (a drained pod's call reached no other pod).
RETURN = (
    _NOW
    + """
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HINCRBY', KEYS[3], ARGV[1], 1)
redis.call('HSET', KEYS[5], ARGV[1], 'r')
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[3]), ARGV[1])
return 1
"""
)

# ARGV: id, generation, visibility ms. Extend a hold (the worker's heartbeat).
EXTEND = (
    _NOW
    + """
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZADD', KEYS[2], 'XX', now + tonumber(ARGV[3]), ARGV[1])
return 1
"""
)

# KEYS: generation and visibility key of each message. ARGV: id, generation, visibility ms of
# each. Every hold of a process in one call (the heartbeat). Reply: 1 or 0 (stale) per message.
EXTEND_MANY = (
    _NOW
    + """
local out = {}
for i = 1, #KEYS / 2 do
  local id, ms = ARGV[3 * i - 2], tonumber(ARGV[3 * i])
  if redis.call('HGET', KEYS[2 * i - 1], id) == ARGV[3 * i - 1] then
    redis.call('ZADD', KEYS[2 * i], 'XX', now + ms, id)
    table.insert(out, 1)
  else
    table.insert(out, 0)
  end
end
return out
"""
)

# ARGV: id, generation, delay ms, max receives, dead-letter retention ms. The handler failed:
# count one failure; at `max_receives` the message goes to the dead-letter stream.
# Reply: 'retry', 'dead' or 'stale'.
FAIL = (
    _NOW
    + _DLQ
    + """
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then return 'stale' end
local fails = redis.call('HINCRBY', KEYS[4], ARGV[1], 1)
local maxr = tonumber(ARGV[4])
if maxr > 0 and fails >= maxr then
  local entry = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1])
  if #entry > 0 then to_dlq(KEYS, ARGV[1], entry[1][2][2], fails, tonumber(ARGV[5])) end
  return 'dead'
end
redis.call('HSET', KEYS[5], ARGV[1], 'r')
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[3]), ARGV[1])
return 'retry'
"""
)

# Reply: {messages in the stream, held, dead letters}.
DEPTH = """
local held = 0
for _, v in ipairs(redis.call('HVALS', KEYS[5])) do if v == 'h' then held = held + 1 end end
return {redis.call('XLEN', KEYS[1]), held, redis.call('XLEN', KEYS[6])}
"""

# ARGV: retention ms. Dead letters older than the retention go: XADD trimmed them only when a
# new one came, so a quiet queue kept its prompts for good (RESCAN X10). Reply: how many.
TRIM_DEAD = (
    _NOW
    + """
return redis.call('XTRIM', KEYS[6], 'MINID', tostring(now - tonumber(ARGV[1])))
"""
)

# ARGV: max count, retention ms, doorbell key. Dead letters back on the queue (new ids: they go
# to the back), with SEND's trim and doorbell. An entry without a body is dropped and counted:
# XADD with a nil body raised part-way, after earlier moves that Lua does not roll back, and
# the redriven calls rang no doorbell (EN-12). Reply: {moved, dropped}.
REDRIVE = (
    _NOW
    + """
local moved, dropped = 0, 0
for _, entry in ipairs(redis.call('XRANGE', KEYS[6], '-', '+', 'COUNT', tonumber(ARGV[1]))) do
  local fields = entry[2]
  local body
  for k = 1, #fields, 2 do if fields[k] == 'body' then body = fields[k + 1] end end
  if body then
    redis.call('XADD', KEYS[1], 'MINID', '~', tostring(now - tonumber(ARGV[2])), '*',
      'body', body)
    moved = moved + 1
  else
    dropped = dropped + 1
  end
  redis.call('XDEL', KEYS[6], entry[1])
end
if moved > 0 then
  redis.call('RPUSH', ARGV[3], '1')
  redis.call('LTRIM', ARGV[3], -64, -1)
  redis.call('PEXPIRE', ARGV[3], 60000)
end
return {moved, dropped}
"""
)

# ARGV: doorbell key. A token back on a doorbell, with SEND's cap and TTL: a bare RPUSH on an
# expired bell made one with no TTL that outlived a removed model (EN-13).
RING = """
redis.call('RPUSH', ARGV[1], '1')
redis.call('LTRIM', ARGV[1], -64, -1)
redis.call('PEXPIRE', ARGV[1], 60000)
return 1
"""


@dataclass(frozen=True, slots=True)
class Message:
    """A received message: its stream id (its place), generation, body and receive time.

    `received_ms` is the store's clock when this receive took it (0 when unknown): with
    `position_ms`, the time it waited, both on one clock.
    """

    queue: str
    id: str
    generation: int
    body: bytes
    received_ms: int = 0

    @property
    def position_ms(self) -> int:
        """The store's time at send, in ms (deadlines and ages are measured from it)."""
        return int(self.id.split("-", 1)[0])


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """A message moved to the dead-letter stream by this receive."""

    queue: str
    id: str
    body: bytes


@dataclass(frozen=True, slots=True)
class QueueSettings:
    """Per-queue numbers (from the model's `queue` setting and `max_waiting`)."""

    visibility_secs: float = 60.0
    max_receives: int = 10
    retention_secs: int = RETENTION_SECS
    dlq_retention_secs: int = 86_400
    max_waiting: int = 50_000


class QueueFull(Exception):  # the plan's error kind is `queue_full`
    """`max_waiting` calls already wait on this queue."""

    def __init__(self, queue: str, depth: int) -> None:
        """Keep the queue and its depth."""
        super().__init__(f"queue {queue} is full ({depth} waiting)")
        self.queue = queue
        self.depth = depth


class NotReplicated(Exception):  # the plan's error kind is `queue_unavailable`
    """The send reached the primary but not `queue_wait_replicas` replicas in time."""


class StreamQueues:
    """Send, receive, return, extend, fail and acknowledge on named queues of one prefix."""

    def __init__(self, client: redis.asyncio.Redis, keys: Keys) -> None:
        """Register the scripts; no I/O until the first call."""
        self._client = client
        self.keys = keys
        self._send = client.register_script(SEND)
        self._receive = client.register_script(RECEIVE)
        self._receive_first = client.register_script(RECEIVE_FIRST)
        self._extend_many = client.register_script(EXTEND_MANY)
        self._ack = client.register_script(ACK)
        self._return = client.register_script(RETURN)
        self._extend = client.register_script(EXTEND)
        self._fail = client.register_script(FAIL)
        self._depth = client.register_script(DEPTH)
        self._redrive = client.register_script(REDRIVE)
        self._trim_dead = client.register_script(TRIM_DEAD)
        self._ring = client.register_script(RING)

    async def _send_and_wait(
        self, queue: str, args: list[bytes | int | str], replicas: int, wait_ms: int
    ) -> tuple[Any, Any]:
        """SEND and WAIT on one connection (WAIT counts only that connection's writes).

        A raw EVALSHA: a script run through a redis-py pipeline first asks SCRIPT EXISTS, one
        more round trip on every send. A NOSCRIPT reply is handled by `send`.
        """
        keys = self.queue_keys(queue)
        async with self._client.pipeline(transaction=False) as pipe:
            pipe.execute_command("EVALSHA", self._send.sha, len(keys), *keys, *args)
            pipe.execute_command("WAIT", replicas, wait_ms)
            reply, acked = await pipe.execute()
        return reply, acked

    def bell(self, queue: str) -> str:
        """The queue's doorbell: a list a send pushes one token on (see SEND)."""
        return f"{self.keys.prefix}:bell:{queue}"

    async def wait_bell(self, queues: Sequence[str], timeout_secs: float) -> str | None:
        """Block until one of the queues' doorbells rings (that queue) or `timeout_secs` pass."""
        bells = {self.bell(q): q for q in queues}
        rung = await self._client.blpop(list(bells), timeout=timeout_secs)
        return None if rung is None else bells.get(_text(rung[0]))

    async def ring_again(self, queue: str) -> None:
        """Put back a doorbell token this worker took but cannot serve: another worker wakes."""
        await self._ring([], [self.bell(queue)])

    def queue_keys(self, queue: str) -> list[str]:
        """The keys of one queue, in `_PARTS` order (stream first)."""
        return [self.keys.queue(queue, part) for part in _PARTS]

    async def send(
        self,
        queue: str,
        body: bytes,
        settings: QueueSettings,
        *,
        wait_replicas: int = 0,
        wait_ms: int = 100,
    ) -> str:
        """Append a message; returns its id. `QueueFull` above `max_waiting`.

        With `wait_replicas`, the send and a `WAIT` go down one connection (WAIT counts that
        connection's writes) and too few acknowledgements raise `NotReplicated`.
        """
        args: list[bytes | int | str] = [
            body,
            settings.max_waiting,
            settings.retention_secs * 1000,
            self.bell(queue),
        ]
        if not wait_replicas:
            reply = await self._send(self.queue_keys(queue), args)
            acked = None
        else:
            try:
                reply, acked = await self._send_and_wait(queue, args, wait_replicas, wait_ms)
            except NoScriptError:
                # A first send, or a failover to a node without the script cached: load it and
                # run once more. Exactly once, like redis-py's own reload for plain script
                # calls: a second NOSCRIPT means the cache was flushed again mid-call.
                await self._client.script_load(SEND)
                reply, acked = await self._send_and_wait(queue, args, wait_replicas, wait_ms)
        if _text(reply[0]) == "full":
            raise QueueFull(queue, int(reply[1]))
        if acked is not None and int(acked) < wait_replicas:
            raise NotReplicated(
                f"queue {queue}: {acked} of {wait_replicas} replicas in {wait_ms} ms"
            )
        return _text(reply[1])

    async def receive(
        self, queue: str, count: int, settings: QueueSettings
    ) -> tuple[list[Message], list[DeadLetter]]:
        """Up to `count` messages, oldest first, held for the visibility time; and any dead."""
        taken, dead, now = await self._receive(
            self.queue_keys(queue), _receive_args(count, settings)
        )
        letters = [
            DeadLetter(queue, _text(dead[i]), _bytes(dead[i + 1])) for i in range(0, len(dead), 2)
        ]
        return _messages(queue, taken, int(now)), letters

    async def receive_first(
        self, queues: Sequence[str], count: int, settings: QueueSettings
    ) -> tuple[list[Message], list[DeadLetter]]:
        """`receive` from the first of `queues` that has messages, in one round trip.

        A model's worker reads its priority queue, then its normal one: two scripts per poll
        before. Dead letters of every queue read come back too.
        """
        if not queues:
            return [], []
        keys = [key for queue in queues for key in self.queue_keys(queue)]
        index, taken, dead, now = await self._receive_first(keys, _receive_args(count, settings))
        letters = [
            DeadLetter(queues[int(dead[i]) - 1], _text(dead[i + 1]), _bytes(dead[i + 2]))
            for i in range(0, len(dead), 3)
        ]
        if not int(index):
            return [], letters
        return _messages(queues[int(index) - 1], taken, int(now)), letters

    async def receive_wait(
        self, queue: str, count: int, settings: QueueSettings, wait_secs: float
    ) -> tuple[list[Message], list[DeadLetter]]:
        """`receive`, polling with growing pauses (20 ms to 500 ms) for up to `wait_secs`."""
        loop = asyncio.get_running_loop()
        until = loop.time() + wait_secs
        pause_secs = 0.02
        while True:
            messages, dead = await self.receive(queue, count, settings)
            if messages or dead or loop.time() >= until:
                return messages, dead
            await asyncio.sleep(min(pause_secs, max(0.0, until - loop.time())))
            pause_secs = min(pause_secs * 2, 0.5)

    async def ack(self, message: Message) -> bool:
        """Delete a handled message; False when the receipt is stale."""
        return bool(
            await self._ack(self.queue_keys(message.queue), [message.id, message.generation])
        )

    async def return_(self, message: Message, delay_secs: float = 0.0) -> bool:
        """Make a message visible again in its place (after `delay_secs`), with no failure."""
        return bool(
            await self._return(
                self.queue_keys(message.queue),
                [message.id, message.generation, int(delay_secs * 1000)],
            )
        )

    async def extend(self, message: Message, visibility_secs: float) -> bool:
        """Extend the hold of a message (heartbeat); False when the receipt is stale."""
        return bool(
            await self._extend(
                self.queue_keys(message.queue),
                [message.id, message.generation, int(visibility_secs * 1000)],
            )
        )

    async def extend_many(self, holds: Sequence[tuple[Message, float]]) -> list[bool]:
        """Extend several holds (message, visibility secs) in one call; False where stale."""
        if not holds:
            return []
        keys: list[str] = []
        args: list[str | int] = []
        for message, visibility_secs in holds:
            keys += [self.keys.queue(message.queue, "gen"), self.keys.queue(message.queue, "vis")]
            args += [message.id, message.generation, int(visibility_secs * 1000)]
        return [bool(flag) for flag in await self._extend_many(keys, args)]

    async def fail(self, message: Message, settings: QueueSettings, delay_secs: float = 0.0) -> str:
        """Count a failure: `retry` (visible again after the delay), `dead` or `stale`."""
        return _text(
            await self._fail(
                self.queue_keys(message.queue),
                [
                    message.id,
                    message.generation,
                    int(delay_secs * 1000),
                    settings.max_receives,
                    settings.dlq_retention_secs * 1000,
                ],
            )
        )

    async def depth(self, queue: str) -> tuple[int, int, int]:
        """(messages in the queue, held by workers, dead letters)."""
        reply = await self._depth(self.queue_keys(queue), [])
        return int(reply[0]), int(reply[1]), int(reply[2])

    async def dead_letters(self, queue: str, count: int = 10) -> list[dict[str, Any]]:
        """Dead letters, oldest first: id, source id, failures, body."""
        entries = await self._client.xrange(self.keys.queue(queue, "dlq"), count=count)
        out = []
        for entry_id, fields in entries or ():
            row = {_text(k): v for k, v in (fields or {}).items()}
            out.append(
                {
                    "id": _text(entry_id),
                    "src": _text(row.get("src", b"")),
                    "fails": int(row.get("fails", 0)),
                    "body": _bytes(row.get("body", b"")),
                }
            )
        return out

    async def redrive(
        self, queue: str, count: int = 1000, retention_secs: int = RETENTION_SECS
    ) -> int:
        """Move up to `count` dead letters back to the queue; returns how many.

        A dead letter without a body cannot run again: it is dropped, with a warning.
        """
        moved, dropped = await self._redrive(
            self.queue_keys(queue), [count, retention_secs * 1000, self.bell(queue)]
        )
        if int(dropped):
            logger.warning("redrive_dropped_bodyless", queue=queue, count=int(dropped))
        return int(moved)

    async def trim_dead(self, queue: str, keep_secs: int) -> int:
        """Drop dead letters older than `keep_secs` (on a timer); returns how many went."""
        return int(await self._trim_dead(self.queue_keys(queue), [int(keep_secs) * 1000]))

    async def mark_consumer(self, queue: str, ttl_secs: int) -> None:
        """Say a process consumes this queue; the mark lapses `ttl_secs` after the last one."""
        await self._client.set(self.keys.queue(queue, "consumer"), b"1", ex=ttl_secs)

    async def has_consumer(self, queue: str) -> bool:
        """True while some process marks itself as this queue's consumer."""
        return bool(await self._client.exists(self.keys.queue(queue, "consumer")))

    async def purge_dead(self, queue: str) -> None:
        """Delete every dead letter of a queue."""
        await self._client.delete(self.keys.queue(queue, "dlq"))

    async def delete(self, names: Sequence[str]) -> None:
        """Delete queues and everything in them (model removal)."""
        await self._client.delete(*[k for name in names for k in self.queue_keys(name)])


def _receive_args(count: int, settings: QueueSettings) -> list[int]:
    return [
        count,
        int(settings.visibility_secs * 1000),
        settings.max_receives,
        settings.dlq_retention_secs * 1000,
    ]


def _messages(queue: str, taken: Sequence[Any], now_ms: int) -> list[Message]:
    return [
        Message(queue, _text(taken[i]), int(taken[i + 1]), _bytes(taken[i + 2]), now_ms)
        for i in range(0, len(taken), 3)
    ]


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _bytes(value: Any) -> bytes:
    return value if isinstance(value, bytes) else str(value).encode()
