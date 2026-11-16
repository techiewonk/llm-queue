# Run on a shared cache store

Use this when llm-queue shares a Valkey or Redis (for example ElastiCache) with your app's own
cache, and the store's `maxmemory-policy` is `volatile-lru` (ElastiCache's default) or another
`volatile-*` policy. The cache needs eviction: when the store fills, old cache keys go. llm-queue
needs the opposite: its queue messages, budgets and results must never be evicted.

From 0.1.0b3 both work on one store. With key expiry `swept`, none of the keys llm-queue needs
has a TTL, and a `volatile-*` policy only evicts keys that have one. Your cache keys (which have
TTLs) are evicted as before; llm-queue's are not.

## What happens in each case

| Store policy | llm-queue keys | Your cache keys | Ready (strict mode)? |
|---|---|---|---|
| `noeviction` | Kept | Kept; a full store refuses writes (your cache too) | Yes |
| `volatile-lru` (or `-lfu`, `-ttl`, `-random`), key expiry `swept` | Kept: none has a TTL | Evicted when the store fills | Yes |
| `volatile-*`, key expiry `native` | Can be evicted (lost calls, overspent budgets) | Evicted | No |
| `allkeys-*` | Can be evicted, in any mode | Evicted | No: not supported |

## 1. Settings

Nothing to set: the default `LLMQ_KEY_EXPIRY=auto` reads the store's policy and picks `swept`
for any policy but `noeviction`. To pin it (so a policy that cannot be read, or a later change,
never flips it):

```bash
LLMQ_KEY_EXPIRY=swept    # or auto (the default)
```

Give every process that shares the key prefix the same value. Do not set `native` here: in
strict mode the process would be not ready, and outside strict mode its keys could be evicted.

Two settings tune the sweep that deletes expired keys; the defaults suit most loads:

| Setting | Default | What it does |
|---|---|---|
| `LLMQ_TTL_SWEEP_BATCH` | 500 | Keys deleted per sweep script run |
| `LLMQ_TTL_SWEEP_ROUNDS` | 100 | Runs per sweep (every 10 s), at most: 50000 keys per 10 s by default |

Raise them only if `ZCARD P:ttl` keeps growing while the load is steady (see step 4).

## 2. What changes

- Keys expire up to 10 s late (one sweep). Lifetimes are otherwise the same.
- Seven short-lived keys keep a real TTL on purpose (locks, the readiness probe, debug capture).
  The store may evict them; the cost is duplicate work at most. See
  [Key expiry modes](../explanation/storage.md#key-expiry-modes).
- One extra `ZADD` per expiry write, and one sweep every 10 s: every process (worker or
  caller) tries, and the one holding the lock `P:lock:ttlsweep` runs it.

Example: a call's status record lives 1800 s. With `swept` it has no TTL; the zset `P:ttl`
holds `llmq:{r:ID}:status` with score "now + 1800 s", and the sweep after that time deletes it.
`TTL llmq:{r:ID}:status` in `valkey-cli` answers `-1` (no TTL), which is expected.

## 3. What readiness, health and metrics show

| Where | Shows | Expect |
|---|---|---|
| `GET /health/ready`, `await llmq.health()` | `ready` | `true` |
| `health()["eviction_policy"]` | the state store's policy | `volatile-lru` |
| `health()["key_expiry"]` | the state store's mode | `swept` |
| `llmq_key_expiry_swept{store}` | 1 in `swept`, 0 in `native` | 1 |
| `llmq_store_eviction_unsafe{store}` | 1 when the policy can evict llm-queue keys in its mode | 0 |
| `llmq_ttl_swept_total` | keys the sweep deleted | rising with traffic |
| log `key_expiry_changed` (warning) | a store's mode changed, with `before`, `mode`, `policy` | once, at the first start on this store |

## 4. Alarms to keep (CloudWatch)

`Evictions` is now **expected to be above 0**: those are your app's cache keys. llm-queue keys
cannot be evicted (they have no TTL), so do not alarm on `Evictions` for llm-queue's sake.

What to watch instead is memory. When the store is full and only keys without a TTL are left
(llm-queue's keys, and any of your own keys without a TTL), nothing more can be evicted and the
store refuses writes (`OOM`). llm-queue then fails loudly, never silently: calls fail
`queue_unavailable` (or follow each model's `store_down` rule), admin operations raise
`StoreUnavailable`, and `health()["stores"]` reports the store `down`.

| Alarm | On | Why |
|---|---|---|
| Memory high | ElastiCache `DatabaseMemoryUsagePercentage` Maximum > 90 for 5 min | The cache keeps it high by design; a level near 100 with few evictions means non-TTL keys are filling it |
| Store refusing writes | `llmq_store_up` Minimum < 1; `llmq_store_errors_total` Sum > 0 for 3 periods | An `OOM` refusal reads as the store being down |
| Calls failing on the store | `llmq_caller_failures_total{outcome="queue_unavailable"}` Sum > 0 | What callers see when writes are refused |
| Not ready on the policy | `llmq_store_eviction_unsafe` Maximum >= 1 | Someone changed the policy to `allkeys-*`, or set `LLMQ_KEY_EXPIRY=native` |

The other alarms in [Monitor](monitor.md) stay as they are.

## 5. Sizing

- llm-queue's own memory is the same as on a dedicated store, plus the index: one `P:ttl`
  member per key with a lifetime (roughly the key name and about 20 bytes; an estimate, not a
  measurement). Request records dominate: about 4 keys per call, kept for
  `LLMQ_STATUS_TTL_SECS` (1800 s). For example, 20 calls per second keep about 36000 calls,
  so about 144000 index members.
- Leave room for llm-queue's peak on top of what the cache needs to be useful. Eviction only
  frees cache keys; it never makes room by dropping queue work.
- Results are the largest values. Large results can go to S3 instead (see
  [Keep large results in S3](large-results.md)).

## 6. `allkeys-*` is refused

`allkeys-lru` and the other `allkeys-*` policies evict any key, TTL or not: queue messages, the
registry, `P:ttl` itself. No key expiry mode protects against that. In strict mode the process
stays not ready with `not_ready_reason`; outside strict mode it logs `store_eviction_policy` at
start. Use a `volatile-*` policy or `noeviction`, or give llm-queue its own store
(`LLMQ_STORE_URL`, and `LLMQ_QUEUE_STORE_URL` for the queues).

## 7. Check it

```python
health = await llmq.health()
assert health["key_expiry"] == "swept"
assert health["eviction_policy"].startswith("volatile-")
assert health["ready"]
```

```bash
valkey-cli -u "$LLMQ_STORE_URL" GET llmq:expiry        # "swept"
valkey-cli -u "$LLMQ_STORE_URL" ZCARD llmq:ttl         # keys waiting to expire
valkey-cli -u "$LLMQ_STORE_URL" ZRANGE llmq:ttl 0 0 WITHSCORES   # the next one due (store ms)
```

`llmq` is the default `LLMQ_KEY_PREFIX`. `ZCARD llmq:ttl` should go up and down with traffic.
If it only grows while the load is steady, the sweep is behind: check that `llmq_ttl_swept_total`
rises (any running process sweeps, worker or caller) and that no `ttl_sweep_failed` warnings are
logged; then raise `LLMQ_TTL_SWEEP_ROUNDS`.

## Moving an existing deployment

1. Upgrade every process to 0.1.0b3 (0.1.0b2 pods do not read the store's mode and set native
   TTLs).
2. Keep `LLMQ_KEY_EXPIRY=auto` (or set `swept`).
3. Point it at the shared store, or change the store's policy to `volatile-lru`.

The switch loses nothing. In `swept` mode, setting an expiry also clears the key's native TTL
(`PERSIST`), so each key moves to the index on its next write. A key written before the switch
and not since keeps its native TTL and still expires then (at most a day, for the slowest);
until then it can still be evicted. Nothing else is needed. To go back to 0.1.0b2, drain the
index first: see [Upgrade](upgrade.md#3-rolling-back-to-010b2-after-running-swept).
See [Store keys](../reference/store-keys.md) for what each key does in each mode.
