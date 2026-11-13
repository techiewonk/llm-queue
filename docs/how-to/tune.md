# Tune

Timeouts, pool sizes and loop pacing are set in code with `Tuning`. Every value has a measured
default, so most apps pass nothing. Set only what you change:

```python
from llm_queue import LLMQueue, StoreOptions, Tuning

llmq = LLMQueue.from_env(tuning=Tuning(store=StoreOptions(read_timeout_secs=20)))
```

`Tuning` has four parts: `store` (`StoreOptions`), `bedrock` (`BedrockOptions`), `engine`
(`EngineTimings`) and `registry` (`RegistryOptions`).

## StoreOptions

The Valkey / Redis client of every process. The pool size is `LLMQ_STORE_MAX_CONNECTIONS`.

| Field | Default | What it does |
|---|---|---|
| `retries` | 3 | Retries of a failed store command, with jittered backoff (~1.5 s in all) |
| `connect_timeout_secs` | 5 | A store node that does not answer in a failover fails fast |
| `read_timeout_secs` | 15 | A reply that never comes fails the command instead of hanging its call |
| `health_check_secs` | 30 | A connection idle this long is pinged before use |
| `pool_wait_secs` | 10 | A burst waits this long for a free connection |

## BedrockOptions

The Bedrock and STS clients.

| Field | Default | What it does |
|---|---|---|
| `read_timeout_secs` | 300 | Bedrock / STS read timeout (each attempt also has `attempt_timeout_secs`) |
| `connect_timeout_secs` | 10 | Bedrock / STS connect timeout |
| `max_pool_connections` | 100 | Connections per client (one client per account x region) |
| `role_session_secs` | 3600 | Assumed-role session length (900-43200; 3600 is the role-chaining maximum) |

## EngineTimings

Pacing of the engine's loops.

| Field | Default | What it does |
|---|---|---|
| `idle_poll_secs` | 0.05 | Idle receive: poll this often ... |
| `idle_slow_secs` | 1 | ... then this often ... |
| `idle_after_empty` | 20 | ... after this many empty polls. A send's doorbell wakes a worker at once |
| `result_recheck_secs` | 60 | A waiting caller's last-resort read of its result |
| `health_probe_secs` | 3 | A store probe slower than this reports "down" |
| `stream_read_block_ms` | 100 | An opened chunk stream joins the shared read within this |
| `admission_probe_wait_ms` | 200 | A breaker's probe call is in flight: check again after this |
| `admission_slot_wait_ms` | 50 | No free slot: retry after this ... |
| `admission_slot_jitter_ms` | 150 | ... plus up to this, at random (spreads the retries) |
| `admission_wait_ms` | 100 | A refusal with no better estimate retries after this |
| `limiter_cut_ms` | 1000 | A throttle cuts the adaptive concurrency limit at most once per this ... |
| `limiter_cut_factor` | 0.7 | ... to this fraction of itself (below 1) |
| `budget_key_ms` | 180000 | Budget and limiter keys live this long (at least 130000: a fixed window spans 2 minutes) |

## RegistryOptions

| Field | Default | What it does |
|---|---|---|
| `history_size` | 20 | Registry versions kept for rollback |
| `edit_retries` | 3 | Retries of a live edit that raced another one |

Audit retention is `LLMQ_AUDIT_RETENTION_DAYS`, not a tuning field.

## Checks at start-up

`from_env` refuses bad tuning with `ConfigError`, every problem listed at once:

- wrong types, zero or negative values;
- `store.read_timeout_secs` must be at least 1 s above `idle_slow_secs`, `LLMQ_QUEUE_WAIT_MS`
  and `stream_read_block_ms` (each a blocking command), or that command times out;
- `limiter_cut_factor` below 1; `budget_key_ms` at least 130000;
- `idle_poll_secs` not above `idle_slow_secs`;
- `role_session_secs` from 900 to 43200 (STS limits).

## Circuit breaker timing (registry)

The breaker is tuned per model in the registry, not in `Tuning`. Each setting can be set on
defaults, an account, a model or a deployment:

| Setting | Default | What it does |
|---|---|---|
| `breaker_threshold` | 5 | Failures that open the breaker |
| `breaker_window_secs` | 60 | Failures count toward the threshold only within this window |
| `breaker_cooldown_secs` | 15 | An open breaker allows one probe call after this |
| `breaker_probe_secs` | 60 | A probe call that never reported back stops blocking others after this |

How the breaker moves between states is in [Failures and recovery](../explanation/resilience.md#circuit-breaker).
