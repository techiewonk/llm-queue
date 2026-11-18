# Tune

Timeouts, pool sizes and loop pacing are set in code with `Tuning`. Every value has a measured
default, so most apps pass nothing. Set only what you change; the rest keep their defaults:

```python
from llm_queue import LLMQueue, StoreOptions, Tuning

llmq = LLMQueue.from_env(tuning=Tuning(store=StoreOptions(read_timeout_secs=20)))
```

`Tuning` has six parts, each importable from `llm_queue`: `store` (`StoreOptions`), `bedrock`
(`BedrockOptions`), `openai` (`OpenAIOptions`), `warm` (`WarmOptions`), `engine`
(`EngineTimings`) and `registry` (`RegistryOptions`). Every field, with its default and range,
is in [Configuration](../reference/configuration.md#tuning-in-code).

## What people change

| Need | Field (default) |
|---|---|
| A slow or remote store (cross-region ElastiCache) | `store.read_timeout_secs` (15 s), `store.connect_timeout_secs` (5 s), `engine.health_probe_secs` (3 s) |
| Long generations past five minutes | `bedrock.read_timeout_secs` / `openai.read_timeout_secs` (300 s), together with the registry's `attempt_timeout_secs` (60 s) and `timeout_secs` (120 s) |
| Many calls in flight to one OpenAI-compatible server | `openai.max_connections` (100), `openai.max_keepalive_connections` (20) |
| Cold first calls after idle periods | `warm.connections` (2 per Bedrock client), `warm.refresh_secs` (10 s) |
| botocore instead of the aiohttp Bedrock transport | `bedrock.transport` (`aiohttp`); `botocore` is slower (measured: aiohttp +78-104 % calls/s) |
| Long assumed-role sessions | `bedrock.role_session_secs` (3600; 900 to 43200, STS limits) |
| More rollback history | `registry.history_size` (20 versions) |

Bedrock connection pools are sized from the registry (the concurrency slots routed to each
client, at least 10), not from `bedrock.max_pool_connections`. The store pool is
`LLMQ_STORE_MAX_CONNECTIONS` (200) and audit retention `LLMQ_AUDIT_RETENTION_DAYS` (90): both
are process settings, not tuning fields.

## Checks at start-up

`from_env` refuses bad tuning with `ConfigError`, every problem listed at once:

- wrong types, `NaN` or infinite values, and zero or negative values (0 is allowed only for
  `store.retries`, `warm.connections` and `warm.refresh_secs`);
- upper bounds: time fields at most one day (86400 s, 86400000 ms), `store.retries` at most
  100, `registry.edit_retries` at most 10, `bedrock.role_session_secs` 900 to 43200;
- lower bounds: `engine.budget_key_ms` at least 130000 (a fixed window spans 2 minutes),
  `engine.job_ttl_ms` at least 60000, `engine.slow_stale_ms` at least 1000, the Bedrock and
  OpenAI read timeouts at least 1 s, `registry.history_size` at least 2;
- `store.read_timeout_secs` at least 1 s above `engine.idle_slow_secs`, `LLMQ_REPLICA_WAIT_MS` and
  `engine.stream_read_block_ms` (each a blocking command), or that command times out;
- `LLMQ_RESULT_TTL_SECS` at least `engine.result_recheck_secs`, or a caller that missed its
  wake-up never reads its result;
- `engine.limiter_cut_factor` below 1; `engine.idle_poll_secs` not above
  `engine.idle_slow_secs`; `engine.throttle_base_ms` not above `engine.throttle_max_ms`;
- `warm.refresh_secs` below `warm.keepalive_secs` (unless 0), and
  `openai.max_keepalive_connections` not above `openai.max_connections`.

At `start()`, once the registry is loaded, a model whose `attempt_timeout_secs` is above its
client's read timeout, or whose timeout is longer than `LLMQ_STATUS_TTL_SECS`, is logged as a
`config_cross_check` warning (the registry is shared, so it never stops a pod).

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
