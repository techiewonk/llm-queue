# Configuration

Every process setting (`LLMQ_*`), the `Tuning` fields set in code, and where the registry
settings are listed. How to use them is in [Configure](../how-to/configure.md) and
[Tune](../how-to/tune.md).

## Process settings (`LLMQ_*`)

Read by `LLMQueue.from_env()` from `secrets=` first (when given), then the environment. Every
problem is reported at once as a `ConfigError`.

### Stores

| Setting | Default | Meaning |
|---|---|---|
| `LLMQ_STORE_URL` | required | The state store, `redis://` or `rediss://` (`valkey://` / `valkeys://` too). Outside `dev` / `local` / `test` / `ci` it must be `rediss://` with the certificate and host name checked |
| `LLMQ_STORE_PASSWORD` | | Optional: read from `from_env(secrets=)` on every new connection (rotation) |
| `LLMQ_QUEUE_STORE_URL` | the state store | Optional separate queue store (same TLS rule) |
| `LLMQ_QUEUE_STORE_PASSWORD` | | Optional: as `LLMQ_STORE_PASSWORD`, for the queue store |
| `LLMQ_STORE_MAX_CONNECTIONS` | 200 | Store pool size (worker slots + callers + 3) |
| `LLMQ_QUEUE_FAILOVER_SECS` | 5 | With a separate queue store: errors this long before calls run direct |
| `LLMQ_QUEUE_WAIT_REPLICAS` | 0 (off) | A send waits for this many replicas to acknowledge it ... |
| `LLMQ_QUEUE_WAIT_MS` | 100 | ... for this long (must stay below the store read timeout, see [Tune](../how-to/tune.md)) |

### Environment and names

| Setting | Default | Meaning |
|---|---|---|
| `LLMQ_ENVIRONMENT` | `dev` | `dev`, `local`, `test`, `ci`, or any other name (then TLS and the salt are required; `production` is read as `prod`) |
| `LLMQ_ID_HASH_SALT` | | Key of the user / session id hash (HMAC); required outside dev |
| `LLMQ_KEY_PREFIX` | `llmq` | Prefix of this environment's store keys |
| `LLMQ_QUEUE_PREFIX` | empty | Prefix of this environment's queue names |
| `LLMQ_MODE` | `both` | `both`, `caller` or `worker` |
| `LLMQ_METRICS_SINK` | `emf` | `emf` (CloudWatch), `prometheus` or `none` |

### Registry

| Setting | Default | Meaning |
|---|---|---|
| `LLMQ_REGISTRY` | | The registry document (JSON) |
| `LLMQ_REGISTRY_FILE` | | A file holding it (set one of the two, not both) |
| `LLMQ_REGISTRY_LIVE_EDIT` | `safe` | `safe`, `all` or `off` (see [Live edits](../how-to/configure.md#live-edits)) |
| `LLMQ_CFG_RELOAD_SECS` | 30 | How often each process reloads the registry |
| `LLMQ_AUDIT_RETENTION_DAYS` | 90 | How long registry audit entries are kept |
| `LLMQ_BASE_URL_ALLOWED_HOSTS` | empty | Comma-separated hosts an account's `base_url` may point at (empty: any host) |
| `LLMQ_DEBUG_CAPTURE_FROM_CONFIG` | false | Allows a model's `debug_capture` (prompt text in the store); refused in `prod` |

### Calls and processes

| Setting | Default | Meaning |
|---|---|---|
| `LLMQ_LEASE_SECS` | 60 | Lease of a running call's grant; renewed by the heartbeat (every third of it) |
| `LLMQ_CALLER_TTL_SECS` | 30 | How long a caller process counts as alive without a heartbeat |
| `LLMQ_RESULT_TTL_SECS` | 300 | How long a stored result lives |
| `LLMQ_RESULT_DEDUPE_SECS` | 0 | Keep a result with a library-made id this long after it was read (0: delete on read) |
| `LLMQ_STATUS_TTL_SECS` | 1800 | How long a call's status record lives |
| `LLMQ_DRAIN_SECS` | 45 | Default `drain_secs` of `stop()` |
| `LLMQ_LOCAL_LIMIT_CAP` | 10 | Calls per process and model run without the store under `store_down: static_cap` |

### Tests only

| Setting | Meaning |
|---|---|
| `LLMQ_BEDROCK_ENDPOINT_URL`, `LLMQ_STS_ENDPOINT_URL` | Point Bedrock / STS at fakes (fakellm, moto) |

## `Tuning` (in code)

Passed as `LLMQueue.from_env(tuning=Tuning(...))`. Defaults:

| Part | Field (default) |
|---|---|
| `StoreOptions` (`tuning.store`) | `retries` (3), `connect_timeout_secs` (5), `read_timeout_secs` (15), `health_check_secs` (30), `pool_wait_secs` (10) |
| `BedrockOptions` (`tuning.bedrock`) | `read_timeout_secs` (300), `connect_timeout_secs` (10), `max_pool_connections` (100), `role_session_secs` (3600) |
| `EngineTimings` (`tuning.engine`) | `idle_poll_secs` (0.05), `idle_slow_secs` (1), `idle_after_empty` (20), `result_recheck_secs` (60), `health_probe_secs` (3), `stream_read_block_ms` (100), `admission_probe_wait_ms` (200), `admission_slot_wait_ms` (50), `admission_slot_jitter_ms` (150), `admission_wait_ms` (100), `limiter_cut_ms` (1000), `limiter_cut_factor` (0.7), `budget_key_ms` (180000) |
| `RegistryOptions` (`tuning.registry`) | `history_size` (20), `edit_retries` (3) |

What each field does and the start-up checks are in [Tune](../how-to/tune.md).

## Registry settings

The registry document's settings (budgets, balancing, fallback, retries, breaker, timeouts,
queues, inference defaults, alerts) can each be set at some of the levels deployment > model >
account > defaults. Every setting with its levels, type, range and default is in the
[design plan](https://github.com/techiewonk/llm-queue/blob/main/docs/ACCOUNT_ROUTING_PLAN.md),
sections 3.9 and 3.19.

For a running system, `llm-queue settings get --model M [--deployment D]` prints the effective
value of every setting and the level it came from.
