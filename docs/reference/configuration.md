# Configuration

Where the process settings (`LLMQ_*`) are listed, the `Tuning` fields set in code, and where the
registry settings are listed. How to use them is in [Configure](../how-to/configure.md) and
[Tune](../how-to/tune.md).

## Process settings (`LLMQ_*`)

Every `LLMQ_*` variable, its default, and what `LLMQ_ENVIRONMENT` turns on (unset is strict) are
in [Environment variables](environment.md).

Jobs have their own process settings, `LLMQ_JOB_*` (read by `LLMQueue.from_env`, or passed as
`LLMQueue(..., job_settings=JobSettings(...))`); a bad value is a `ConfigError` at start:

| Variable | Default | Meaning |
|---|---|---|
| `LLMQ_JOB_DEDUPE_SECS` | 86400 (24 h) | `send_job(job_id=X)` again within this queues nothing and returns `X`; 0 turns it off |
| `LLMQ_JOB_RETRY_BASE_SECS` | 5 | First retry delay; it doubles after each failure ... |
| `LLMQ_JOB_RETRY_MAX_SECS` | 300 | ... up to this (at least the base) |
| `LLMQ_JOB_RETRY_FOR_SECS` | 3600 | Retries go on until the delays add up to this; then the job is dead |
| `LLMQ_JOB_DEAD_RETENTION_SECS` | 604800 (7 days) | How long dead jobs are kept (60 to 1209600) |
| `LLMQ_JOB_CONCURRENCY` | 4 | Running jobs per job name per process (`register_job(..., concurrency=N)` wins) |
| `LLMQ_JOB_PROCESS_CONCURRENCY` | 32 | Running jobs per process, every name together |
| `LLMQ_JOB_MAX_PAYLOAD_BYTES` | 1048576 (1 MB) | Largest job message; a larger send fails `request_too_large` (at least 1024) |
| `LLMQ_JOB_MAX_RETURNS` | 5 | Returns at shutdown before a job is dead-lettered; 0 never |

See [Run background jobs](../how-to/jobs.md).

## `Tuning` (in code)

Passed as `LLMQueue.from_env(tuning=Tuning(...))`. Defaults:

| Part | Field (default) |
|---|---|
| `StoreOptions` (`tuning.store`) | `retries` (3), `connect_timeout_secs` (5), `read_timeout_secs` (15), `health_check_secs` (30), `pool_wait_secs` (10) |
| `BedrockOptions` (`tuning.bedrock`) | `read_timeout_secs` (300), `connect_timeout_secs` (10), `max_pool_connections` (100: used only until the registry sizes the pool, see below), `role_session_secs` (3600), `transport` (`"aiohttp"`: orjson body, aiohttp session, botocore only for credentials, SigV4 signing and stream frames; `"botocore"`: the aiobotocore client) |
| `EngineTimings` (`tuning.engine`) | `idle_poll_secs` (0.05), `idle_slow_secs` (1), `idle_after_empty` (20), `result_recheck_secs` (60), `health_probe_secs` (3), `stream_read_block_ms` (100), `admission_probe_wait_ms` (200), `admission_slot_wait_ms` (50), `admission_slot_jitter_ms` (150), `admission_wait_ms` (100), `limiter_cut_ms` (1000), `limiter_cut_factor` (0.7), `budget_key_ms` (180000), `throttle_base_ms` (1000), `throttle_max_ms` (30000), `slow_stale_ms` (300000), `job_ttl_ms` (3600000) |
| `RegistryOptions` (`tuning.registry`) | `history_size` (20), `edit_retries` (3) |
| `OpenAIOptions` (`tuning.openai`) | `read_timeout_secs` (300), `connect_timeout_secs` (10), `max_connections` (100), `max_keepalive_connections` (20), `keepalive_secs` (15), `http` (`"aiohttp"`: the SDK's aiohttp client with the same limits and timeouts, except `max_keepalive_connections`, which aiohttp has no equivalent for; falls back to httpx with a warning when the SDK has none) |
| `WarmOptions` (`tuning.warm`) | `connections` (2; 0 = off), `refresh_secs` (10; 0 = warm once, no repeats), `keepalive_secs` (15) |

What each field does and the start-up checks are in [Tune](../how-to/tune.md).

Bedrock connection pools are sized from the registry: each (account, region) client gets the sum
of the concurrency slots routed to it, at least 10 connections (no upper cap). `WarmOptions`
(2 warm connections per client, refreshed every 10 s, kept idle 15 s) controls the warm
connections; see [Clients and connections](../explanation/clients.md). Both are importable
from `llm_queue` and passed like the other parts:

```python
from llm_queue import LLMQueue, OpenAIOptions, Tuning, WarmOptions

queue = LLMQueue.from_env(
    tuning=Tuning(openai=OpenAIOptions(read_timeout_secs=120), warm=WarmOptions(connections=4))
)
```

## Registry settings

The registry document's settings (budgets, balancing, fallback, retries, breaker, timeouts,
queues, inference defaults, alerts) can each be set at some of the levels deployment > model >
account > defaults. Every setting with its levels, type, range and default is in the
design plan (maintainers' notes in docs/internal, not published),
sections 3.9 and 3.19.

For a running system, `llm-queue settings get --model M [--deployment D]` prints the effective
value of every setting and the level it came from.

Settings added or changed in 0.1.0b2:

| Setting | Levels | Default | Meaning |
|---|---|---|---|
| `converse_api` | defaults, model | `converse` | Non-stream Bedrock chat: `converse` (plain Converse, IAM `bedrock:InvokeModel`) or `stream` (ConverseStream, `bedrock:InvokeModelWithResponseStream`, with the first-response timer). See [Bedrock APIs and IAM](../how-to/bedrock.md) |
| `max_message_bytes` | defaults, model | 4194304 (4 MB) | A call whose message (inline images and documents included) is larger fails `request_too_large` before it is queued; 1024 to 268435456 |
| `inference.extra` | as `inference` | | Provider-specific fields: plain lowercase names only; SDK arguments and request fields (`extra_body`, `store`, `metadata`, `user`, `model`, `messages`, `stream`, `tools`, ...) are refused. Protected in `safe` live-edit mode |
| `queue.retention_secs`, `queue.dlq_retention_secs` | | | Now at most two weeks (1209600): dead letters hold the prompt |
| `embed_max_chars` | model (embedding) | the family's: Titan 50000, Cohere 2048, OpenAI none | Longest text an `embed` sends; a longer one fails `invalid_request` with its index before any call. 0 turns the check off |
| `family` | model field (Bedrock embedding) | guessed from `model_id` | `titan` or `cohere`: the request body of a model whose id does not say (an application inference profile ARN) |
| `inference.embedding_types` | as `inference` | | Only `["float"]`: the other types were paid for, then unreadable |
| `alerts` | | | Only `quota_used_pct` (1-100, default 90; `quota_near_limit` now checks TPM too) and `dlq_messages` (1 or more, default 1). `quota_used_mins`, `spill_pct`, `spill_window_secs` and `waiting_age_secs` did nothing and were removed |
| `live_edit` (top level) | document | unset | The document's live-edit mode (deploy-only) |
| `min_lib_version` (top level) | document | unset | Now a version such as `0.1.0b2`; checked against the running library |

Validation added in 0.1.0b2 (each refused with its path): a TPM below one call's reservation
(`max output x tpm_output_weight`); a `model_fallback` to another provider; a fallback cycle of
any length (unless every step requires headroom); a fallback to a disabled model; a fallback
target whose residency is not within the source model's; residency that leaves a model with no
deployment (an error, no longer a warning); `breaker_probe_secs` below `attempt_timeout_secs`;
unknown `blocked_regions`. `LLMQ_CALLER_TTL_SECS` must be at least
`LLMQ_LEASE_SECS / 3 + max(5, LLMQ_LEASE_SECS / 6)`. The registry audit is capped at about
10,000 entries besides its age limit.

