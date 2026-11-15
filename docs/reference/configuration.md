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

The registry document's settings can each be set at some of the levels deployment > model >
account > defaults (the most specific one wins; objects marked *merged* combine key by key).
Bounds: every number is finite and at most 2**53 - 1 (9007199254740991); every timeout and wait
is at most one day (86400 s); tighter bounds are listed below. A value outside its bound is
refused with its path when the registry is loaded or edited.

| Setting | Levels | Default | Range and meaning |
|---|---|---|---|
| `enabled` | account, model, deployment | true | Off: no new calls (live edit; `llm-queue drain` sets it) |
| `rpm`, `tpm` | defaults, model, deployment | none | Requests / tokens per minute of a deployment; whole numbers of at least 1 |
| `rate_limits` | all four | chat `both`, embedding `rpm` | Which budgets apply: `both`, `rpm`, `tpm` or `off` |
| `rate_window` | all four | `smooth` | `smooth` (sliding) or `fixed` (calendar minute) |
| `rate_burst` | all four | 0.1 | Share of a minute's budget usable at once, 0 to 1 |
| `tpm_output_weight` | all four | 1 | Output tokens count this many times against TPM, 1 to 1000 |
| `tpm_count_cached` | all four | false | Cache reads count against TPM |
| `default_output_tokens` | all four | 1000 | Output reserved for a call that sets no `max_tokens`, 1 to 10000000 |
| `shared_rpm`, `shared_tpm` | account | none | One budget shared by every deployment of the account |
| `max_concurrency` | all four | none (200 in status) | Calls in flight per deployment, 1 to 100000 |
| `priority_reserve` | defaults, account, model | chat 0.2, embedding 0 | Share kept for priority calls, 0 to 1 |
| `normal_min_share` | defaults, model | 0.1 | Share normal calls always get, 0 to 1; with `priority_reserve` below 1 |
| `timeout_secs` | defaults, account, model | 120 | A call's deadline; at most `timeout_max_secs` |
| `timeout_max_secs` | defaults, model | 900 | The largest `timeout_secs` a caller may ask for |
| `max_wait_secs` | defaults, model | none | Longest wait for budget before `rate_limited` |
| `attempt_timeout_secs` | all four | 60 | One provider attempt; below `timeout_secs` |
| `throttle_retry_attempts` | all four | 6 | Retries of a throttled attempt, 1 to 20 |
| `retry_base_secs`, `retry_multiplier`, `retry_spread`, `retry_max_secs` | all four | 1, 4, 2.5, 30 | Backoff: base x multiplier^n, spread, capped; multiplier and spread 1 to 100 |
| `breaker_threshold` | all four | 5 | Failures that open a deployment's breaker, 1 to 10000 |
| `breaker_cooldown_secs`, `breaker_window_secs`, `breaker_probe_secs` | all four | 15, 60, 60 | Breaker timing; the probe at least `attempt_timeout_secs` |
| `balance` | defaults, model | `ordered` | `ordered`, `round_robin`, `weighted`, `headroom` or `custom:<name>` |
| `balance_opts` | defaults, model | `{"pick": "percent"}` | *merged*; `pick`: `percent`, `max` or `spread` |
| `balance_priority` | model | as `balance` | The strategy for priority calls |
| `weight` | deployment | 1 | Share under `weighted`; at least one usable deployment must be above 0 |
| `tags` | deployment | none | Text or number labels a custom balancer reads |
| `max_attempts` | defaults, model | 4 | Attempts per call across deployments, 1 to 100 |
| `retry_budget_pct`, `retry_budget_min` | defaults, model | 20, 10 | Retries allowed as a share of calls, with a floor |
| `stream_idle_secs` | defaults, model | 30 | A stream with no event this long is cut |
| `stream_max_bytes` | model | 8388608 | Largest streamed answer, 1024 to 1073741824 |
| `stream_flush_ms` | model, deployment | 30 | Chunk batching, 0 to 1000 |
| `max_message_bytes` | defaults, model | 4194304 | Larger messages fail `request_too_large` before they are queued, 1024 to 268435456 |
| `converse_api` | defaults, model | `converse` | Bedrock non-stream chat: `converse` or `stream` |
| `supports_tools` | model | true | False: a call with tools served by this model as a fallback loses its tool fields |
| `max_waiting` | defaults, model | 50000 | Calls a queue holds before `queue_full`; **0 means no limit**, not "do not queue" |
| `queue_pct` | defaults, model | 100 | Share of users (by id hash) whose calls are queued; the rest run direct (a canary), 0 to 100 |
| `worker_slots` | defaults, model | 20 | Calls a worker runs at once per model, 1 to 1000 (deploy only) |
| `queue` | defaults, model | see below | *merged*: `name` (model only; 1-71 letters, digits, `_`, `.`, `-`, not starting `jobs-`), `visibility_secs` (60, 1 to 43200), `max_receives` (10, 1 to 1000), `retention_secs` (**86400**, one day, 60 to 1209600), `dlq_retention_secs` (86400, 60 to 1209600) |
| `sticky` | defaults, model | none | `{"key": "session" or "custom:<name>", "idle_ttl_secs"}` |
| `sticky_move_after_secs` | defaults, model | 1 | A sticky session keeps its deployment while it would wait at most this long |
| `require_headroom_pct` | defaults, model | 20 | Free share a fallback target listed with `require_headroom` needs, 0 to 100 |
| `fallback` | defaults, model | none | Fallback regions: `{"on": [exhausted, broken, error], "after_secs" (2), "regions": account or any}` |
| `model_fallback` | model | none | Fallback models (protected): `{"on", "after_secs" (5), "models": [alias or {model, require_headroom}]}`. A target may use another provider (see below) |
| `store_down` | defaults, model | `pause` | `pause`, `static_cap` or `fail_fast` |
| `embed_batch_size` | model | 1 | Texts per embedding request, 1 to 4096 (embedding models only) |
| `embed_max_chars` | model | the family's (Titan 50000, Cohere 2048) | Longest text an `embed` sends, 0 to 10000000 (0: off; embedding models only) |
| `inference` | all four | none | *merged* defaults of the request: `max_tokens`, `temperature`, `top_p`, `stop`, `top_k`, `seed`, penalties, `extra`; embedding `dimensions`, `normalize`, `input_type`, `truncate`, `embedding_types` |
| `inference_limits` | all four | none | *merged*: `max_tokens` cap (also `max_completion_tokens`; a call without one gets the cap), `locked` names, `on_exceed` (`clamp` or `reject`) |
| `params` | deployment | none | Extra request fields of one deployment (guardrails); protected |
| `health` | all four | see below | *merged*: `disable_errors` (3), `disable_window_secs` (300), `probe_after_secs` (900), `probe_max_secs` (7200), `slow_ratio` (3), `slow_min_secs` (5), `slow_min_samples` (20) |
| `alerts` | defaults, model | `{"quota_used_pct": 90, "dlq_messages": 1, "queue_age_secs": 600}` | *merged*; `queue_age_secs` (1 or more) turns `queue_stalled` on when the oldest queued call is older; the four names removed in 0.1.0b2 are ignored with a warning until 0.2 |
| `admission`, `fallback_policy`, `token_estimator`, `probe` | defaults, model | none | Names of registered hooks (1-64 letters, digits, `_`, `.`, `-`) |
| `debug_capture` | model | false | Store prompt and answer for an hour (dev only; protected) |

Names (aliases, accounts, regions) are 1-64 lowercase letters, digits, `_`, `.` or `-`,
starting with a letter or digit; a trailing newline or padding is refused. A stored document is
read leniently: an unknown setting or sub-field from a newer library is ignored with a warning,
while the process's own `LLMQ_REGISTRY` stays strict.

For a running system, `llm-queue settings get --model M [--deployment D]` prints the effective
value of every setting and the level it came from.

Settings added or changed in 0.1.0b2:

| Setting | Levels | Default | Meaning |
|---|---|---|---|
| `converse_api` | defaults, model | `converse` | Non-stream Bedrock chat: `converse` (plain Converse, IAM `bedrock:InvokeModel`) or `stream` (ConverseStream, `bedrock:InvokeModelWithResponseStream`, with the first-response timer). See [Bedrock APIs and IAM](../how-to/bedrock.md) |
| `max_message_bytes` | defaults, model | 4194304 (4 MB) | A call whose message (inline images and documents included) is larger fails `request_too_large` before it is queued; 1024 to 268435456 |
| `inference.extra` | as `inference` | | Provider-specific fields: plain lowercase names only; SDK arguments and request fields (`extra_body`, `store`, `metadata`, `user`, `model`, `messages`, `stream`, `tools`, ...) are refused. Protected in `safe` live-edit mode |
| `queue.retention_secs`, `queue.dlq_retention_secs` | | one day each | Now at most two weeks (1209600): messages and dead letters hold the prompt. `retention_secs` defaults to one day (was four days) and is applied on every send |
| `embed_max_chars` | model (embedding) | the family's: Titan 50000, Cohere 2048, OpenAI none | Longest text an `embed` sends; a longer one fails `invalid_request` with its index before any call. 0 turns the check off |
| `family` | model field (embedding) | `openai` for the `openai` provider, else from `model_id` | `titan`, `cohere`, `openai` or a name added with `register_embedding_family`: the request and reply shape. Required when the id names no family (an application inference profile ARN); the registry load fails without it |
| `inference.embedding_types` | as `inference` | | Only `["float"]`: the other types were paid for, then unreadable |
| `alerts` | | | `quota_used_pct` (1-100, default 90; `quota_near_limit` now checks TPM too), `dlq_messages` (1 or more, default 1) and the new `queue_age_secs` (default 600, for `queue_stalled`). `quota_used_mins`, `spill_pct`, `spill_window_secs` and `waiting_age_secs` did nothing and were removed: a document naming them still loads, with a deprecation warning, until 0.2 |
| `live_edit` (top level) | document | unset | The document's live-edit mode (deploy-only) |
| `min_lib_version` (top level) | document | unset | Now a version such as `0.1.0b2`; checked against the running library |

Validation added in 0.1.0b2 (each refused with its path): a TPM below one call's reservation
(`max output x tpm_output_weight`); a fallback cycle of any length (unless every step requires
headroom); a fallback target whose residency is not within the source model's; residency that
leaves a model with no deployment (an error, no longer a warning); `breaker_probe_secs` below
`attempt_timeout_secs`; a fallback `after_secs` that could never fire inside `timeout_secs`;
`NaN`, infinities and numbers above their bound; names with a trailing newline or padding;
malformed role ARNs; queue names that are not plain or start with `jobs-`.

Warnings (the document loads; each is logged with its path): a fallback to a disabled model; a
`blocked_regions` entry no deployment uses or a `blocked_accounts` entry that is not an
account; a model with no usable confirmed deployment; queue retention below the timeout; a
retry base above its cap; `probe_after_secs` above `probe_max_secs`; duplicate fallback causes
or models; a `fallback` rule with no fallback regions; settings the model's type or provider
never reads; a `model_fallback` to a model of another provider. Such a fallback is allowed, but
the request is passed on in the source model's shape (Converse or OpenAI chat): converting it
between providers is not done yet, so list such a target only when its provider accepts that
shape.

`LLMQ_CALLER_TTL_SECS` must be at least `LLMQ_LEASE_SECS / 3 + max(5, LLMQ_LEASE_SECS / 6)`. The
registry audit is capped at about 10,000 entries besides its age limit.

