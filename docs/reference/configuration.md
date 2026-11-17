# Configuration

Every setting llm-queue reads, with its default and its range, in three groups:

- **Process settings** (`LLMQ_*`), read by `LLMQueue.from_env()`: in
  [Environment variables](environment.md), and the job limits (`LLMQ_JOB_*`) [below](#job-settings-llmq_job_).
- **`Tuning`**, passed in code: timeouts, pool sizes and loop pacing, [below](#tuning-in-code).
- **Registry settings**, in the registry document: everything about models,
  [below](#registry-settings). The document's shape is in [Registry document](registry.md).

How to use them is in [Configure](../how-to/configure.md) and [Tune](../how-to/tune.md). A unit
test (`tests/unit/test_config_docs.py`) fails when a setting is missing from these pages or a
documented default or range differs from the code.

How to read the **Range** column: `1 to 100` includes both ends; `above 0` excludes 0; `or more`
has no upper bound other than 9007199254740991 (2**53 - 1, the largest number every store keeps
exact); `(whole)` means a whole number (`200.0` is refused). Every bad value is refused at start
(process settings, `Tuning`) or when the registry is loaded or edited, with every problem listed
at once.

## Job settings (`LLMQ_JOB_*`)

Read by `LLMQueue.from_env` from the environment (or its `environ=` mapping; not through
`secrets=`), or passed as `LLMQueue(..., job_settings=JobSettings(...))`. A bad value is a
`ConfigError` at start. See [Run background jobs](../how-to/jobs.md).

| Variable | Default | Range | Meaning |
|---|---|---|---|
| `LLMQ_JOB_DEDUPE_SECS` | 86400 | 0 or more (whole) | `send_job(job_id=X)` again within this (24 h) queues nothing and returns `X`; 0 turns deduplication off |
| `LLMQ_JOB_RETRY_BASE_SECS` | 5 | above 0 | First retry delay; it doubles after each failure ... |
| `LLMQ_JOB_RETRY_MAX_SECS` | 300 | at least `LLMQ_JOB_RETRY_BASE_SECS` | ... up to this |
| `LLMQ_JOB_RETRY_FOR_SECS` | 3600 | 0 or more | Retries go on until the delays add up to this (about one hour); then the job is dead. A failed LLM call's `retry_after` is honoured when longer |
| `LLMQ_JOB_DEAD_RETENTION_SECS` | 604800 | 60 to 1209600 (whole) | How long dead jobs are kept (7 days; at most two weeks) |
| `LLMQ_JOB_CONCURRENCY` | 4 | above 0 (whole) | Running jobs per job name per process; `register_job(..., concurrency=N)` wins |
| `LLMQ_JOB_PROCESS_CONCURRENCY` | 32 | above 0 (whole) | Running jobs per process, every name together |
| `LLMQ_JOB_MAX_PAYLOAD_BYTES` | 1048576 | 1024 or more (whole) | Largest job message (1 MB); a larger send fails `request_too_large` |
| `LLMQ_JOB_MAX_RETURNS` | 5 | 0 or more (whole) | A job cut by a shutdown goes back to the queue; after this many returns it is dead-lettered instead. 0: never dead-lettered for returns |

## `Tuning` (in code)

Passed as `LLMQueue.from_env(tuning=Tuning(...))`; only the values you set change. Each part is
importable from `llm_queue`:

```python
from llm_queue import LLMQueue, OpenAIOptions, Tuning, WarmOptions

queue = LLMQueue.from_env(
    tuning=Tuning(openai=OpenAIOptions(read_timeout_secs=120), warm=WarmOptions(connections=4))
)
```

Every number is finite. Time fields are at most one day (86400 s, 86400000 ms); the checks
across fields are listed under [Checks at start-up](../how-to/tune.md#checks-at-start-up).

### `StoreOptions` (`tuning.store`)

The Valkey / Redis client of every process. The pool size is `LLMQ_STORE_MAX_CONNECTIONS`.

| Field | Default | Range | Meaning |
|---|---|---|---|
| `store.retries` | 3 | 0 to 100 (whole) | Retries of a failed store command, with jittered backoff (about 1.5 s in all) |
| `store.connect_timeout_secs` | 5 | above 0 to 86400 | A store node that does not answer in a failover fails fast |
| `store.read_timeout_secs` | 15 | above 0 to 86400 | A reply that never comes fails the command instead of hanging its call; at least 1 s above every blocking command (`engine.idle_slow_secs`, `LLMQ_QUEUE_WAIT_MS`, `engine.stream_read_block_ms`) |
| `store.health_check_secs` | 30 | above 0 to 86400 | A connection idle this long is pinged before use |
| `store.pool_wait_secs` | 10 | above 0 to 86400 | A burst waits this long for a free connection |

### `BedrockOptions` (`tuning.bedrock`)

| Field | Default | Range | Meaning |
|---|---|---|---|
| `bedrock.read_timeout_secs` | 300 | 1 to 86400 | Bedrock / STS read timeout (each attempt also has `attempt_timeout_secs`) |
| `bedrock.connect_timeout_secs` | 10 | above 0 to 86400 | Bedrock / STS connect timeout |
| `bedrock.max_pool_connections` | 100 | above 0 (whole) | Connections per client, used only until the registry sizes the pool (below) |
| `bedrock.role_session_secs` | 3600 | 900 to 43200 (whole) | Assumed-role session length (STS limits; 3600 is the role-chaining maximum) |
| `bedrock.transport` | `aiohttp` | `botocore`, `aiohttp` | `aiohttp`: orjson body and an aiohttp session, botocore only for credentials, SigV4 signing and stream frames (measured +78-104 % calls/s against `botocore`); `botocore`: the aiobotocore client |

Bedrock connection pools are sized from the registry: each (account, region) client gets the
sum of the concurrency slots routed to it, at least 10 connections (no upper cap). See
[Clients and connections](../explanation/clients.md).

### `OpenAIOptions` (`tuning.openai`)

One client per OpenAI-compatible account.

| Field | Default | Range | Meaning |
|---|---|---|---|
| `openai.read_timeout_secs` | 300 | 1 to 86400 | Read timeout (each attempt also has `attempt_timeout_secs`) |
| `openai.connect_timeout_secs` | 10 | above 0 to 86400 | Connect timeout |
| `openai.max_connections` | 100 | above 0 (whole) | Connection pool size |
| `openai.max_keepalive_connections` | 20 | above 0 (whole) | Idle connections kept (httpx only); at most `openai.max_connections` |
| `openai.keepalive_secs` | 15 | above 0 to 86400 | How long an idle connection is kept |
| `openai.http` | `aiohttp` | `httpx`, `aiohttp` | `aiohttp`: the SDK's aiohttp client with the same limits and timeouts; falls back to httpx, with a warning, when the SDK has none |

### `WarmOptions` (`tuning.warm`)

Warm connections per Bedrock client: unsigned HEAD requests (not billable) that do DNS, TCP and
TLS before the first call.

| Field | Default | Range | Meaning |
|---|---|---|---|
| `warm.connections` | 2 | 0 or more (whole) | Connections warmed per client; 0 turns warming off |
| `warm.refresh_secs` | 10 | 0 to 86400 | Warm again this often, below `warm.keepalive_secs` and AWS's 20 s idle close; 0 warms once |
| `warm.keepalive_secs` | 15 | above 0 to 86400 | aiohttp keeps an idle pooled connection this long |

### `EngineTimings` (`tuning.engine`)

Pacing of the engine's loops and admission scripts.

| Field | Default | Range | Meaning |
|---|---|---|---|
| `engine.idle_poll_secs` | 0.05 | above 0 to 86400 | Idle receive: poll this often ... |
| `engine.idle_slow_secs` | 1 | above 0 to 86400 | ... then this often (not below `idle_poll_secs`) ... |
| `engine.idle_after_empty` | 20 | above 0 (whole) | ... after this many empty polls. A send's doorbell wakes a worker at once |
| `engine.result_recheck_secs` | 60 | above 0 to 86400 | A waiting caller's last-resort read of its result; `LLMQ_RESULT_TTL_SECS` must be at least this |
| `engine.health_probe_secs` | 3 | above 0 to 86400 | A store probe slower than this reports "down" |
| `engine.stream_read_block_ms` | 100 | above 0 to 86400000 (whole) | An opened chunk stream joins the shared read within this |
| `engine.admission_probe_wait_ms` | 200 | above 0 to 86400000 (whole) | A breaker's probe call is in flight: check again after this |
| `engine.admission_slot_wait_ms` | 50 | above 0 to 86400000 (whole) | No free slot: retry after this ... |
| `engine.admission_slot_jitter_ms` | 150 | above 0 to 86400000 (whole) | ... plus up to this, at random (spreads the retries) |
| `engine.admission_wait_ms` | 100 | above 0 to 86400000 (whole) | A refusal with no better estimate retries after this |
| `engine.limiter_cut_ms` | 1000 | above 0 to 86400000 (whole) | A throttle cuts the adaptive concurrency limit at most once per this ... |
| `engine.limiter_cut_factor` | 0.7 | above 0, below 1 | ... to this fraction of itself |
| `engine.budget_key_ms` | 180000 | 130000 to 86400000 (whole) | Budget and limiter keys live this long (a fixed window spans 2 minutes) |
| `engine.throttle_base_ms` | 1000 | above 0 to 86400000 (whole) | A throttled deployment is skipped by every pod for a jittered backoff from this ... |
| `engine.throttle_max_ms` | 30000 | above 0 to 86400000 (whole) | ... doubling per throttle in a row up to this (or the provider's Retry-After when longer); not below `throttle_base_ms` |
| `engine.slow_stale_ms` | 300000 | 1000 to 86400000 (whole) | A latency average this old no longer marks a deployment slow |
| `engine.job_ttl_ms` | 3600000 | 60000 to 86400000 (whole) | A claim record made by a worker lives this long |

### `RegistryOptions` (`tuning.registry`)

Audit retention is `LLMQ_AUDIT_RETENTION_DAYS`, not a tuning field.

| Field | Default | Range | Meaning |
|---|---|---|---|
| `registry.history_size` | 20 | 2 or more (whole) | Registry versions kept for `registry rollback` (rollback) |
| `registry.edit_retries` | 3 | above 0 to 10 (whole) | Retries of a live edit that raced another one (each backs off up to 2 s) |

## Registry settings

Each setting may be set at some of the levels deployment > model > account > defaults; the most
specific one wins, except objects marked *merged*, which combine key by key across levels.
`all four` means defaults, account, model and deployment. Every timeout and wait is at most one
day (86400 s). `llm-queue models get M` (and `deployments get M D`) prints the effective value
of every setting and the level it came from.

| Setting | Levels | Default | Range | Meaning |
|---|---|---|---|---|
| `enabled` | account, model, deployment | true | true or false | False: no new calls (a live edit; `llm-queue drain` sets it) |
| `rpm` | defaults, model, deployment | none | 1 or more (whole) | Requests per minute of one deployment; none: no RPM budget |
| `tpm` | defaults, model, deployment | none | 1 or more (whole) | Tokens per minute of one deployment; none: no TPM budget. Must hold one call's reservation (max output x `tpm_output_weight`) |
| `quota_rpm` | defaults, model, deployment | none | 1 or more (whole) | The provider's RPM quota of one deployment, instead of `rpm`: the budget is floor(`quota_rpm` x `budget_pct`). Setting both `rpm` and `quota_rpm` on one level is refused |
| `quota_tpm` | defaults, model, deployment | none | 1 or more (whole) | The provider's TPM quota, instead of `tpm`: the budget is floor(`quota_tpm` x `budget_pct`). Not beside `tpm` on one level |
| `budget_pct` | all four | 1 | above 0 to 1 | Share of `quota_rpm` / `quota_tpm` to use (0.9: keep 10 % for other users of the quota); no effect on a plain `rpm` / `tpm` |
| `rate_limits` | all four | chat `both`, embedding `rpm` | `both`, `rpm`, `tpm`, `off` | Which budgets apply. An embedding model with a `tpm` defaults to `both` |
| `tpm_output_weight` | all four | 1 | 1 to 1000 | Output tokens count this many times against TPM |
| `tpm_count_cached` | all four | false | true or false | Cache reads count against TPM |
| `default_output_tokens` | all four | 1000 | 1 to 10000000 (whole) | Output reserved for a call that sets no `max_tokens` (the most specific level wins, deployment included) |
| `shared_rpm` | account | none | 1 or more (whole) | One RPM budget shared by every deployment of the account |
| `shared_tpm` | account | none | 1 or more (whole) | One TPM budget shared by every deployment of the account |
| `rate_window` | all four | `smooth` | `smooth`, `fixed` | `smooth` (sliding minute) or `fixed` (the calendar minute) |
| `rate_burst` | all four | 0.1 | 0 to 1 | Share of a minute's budget usable at once |
| `stream_flush_ms` | model, deployment | 30 | 0 to 1000 (whole) | Stream chunks are batched for this long before they are sent to the caller |
| `max_concurrency` | all four | none | 1 to 100000 (whole) | Calls in flight per deployment; none: no limit (status shows 200) |
| `priority_reserve` | defaults, account, model | chat `0.2`, embedding `0` | 0 to 1 | Share of the budget kept for priority calls |
| `normal_min_share` | defaults, model | 0.1 | 0 to 1 | Share normal calls always get, even with priority calls waiting |
| `timeout_secs` | defaults, account, model | 120 | above 0 to 86400 | A call's deadline when the caller sets none; at most `timeout_max_secs`. Honoured at account level too (the most specific level wins) |
| `timeout_max_secs` | defaults, model | 900 | above 0 to 86400 | The largest timeout a caller may ask for |
| `max_wait_secs` | defaults, model | none | above 0 to 86400 | Longest wait for budget before the call fails `rate_limited`; none: until the deadline |
| `attempt_timeout_secs` | all four | 60 | above 0 to 86400 | One provider attempt; below `timeout_secs` |
| `throttle_retry_attempts` | all four | 6 | 1 to 20 (whole) | Retries of a throttled attempt |
| `retry_base_secs` | all four | 1 | 0 to 86400 | Retry backoff: `retry_base_secs` x `retry_multiplier`^n ... |
| `retry_multiplier` | all four | 4 | 1 to 100 | ... growth per retry ... |
| `retry_spread` | all four | 2.5 | 1 to 100 | ... random spread ... |
| `retry_max_secs` | all four | 30 | above 0 to 86400 | ... capped at this |
| `breaker_threshold` | all four | 5 | 1 to 10000 (whole) | Failures within `breaker_window_secs` that open a deployment's breaker |
| `breaker_cooldown_secs` | all four | 15 | above 0 to 86400 | How long an open breaker stays open before a probe call |
| `breaker_window_secs` | all four | 60 | above 0 to 86400 | Failures count toward the threshold only within this window |
| `breaker_probe_secs` | all four | 60, or the attempt timeout when longer | above 0 to 86400 | A probe call that never reported back stops blocking others after this; at least `attempt_timeout_secs`. Unset, it is the larger of 60 and the deployment's `attempt_timeout_secs`; an explicit lower value is refused |
| `balance` | defaults, model | `ordered` | `ordered`, `round_robin`, `weighted`, `headroom` or `custom:<name>` | How a deployment is picked among the usable ones |
| `balance_opts` | defaults, model | see fields | object, fields below | *merged*: options of the strategy |
| `balance_priority` | model | none | as `balance` | The strategy for priority calls; none: `balance` |
| `max_attempts` | defaults, model | 4 | 1 to 100 (whole) | Attempts per call across deployments |
| `retry_budget_pct` | defaults, model | 20 | 0 to 100 (whole) | Retries allowed as a share of first attempts (over about a minute) ... |
| `retry_budget_min` | defaults, model | 10 | 0 to 1000000 (whole) | ... but always at least this many |
| `stream_idle_secs` | defaults, model | 30 | above 0 to 86400 | A stream with no event this long is cut (`deadline_exceeded`) |
| `stream_max_bytes` | model | 8388608 | 1024 to 1073741824 (whole) | Largest streamed answer (8 MB) |
| `max_message_bytes` | defaults, model | 4194304 | 1024 to 268435456 (whole) | A call whose messages (inline images and documents included) are larger fails `request_too_large` before it is queued (4 MB) |
| `converse_api` | defaults, model | `converse` | `converse`, `stream` | Bedrock non-stream chat: plain Converse (IAM `bedrock:InvokeModel`), or ConverseStream (`bedrock:InvokeModelWithResponseStream`) for its first-response timer. See [Bedrock APIs and IAM](../how-to/bedrock.md) |
| `supports_tools` | model | true | true or false | False: a call with tools served by this model as a fallback loses its tool fields instead of being refused |
| `max_waiting` | defaults, model | 50000 | 0 to 100000000 (whole) | Calls a queue holds before `queue_full`; **0 means no limit**, not "do not queue" |
| `sticky_move_after_secs` | defaults, model | 1 | above 0 to 86400 | A sticky session keeps its deployment while it would wait at most this long |
| `require_headroom_pct` | defaults, model | 20 | 0 to 100 (whole) | Free share a fallback model listed with `require_headroom` needs |
| `tags` | deployment | none | object of text or numbers | Labels a custom balancer reads |
| `weight` | deployment | 1 | 0 or more | Share under `weighted`; at least one usable deployment must be above 0 |
| `sticky` | defaults, model | none | object, fields below | Keeps a session on one deployment (prompt caches) |
| `fallback` | defaults, model | none | object, fields below | When the `fallback` regions are used |
| `model_fallback` | model | none | object, fields below | When calls spill to other models (protected). A target may use another provider: [converted](#fallback-to-a-model-of-another-provider) |
| `store_down` | defaults, model | `pause` | `pause`, `static_cap`, `fail_fast` | What calls do while the store is down: wait, run on the local executor (budgets divided by `LLMQ_LOCAL_PROCESS_COUNT`, `LLMQ_LOCAL_CONCURRENCY` per process and model), or fail at once. A detected outage is remembered for a few seconds, so `fail_fast` and `static_cap` answer within about 100 ms |
| `queue_pct` | defaults, model | 100 | 0 to 100 (whole) | Share of calls queued (by request id hash); the rest run direct (a canary) |
| `worker_slots` | defaults, model | 20 | 1 to 1000 (whole) | Calls a worker runs at once per model (deploy only: a live edit does not change it) |
| `queue` | defaults, model | see fields | object, fields below | *merged*: the model's queue |
| `embed_batch_size` | model | 1 | 1 to 4096 (whole) | Texts per embedding request (embedding models only). A refused batch is split and retried, so only the bad text fails, with its index |
| `embed_max_chars` | model | none | 0 to 10000000 (whole) | Longest text an `embed` sends; a longer one fails `invalid_request` with its index before any call. None: the family's limit (Titan 50000, Cohere 2048, OpenAI not checked: it counts tokens); 0 turns the check off (embedding models only) |
| `inference` | all four | none | object, fields below | *merged*: request defaults |
| `inference_limits` | all four | none | object, fields below | *merged*: caps on what a caller may ask |
| `params` | deployment | none | any object | Extra request fields of one deployment (guardrails); protected |
| `health` | all four | see fields | object, fields below | *merged*: when a deployment is disabled, probed and marked slow |
| `alerts` | defaults, model | see fields | object, fields below | *merged*: alert thresholds of `status()` and `/health` |
| `health_summary` | defaults, model | see fields | object, fields below | *merged*: thresholds of the per-model health summary (`healthy` / `warning` / `critical`) in `status()`, `status.health` and `GET /` |
| `admission` | defaults, model | none | a hook name | Name of a registered admission hook (1-64 letters, digits, `_`, `.`, `-`) |
| `fallback_policy` | defaults, model | none | a hook name | Name of a registered fallback policy hook |
| `token_estimator` | defaults, model | none | a hook name | Name of a registered token estimator |
| `probe` | defaults, model | none | a hook name | Name of a registered health probe |
| `debug_capture` | model | false | true or false | Store prompt and answer for an hour (dev only, with `LLMQ_DEBUG_CAPTURE_FROM_CONFIG`; protected) |
| `fairness` | defaults, model | see fields | object, fields below | *merged*: per-tenant fairness, off by default. See [Tenant fairness](../explanation/fairness.md) |

### Fields of the object settings

A field left out takes the default below (the *merged* objects merge it across levels).

| Field | Default | Range | Meaning |
|---|---|---|---|
| `balance_opts.pick` | `percent` | `percent`, `max`, `spread` | How `headroom` picks: the largest free share (`percent`), the most free budget in absolute terms (`max`), or at random in proportion to free share (`spread`) |
| `sticky.key` | none | `session` or `custom:<name>` | Required: what keeps a call on its deployment (the session id, or a registered key function) |
| `sticky.idle_ttl_secs` | 1800 | above 0 to 86400 | A session unused this long forgets its deployment |
| `fallback.on` | none | non-empty list of `exhausted`, `broken`, `error` | Required: the causes that open the fallback regions |
| `fallback.after_secs` | 2 | 0 to 86400 | How long the cause must last first |
| `fallback.regions` | `account` | `account`, `any` | Fallback regions of the same account only, or of any account |
| `model_fallback.on` | none | non-empty list of `exhausted`, `broken`, `error` | Required: the causes that send calls to other models |
| `model_fallback.after_secs` | 5 | 0 to 86400 | How long the cause must last first |
| `model_fallback.models` | none | non-empty list of aliases or `{model, require_headroom}` | Required: the target models, in order. `require_headroom` (default false): use the target only while it has `require_headroom_pct` free |
| `queue.name` | `llm-<alias>` | 1-71 letters, digits, `_`, `.`, `-` | The queue (stream) name, model level only; may not start with `jobs-` |
| `queue.visibility_secs` | 60 | 1 to 43200 (whole) | A received message not finished in this time is delivered again (at least once) |
| `queue.max_receives` | 10 | 1 to 1000 (whole) | Deliveries before a message goes to the dead letters |
| `queue.retention_secs` | 86400 | 60 to 1209600 (whole) | How long a message (which holds the prompt) is kept: one day by default, applied on every send; at most two weeks |
| `queue.dlq_retention_secs` | 86400 | 60 to 1209600 (whole) | How long a dead letter is kept |
| `inference.max_tokens` | none | 1 to 10000000 (whole) | Output tokens when the call sets none |
| `inference.temperature` | none | 0 to 2 | Sampling temperature |
| `inference.top_p` | none | 0 to 1 | Nucleus sampling |
| `inference.stop` | none | text or list of text | Stop sequences |
| `inference.top_k` | none | 1 to 1000000 (whole) | Top-k sampling |
| `inference.seed` | none | -9007199254740991 to 9007199254740991 (whole) | Sampling seed |
| `inference.frequency_penalty` | none | -2 to 2 | Frequency penalty |
| `inference.presence_penalty` | none | -2 to 2 | Presence penalty |
| `inference.extra` | none | object of plain lowercase names | Provider-specific fields; SDK arguments and request fields (`extra_body`, `extra_headers`, `store`, `metadata`, `user`, `model`, `messages`, `stream`, `tools`, ...) are refused. Protected in `safe` live-edit mode |
| `inference.dimensions` | none | 1 to 65536 (whole) | Embedding size |
| `inference.normalize` | none | true or false | Normalised embeddings (Titan) |
| `inference.input_type` | none | text | Cohere `input_type` (`search_document`, `search_query`, ...) |
| `inference.truncate` | none | text | Cohere `truncate` (`NONE`, `START`, `END`) |
| `inference.embedding_types` | none | `["float"]` only | Cohere `embedding_types`; the other types were paid for, then unreadable |
| `inference_limits.max_tokens` | none | 1 to 10000000 (whole) | Output cap (also `max_completion_tokens`); a call without one gets the cap |
| `inference_limits.locked` | none | list of inference names | Inference fields a caller may not change |
| `inference_limits.on_exceed` | `clamp` | `clamp`, `reject` | A call asking above the cap is clamped to it, or refused `invalid_request` |
| `health.disable_errors` | 3 | 1 to 10000 (whole) | Errors within `disable_window_secs` that disable a deployment ... |
| `health.disable_window_secs` | 300 | above 0 to 86400 | ... within this window |
| `health.probe_after_secs` | 900 | above 0 to 86400 | A disabled deployment is probed after this ... |
| `health.probe_max_secs` | 7200 | above 0 to 86400 | ... backing off up to this |
| `health.slow_ratio` | 3 | above 1 to 1000 | A deployment this many times slower than its peers is marked slow ... |
| `health.slow_min_secs` | 5 | 0 to 86400 | ... when also slower than this ... |
| `health.slow_min_samples` | 20 | 1 to 1000000 (whole) | ... over at least this many calls |
| `alerts.quota_used_pct` | 90 | 1 to 100 | `quota_near_limit` alert when RPM or TPM use passes this share |
| `alerts.dlq_messages` | 1 | 1 or more (whole) | `dead_letters` alert at this many dead letters |
| `alerts.queue_age_secs` | 600 | 1 or more | `queue_stalled` alert when the oldest queued call is older than this |
| `health_summary.budget_warning_pct` | 80 | 1 to 100 | `warning` when this minute's use of the usable deployments' budgets (the fuller of RPM and TPM, summed over them) reaches this share |
| `health_summary.budget_critical_pct` | 98 | 1 to 100 | ... `critical` at this share |
| `health_summary.waiting_warning` | 1000 | 1 to 100000000 (whole) | `warning` at this many waiting calls (queued and held for budget) |
| `health_summary.waiting_critical` | 10000 | 1 to 100000000 (whole) | ... `critical` at this many |
| `health_summary.wait_warning_secs` | 30 | 1 to 86400 | `warning` when the oldest waiting call has waited this long |
| `health_summary.wait_critical_secs` | 120 | 1 to 86400 | ... `critical` at this wait |
| `fairness.mode` | `off` | `off`, `share`, `quota`, `fair_queue` | `share`: weighted share of RPM, TPM and slots, unused share lent to others; `quota`: hard per-tenant RPM / TPM; `fair_queue`: round robin between tenants among waiters |
| `fairness.weights` | none | tenant id -> above 0 to 1000000 | `share`: a tenant's weight |
| `fairness.default_weight` | 1 | above 0 to 1000000 | `share`: the weight of a tenant not in `weights` |
| `fairness.rpm` | none | 1 or more (whole) | `quota`: each tenant's RPM cap |
| `fairness.tpm` | none | 1 or more (whole) | `quota`: each tenant's TPM cap |
| `fairness.quotas` | none | tenant id -> `{rpm, tpm}` | `quota`: caps of one tenant |
| `fairness.active_secs` | 60 | 1 to 86400 | A tenant counts as active this long after its last call |
| `fairness.tenant_keys` | `names` | `names`, `hashed` | Written `hashed` by every registry write path once the names in `weights` / `quotas` are replaced by their keyed hashes; an exported document keeps it, so it is not hashed twice. A 16-hex key in a block of `names` is refused |

The tenant is the request context's tenant id, else its user id; tenant ids are 1-256
characters. A `quota` with no cap is refused; a cap above the model's budget, a `tpm` cap below
one call's reservation and fields the mode never reads are warned about. Only trusted code
should set `priority=True`: priority calls are served before every tenant's share.

The 0.1.0b1 alert names `quota_used_mins`, `spill_pct`, `spill_window_secs` and
`waiting_age_secs` did nothing and were removed: a document naming them still loads, with a
deprecation warning, until 0.2.

### Validation

Besides each setting's range, a document is refused (with the path) for: a TPM below one call's
reservation (`max output x tpm_output_weight`); a fallback cycle of any length (unless every
step requires headroom); a fallback target whose residency is not within the source model's;
residency that leaves a model with no deployment; an explicit `breaker_probe_secs` below
`attempt_timeout_secs`; `rpm` and `quota_rpm` (or `tpm` and `quota_tpm`) on one level, or a
quota x `budget_pct` that rounds down to 0; a fallback `after_secs` that could never fire inside `timeout_secs`;
`NaN`, infinities and numbers above their bound; names with a trailing newline or padding;
malformed role ARNs; queue names that are not plain or start with `jobs-`; an embedding model
whose family cannot be told (set `family`).

Warnings (the document loads; each is logged with its path): a fallback to a disabled model; a
`blocked_regions` entry no deployment uses or a `blocked_accounts` entry that is not an
account; a model with no usable confirmed deployment; queue retention below the timeout; a
retry base above its cap; `probe_after_secs` above `probe_max_secs`; duplicate fallback causes
or models; a `fallback` rule with no fallback regions; settings the model's type or provider
never reads; a `model_fallback` to a model of another provider (allowed: see below).

A stored document is read leniently: an unknown setting or sub-field from a newer library is
ignored with a warning, while the process's own `LLMQ_REGISTRY` stays strict. Protected fields
in a stored document (`role_arn`, `model_id`, residency, `live_edit`, ...) must match the pod's
own deploy config, or the stored document is refused (logged and counted).

### Fallback to a model of another provider

A `model_fallback` target may be a model of the other provider: a Bedrock (Converse) model may
fall back to an OpenAI-compatible model and the other way round. The caller still writes its
request in its own model's shape and reads the reply in that shape; the worker converts both:

| Part | Converse | OpenAI chat |
|---|---|---|
| System prompt | `system` text blocks | a `system` (or `developer`) message |
| Text and images | `text`, `image` (inline bytes: png, jpeg, gif, webp) | string content, `text` and `image_url` parts (base64 data URLs) |
| Tool use | `toolUse` blocks in an assistant turn | assistant `tool_calls` (arguments as JSON text) |
| Tool results | `toolResult` blocks in a user turn (`status: error` becomes an `error: ` prefix) | `tool` messages |
| Tools | `toolConfig.tools[].toolSpec` (`inputSchema.json`) | `tools[].function` (`parameters`) |
| Tool choice | `toolChoice` `auto` / `any` / `tool` | `tool_choice` `auto` / `required` / a named function (`none` has no Converse counterpart: left out) |
| Output and sampling | `inferenceConfig.maxTokens`, `temperature`, `topP`, `stopSequences` | `max_tokens` (or `max_completion_tokens`), `temperature`, `top_p`, `stop` |
| Reply | `output.message` (text, `toolUse`), `stopReason`, `usage` (input, output, total, cache read) | `choices[0].message` (content, `tool_calls`), `finish_reason`, `usage` (prompt, completion, total, cached) |

Stop reasons map `end_turn` / `stop_sequence` to `stop`, `max_tokens` to `length`, `tool_use` to
`tool_calls` and the guardrail / content filter reasons to `content_filter` (and back). Fields
with no counterpart are dropped: Converse `additionalModelRequestFields`, guardrails, cache points
and reasoning blocks of past turns; OpenAI `response_format`, `logprobs`, `n`, penalties and
`seed`. The target's own `inference` defaults and limits apply after the conversion.

A request the other shape cannot carry (a `document` or `video` block, an image by URL or S3
location instead of inline bytes, an image inside a tool result, a non-text OpenAI part such as
audio, tool arguments that are not JSON) skips that target (logged `fallback_shape_skipped`);
the call then waits, tries the next target or fails as it would without it. **Streams do not
fall back across shapes**: their events are not converted, so a stream skips such a target
(a disabled model whose only targets are of the other shape fails a stream with
`model_unavailable`). The validator warns where such a target is configured.

## Changed in 0.1.0b2

| Setting | Change |
|---|---|
| `converse_api`, `max_message_bytes`, `embed_max_chars`, `alerts.queue_age_secs`, `fairness`, `family` (model field) | New |
| `queue.retention_secs` | Default one day (was four days), applied on every send; at most two weeks, like `queue.dlq_retention_secs` |
| `inference.extra` | Plain lowercase names only; SDK arguments and request fields are refused |
| `inference.embedding_types` | Only `["float"]` |
| `timeout_secs`, `default_output_tokens` | Honoured at account level (and `default_output_tokens` at deployment level); the most specific level wins |
| `alerts` | Four names removed (see above); `quota_near_limit` checks TPM too |
| every number | Finite and at most 2**53 - 1; timeouts and waits at most one day |
| `min_lib_version` (top level) | A version such as `0.1.0b2`, checked against the running library |
