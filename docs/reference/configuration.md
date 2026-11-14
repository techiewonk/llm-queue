# Configuration

Where the process settings (`LLMQ_*`) are listed, the `Tuning` fields set in code, and where the
registry settings are listed. How to use them is in [Configure](../how-to/configure.md) and
[Tune](../how-to/tune.md).

## Process settings (`LLMQ_*`)

Every `LLMQ_*` variable, its default, and what `LLMQ_ENVIRONMENT` turns on (unset is strict) are
in [Environment variables](environment.md).

## `Tuning` (in code)

Passed as `LLMQueue.from_env(tuning=Tuning(...))`. Defaults:

| Part | Field (default) |
|---|---|
| `StoreOptions` (`tuning.store`) | `retries` (3), `connect_timeout_secs` (5), `read_timeout_secs` (15), `health_check_secs` (30), `pool_wait_secs` (10) |
| `BedrockOptions` (`tuning.bedrock`) | `read_timeout_secs` (300), `connect_timeout_secs` (10), `max_pool_connections` (100: used only until the registry sizes the pool, see below), `role_session_secs` (3600) |
| `EngineTimings` (`tuning.engine`) | `idle_poll_secs` (0.05), `idle_slow_secs` (1), `idle_after_empty` (20), `result_recheck_secs` (60), `health_probe_secs` (3), `stream_read_block_ms` (100), `admission_probe_wait_ms` (200), `admission_slot_wait_ms` (50), `admission_slot_jitter_ms` (150), `admission_wait_ms` (100), `limiter_cut_ms` (1000), `limiter_cut_factor` (0.7), `budget_key_ms` (180000) |
| `RegistryOptions` (`tuning.registry`) | `history_size` (20), `edit_retries` (3) |

What each field does and the start-up checks are in [Tune](../how-to/tune.md).

Bedrock connection pools are sized from the registry: each (account, region) client gets the sum
of the concurrency slots routed to it, at least 10 and at most 200 connections. `WarmOptions`
(2 warm connections per client, refreshed every 10 s, kept idle 15 s) controls the warm
connections; see [Clients and connections](../explanation/clients.md).

<!-- verify after merge: how WarmOptions and OpenAIOptions are passed (they were exported but not wired into from_env / Tuning on the client-hub branch). -->

## Registry settings

The registry document's settings (budgets, balancing, fallback, retries, breaker, timeouts,
queues, inference defaults, alerts) can each be set at some of the levels deployment > model >
account > defaults. Every setting with its levels, type, range and default is in the
[design plan](https://github.com/techiewonk/llm-queue/blob/main/docs/ACCOUNT_ROUTING_PLAN.md),
sections 3.9 and 3.19.

For a running system, `llm-queue settings get --model M [--deployment D]` prints the effective
value of every setting and the level it came from.

Settings added or changed in 0.1.0b2:

| Setting | Levels | Default | Meaning |
|---|---|---|---|
| `converse_api` | defaults, model | `converse` | Non-stream Bedrock chat: `converse` (plain Converse, IAM `bedrock:InvokeModel`) or `stream` (ConverseStream, `bedrock:InvokeModelWithResponseStream`, with the first-response timer). See [Bedrock APIs and IAM](../how-to/bedrock.md) |
| `max_message_bytes` | defaults, model | 4194304 (4 MB) | A call whose message (inline images and documents included) is larger fails `invalid_request` before it is queued; at least 1024 |
| `inference.extra` | as `inference` | | Provider-specific fields: plain lowercase names only; SDK arguments and request fields (`extra_body`, `store`, `metadata`, `user`, `model`, `messages`, `stream`, `tools`, ...) are refused. Protected in `safe` live-edit mode |
| `queue.retention_secs`, `queue.dlq_retention_secs` | | | Now at most two weeks (1209600): dead letters hold the prompt |
| `alerts` | | | Only `quota_used_pct` (1-100, default 90; `quota_near_limit` now checks TPM too) and `dlq_messages` (1 or more, default 1). `quota_used_mins`, `spill_pct`, `spill_window_secs` and `waiting_age_secs` did nothing and were removed |
| `live_edit` (top level) | document | unset | The document's live-edit mode (deploy-only) |
| `min_lib_version` (top level) | document | unset | Now a version such as `0.1.0b2`; checked against the running library |

<!-- verify after merge: `converse_api` — registry/settings.py allows "converse" | "stream", but clients/providers.py compares "converse_stream" and runtime.py never passes Timing.attempt_secs (dev/0.1.0b2 at 84c979d). -->

Validation added in 0.1.0b2 (each refused with its path): a TPM below one call's reservation
(`max output x tpm_output_weight`); a `model_fallback` to another provider; a fallback cycle of
any length (unless every step requires headroom); a fallback to a disabled model; a fallback
target whose residency is not within the source model's; residency that leaves a model with no
deployment (an error, no longer a warning); `breaker_probe_secs` below `attempt_timeout_secs`;
unknown `blocked_regions`. `LLMQ_CALLER_TTL_SECS` must be at least
`LLMQ_LEASE_SECS / 3 + max(5, LLMQ_LEASE_SECS / 6)`. The registry audit is capped at about
10,000 entries besides its age limit.

