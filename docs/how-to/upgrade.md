# Upgrade

What an app has to do when it moves to a new release, newest first. The full list of changes is
in [CHANGELOG.md](../project/changelog.md).

## From 0.1.0b2 to 0.1.0b3

0.1.0b3 keeps the message format and the key layout of 0.1.0b2, so pods of both releases can
share a queue on a `noeviction` store. What changes is how keys expire (key expiry modes, for a
shared `volatile-*` cache store) and that removed models and deployments are purged
automatically.

### 1. Upgrade every pod

| Store policy | Mixed 0.1.0b2 / 0.1.0b3 fleet |
|---|---|
| `noeviction` | Fine. `LLMQ_KEY_EXPIRY=auto` picks `native` there, as 0.1.0b2 does. Roll out as usual |
| `volatile-*` | Finish the rollout first. 0.1.0b2 pods do not read the store's mode (`P:expiry`) and always write native TTLs, so their keys stay evictable; in strict mode they are not ready on such a store anyway |
| `allkeys-*` | Not supported by either release |

Upgrade all pods; do not keep 0.1.0b2 pods on a store you share with a cache.

### 2. Switching key expiry modes

Moving a store from `native` to `swept` (an upgrade on a `volatile-lru` node, or a policy change
under `auto`) loses nothing. In `swept` mode setting an expiry also clears the key's native TTL
(`PERSIST`); in `native` mode it also removes the key's `P:ttl` member; reading the time left
counts either. Each key moves to the new mode on its next write; a key never written again
keeps its old expiry (native TTL or index member) and still goes then. Until then, keys that
still have a native TTL can be evicted by a `volatile-*` policy (at most a day, for the slowest
keys). See [Key expiry modes](../explanation/storage.md#key-expiry-modes).

### 3. Rolling back to 0.1.0b2 after running `swept`

0.1.0b2 has no expiry sweep: a key whose expiry is only in `P:ttl` would never expire under it.
Drain the index before you roll back:

1. Make sure the store is `noeviction` (0.1.0b2 is not ready on `volatile-*` in strict mode).
2. Set `LLMQ_KEY_EXPIRY=native` on every 0.1.0b3 pod and roll it out. Each write now moves its
   key back to a native TTL, and the index sweep keeps running in `native` mode, deleting the
   indexed keys as they come due.
3. Keep them running for at least the longest key lifetime: 1 day by default, longer if you
   raised a retention or TTL setting.
4. Check the index is about empty on each store (a separate queue store has its own), then roll
   back:

   ```bash
   valkey-cli -u "$LLMQ_STORE_URL" ZCARD llmq:ttl        # about 0; llmq is LLMQ_KEY_PREFIX
   ```

Leftover `P:expiry`, `P:ttl` and `P:purge` keys are harmless to 0.1.0b2.

### 4. Purge

| Change | What to do |
|---|---|
| Auto purge is on (`LLMQ_AUTO_PURGE=true`): a model or deployment dropped from the registry has its store state deleted after `LLMQ_PURGE_GRACE_SECS` (900) unless it comes back | Nothing, or set `LLMQ_AUTO_PURGE=false` to keep the 0.1.0b2 behaviour. If you raised `timeout_max_secs` above 900, raise the grace to at least the same |
| `llm-queue registry gc` prints the purge report: `deleted_keys` and `dry_run` instead of `would_delete` | Update scripts that parse its output |
| `registry gc` also purges removed deployments of live models, and keeps a removed model's queue that still holds messages unless `--force` | Add `--force` where you relied on it deleting such queues |
| New error `PurgeRefused` (HTTP 409) | Catch it where you call the purge service; see [Errors](../reference/errors.md#purgerefused) |

See [Purge store state](purge.md) and [Run on a shared cache store](shared-cache-store.md).

## From 0.1.0b1 to 0.1.0b2

0.1.0b2 keeps no backward compatibility with 0.1.0b1 (both are betas). This section lists every
change an app pinned to 0.1.0b1 may have to make, in the order to make them.

### 1. Drain every 0.1.0b1 worker first

Mixed fleets are **not supported**: 0.1.0b1 and 0.1.0b2 pods must not share a queue. A message
carrying bytes (images, documents) is written in a form 0.1.0b1 does not decode, and 0.1.0b1
sends it to Bedrock as is. Before the first 0.1.0b2 pod starts:

1. Stop sending to 0.1.0b1 (or `llm-queue drain M --yes --wait` each model).
2. Let the queues empty, then stop the 0.1.0b1 pods.
3. Deploy 0.1.0b2, then set the models back on (`llm-queue registry set --model M enabled=true`).

### 2. Environment

| Change | What to do |
|---|---|
| `LLMQ_ENVIRONMENT` unset is strict (was dev) | Set `LLMQ_ENVIRONMENT=dev` on laptops and in CI |
| Strict mode requires `LLMQ_KEY_PREFIX` | Set one per app and environment (`myapp-prod`): 1-64 letters, digits, `_`, `.`, `:`, `-` |
| Strict mode requires `LLMQ_ID_HASH_SALT` of at least 16 characters | Use a random secret of 32 characters or more |
| Strict mode requires `https` for `LLMQ_BEDROCK_ENDPOINT_URL` / `LLMQ_STS_ENDPOINT_URL` | Use `https` overrides, or none |
| `LLMQ_BASE_URL_ALLOWED_HOSTS` takes bare host names | Drop schemes, ports and paths (`api.openai.com`) |
| Every `LLMQ_*` number has a range; whole-number variables refuse `200.0` | Fix any value the start-up error names; see [Environment variables](../reference/environment.md) |
| `LLMQ_DRAIN_SECS` default 25 (was 45) | Keep `terminationGracePeriodSeconds` above it |
| `LLMQ_CALLER_TTL_SECS` minimum | At least `LLMQ_LEASE_SECS / 3 + max(5, LLMQ_LEASE_SECS / 6)`; the defaults pass |

### 3. Registry document

| Change | What to do |
|---|---|
| `api_key_ref` / `external_id_ref` must start with `LLMQ_ACCOUNT_` | Rename the secrets (and the references) |
| Numbers are bounded: 2**53 - 1 for all, one day for timeouts and waits, tighter per setting | Fix any value the load error names ([Configuration](../reference/configuration.md#registry-settings)) |
| Names (aliases, accounts, regions) are 1-64 characters with no trailing newline; `queue.name` is 1-71 plain characters, not starting `jobs-` | Rename the ones the load error names |
| `queue.retention_secs` defaults to one day (was four) and is applied on every send | Set it on models whose calls may wait longer |
| Removed alerts `quota_used_mins`, `spill_pct`, `spill_window_secs`, `waiting_age_secs` | Delete them; they load with a deprecation warning until 0.2 |
| `inference.extra` takes plain lowercase names only, never SDK arguments | Move refused names into the request |
| `inference.embedding_types` takes only `["float"]` | Drop other types |
| An embedding model whose id names no family (`titan-embed`, `cohere.embed`) fails the load; it was sent Titan's body | Set its `family`, or register one ([Embedding families](call.md#embedding-families)) |
| Checks that were warnings are errors (residency leaving no deployment, a fallback `after_secs` that can never fire, ...) | Fix what the load error names |
| A `role_arn` must be an IAM role ARN; padded `model_id` / `profile_id` are refused | Fix the values |
| `min_lib_version` must be a version such as `0.1.0b2` | Fix it or remove it |

### 4. Code

| Change | What to do |
|---|---|
| A sync call (`converse_sync`, `embed_sync`, LangChain `invoke`) on the queue's own event-loop thread raises `LLMQueueError` | In async code, `await llmq.converse(...)`, `await chat.ainvoke(...)` |
| A message above the model's `max_message_bytes` (4 MB) fails `request_too_large` before it is queued (0.1.0b1 had no cap) | Send large files by reference, or raise the setting |
| New kinds `invalid_request`, `context_length_exceeded`, `worker_shutdown` | Add them to any kind -> action table; `llm_queue.Kind` lists every kind |
| `stream()` on a non-chat model raises `CallFailed("invalid_request")` (was `ConfigError`) | Catch `CallFailed` |
| A failing or unregistered admission hook refuses the call (`rejected`; was: admitted) | Register every hook the registry names in every process |
| `inference_limits.max_tokens` also caps `max_completion_tokens`, refuses non-numbers, and is written into a call that sets none (so the call reserves the cap against TPM) | Keep the cap near real answer sizes |
| `start()` after a failed `start()`, or from a second loop, raises `LLMQueueError` | Build a new `LLMQueue` |
| `Tuning` and the options classes are `slots=True` | Use `dataclasses.asdict` / `replace`, not `vars()` |
| `LLMQueue.engine` / `.jobs` are deprecated (warn until 0.2) | Use the public methods |
| `RegistryStore.boot(actor=None)`: a boot publish is audited as `boot@<host>:<pid>` (was `boot`) | Pass `actor=` to name it yourself |
| A custom `Provider.call` gets `converse_api=` only when its signature accepts it | Nothing for 0.1.0b1 providers |
| Delivery is at least once: a call whose record the store lost is sent again | Make side effects of calls and jobs idempotent |
| A `request_id` whose last run failed, expired or was cancelled runs again (only a success is replayed) | Use a new id when a retry must not run the call twice |
| Cancel wins: any caller's `cancel()` stops the call for every caller of that id | Do not share an id between callers that must not cancel each other |
| An app's `worker_id` is a prefix; a random part is always added | Match worker ids by prefix in dashboards |
| `register_job` refuses a function that is not async; `send_job(job_id=X)` is deduplicated for 24 h; failed jobs retry for about an hour, then are dead-lettered for 7 days | Make job functions `async def`; see [Run background jobs](jobs.md) |

### 5. HTTP, CLI and operations

| Change | What to do |
|---|---|
| `GET /health/live` answers 503 when a background loop died or the heartbeat stalled (was always 200) | Keep the liveness probe's `failureThreshold` at 3 or more |
| In strict mode an evicting `maxmemory-policy` fails readiness | Use `noeviction`, or keep `volatile-*` with key expiry `swept` (0.1.0b3; the default `LLMQ_KEY_EXPIRY=auto` picks it on a `volatile-*` store). ElastiCache's default parameter group uses `volatile-lru`: from 0.1.0b3 pods are ready on it as they are; on 0.1.0b2, create a custom parameter group with `maxmemory-policy noeviction` and attach it, or pods start but never report ready. See [Shared cache store](shared-cache-store.md). `allkeys-*` is never accepted |
| `http_router(allow_writes=True)` needs `dependencies=`; writes take JSON bodies and the `X-LLMQ-Admin` header | Update admin clients |
| A lost write race answers 409 (was 400); routes hit before the registry loads answer 503 (was 500) | Retry on 409 after `Retry-After` |
| `dlq purge`, `dlq redrive`, `drain`, `registry gc`, `cancel` need `--yes` or `--dry-run` | Add `--yes` to scripts |
| `llm-queue request ID` exits 1 for an unknown id; `drain --wait` exits 3 on timeout (both were 0) | Check exit codes in deploy scripts |
| A `/drain` preStop route must answer GET | Use `@app.api_route("/drain", methods=["GET", "POST"])` as in sample 09 |

### 6. OpenAI transport and LangChain

The OpenAI-compatible transport (`QueuedChatOpenAI`, `QueuedOpenAIEmbeddings`, a raw SDK client on
`QueuedHTTPTransport`) answers some kinds with another status, so the SDK raises another class:

| Kind | 0.1.0b1 | 0.1.0b2 | SDK exception now |
|---|---|---|---|
| `retry_budget_exhausted` | 429 | 503 | `InternalServerError` (was `RateLimitError`) |
| `dead_lettered` | 500 | 502 | `InternalServerError` (same class) |
| `provider_error` | 400 | 502 | `InternalServerError` (was `BadRequestError`) |
| `no_deployment`, `worker_shutdown` | 400 | 503 | `InternalServerError` (was `BadRequestError`) |
| `rejected` | 400 | 403 | `PermissionDeniedError` |
| `request_id_conflict` | 400 | 409 | `ConflictError` |
| `result_expired` | 400 | 410 | `APIStatusError` |
| a kind with no row | 400 | 500 | `InternalServerError` |

LangChain `with_retry` and fallbacks keyed on exception classes behave accordingly: a
`retry_budget_exhausted` is no longer retried as a rate limit.

- `QueuedChatOpenAI` turns the Responses API off (`use_responses_api=True` is refused); the queue
  serves `/chat/completions` and `/embeddings` only.
- Every adapter checks `llmq=` at construction, and `allow_fallback` is `"all"`, `"regions"` or
  `"none"`.
- The adapters stream only with `queue_streaming=True` (off by default): then `astream()`
  goes through `converse_stream` and yields text and tool-call deltas. Off, `astream()`
  yields the whole answer as one chunk, as in 0.1.0b1.
- The first started queue stays the LangChain default (0.1.0b1: the last one).
