# Upgrade

What an app has to do when it moves to a new release, newest first. The full list of changes is
in [CHANGELOG.md](../project/changelog.md).

## From 0.1.1 to 0.2.0

### 1. Upgrade the whole fleet; drain 0.1.1 first

0.2.0 messages carry keyed hashes of `tenant_id` and `user_id` where 0.1.1 carried the raw ids,
so the message format is version 3. Workers of the two releases do not share a queue: a 0.1.1
worker returns a 0.2.0 message to the queue (it is newer), and a 0.2.0 worker refuses a 0.1.1
one (it would read the raw tenant as a hash, and write the raw name into fairness keys). Job
messages carry the sender's hashes too. Set `LLMQ_ID_HASH_SALT` to one value in every service
that shares the store, then upgrade the whole fleet at once:

1. Stop sending: pause or scale down the 0.1.1 callers (or drain their pods, `LLMQ_DRAIN_FILE`).
2. Let the 0.1.1 workers empty the queues (`llm-queue queues list` shows 0 waiting and held),
   then stop them.
3. Start 0.2.0 everywhere.

Tenant names in the registry's `fairness.weights` / `quotas` are hashed on every write path
(the boot document, `registry set`, publish, rollback); the hashed block is marked
`tenant_keys: hashed`, so an exported document is never hashed twice. A 16-hex tenant name in
a block without that mark is refused (it reads as a hash).

### 2. Code that used `LLMQueue.engine` or `LLMQueue.jobs`

Both are gone (deprecated since 0.1.0b2). Use `llmq.admin.*` for every admin and status
operation (registry, models, deployments, blocks, requests, purges, dead letters, audit),
`status()`, `health()`, `request_status()` and `describe()` for state, and `register_job` /
`send_job` for jobs. `PurgeService` and `RegistryStore` are no longer exported from
`llm_queue`: use `llmq.admin.purge` and `llmq.admin.registry`, or `AdminServices.from_env()`
without a queue.

### 3. Settings and registry

Any `LLMQ_*` name that no setting reads, including a renamed or removed one, stops a strict
start with a `ConfigError` naming it (dev logs `setting_unknown`). `LLMQ_TENANT_INDEX` is
removed. Renamed:

| Old name | New name |
|---|---|
| `LLMQ_LOCAL_LIMIT_CAP` | `LLMQ_LOCAL_CONCURRENCY` |
| `LLMQ_PURGE_BATCH` | `LLMQ_ADMIN_BATCH` |
| `LLMQ_AUTO_PURGE` | `LLMQ_PURGE_AUTO_ENABLED` |
| `LLMQ_PURGE_WAIT` | `LLMQ_PURGE_WAIT_ENABLED` |
| `LLMQ_HTTP_PURGE_WAIT_SECS` | `LLMQ_ADMIN_HTTP_WAIT_SECS` |
| `LLMQ_TENANT_PURGE_DEFAULT` | `LLMQ_PURGE_TENANT_REMOVE` |
| `LLMQ_QUEUE_WAIT_REPLICAS` | `LLMQ_REPLICA_WAIT_COUNT` |
| `LLMQ_QUEUE_WAIT_MS` | `LLMQ_REPLICA_WAIT_MS` |
| `LLMQ_QUEUE_WAIT_SCOPES` | `LLMQ_REPLICA_WAIT_SCOPES` |
| `LLMQ_QUEUE_FAILOVER_SECS` | `LLMQ_QUEUE_STORE_FAILOVER_SECS` |
| `LLMQ_CFG_RELOAD_SECS` | `LLMQ_REGISTRY_RELOAD_SECS` |
| `LLMQ_KEY_EXPIRY` | `LLMQ_KEY_EXPIRY_MODE` |
| `LLMQ_TTL_SWEEP_BATCH` | `LLMQ_KEY_EXPIRY_SWEEP_BATCH` |
| `LLMQ_TTL_SWEEP_ROUNDS` | `LLMQ_KEY_EXPIRY_SWEEP_ROUNDS` |
| `LLMQ_REGISTRY_LIVE_EDIT` | `LLMQ_REGISTRY_LIVE_EDIT_MODE` |
| `LLMQ_DEBUG_CAPTURE_FROM_CONFIG` | `LLMQ_DEBUG_CAPTURE_ENABLED` |
| `LLMQ_BASE_URL_ALLOWED_HOSTS` | `LLMQ_HTTP_ALLOWED_HOSTS` |
| `LLMQ_MODE` | `LLMQ_ROLE` |
| `LLMQ_WORKER_ID` | `LLMQ_INSTANCE_ID` |
| `LLMQ_DEFAULT_TENANT` | `LLMQ_TENANT_DEFAULT` |
| `LLMQ_ADMIN_DLQ_BODIES_ENABLED` | `LLMQ_ADMIN_DEAD_LETTER_BODIES_ENABLED` |
| `LLMQ_SECRET_NAMES` | `LLMQ_SECRETS_NAMES` |

The 0.1.0b1 alert names `quota_used_mins`, `spill_pct`, `spill_window_secs` and
`waiting_age_secs` are now unknown fields: delete them from the registry.

### 4. New refusals in strict mode

- Admin tokens shorter than 32 characters (`LLMQ_ADMIN_TOKEN`, `LLMQ_ADMIN_READ_TOKEN`, or the
  tokens passed to `http_router()`).
- The `botocore` Bedrock transport with `LLMQ_BEDROCK_ENDPOINT_URL` set: it follows redirects
  with the signed request. Keep the default `aiohttp` transport.
- Registry placeholders that name a credential, and an account `*_ref` that names a process
  setting.

### 5. The admin API

Every admin operation moved under `llmq.admin`, and the CLI commands and HTTP routes were
renamed to match one operations table ([Admin API](../reference/admin-api.md)). Nothing old is
kept. Every write is now a **dry run by default** on Python, the CLI and HTTP: add
`dry_run=False` in code and `--yes` on the CLI (it replaces `--dry-run`, which is gone).

| 0.1.x | 0.2.0 |
|---|---|
| `llmq.purge.<method>(...)` | `llmq.admin.purge.<method>(...)` (returns the report dict; dry run unless `dry_run=False`) |
| `PurgeService.plan_model` / `plan_deployment` / `plan_tenant` | `model` / `deployment` / `tenant` with the default `dry_run=True` |
| `PurgeService` methods: `dry_run=False` by default | `dry_run=True` by default: pass `dry_run=False` to purge |
| `llmq.purge.cancel_scheduled(target)` | `llmq.admin.purge.unschedule(target, actor=, dry_run=False)` |
| `llmq.registry` (the `RegistryStore`) | `llmq.admin.registry` (`get`, `versions`, `diff`, `schema`, `set`, `publish`, `rollback`; `.current` read-only), `llmq.admin.models`, `.deployments`, `.blocks`, `.audit` |
| `llmq.registry.set_enabled(m, d, False, actor=)` | `llmq.admin.deployments.disable(m, d, actor=, dry_run=False)` |
| `llmq.registry.block(region=, on=, actor=)` | `llmq.admin.blocks.add(region=, actor=, dry_run=False)` / `.remove(...)` |
| `llmq.registry.log(count)` | `llmq.admin.audit.list(count)` |
| `llmq.cancel(request_id)` | `llmq.admin.requests.cancel(request_ids=[id], state="all", actor=, dry_run=False)` |
| `llmq.job_dead_letters(name)` / `llmq.redrive_jobs(name)` | `llmq.admin.dlq.list(job=name)` / `llmq.admin.dlq.redrive(job=name, actor=, dry_run=False)` |
| `PurgeRefused` | `AdminRefused` (category `bad_request`, HTTP 409) |
| `llm-queue request ID` | `llm-queue requests get ID` |
| `llm-queue cancel ID ... --yes` | `llm-queue requests cancel --id ID ... [--state waiting\|running\|all] --yes` |
| `llm-queue block` / `unblock --region R` | `llm-queue blocks add` / `remove --region R --yes` |
| `llm-queue drain M --yes --wait --timeout S` | `llm-queue models drain M --wait-secs S --yes` |
| `llm-queue settings get --model M [--deployment D]` | `llm-queue models get M`, `llm-queue deployments get M D` |
| `llm-queue registry enable` / `disable M D` | `llm-queue deployments enable` / `disable M D --yes` |
| `llm-queue registry log` | `llm-queue audit list` |
| `llm-queue registry gc` | `llm-queue purge removed` |
| `llm-queue registry publish --from-version N` | `llm-queue registry rollback N` |
| `--dry-run` on every changing command | Gone: every change is a dry run until `--yes` (registry `set` / `publish` too) |
| `POST /registry/block` (`on: false` to lift) | `POST /blocks`, `POST /blocks/remove` |
| `POST /registry/gc` | `POST /purge/removed` |
| `POST /purge/scheduled/cancel` | `POST /purge/unschedule` |
| `POST /registry` body: the values | Body `{values, model?, dry_run?}`; a dry run by default |
| `GET /drain` open | `GET /drain` needs a read guard |
| Read routes mounted open with no guard (a warning) | Not mounted: with no token and no `dependencies` only the probes and `/metrics` are (warning `admin_routes_unauthenticated`), unless `insecure_reads=True` |

With no token and no `dependencies` the HTTP router mounts only the probes and `/metrics` (pass
`insecure_reads=True` to keep open reads on an internal port). Set `LLMQ_ADMIN_READ_TOKEN` for dashboards that only
read, and `LLMQ_ADMIN_WRITES_ENABLED=true` (or `allow_writes=True`) where writes are wanted.

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
| New error `PurgeRefused` (HTTP 409) | Catch it where you call the purge service; see [Errors](../reference/errors.md#adminrefused) (renamed `AdminRefused` in 0.2.0) |

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
