# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[semantic versioning](https://semver.org/); while it is 0.x, a minor release may break the
public API (see STABILITY.md).

## [Unreleased]

## [0.2.0] - 2026-11-18

### Upgrading from 0.1.1

1. **Upgrade the whole fleet; drain 0.1.1 first.** The message format is now version 3:
   `tenant_id` and `user_id` carry keyed hashes, not raw ids, and job messages carry the
   sender's hashes. 0.1.1 and 0.2.0 workers do not share a queue (a 0.1.1 worker returns a
   0.2.0 message; a 0.2.0 worker refuses a 0.1.1 one). Set `LLMQ_ID_HASH_SALT` to the same value
   everywhere, let the 0.1.1 workers empty the queues (stop the 0.1.1 callers first), then
   start 0.2.0 everywhere. Tenant names in `fairness` blocks are hashed on every registry write
   (`tenant_keys: hashed`).
2. **`LLMQueue.engine` and `LLMQueue.jobs` are gone** (they were deprecated in 0.1.0b2). Use
   the public API instead: `llmq.admin.*` for every admin and status read or write (registry,
   models, deployments, blocks, requests, purges, dead letters, audit), `status()`,
   `health()`, `request_status()` and `describe()` for state, and `register_job` /
   `send_job` for jobs.
3. **Removed names.** The admin API names in the table under "Removed" (`llmq.purge`,
   `llmq.registry`, `llmq.cancel`, `PurgeRefused`, the old CLI commands and routes);
   `PurgeService` and `RegistryStore` from `llm_queue` (use `llmq.admin.purge` /
   `llmq.admin.registry`, or `AdminServices.from_env()` without a queue); the 0.1.0b1 alert
   names `quota_used_mins`, `spill_pct`, `spill_window_secs` and `waiting_age_secs` (now unknown
   fields); the settings `LLMQ_TENANT_INDEX`, `LLMQ_PURGE_BATCH` (`LLMQ_ADMIN_BATCH`) and
   `LLMQ_LOCAL_LIMIT_CAP` (`LLMQ_LOCAL_CONCURRENCY`). Any `LLMQ_*` name no setting reads
   (renamed, removed or misspelt) stops a strict start with a `ConfigError` naming it; the
   renamed settings are in the table below.
4. **New refusals in strict mode** (any `LLMQ_ENVIRONMENT` but `dev`, `local`, `test`, `ci`):
   admin tokens shorter than 32 characters (`LLMQ_ADMIN_TOKEN`, `LLMQ_ADMIN_READ_TOKEN`, or
   the tokens given to `http_router()`); the `botocore` Bedrock transport together with
   `LLMQ_BEDROCK_ENDPOINT_URL` (it follows redirects with the signed request: keep the default
   `aiohttp` transport); registry placeholders that name a credential; an account `*_ref` that
   names a process setting.
5. **Every admin write is a dry run by default**, on Python, the CLI and HTTP: pass
   `dry_run=False`, `--yes` or `"dry_run": false`. With no token and no `dependencies` the
   HTTP router mounts only the probes and `/metrics`.
6. **`queue_ready(enabled, llmq=None)` is `queue_ready(llmq=None)`**: set `LLMQ_ENABLED=false`
   instead of passing the app's own flag. `describe()` shows `tenant_default_hash`, not the
   tenant name.

| Old name | New name |
|---|---|
| `LLMQ_LOCAL_LIMIT_CAP` | `LLMQ_LOCAL_CONCURRENCY` |
| `LLMQ_METRICS_SINK` | `LLMQ_METRICS_SINK_MODE` |
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
| `LLMQ_MODE` | `LLMQ_ROLE_MODE` |
| `LLMQ_WORKER_ID` | `LLMQ_INSTANCE_ID` |
| `LLMQ_DEFAULT_TENANT` | `LLMQ_TENANT_DEFAULT` |
| `LLMQ_ADMIN_DLQ_BODIES_ENABLED` | `LLMQ_ADMIN_DEAD_LETTER_BODIES_ENABLED` |
| `LLMQ_SECRET_NAMES` | `LLMQ_SECRETS_NAMES` |

See [Upgrade](docs/how-to/upgrade.md#from-011-to-020).

### Added

- New settings: `LLMQ_ADMIN_READ_TOKEN` (read routes only), `LLMQ_ADMIN_WRITES_ENABLED` (false),
  `LLMQ_ADMIN_ACTOR_HEADER` (`X-LLMQ-Actor`: a write under the token guard is audited as
  `token:<value>`, `token:unknown` without it), `LLMQ_ADMIN_INDEX_CACHE_SECS` (2),
  `LLMQ_ADMIN_DEAD_LETTER_BODIES_ENABLED` (false). New: `GET /` admin index with a live summary;
  `AdminServices.from_env()` for admin workers and the CLI; routes for registry versions, diff,
  schema, publish and rollback, audit, models, deployments, blocks, queues, jobs, dead letters and
  `describe`; log event `llmq_admin` per audited write. `llmq.admin.requests`, `tenants.list`,
  `purge.queue` and `status.get(model, tenant=...)` are served by `RequestsAdmin` over the request
  index (below).
- Local execution mode: `LLMQ_EXECUTION_MODE=local` (default `queue`) runs every call in its
  own process straight against the provider, with the same API, classes, LangChain adapters
  and `install()`, and never contacts the store (no store client is built; `LLMQ_STORE_URL`
  is not needed). Each deployment's rpm / tpm (and account shared budgets) divided by
  `LLMQ_LOCAL_PROCESS_COUNT` (default 1) is this process's budget, as token buckets with the
  registry's window rules; at most `LLMQ_LOCAL_CONCURRENCY` (default 10) calls per model run at
  once, priority calls first. Retries, backoff, attempt and call timeouts, `max_wait_secs`,
  region and model fallback (with shape conversion), sticky sessions, breakers, throttle
  backoff and target-error health work as in the queue, in memory; streams and batched
  embeddings too. `describe()`, `health()`, `status()` and the probes say `mode: "local"` and
  show the local budgets; `LLMQueue.execution_mode` reads it. Store-only operations
  (`request_status`, jobs, and every `llmq.admin` operation on requests, tenants, purges,
  registry versions and writes, audit, models, deployments, blocks, queues and dead letters)
  raise the new `LocalModeRefused` (a `ConfigError`); the generated HTTP routes answer 503 with
  `"reason": "local_mode"`. `llmq.admin.status`, `ops.index` (`GET /`), `describe`, `drain.*`,
  `registry.get` and `tenants.hash` answer from the process. Local mode reads its settings and secrets like queue mode. See
  [Queue and local modes](docs/how-to/modes.md), with the switch ladder from the queue to local mode.
  The concurrency cap, budgets and breaker probe are one per process, shared by the started
  engine and the sync calls' private engine. `stop()` and `drain()` wait for each call, not
  for the request handler that made it; only calls the shutdown cancels fail
  `worker_shutdown` (a caller's own cancel stays a cancel). A `fixed` window whose
  per-process share is below one unit a minute, or one estimate above it, carries what went
  over into the next minutes as a debt. A breaker probe that fails before the provider (the
  request cannot be built) ends the probe and gives its budget back.
- Configurable secrets (`llm_queue.secrets`): `LLMQ_SECRETS_SOURCE` is an ordered list of
  sources (`aws-secretsmanager:<secret-id>` with one JSON secret, `file:<path>` for a
  Kubernetes secret volume, a `.json` or a `.env` file, `env`, `none` by default) with
  `${NAME}` / `${NAME:-default}` expanded from the environment; `LLMQ_SECRETS_NAMES` (a JSON
  object, merged under `secrets_names=`) maps settings to the secret's own keys;
  `LLMQ_SECRETS_REGION`, `LLMQ_SECRETS_CACHE_SECS` (300; 0 reads on every lookup) and
  `LLMQ_SECRETS_TIMEOUT_SECS` (10). A source that cannot be read at start is a `ConfigError`
  naming it; a refresh that fails later keeps the last values (`secrets_refresh_failed`) and is
  tried again after min(cache, 30 s); on the event loop an expired source serves its last values
  while a thread reads it again, and the admin routes' token check reads it off the loop. The
  source serves every reader: settings, store passwords on each new connection, the admin
  token, `LLMQ_JOB_*`, account secrets, registry placeholders, the CLI and
  `AdminServices.from_env`. In code: `secrets_from_config(spec)`, `SecretReader` and the
  `SecretSource` protocol (whatever an app's own source raises is a failed read: a
  `ConfigError` naming it at start, the last values kept later). A setting named twice in
  `LLMQ_SECRETS_NAMES` is refused; names and keys are stripped in it and in `secrets_names=`.
  A `.env` file's inline comment is a `#` after whitespace; quoted values are kept as written. Precedence: `secrets=` in code, else `LLMQ_SECRETS_SOURCE`; then the
  environment; then `defaults=`. `describe()` gains `secrets` (source kinds and ids, mapped
  names, cache time; never a value), and `InfraSettings` the matching fields
  (`secrets_source`, `secret_sources`, `secrets_names`, ...). `InfraSettings.from_layers(layers)`
  reads built `Layers`, so a source is read once per `from_env`.
- Registry placeholders: the boot document (`LLMQ_REGISTRY` / `LLMQ_REGISTRY_FILE`) may hold
  `${NAME}` and `${NAME:-default}` in any string value, read from `secrets=` and then the
  environment; `$${` is a literal `${`. A string that is one whole placeholder and expands to
  a JSON list, object, number or boolean becomes that value. An unset name without a default
  stops the start, naming the variable and its JSON path. Expansion runs on the parsed
  document, so a value cannot break the JSON, and repeated keys are still refused. Stored
  documents and live edits are never expanded. A placeholder may not read a credential
  (`LLMQ_*_PASSWORD`, `LLMQ_*_TOKEN`, `LLMQ_ID_HASH_SALT`, `LLMQ_SECRET*`, `LLMQ_ACCOUNT_*`,
  the AWS key variables): the expanded document is stored and served by `GET /registry`. The
  names read are logged at start (`registry_placeholders`), never the values.
- YAML registry files: a `LLMQ_REGISTRY_FILE` ending `.yaml` / `.yml` is read as YAML (new
  extra `llm-queue[yaml]`), with repeated keys refused like in JSON.
- `residency.allowed_regions: "auto"`: every region the document's deployments use (and their
  profiles route to), recomputed on each load. The list form is unchanged.
- `quota_rpm`, `quota_tpm` and `budget_pct` (default 1; above 0 to 1): give a deployment's
  provider quota and the share to use; the budget is `floor(quota x budget_pct)`. Setting
  `rpm` and `quota_rpm` (or `tpm` and `quota_tpm`) on one level is refused, as is a share that
  rounds a quota down to 0. `ResolvedRegistry.quota()` returns the quota behind a budget.
- `llm_queue.fastapi.install(app, llmq=None, *, prefix="/internal/llm-queue",
  on_start_error="raise", drain_secs=None, mount_admin=True, context_middleware=True,
  context=None, **router_options)` (extra `http`): wraps the app's lifespan so the queue
  starts before the app's startup and stops after its shutdown, adds the request context
  middleware and mounts `http_router(**router_options)` under the prefix, with the drain
  routes when a token or `dependencies` guards them. `llmq=None` builds
  `LLMQueue.from_env()`. `on_start_error="log"` keeps the app up after a failed start
  (`llmq_start_failed`; queued calls fail at once, readiness false). The lower-level
  `serve(app, llmq)` (an async context manager for your own lifespan) and `mount(app, llmq)`
  are in the same module. See [Use with FastAPI](docs/how-to/fastapi.md).
- `llm_queue.RequestContextMiddleware(app, *, tenant_id=None, user_id=None, session_id=None,
  priority=None)` (`llm_queue.asgi`, pure ASGI, HTTP and websocket): each value a constant or
  a callable of the ASGI scope (sync or async).
- Background helpers: `llm_queue.background()` (`request_context(priority=False)`),
  `@llm_queue.background_priority` (sync and coroutine functions),
  `llm_queue.create_background_task(coro, *, name=None)` (keeps the task referenced until done;
  logs `background_task_failed`) and `llm_queue.run_in_thread(fn, *args, **kwargs)` (a daemon
  thread in a copy of the context; logs `background_thread_failed`). Each keeps the ids and
  drops the priority.
- `LLMQueue.ready`: sync readiness (started, registry loaded, not draining, and in strict mode
  no evicting store); `llm_queue.queue_ready(llmq=None)` for an app's own probe.
- `LLMQueue.from_env(defaults=...)` (D1): a third layer below `secrets=` and the environment,
  for the app's own `LLMQ_*` values (`True` / `False` read as switches; a name without
  `LLMQ_` is refused). The secret-then-environment order is unchanged. Conflict groups: the
  highest layer that sets `LLMQ_STORE_URL` or a store part picks that form, and the highest
  that sets `LLMQ_REGISTRY` or `LLMQ_REGISTRY_FILE` picks that one; the other form is ignored
  in every layer, and one layer with both is refused. `llm_queue.engine.settings.Layers` is the
  lookup; `InfraSettings.from_env` takes `defaults=` and `secrets_names=` too.
- `from_env(secrets_names={"LLMQ_STORE_HOST": "valkey_host", ...})` (H7): read a setting from a
  secret stored under another name; applies to the store passwords, `LLMQ_ADMIN_TOKEN`,
  `LLMQ_JOB_*` and account secrets too.
- The store as parts (D3): `LLMQ_STORE_HOST`, `LLMQ_STORE_PORT` (6379), `LLMQ_STORE_DB` (0),
  `LLMQ_STORE_TLS_ENABLED` (true) and `LLMQ_STORE_PASSWORD` (percent-escaped) build the URL,
  which gets the same strict TLS check as `LLMQ_STORE_URL`.
- `LLMQ_STORE_MAX_CONNECTIONS=auto` (D7): the boot registry's `worker_slots` summed plus
  `STORE_POOL_MARGIN` (10), within 4 to 100,000, computed once by `from_env`. A number works
  as before (default 200).
- Model factories (E1): `llm_queue.langchain.chat_model(model_id, *, registry=None, **kw)`
  and `embeddings(...)` return the queued LangChain class for a model the
  boot registry knows (by alias or model id; Bedrock or OpenAI-compatible adapter by the
  model's provider, `**kw` passed through), else None, so `chat_model(...) or <direct model>`
  keeps the app's direct path for a model outside the registry. `registry=` is a document,
  JSON text, a path, an `LLMQueue` or None: the started queue (its off switch and the boot
  registry its settings read from secrets, the environment and `defaults=`), else, before any
  queue has started, `LLMQ_ENABLED` and `LLMQ_REGISTRY` / `LLMQ_REGISTRY_FILE` from the
  environment (pass `registry=llmq` when a secret holds the switch). The registry is parsed once per queue, and before start once per file version
  (path, mtime, size): a call costs the adapter's construction only. No langchain module is
  imported for an unknown model.
  `aliases(registry)` maps model ids to aliases; `boot_registry(registry)` returns the parsed
  registry. See [Use LangChain models](docs/how-to/langchain.md).
- The boot registry is read once per process: `InfraSettings.registry_document()` returns the
  document `from_env` read, so a ConfigMap updated in place does not change what
  `describe()`, the admin services or the engines see until a restart.
- `LLMQueue.embed_one(model, text, **kw)` and `embed_one_sync(...)` (H8): one text's vector.
- Requests by model and tenant (T): every call in flight is indexed under its model and tenant
  hash (`P:{m:ALIAS}:tq:H`, `P:{m:ALIAS}:tq`) by the scripts that send and end it (no extra
  round trip; `LLMQ_REQUESTS_INDEX_ENABLED`, on). `RequestsAdmin` (`Engine.requests_admin`,
  `engine/requests_admin.py`) lists, counts and gets calls by model and tenant, and cancels,
  reprioritizes and extends them: dry run by default, audited with counts and the tenant hash
  only. Cancel defaults to waiting calls and answers them `cancelled` at once; reprioritize
  moves waiting messages to the other class's queue (`MOVE`) keeping their deadline and place
  (stream field `pos`), never a call a worker holds; extend moves one call's deadline in the
  records, the workers and the waiting callers. `status(model, tenant=...)` adds the tenant's
  calls by stage and fairness numbers; every model report adds `tenants` and `request_index`.
  `llmq_calls_total` and `llmq_wait_seconds` get a `tenant` label bounded by
  `LLMQ_METRICS_TENANTS`. New settings: `LLMQ_TENANT_NAMES_ENABLED`, `LLMQ_ADMIN_BATCH`,
  `LLMQ_ADMIN_SCAN_MAX`, `LLMQ_REQUESTS_LIST_MAX`, `LLMQ_REQUESTS_MOVE_BATCH`,
  `LLMQ_REQUESTS_MOVE_BYTES`, `LLMQ_METRICS_TENANTS`.
- Drain API (plan A1-A5). `pause_receiving()` stays sync and callable from any thread;
  `resume_receiving()` undoes it (ready again when started; `LLMQueueError` after `stop()`);
  `await drain(drain_secs=None)` pauses and waits for running calls and jobs (it does not
  stop the queue); `draining` / `draining_since` properties and `drain_status()`.
- `health()` reports `draining` and `draining_since`; a paused process answers
  `not_ready_reason="draining"` (before the eviction reason).
- `LLMQ_DRAIN_FILE` (default off) and `LLMQ_DRAIN_POLL_SECS` (1 s): a file whose mtime is
  newer than the queue's construction (minus 1 s) pauses every process of a pod, in every
  role; a resumed process pauses again only on a newer mtime. `llm_queue.drain.DrainWatcher`
  (also `llm_queue.DrainWatcher`) is the same watcher, standalone (no heavy imports) for apps
  without a queue.
- `http_router(drain_routes=True)`: `POST /drain`, `POST /resume` (guarded like the write
  routes: the admin token, or `dependencies` plus `X-LLMQ-Admin`) and `GET /drain` (the state, behind
  the read guard like every read route). They reach one process; multi-process pods use the
  drain file.
- `JobRunner.resume()`.
- `LLMQ_ADMIN_CSRF_HEADER` (`X-LLMQ-Admin`): the header a write needs when the write routes
  are guarded by `dependencies` only; `LLMQ_SYNC_STORE_MAX_CONNECTIONS` (20): the pool of the
  private engine that serves sync calls without a started queue (was fixed at 20).
- The off switch `LLMQ_ENABLED` (default `true`; 0.2.0 H1). `false` makes `from_env()` return a
  disabled queue (`llmq.enabled` False) that reads no other setting but `LLMQ_ENVIRONMENT` and
  the drain file: the factories return None (the app's direct path), direct calls, jobs,
  `status()` and admin store operations raise the new `QueueDisabled` (`config_error`, not
  retryable; HTTP 503 `reason: disabled`, CLI exit 1), `install()` mounts only the two probes
  (`enabled: false`), `health()` / `describe()` say so, `llmq_disabled` is logged once at start,
  and the drain file still fails readiness. The fifth step of the switch ladder
  (docs/how-to/modes.md).
- `LLMQ_CONFIG_ERROR_MODE` (`disable` by default, or `raise`): with `disable`, a `ConfigError`
  at `from_env()` (a bad setting, a secrets source that cannot be read) is logged once as
  `llmq_config_failed` (the problems, never a value) and a disabled queue is returned instead
  of raising: `health()` / `describe()` say `enabled: false`, `disabled_reason: config_error`
  (`switch` for `LLMQ_ENABLED=false`), the factories return None, `install()` mounts the probes
  only and the drain file still works. Layered like `LLMQ_ENABLED` (the secret wins); a
  secrets source that cannot be read is handled by the environment / `defaults=` value.
- Health summary (0.2.0 F2): each model in `status()` has `health` (`state` `healthy` /
  `warning` / `critical`, `reasons`, budget use this minute, waiting calls, oldest wait,
  usable deployments) and the report has the worst state as `health`; the new operation
  `status.health` serves the summary alone on every surface (`llmq.admin.status.health()`,
  `llm-queue health [--model M]`, `GET /status/health`), and `GET /` shows each model's
  state. Thresholds are the registry setting `health_summary` (defaults and per model, live
  editable): `budget_warning_pct` 80 / `budget_critical_pct` 98, `waiting_warning` 1000 /
  `waiting_critical` 10000, `wait_warning_secs` 30 / `wait_critical_secs` 120. Its gate is F2
  in `scripts/gates.py`.
- One `llmq_configured` info line at every `start()` with the log-safe `describe()` summary
  (0.2.0 D2): how the process is set up, with no password, salt, token, tenant name or
  registry body. A `describe()` that fails there logs `llmq_configured_failed` and the start
  goes on.
- Size-aware backfill (plan F1), off by default: registry `queue.backfill` per model
  (live-editable), else `LLMQ_QUEUE_BACKFILL_ENABLED`. A call may pass older waiters of its
  class when they would still be granted at their predicted time, so the rest of a `fixed`
  minute that a large waiting call cannot use serves smaller calls. Never past a waiting
  priority call, never over a budget, account budgets unchanged; off, admission is unchanged.
  Gate F1 in `scripts/gates.py`.

### Changed

- The admin drain writes follow the dry-run rule of every admin write:
  `llmq.admin.drain.start()` / `resume()` take `dry_run=True` by default, and `POST /drain` /
  `POST /resume` need the body `{"dry_run": false}` to act. A dry run changes nothing and writes
  no audit entry; it returns the current `drain_status()` with `dry_run: true` and
  `would_change`. A preStop hook that posts to `POST /drain` must send `{"dry_run": false}`.
  Unchanged: `LLMQ_DRAIN_FILE` and `LLMQueue.pause_receiving()` / `resume_receiving()` /
  `drain()` (the process API, not admin writes) act at once.
- In a strict environment with `LLMQ_BEDROCK_ENDPOINT_URL` set, the `botocore` Bedrock transport
  (`Tuning(bedrock=BedrockOptions(transport="botocore"))`) is refused at `from_env`: it follows
  redirects with the signed request (see [Secure](docs/how-to/secure.md)).
- In a strict environment `LLMQ_ADMIN_TOKEN` and `LLMQ_ADMIN_READ_TOKEN` (or the tokens passed
  to `http_router()`, a callable's value included) must be at least 32 characters: a shorter one
  stops the mount with a `ConfigError`, and a token rotated to a shorter value is refused per
  request.
- Account secrets (`api_key_ref`, `external_id_ref`) are looked up in the secrets and then the
  environment (with `secrets=` they were looked up in `secrets=` only).
- The `llm-queue` CLI and `AdminServices.from_env` read settings and
  store passwords through the same layers as `LLMQueue.from_env` (the CLI read the
  environment only).
- `LLMQ_STORE_PASSWORD` / `LLMQ_QUEUE_STORE_PASSWORD` set in the environment are used without
  `secrets=` (D3; 0.1.1 ignored them there and logged in with the URL's password, or none). They
  are still read on every new connection and win over a password in the URL.
- A missing store now reads `LLMQ_STORE_URL (or LLMQ_STORE_HOST) is required`.
- `LLMQ_ENABLED` and `LLMQ_CONFIG_ERROR_MODE` are read like every other setting: the secret
  first, then the environment, then `defaults=` (the secret wins, so one secret switches every
  pod; `LLMQ_SECRETS_NAMES` may map them). With no secrets source, `LLMQ_ENABLED=false` reads
  nothing else; with one, the source is read once at `from_env` even with the environment off.
  A source that cannot be read is handled by the environment / `defaults=`
  `LLMQ_CONFIG_ERROR_MODE` (default `disable`: a disabled queue, not a crash).
- `LLMQ_REGISTRY` in a secret and `LLMQ_REGISTRY_FILE` in the environment (or the other way)
  were refused as "both set"; the conflict group now lets the secret's form win.
- `STORE_POOL_MARGIN` moved from `llm_queue.core` to `llm_queue.engine.settings`; the internal
  `core._SecretFirst` mapping was replaced by `Layers`.
- Messages carry only keyed hashes of `tenant_id` and `user_id` (owner D1): stream bodies and
  dead letters no longer hold them in clear. Worker-side hooks (balancer, fallback policy,
  sticky key) see the hashes; the caller's admission hook still sees the raw ids. Tenant names
  in the boot registry's `fairness.weights` / `quotas` are replaced by their hashes when the
  file is read (D2), so only hashes reach the store. A live edit must name the hash.
- The admission class of a received call follows the queue it came from, not its body (a
  reprioritized call is served in its new class). Calls sent by an older pod are not indexed.
- `store_down: static_cap` runs on the local executor: the per-process budgets (rpm / tpm
  divided by `LLMQ_LOCAL_PROCESS_COUNT`), `LLMQ_LOCAL_CONCURRENCY` calls per model, retries,
  breakers and fallbacks, where it was one attempt per confirmed deployment with no budget.
  A `static_cap` call can now wait for a budget or a breaker and retry: the wait is capped by
  the new `LLMQ_STATIC_CAP_WAIT_SECS` (default 5 s, then `rate_limited`). Each process takes
  the model's rpm / tpm divided by `LLMQ_LOCAL_PROCESS_COUNT`, which queue mode never needed
  before and which defaults to 1 (the whole budget per process): set it to the processes that
  share the quota; start logs `static_cap_process_count_unset` when a model uses `static_cap`
  and it is left at 1. `Engine.stop()` drains `static_cap` calls with the others and ends
  those still running with `worker_shutdown` (it never waited for them).
  `LLMQ_LOCAL_LIMIT_CAP` is renamed `LLMQ_LOCAL_CONCURRENCY` (same default, 10), the one
  setting for local mode and `static_cap`.
  The `static_cap_attempt_failed` log event is now `local_attempt_failed` (field `path`).
- `llmq_calls_total` has a `path` label (`queue`, `direct`, `static_cap`, `local`,
  `dead_letter`); `health()` has `mode`.
- `breaker_probe_secs` left unset is now the larger of 60 and the deployment's
  `attempt_timeout_secs`, so a registry with an attempt timeout above 60 s and no probe time
  loads instead of being refused. An explicit value below the attempt timeout is still refused.
- `category` and `retryable` on every `LLMQueueError` (C1): `category` is one of `throttled`,
  `timeout`, `bad_request`, `unavailable`, `config_error`, `cancelled`, `internal`
  (`llm_queue.CATEGORIES`); `retryable` is True when a later retry by the caller can succeed
  (the queue already retried). A `CallFailed` takes both from its kind
  (`llm_queue.KIND_CATEGORIES`); `llm_queue.error_category(exc)` and
  `llm_queue.error_retryable(exc)` answer for any exception (`internal` / False for one that is
  not llm-queue's). Computed, so pickling is unchanged. See
  [Errors](docs/reference/errors.md#category-and-retryable).
- The provider's request id reaches the caller (C2): `Meta.provider_request_id` is Bedrock's
  `x-amzn-RequestId` or the OpenAI-compatible server's `x-request-id` of the attempt that
  answered, on plain and streamed calls (also in a stream's `meta_event=True` last event). The
  LangChain chat adapters add boto's `response_metadata["ResponseMetadata"]` (`RequestId`,
  `HTTPStatusCode` 200, `RetryAttempts` = attempts - 1; no `HTTPHeaders`), so code that reads
  the AWS request id from a direct `ChatBedrockConverse` reply reads it here too. fakellm
  sends both headers.
- The LangChain chat adapters put how the queue served a reply under
  `response_metadata["llmq"]` (the `Meta` dict) instead of flat keys
  (`response_metadata["deployment"]` is now `response_metadata["llmq"]["deployment"]`), on whole
  replies and on a `queue_streaming` stream's last chunk. `model_name`, `finish_reason` and
  `stopReason` stay where LangChain puts them.
- `pause_receiving()` now hands held calls (waiting for budget, not started) back to the
  queue at once, for another pod (`LLMQ_DRAIN_HANDBACK_ENABLED`, default true; false keeps
  0.1.1's behaviour). Messages a receive in flight took when the pause landed go back
  unadmitted, and one that would wait for budget is released instead of held. A one-replica
  deploy therefore waits for the new pod to run those calls.
- A registry account's `api_key_ref` / `external_id_ref` that names a process setting
  (`llm_queue.engine.settings.SETTING_NAMES`) is refused, whatever its prefix.
- Key expiry: the store's mode wins. The first engine sets `P:expiry`; afterwards only
  `swept` replaces `native`, and a process whose `LLMQ_KEY_EXPIRY_MODE` differs follows the store
  and logs `key_expiry_conflict` (pods with different settings flipped the mode every minute,
  leaving evictable keys on `volatile-*` stores). A change is announced, so every process's
  own writes follow it at once. To go back to `native`, stop every process and delete
  `P:expiry`.
- `cancel()` needs a registered request id: a cancel that lands before the call registers is
  dropped by the registration (documented; unchanged behaviour).
- A failed secrets refresh is retried after min(cache, 30 s) spread by +-20 %, so a fleet does
  not retry in step; a store login refused with the cached password re-reads the secrets at once
  (at most every 5 s), so a rotated `LLMQ_STORE_PASSWORD` needs no cache period.

### Removed

- The admin API's 0.1.x names. Every admin operation is now one service method under
  `llmq.admin`, and the `llm-queue` CLI and the HTTP routes are generated from one operations
  table, so the three agree (see [Admin API](docs/reference/admin-api.md)). Every write takes
  `actor`, is audited, and is a dry run by default on every surface. No old name is kept:

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

- `LLMQ_TENANT_INDEX` and its cross-model index `P:tenant:H:reqs` (with
  `PurgeService.index_call`): a tenant purge of `queued` / `running` calls reads the request
  index of each model. `LLMQ_PURGE_BATCH` (now `LLMQ_ADMIN_BATCH`).
- An `LLMQ_*` name no setting reads (a typo, or a renamed or removed setting such as
  `LLMQ_LOCAL_LIMIT_CAP`) is reported at start: a `ConfigError` naming it when strict, the
  `setting_unknown` warning in dev. Before, it was ignored and the default used.
- Support for custom providers without `converse_api`: `Provider.call` must take it (the
  documented contract); the worker no longer inspects each provider's signature.
- `LLMQueue.engine` and `LLMQueue.jobs` (deprecated since 0.1.0b2): see "Upgrading from 0.1.1".
- `PurgeService` and `RegistryStore` from `llm_queue.__all__`: `llmq.admin` is the one write
  path; `PurgeReport` and `PublishResult` stay as return types.
- The 0.1.0b1 alert names `quota_used_mins`, `spill_pct`, `spill_window_secs` and
  `waiting_age_secs`, accepted with a warning until now, are unknown fields (refused in strict
  mode).
- `describe()` / `GET /describe` / `llm-queue describe` no longer show `tenant_default` (the
  name, review B-04): they show `tenant_default_hash`, and the name only with
  `LLMQ_TENANT_NAMES_ENABLED=true`.
- `queue_ready(enabled, llmq=None)` loses `enabled`: it is `queue_ready(llmq=None)`. The app's own
  flag is replaced by `LLMQ_ENABLED=false`, whose queue is ready unless draining.

### Fixed

- `QueuedChatBedrockConverse` without `model_alias` (only `model_id`) treated every reply as a
  model fallback (C3): the served alias never equals the model id, so each reply paid a
  registry lookup to fill `model_name`. A fallback is now read from the engine's
  `Meta.fallback == "model"`.
- Admin HTTP bodies are parsed with orjson: a deeply nested body is a 400 (it was a 500 from
  `RecursionError`), and `NaN` / `Infinity` are refused. A non-finite number from a query
  (`float("nan")`) is a bad field too: `{"wait_secs": NaN}` disabled a model and returned at
  once.
- The `job` of `dlq.list` / `dlq.show` / `dlq.redrive` / `dlq.purge` is checked against the
  job-name rule (1-64 letters, digits, `_`, `-`) before it goes into a key: braces and colons
  reached the stream key (`ConfigError`, HTTP 400). A job name with a trailing newline is
  refused at registration too (the rule was matched with `$`).
- `requests.extend` reaches every place a deadline lives: a worker that takes the call after
  the extend gets its deadline from `PICK_TAKE` and no longer ends it at `max_wait_secs`; a
  running attempt's timer moves (it was cut at the old deadline and run again); `stream()`
  reads the new deadline and the chunks live past it. It never shortens a call (an extend
  below the call's own timeout is `already`), refuses a `timeout_secs` above the queue's
  `retention_secs` as well as `timeout_max_secs`, and needs the request index. An attempt's
  first-response budget is now `attempt_timeout_secs` alone; the call's deadline is the
  outer timer's.
- Request admin: a send short of `replica_wait_count` keeps its message id (`list` showed
  none and reprioritize skipped the call as `direct`); a separate queue store's moved calls
  have their records written again after a store error (`request_records_stale` when that
  keeps failing); a takeover under another tenant leaves the old tenant's index; lapsed
  tenants leave the names hash (`tqn`); `requests.count` with the index off is refused, not
  zeros; the no-tenant status block reads the fairness numbers fairness writes; a cancel logs
  `call_cancel` only for the calls it cancelled and answers waiting calls in one round trip per
  batch; a reprioritize stopped by a full target counts every call left in `skipped.full`;
  `requests.get` of a call whose model was removed returns its records, not a `KeyError`.
- Request admin, separate queue store: a reprioritize whose process stopped between `MOVE` and
  the record write (or whose record write kept failing, `request_records_stale`) left the
  calls' records on their old message, logged and never repaired: the call was listed in its
  old class and a later reprioritize skipped it as `gone`. `MOVE` now marks each move in the
  queue store (`P:{q:NAME}:moved`, kept for the queue's retention) and the next admin read of
  the model (`list`, `count`, `get`, cancel, reprioritize) writes the records and clears the
  marks (`request_records_repaired`, audited as `requests_reprioritize_repair`). The write is
  idempotent and never puts back an older message id (RQ-8).
- Tenant names in `fairness.weights` / `quotas` are hashed on every registry write path, not
  only when the boot file is read: `registry set fairness.weights.acme=3` stored the raw name,
  which never matched a call. A hashed block is marked `tenant_keys: hashed` (new registry
  field), so an exported document is not hashed twice; a 16-hex name in a block of names is
  refused. Job messages carry the hashes of the sender's ids (`ctx.user_hash` /
  `ctx.tenant_hash`), not the ids; the job's calls use them as they are
  (`llm_queue.ids.HashedId`).
- No tenant name on a read route: `describe()` shows `tenant_default_hash` (the name only with
  `LLMQ_TENANT_NAMES_ENABLED`); `GET /status`, `/requests` and `/requests/count` refuse
  `?tenant=` (400) and take `tenant_hash` (the CLI and Python still take a name and hash it
  locally); `POST /tenants/hash` needs the write guard, as with the hashes the reads show it
  would tell a read token which tenant a hash is.
- Every admin write checks its actor before it acts (1-128 characters, no control
  characters): dead letters, purges, unschedule, drains, model drains and request changes took
  any text before, and only registry writes checked it (at 200 characters, while HTTP cut at
  128). Every audit entry outside the registry's own script goes through one writer
  (`Audit.write`), which logs one `llmq_admin` line with a `kind`; purges also keep their
  `llmq_purge` line.
- `AdminServices.from_env`, `RequestsAdmin.from_env` and `PurgeService.from_env` take
  `defaults=` and `secrets_names=` like `LLMQueue.from_env`, and share one wiring
  (`AdminContext.from_layers(layers)`): an admin worker could not reproduce an app configured
  through `defaults=`.
- A late failure or cancel reply after a stored success no longer marks the call
  `failed`/`cancelled`: the next call with that app request id got `new`, deleted the success
  and ran again (double spend). A `done` call stays done; a direct run whose step after the
  stored result raises returns the success instead of the failure.
- A registration replayed by the store client after a lost reply answers its first outcome
  (`new`), not `attached` to its own record (a 6 s wait before sending, and a call missing from
  the tenant index). A replayed end-of-call script logs `settle_replayed` instead of
  `ack_stale_receipt` and the `llmq_stale_acks_total` / grant-lost double-run alarms.
- `LLMQueue.stop()` stops receiving calls and jobs first, then drains: calls taken while jobs
  drained got only the rest of the drain (streams failed `worker_shutdown`, others ran again
  elsewhere). This is also `fastapi.install()`'s shutdown.
- `resume_receiving()` is thread-safe, as `pause_receiving()` is.
- The health safeguard "never disable a model's last usable deployment" counts only
  deployments that take calls (enabled, account enabled, not blocked, inside residency), in
  local mode too (FA-3: local mode's own check counted every deployment of the model).
- A cancel that lands while a call's admission is in flight gives the grant back at once
  (slot and budget); its slot was held up to a lease and its budget never refunded.
- A pause also hands back calls waiting between two attempts (retry backoff); they kept the
  drain open until their deadline.
- A retry after a failed attempt on a fallback model asks the call's own model first, then
  the fallback rule again; it stayed on the fallback model for good.
- Admin HTTP: a dead letter id that is not `<ms>-<seq>` and any request the store refuses
  answer 400 (500 with a traceback before); a POST body above `LLMQ_ADMIN_BODY_MAX_BYTES`
  (new, default 1 MiB) answers 413, a body value above 64 KiB 400.
- Request admin at scale (gate G-T2, 10,000 waiting calls of 20 tenants): `list` read three
  commands per call in a pipeline and every tenant's next page; it is now one script that
  merges the tenants' indexes, then one script for the records and one for the stages
  (100 rows: 47 to 13 ms). Scans (`count`, `cancel`, reprioritize) send all their record and
  stage scripts in one round trip, and a cancel marks its calls and answers the waiting ones
  (result, status `cancelled`) in one script per `LLMQ_ADMIN_BATCH` ids, all in one round
  trip (it was a mark, a re-read and one result script per id), then writes its audit entry
  and wakes the callers last (a failed wake logs `request_wake_failed`; the callers then find
  their results by the result sweep). The record, cancel and wake scripts
  build their keys from the ids (five keys per id cost more to pack than to run): cancelling
  10,000 took 9.6 s, now 0.9 s; reprioritizing 1,000 1.9 s, now 0.8 s. Gate G-T2 waits out its callers' first result reads before it measures
  and times with `perf_counter` (Windows' `monotonic` ticks every 15.6 ms).
- A smooth window whose bucket holds one call (`rpm x rate_burst` below 2: rpm 9 to 19 at the
  default `rate_burst` 0.1, rpm 900 at 0.001) granted nothing for about half of all limits:
  the GCRA check summed `tat + cost x per - now - burst x per`, where `now + per` rounds on
  13-digit milliseconds, and a call that exactly fills the bucket came out a few 1e-5 ms over,
  so every call ended `rate_limited` at `max_wait_secs`. The check now subtracts `now` first
  (exact for an idle bucket), in the admission script, the tenant share clock and local mode.
  Budgets stay exact and a waiting priority call still goes first.
- `requests list` of a model without a tenant (HTTP `GET /requests`, CLI `requests list`) no
  longer holds the state store for seconds when the model has many tenants (FA-1): its one
  script read every tenant's index and scanned every tenant once per row (10,000 tenants, a
  page of 500: 0.75 to 1.4 s of script, every worker's admission waiting behind it). The merge
  is a heap, a model with more than 256 tenants is merged from runs of 256 tenants (each reads
  only entries up to the page's last deadline so far), and the tenant index is read 1,000
  hashes at a time; each run now takes about 5 ms (12 ms at most). Order and cursors are
  unchanged.
- An app request id sent again after an extended call ended runs with its own status (FA-2):
  the end of a call set its job record's TTL to the status TTL alone, below the status's TTL
  the extend had grown, so the job expired first and the next registration kept the old
  final status (the new run's status writes refused; `list`, `count`, cancel and
  reprioritize took the call for finished). The final write now keeps the job as long as its
  status, and a registration that finds no job record clears an earlier run's final status,
  result and stream chunks.
- A tenant purge (`keys`) removes the tenant's name from `P:{m:ALIAS}:tqn` (FA-5): with
  `LLMQ_TENANT_NAMES_ENABLED` the raw name stayed until a later call on the model pruned
  lapsed tenants, which with ongoing traffic could be never.
- A tenant purge of `queued` (or `running`) calls answers the waiting ones `cancelled` at
  once, as `requests cancel` does (FA-4): it only set cancel marks, so their callers waited
  until a worker reached the message (minutes in a deep queue) or their deadline. Its
  `cancelled` count and `llmq_cancels_total` count the calls it marked, not those it
  selected (a call that ended meanwhile was counted).
- `fair_queue`: a grant out of round order (backfill, a call that can use other
  deployments, a sticky session) moved the round served to its own, so a newcomer's next
  call got a round behind every lower round still waiting (FA-6: rounds 2 and 3 held, a
  grant at 4, the newcomer got 5). The round served now moves no further than the lowest
  round still waiting (the newcomer gets 3).
- A provider throttle gave its request back to the budget, in queue mode and local mode, so
  the call was sent again in the same minute although the provider had counted the 429. At a
  minute boundary (a `fixed` window's new share meeting the provider's sliding window) 4
  local-mode processes of 150 sent 640 calls in one wall-clock minute against an rpm of 600
  (gate BL1). A throttled attempt now keeps its request charged and gets its tokens back, in
  both modes; a call that never reached the provider still gets everything back.
- A live registry edit (`admin.registry.set` and the other registry writes) during a store
  outage raised redis-py's `ConnectionError` instead of `StoreUnavailable` (scenario X2): the
  read of the document before the edit was not guarded. It raises `StoreUnavailable` like
  every other admin operation.

## [0.1.1] - 2026-11-17

The first release that installs without `--pre`: `pip install llm-queue` (or
`llm-queue==0.1.1`). The code is 0.1.0b3's plus the changes below; the 0.1.0b3 upgrade notes
apply when coming from 0.1.0b2 or earlier.

### Added

- `QueuedBedrockEmbeddings(query_input_type=..., document_input_type=...)`: the Cohere
  `input_type` of queries and documents (defaults `search_query` / `search_document`, as
  before). An index whose documents were embedded with `embed_query` keeps matching with
  `document_input_type="search_query"`; before, `embed_documents` always sent
  `search_document` and changed the vectors.

### Fixed

- A worker-only process (`LLMQ_MODE=worker`) that makes calls itself, such as a job that
  embeds, ran no result reader: each call waited for the `poll_secs` safety net (5 s) before
  it saw its result. The result reader now runs in every role.

### Tooling

- Manual tools in `scripts/`, tested by `tests/unit/scripts/test_release_tools.py`:
  `check_docs.py` (relative links, anchors and the nav; now part of `check.py`'s lint step),
  `smoke_install.py` (a clean install from PyPI, TestPyPI or a wheel), `watch_release.py` (a
  release's publish run and PyPI, without `gh`) and `store_keys_page.py` (one searchable page of
  every store key). CONTRIBUTING lists them under "Manual tools".

### Release checks

All checks passed: lint, types, docs links, unit, integration (native and swept key expiry:
845), coverage, failover, load, 48 of 48 scenarios, samples, and gates except B91. B91 (admission
script p99 <= 2 ms at 20 models x 30 deployments) is not judged on the Windows / Docker Desktop
dev machine: 0.1.1 measured 2.79-3.26 ms and 0.1.0b2 2.83-3.16 ms there, back to back, so no
regression; the 2 ms target stands for Linux reference runs.

### Documentation

- In-depth architecture, call flows for every path and failure mode, and the list of tested
  scenarios, gates and drills with what each proves.

## [0.1.0b3] - 2026-11-16

Key expiry modes, so llm-queue can share a `volatile-*` cache store (ElastiCache's default
`volatile-lru`), and a purge service for removed models, deployments and tenants.

### Upgrading from 0.1.0b2

The message format and the key layout are unchanged, so 0.1.0b2 and 0.1.0b3 pods can share a
queue on a `noeviction` store. [Upgrade](https://techiewonk.github.io/llm-queue/how-to/upgrade/)
has the same list with more detail.

1. **Upgrade every pod.** 0.1.0b2 pods do not read the store's key expiry mode (`P:expiry`)
   and always write native TTLs. On a `noeviction` store `auto` picks `native`, the same as
   0.1.0b2, so a mixed fleet is fine there during a rolling upgrade. On a `volatile-*` store a
   0.1.0b2 pod is not ready in strict mode anyway, and its keys stay evictable: finish the
   rollout before you rely on the store being shared.
2. **Switching modes loses nothing.** In `swept` mode setting an expiry also clears the key's
   native TTL (`PERSIST`); in `native` mode it also removes the key's `P:ttl` member; reading
   the time left counts either. Keys move to the new mode on their next write; a key never
   written again keeps its old expiry and still goes then.
3. **Rolling back to 0.1.0b2 after running `swept`.** 0.1.0b2 has no expiry sweep, so a key
   whose expiry is only in `P:ttl` would never expire under it. Before rolling back, set
   `LLMQ_KEY_EXPIRY=native` on the 0.1.0b3 pods (the store must be `noeviction` for 0.1.0b2
   to be ready in strict mode) and keep them running for at least the longest key lifetime (1
   day by default; longer if you raised a retention or TTL setting). In `native` mode each write
   moves its key back to a native TTL, and the index sweep keeps running and deletes the
   indexed keys as they come due. Roll back when `ZCARD <prefix>:ttl` is about 0 on each store
   (a separate queue store has its own). Leftover `P:expiry`, `P:ttl` and `P:purge` keys are
   harmless to 0.1.0b2.
4. **`llm-queue registry gc` output and scope.** It prints the purge report: `deleted_keys` and
   `dry_run` instead of `would_delete`. It also purges removed deployments of live models, and
   keeps a removed model's queue that still holds messages unless `--force`. Update scripts that
   parse its output.
5. **Auto purge is on by default.** When a publish drops a model or deployment, its store state
   is purged after `LLMQ_PURGE_GRACE_SECS` (900). Set `LLMQ_AUTO_PURGE=false` to keep the
   0.1.0b2 behaviour (state stays until you purge it by hand).

### Changed (behaviour)

- **Auto purge on by default** (`LLMQ_AUTO_PURGE=true`): a model or deployment dropped from
  the registry has its store state deleted after the grace (900 s) unless it comes back.
  0.1.0b2 kept that state until `registry gc`. `LLMQ_AUTO_PURGE=false` restores that.
- **`auto` key expiry picks `swept` on a `volatile-*` store** (and on `allkeys-*` or an
  unreadable policy): llm-queue's keys get no TTL and the sweep deletes them, up to 10 s late.
  0.1.0b2 always set native TTLs. On `noeviction` nothing changes. `LLMQ_KEY_EXPIRY=native`
  restores the 0.1.0b2 behaviour (and leaves a `volatile-*` store not ready in strict mode).
- **`registry gc`** also purges removed deployments of live models, keeps non-empty queues
  unless `--force`, and prints the purge report (`deleted_keys`, `dry_run`) instead of
  `would_delete`.

### Added

- Docs: a reference page listing every log event the library writes, with its level and
  module (`docs/reference/log-events.md`, R3OB-9).
- `llm-queue registry schema` prints the registry document's JSON Schema (draft 2020-12),
  generated from the same checks that validate a document; it needs no store and no `LLMQ_*`
  settings. The schema is published as `docs/reference/registry-schema.json`, next to a new
  reference page on the document's shape with a complete example (`docs/reference/registry.md`).
- `LLMQueue.http_router(metrics_route=True)` serves `GET /metrics` (off by default); the option
  existed only on `status_router(engine, ...)`, which needs the now private engine.
- Purge controls (`docs/how-to/purge.md`): `PurgeService` (`llmq.purge`, or
  `PurgeService.from_env()` standalone) deletes the store state of a removed model
  (`model`), a removed deployment (`deployment`, waiting for its running calls first), a tenant
  (`tenant`, fairness keys with the weight sum corrected; optionally cancelling its queued or
  running calls) or everything the registry no longer has (`removed`), with dry runs
  (`plan_*`). Each returns a `PurgeReport`; real purges are audited (`purge_<kind>` in the
  registry audit), logged `llmq_purge` and counted `llmq_purged_keys_total{kind}`. New error
  `PurgeRefused` (a model or deployment still in the registry without `force`; a tenant purge of
  calls without the tenant index).
- Auto purge: when a publish drops a model or deployment, it is purged after
  `LLMQ_PURGE_GRACE_SECS` (900) unless it came back meanwhile (`LLMQ_AUTO_PURGE`, default on);
  `purge.scheduled()` / `cancel_scheduled()` list and keep pending ones.
- Auto purge runs only in worker processes. A process's first tick after start only records
  the registry it sees (a baseline), so a start never schedules a purge. Before it purges, it
  reads the stored registry strictly and skips the purge on any read error or refusal. An auto
  deployment purge does not wait for running calls: the grace (900 s) is as long as the longest
  call the registry allows (`timeout_max_secs`, 900 s by default).
- CLI `llm-queue purge model|deployment|tenant --yes|--dry-run`; `--no-wait` on `purge
  deployment` and `registry gc`. HTTP routes `POST /purge/model`, `/purge/deployment`,
  `/purge/tenant`, `/registry/gc`, `GET /purge/scheduled`, `POST /purge/scheduled/cancel` (only
  with `allow_writes=True`, `X-LLMQ-Admin`; `dry_run` defaults to true). An HTTP purge waits for
  running calls at most `LLMQ_HTTP_PURGE_WAIT_SECS` (60, 0 to 3600); a larger or omitted
  `wait_secs` is capped to it.
- Settings `LLMQ_AUTO_PURGE`, `LLMQ_PURGE_GRACE_SECS`, `LLMQ_PURGE_WAIT`,
  `LLMQ_PURGE_WAIT_SECS`, `LLMQ_HTTP_PURGE_WAIT_SECS`, `LLMQ_PURGE_BATCH`, `LLMQ_TENANT_PURGE_DEFAULT` and
  `LLMQ_TENANT_INDEX` (off: the per-tenant call index costs one write per call).
- Key expiry modes (`LLMQ_KEY_EXPIRY=auto|native|swept`, default `auto`): llm-queue can share a
  store whose `maxmemory-policy` is `volatile-*` (ElastiCache's default `volatile-lru`, an app
  cache). In `swept` no key the library needs has a TTL: each expiry is a member of the zset
  `P:ttl` and the sweeper deletes due keys (up to `sweep_secs`, 10 s, late), so a `volatile-*`
  policy cannot evict them. `auto` picks `native` on `noeviction` and `swept` otherwise. The mode
  is the store's (`P:expiry`; missing means `native`), written at start, every minute and after
  a store reset. New settings `LLMQ_TTL_SWEEP_BATCH` (500) and `LLMQ_TTL_SWEEP_ROUNDS` (100);
  `health()["key_expiry"]`; gauge `llmq_key_expiry_swept{store}`; counter
  `llmq_ttl_swept_total`; log events `key_expiry_changed`, `key_expiry_publish_failed`
  (warning), `ttl_sweep_failed`. Until a process has published its mode it reports the mode its
  setting asks for (`native` for `auto`). How-to: `docs/how-to/shared-cache-store.md`.
- Switching key expiry modes loses nothing: in `swept` mode setting an expiry also clears the
  key's native TTL (`PERSIST`), in `native` mode it also removes the key's `P:ttl` member, and
  reading the time left counts either; keys move to the new mode on their next write.
- The expiry sweep has its own lock, `P:lock:ttlsweep` (a native TTL on purpose), and runs in
  every process, workers and callers, every `sweep_secs` (10 s): up to 500 x 100 = 50000 keys
  per 10 s by default. The worker sweep keeps `P:lock:sweep`.
- `scripts/evict_drill.py`: an eviction drill that fills a throwaway store under queue traffic
  with `volatile-lru` and `noeviction` (see CONTRIBUTING.md).
- Docs: a reference page listing every key, stream and channel the library keeps in the store,
  with its fields, writers, readers, lifetime in each key expiry mode, what is lost if it is
  evicted under each policy, and code references (`docs/reference/store-keys.md`, with a JSON
  copy `store-keys.json`).

### Changed

- Readiness and the start check accept a `volatile-*` store when key expiry is `swept`
  (`eviction_warning(policy, mode)`): in strict mode such a process is now ready.
  `allkeys-*` still fails readiness in either mode, and `volatile-*` with `native` still does.
  `llmq_store_eviction_unsafe` follows the same rule.
- Docs: the key table in `docs/explanation/storage.md` was out of date and is corrected: the
  limiter, breaker, leases and in-progress keys have sliding TTLs (1 day; `max(budget_key_ms,
  2 x lease)`), rotation slides 1 day, waiter keys slide 180 s (a waiter counts for 2 s), the
  `health` hash has a 1-day TTL only after a latency sample, the breaker's `fail_at`, `thr_n`,
  `thr_until` fields, the tenant fairness, receive claim, offload, job and reset keys, the
  caller TTL (45 s by default, not 30), lock lifetimes (10 s, 5 / 35 s, 45 s), stream and
  dead-letter trimming, the audit's `MAXLEN`, what `PUBLISH` deletes, the result's TTL after
  its first read (cut to 2 s, not deleted), the request record's lifetimes, what the queue
  store holds, and readiness in strict mode. New section "Key expiry modes"; the store
  requirements now read `noeviction` **or** `volatile-*` with `swept`.

## [0.1.0b2] - 2026-11-15

Fixes from three code reviews and a six-part rescan of 0.1.0b1, a process-level client hub, and
behaviour decisions for production. No backward compatibility is kept with 0.1.0b1 (beta).

### Upgrading from 0.1.0b1

Every change an app pinned to 0.1.0b1 may need, as a checklist; each item is described in full
below, and [Upgrade from 0.1.0b1](https://techiewonk.github.io/llm-queue/how-to/upgrade/) has the same list with tables.

0. **Python 3.12 or newer.** Python 3.11 is no longer supported (`requires-python >=3.12`):
   CI tests 3.12, 3.13 and 3.14. The integration suite hung on 3.11 only, and 3.12 lets the
   code use PEP 695 generics.
1. **Drain every 0.1.0b1 worker before the first 0.1.0b2 pod starts.** Mixed fleets are not
   supported (owner Q32): a message with bytes is written in a form 0.1.0b1 does not decode.
2. **Environment:** set `LLMQ_ENVIRONMENT=dev` where checks should be relaxed (unset is strict);
   in strict mode set `LLMQ_KEY_PREFIX`, an `LLMQ_ID_HASH_SALT` of 16 characters or more, and
   `https` endpoint overrides; bare host names in `LLMQ_BASE_URL_ALLOWED_HOSTS`; fix any
   `LLMQ_*` number the start-up error says is out of range.
3. **Registry:** rename secret references to `LLMQ_ACCOUNT_*`; fix numbers, names and queue
   names the load error lists (bounds, no trailing newline, no `jobs-` queue names); set
   `queue.retention_secs` where calls wait longer than a day; delete the four removed alert
   names (they warn until 0.2); keep `inference.embedding_types` at `["float"]`; set
   `family` on a Bedrock embedding model whose id names neither `titan-embed` nor
   `cohere.embed` (an ARN, or a family the app registers).
4. **Code:** await calls on the queue's own loop instead of using `*_sync` there (Q1); catch
   `request_too_large` for oversize messages and add `invalid_request`,
   `context_length_exceeded` and `worker_shutdown` to kind tables (`llm_queue.Kind`); register
   every admission hook the registry names in every process (a missing one now refuses calls);
   use `dataclasses.replace` on the options classes; stop using `LLMQueue.engine` / `.jobs`.
5. **Operations:** keep the liveness probe's `failureThreshold` at 3 or more (`/health/live`
   can now answer 503); set `noeviction` (strict readiness fails otherwise); answer the preStop
   `/drain` route on GET; add `--yes` to `dlq purge`, `dlq redrive`, `drain`, `registry gc` and
   `cancel` in scripts and check the new exit codes; retry registry writes on 409.
6. **OpenAI transport and LangChain:** `retry_budget_exhausted`, `provider_error`,
   `no_deployment` and `worker_shutdown` are now 5xx (`InternalServerError`, not
   `RateLimitError` / `BadRequestError`); the adapters refuse the Responses API, and stream
   tokens only with `queue_streaming=True` (off by default).

### Changed (behaviour)

- **Metric renames and EMF format** (R3OB-12, R3OB-14, R3PF-14): `llmq_provider_latency_ms` is
  now `llmq_provider_latency_seconds` (base units). EMF points are added up per label set and
  written as one line per label set about once a second (counters summed, gauges last value,
  samples as a value array) instead of one line per point; `EmfSink(flush_secs=0)` keeps one
  line per point. Every EMF line from `LLMQueue.from_env` has the `environment` dimension and the
  plain fields `pod` and `version`, so CloudWatch alarms on the old dimension sets need the
  `environment` dimension added. `llmq_throttles_1m` and the status field `throttles_1m` are now
  the larger of the last full minute and this one (`throttles_this_minute` is the raw count).
- **The alert lock is kept by its holder** (R3OB-1): it lasts 45 s and the holder renews it
  each 30 s tick, so one process evaluates alerts until it stops; every other process removes
  the cluster gauges (queues, budgets, breakers, `llmq_alert_*`) from its own Prometheus
  registry, and series of removed models or deployments go away.
- **Quota alerts are steady** (R3OB-6): `quota_near_limit` and `quota_above_observed` turn on
  at once and off after two clear samples in a row; they flipped at each minute rollover.
- **Outage log lines are rate-limited** (R3OB-7): `receive_failed`, `heartbeat_failed`,
  `store_down`, `hold_failed`, `run_failed` and `model_unavailable` go out at most once per
  event and key (model or step) every 10 s, the next one with `suppressed=<n>`.
- **`llmq_call_finished` for every call and more fields** (R3OB-5, R3OB-8): also static-cap and
  dead-lettered calls (`path`); failed calls carry their last deployment, account, region and
  attempts (they were `-` / None); new fields `account`, `region`, `worker`, `fallback`,
  `path`, `admit_wait_ms`, `duration_ms` and `error`. `account_credentials_failed` carries
  `model`, `request_id`, `error_type` and `error`; `hold_lost`, `hold_failed` and `run_failed`
  carry `model`. `/health/live` answers `{"live": false, "reason": ...}` when it fails.
- **Mixed 0.1.0b1 / 0.1.0b2 fleets are not supported** (owner Q32, R3AP-8): drain the 0.1.0b1
  workers before starting 0.1.0b2. Bytes in a message travel as `{"__llmq_b64__": ...}` (was
  `{"__b64__": ...}`, which also decoded a caller's own dict of that shape and failed the
  message on bad base64: FO-25); invalid base64 is now left as data. The format version stays 2.
- **`GET /health/live` can answer 503** (R3AP-7): when a background loop died or the heartbeat
  stalled for a lease (0.1.0b1 always answered 200), so a Kubernetes liveness probe restarts
  such a pod; keep its `failureThreshold` at 3 or more. It now reads the process's own state
  only, never the store (it probed the store inline, up to 3 s: FO-9), and stays 200 once
  `stop()` begins.
- **The OpenAI transport's status codes changed** (R3AP-6; was listed under Added):
  `retry_budget_exhausted` 429 -> 503, `dead_lettered` 500 -> 502, `provider_error` 400 -> 502,
  `no_deployment` and `worker_shutdown` 400 -> 503, `rejected` 400 -> 403,
  `request_id_conflict` 400 -> 409, `result_expired` 400 -> 410, a kind with no row 400 -> 500.
  The OpenAI SDK raises `InternalServerError` for the 5xx ones (was `RateLimitError` /
  `BadRequestError`), so LangChain `with_retry` and fallbacks keyed on those classes change.
- **A failing or unregistered admission hook refuses the call** (`rejected`, FO-4): it admitted
  the call and logged once per process, so a tenant or entitlement gate was silently lost. Every
  failure counts `llmq_hook_failed_total{kind, hook}`.
- **`inference_limits.max_tokens` caps `max_completion_tokens` too** (FO-2), reads whole floats
  and numeric strings (`100000.0`, `"100000"`) and refuses anything else as `invalid_request`
  (FO-3); a lock on `max_tokens` drops `max_completion_tokens`. A capped call that sets no
  `max_tokens` now carries the cap, so the provider's own default no longer applies and the call
  reserves the cap (not `default_output_tokens`) against TPM.
- **A message above `max_message_bytes` fails `request_too_large`** (owner Q38), like a call
  above the token budget; it was `invalid_request`. The message says which setting to raise.
- **Registry numbers are bounded** (owner Q42, R3CF-1, R3CF-2, R3CF-3): `NaN` and infinities
  are refused (also in JSON text, where they passed every range check and were stored as
  `null`); every number is at most 2**53 - 1 (`rpm: 2**64` failed boot with a raw `TypeError`),
  every timeout and wait at most one day, and tighter limits apply per setting
  (`retry_multiplier` and `retry_spread` at most 100, `max_attempts` 100, `max_concurrency`
  100000, `max_receives` 1000, `embed_batch_size` 4096, ...); `registry_version` too.
- **Registry names refuse a trailing newline** (R3CF-4: `$` matched before one, so `"acc\n"`
  became a store key); aliases and account names are at most 64 characters, hook names 1-64;
  an explicit `queue.name` is 1-71 letters, digits, `_`, `.`, `-` and may not start with
  `jobs-` (R3CF-5). A padded `model_id` / `profile_id` and a `role_arn` that is not an IAM role
  ARN are refused (R3CF-16).
- **A stored registry is read leniently inside objects too** (R3CF-8): an unknown sub-field
  (`health.new_field`) from a newer pod made every older pod refuse the whole document; it is
  now dropped with a warning. The process's own config stays strict.
- **The removed alert names load with a deprecation warning until 0.2** (owner Q34, R3AP-5): a
  0.1.0b1 document naming `quota_used_mins`, `spill_pct`, `spill_window_secs` or
  `waiting_age_secs` stopped the pod from starting.
- **The registry's `queue.retention_secs` defaults to one day** (owner Q27; was four days).
- **Process settings have ranges** (R3CF-14): `LLMQ_CFG_RELOAD_SECS` 1-3600,
  `LLMQ_STORE_MAX_CONNECTIONS` 4-100000, `LLMQ_STATUS_TTL_SECS` at least 1,
  `LLMQ_QUEUE_WAIT_REPLICAS` at most 16, TTLs within 2147483647 s, and others listed in
  `docs/reference/environment.md`; an empty value counts as unset; `LLMQ_DRAIN_SECS=0` is valid.
  Whole-number variables say "must be a whole number" for `200.0` (R3CF-17).
- **Strict mode wants an `LLMQ_ID_HASH_SALT` of at least 16 characters** (FO-13): a
  one-character key does not stop brute-forcing hashed phone numbers.
- **`LLMQ_KEY_PREFIX` and `LLMQ_QUEUE_PREFIX` are plain names** (FO-20): letters, digits, `_`,
  `.`, `-`, and `:` in the key prefix (`registry gc` split keys on `{m:` / `{q:` and mis-attributed them). Entries of
  `LLMQ_BASE_URL_ALLOWED_HOSTS` with a scheme, port or path are refused by name (R3CF-13).
- **Registry checks added** (R3CF-10, R3CF-12): a fallback `after_secs` that could never fire
  inside `timeout_secs` is an error; the weight-0 check covers `balance_priority` and counts
  usable deployments only. `LLMQ_RESULT_TTL_SECS` below `Tuning.engine.result_recheck_secs`
  is a `ConfigError` (a caller that missed its wake-up never read its result), and `start()`
  warns (`config_cross_check`) when a model's `attempt_timeout_secs` is above its client's
  read timeout in `Tuning` or its call timeout above `LLMQ_STATUS_TTL_SECS`. Cross-field messages show values and levels and use the dotted
  override path `settings set` takes (R3CF-17).
- **A fallback model of another provider is allowed and converted** (owner Q40, R3CF-6): the
  changelog and docs said it was refused; it never was, and the caller's request reached the
  other provider unchanged (a Converse body at an OpenAI server) with the reply in the other
  shape. Requests and replies are now converted between Converse and OpenAI chat (messages
  with text, images, tool use and results; system prompt; tools and tool choice; output and
  sampling settings; stop reason and usage), by `llm_queue.engine.shapes`. A request with a
  part the other shape cannot carry skips that target; streams do not fall back across shapes.
  The validator still warns where such a target is configured. A disabled fallback target and
  unknown `blocked_regions` stay warnings.
- **Admin HTTP status codes** (FO-6, FO-7, FO-17): a write that lost the compare-and-set race
  answers 409 with `Retry-After` (was 400); a route hit before the registry loaded answers 503
  (was a 500); only an unknown model is a 404 (any internal `KeyError` was).
- **CLI exit codes** (FO-8): `llm-queue request ID` exits 1 for an unknown or expired id and
  `drain --wait` exits 3 when it times out (both exited 0). The OS user is looked up only for
  writes and falls back to `uid:<n>` (FO-5: it failed every command in a container with no
  passwd entry).
- **LangChain adapters** (R3AP-13, R3AP-14): `QueuedChatOpenAI` turns the Responses API off and
  refuses `use_responses_api=True` (it posted to `/responses`, which answered 404); every adapter
  checks `llmq=` against `QueueClient` at construction, and `allow_fallback` is a `Literal`.
- **`RegistryStore.boot()`** audits a boot publish as `boot@<host>:<pid>` when no `actor` is
  given (was `boot`) (R3AP-17).
- `Tuning.problems()` has upper bounds and pair checks (R3CF-15): times at most a day,
  `job_ttl_ms` at least 60000, `registry.history_size` at least 2, `store.retries` at most 100,
  `warm.refresh_secs` below `warm.keepalive_secs`, `openai.max_keepalive_connections` at most
  `openai.max_connections`, read timeouts at least 1 s.

- **Jobs retry for about an hour** (Q14): a failed job waited a fixed 5 s and was dead-lettered
  after 5 tries (about 20 s into any outage). Now 5 s doubling to 5 min until about an hour has
  passed, at least the failing call's `retry_after_secs`; dead jobs are kept 7 days, not 1
  (Q15). A job returned at shutdown 5 times is dead-lettered (Q17). Job LLM calls run at normal
  priority with the sender's user and tenant ids, not the context of `start()` (Q18).
- **`register_job` refuses** a function that is not async (it ran, then failed five times:
  R3JE-5), a call after `start()` or in `caller` mode (the jobs waited unseen: R3JE-6), and a
  name that is not 1-64 letters, digits, `_` or `-` (R3JE-12).
- **`send_job(job_id=X)` is deduplicated for 24 h** and returns `X` (Q13, R3JE-2); a payload
  above 1 MB fails `request_too_large` and one that is not JSON `invalid_request` (it raised a
  bare `TypeError`: R3JE-10, Q16). Jobs run 4 per name and 32 per process at most (Q16).
- **`embed` checks its input first**: a `str` instead of a list was sent letter by letter and a
  `None` raised `AttributeError` (R3JE-19); a text over the model's length limit (Q20) and an
  unknown Cohere `input_type` (R3JE-24) fail `invalid_request` with the text's index. A batch
  the provider refuses for its content is split until only the bad text fails (Q19, R3JE-17).
  Vectors of different lengths in one call fail `provider_error` (R3JE-23).
- **Registry: `inference.embedding_types` takes only `["float"]`** (`["int8"]` was paid for,
  then unreadable; with `["float", "int8"]` the int8 vectors were dropped: R3JE-22).
- **`S3Offload` and `offload_bytes` go together** (`ConfigError` otherwise; one alone turned
  nothing on: R3JE-27). A process without the offload refuses to start when the store says the
  fleet offloads (Q24). Object names are a random id under the prefix (they held a hash of the
  request id: R3JE-33); `prefix` / `key_prefix` are normalised to one trailing `/` (R3JE-31).
- **A request id runs again after a failed end** (owner decision Q2): REGISTER and REGISTER_SEND
  answered `done` with the old failure for an id whose run ended failed, expired or cancelled.
  Only a success is replayed now; such an id registers as new and its old result, status, cancel
  mark and chunk stream are dropped in the same script.
- **Queue retention defaults to 1 day** (Q27): `QueueSettings.retention_secs` and the redrive
  default were 4 days, and so was the registry's `queue.retention_secs`.
- `StreamQueues.depth` counts as held every message whose visibility deadline is ahead, which
  includes one returned with a delay still to run (FL-15).

- **Bedrock calls go over aiohttp by default** (`BedrockOptions(transport=...)`: `"aiohttp"`, or
  `"botocore"` for the aiobotocore client). The body is built and the reply parsed with orjson
  and sent on one aiohttp session per client; botocore still resolves and refreshes
  credentials, signs (SigV4) and decodes stream frames. Measured on fakellm (three runs of 3-4
  alternating rounds): Converse at 20 / 200 concurrent calls +78-104 % calls/s, client CPU per
  call -55-61 %, p99 -27-44 %; embeddings +12-25 % calls/s; stream first event -18-23 %; memory
  per client 669 -> 87 KiB. All nine samples pass on real Bedrock with it. Differences: Converse
  fields botocore does not know are sent (Bedrock refuses them) instead of dropped; FIPS
  endpoints and `AWS_ENDPOINT_URL_*` are not read (`AWS_CA_BUNDLE`, `REQUESTS_CA_BUNDLE` and the proxy variables are).
- **`LLMQ_ENVIRONMENT` unset is strict.** Only `dev`, `local`, `test` and `ci` relax the checks;
  unset (reported as `unset`) or any other name requires TLS stores, `LLMQ_ID_HASH_SALT`,
  `LLMQ_BASE_URL_ALLOWED_HOSTS` when the registry has an `openai` account, and refuses debug
  capture. Set `LLMQ_ENVIRONMENT=dev` on laptops and in CI.
- **Non-stream `converse` uses plain Bedrock Converse** (IAM `bedrock:InvokeModel`) by default.
  The model setting `converse_api: "stream"` keeps ConverseStream underneath
  (`bedrock:InvokeModelWithResponseStream`) for its first-response timer. Streaming calls always
  need `InvokeModelWithResponseStream`.
- **Drain is 25 s** (`LLMQ_DRAIN_SECS`, was 45), inside the Kubernetes default grace period of
  30 s. A call still running when the drain ends is given back (grants refunded, message
  returned) instead of cancelled, with no failure counted; a stream that already sent chunks ends
  `worker_shutdown`.
- **`api_key_ref` / `external_id_ref` must start with `LLMQ_ACCOUNT_`** (upper case). Any other
  name is refused, so a registry cannot name a process secret.
- **`http_router(allow_writes=True)` needs `dependencies=`** (else `ConfigError`); write routes
  take JSON bodies (`POST /registry/block` no longer takes query parameters) and need the
  `X-LLMQ-Admin` header.
- **`redis>=6,<9`** (was `>=5.0.1`): TLS host-name checks by default, and the settings check
  reads URLs with the client's own parser.
- The first attempt's timer is `min(attempt_timeout_secs, time left)`, no longer half the time
  left; any attempt timeout counts against the breaker, so a hung region trips it. Keep
  `attempt_timeout_secs` below `timeout_secs` for failover within one call.
- Throttles count in the retry budget; the retry budget is a sliding minute.
- `LLMQ_CALLER_TTL_SECS` must be at least `LLMQ_LEASE_SECS / 3 + max(5, LLMQ_LEASE_SECS / 6)`.
- Registry: residency that leaves a model with no deployment is an error (was a warning); a TPM
  below one call's reservation, fallback cycles of any length
  (unless every step requires headroom), fallback targets outside the source's residency and
  `breaker_probe_secs` below `attempt_timeout_secs` are refused; `queue.retention_secs` and
  `dlq_retention_secs` are capped at 14 days; `inference.extra` takes an allow-listed set of
  names only; `min_lib_version` must be a version.
- Alert settings `quota_used_mins`, `spill_pct`, `spill_window_secs` and `waiting_age_secs` are
  removed (they did nothing; ignored with a warning until 0.2); `quota_used_pct` is 1-100 and also checks TPM; `dlq_messages` is
  at least 1.
- CLI: `dlq purge`, `dlq redrive`, `drain` and `registry gc` need `--yes` (or `--dry-run`);
  `registry gc` also removes the queues of removed models.
- Safe live-edit mode also protects `model_fallback` and `inference.extra`.
- `Tuning`, `StoreOptions`, `BedrockOptions`, `OpenAIOptions`, `WarmOptions`, `EngineTimings`
  and `RegistryOptions` are `slots=True` dataclasses: `vars()` / `__dict__` and setting an
  attribute they do not declare now fail (use `dataclasses.asdict` / `replace`).
- `RegistryStore.rollback` can raise `RegistryConflict`: it retries when a deploy publishes in
  between, `Tuning.registry.edit_retries` times, then gives up.
- `Tuning.registry.edit_retries` is at most 10 (`ConfigError` above), and each conflict backoff
  wait is capped at 2 s: with `edit_retries=20` the last publish wait could reach about 11.6 h.
- A `start()` retried after a failed one raises `LLMQueueError` naming the first error (chained
  to it); it said only "was stopped; build a new one". The queue cannot be started again: build
  a new one. A `start()` from a second event loop while one runs is an `LLMQueueError`, not
  asyncio's `RuntimeError`.
- `stream()` on a model that is not a chat model raises `CallFailed("invalid_request")`, not
  `ConfigError` (that is for start-up and refused registry edits) (RT-28).
- **OpenAI-compatible calls refuse params that touch protected fields** (owner decision D2,
  R3SE-1): `extra_body` keys `model`, `messages`, `stream`, `n`, `max_tokens`,
  `max_completion_tokens`, `store`, `tools` ...; `extra_headers` `Authorization`, `api-key` and
  other credential or framing headers; top-level `model`, `stream`, `stream_options`, `store` and
  SDK client arguments fail as `CallFailed("invalid_request")` before anything is sent. They
  replaced the route's model, undid the token cap or sent another key. Harmless keys pass.
- **Strict mode requires `LLMQ_KEY_PREFIX`** (Q26); dev and test keep `llmq`. Two environments
  that both left the default shared one store's queues and registry.
- **Strict mode requires `https` for `LLMQ_BEDROCK_ENDPOINT_URL` and `LLMQ_STS_ENDPOINT_URL`**
  (Q30); an `http` override sent prompts, SigV4 headers and STS role keys in clear.
- **A pod refuses a stored registry whose protected fields differ from its deploy config**
  (Q25, R3SE-3): accounts and roles, model ids, residency, region lists, `live_edit`, deploy-only
  settings. It keeps its registry, logs `registry_invalid` and counts
  `llmq_registry_refused_total{reason="protected_mismatch"}`. A protected field changed by a live
  edit under `registry_live_edit=all` is therefore refused by pods that hold a config, and during
  a rolling deploy pods on the old config keep their registry until they are replaced.
- **`LLMQueue.engine` and `LLMQueue.jobs` are deprecated** (Q35): they are private now and the
  old names warn (`DeprecationWarning`) until 0.2.
- `verify=False` / `use_ssl=False` in `credentials=` stays allowed and logs
  `bedrock_tls_verify_off` once per account (Q31).
- Calls borrowing one provider client are capped at its connection pool size; a call beyond it
  waits before its attempt timers start, not for a socket inside them (R3PF-9). OpenAI pools are
  sized by the slots routed to the account, at least `OpenAIOptions.max_connections`.
- A new pool plan (`ClientPool.plan`) retires built Bedrock clients whose size changed; the next
  call builds them at the new size (Q43).
- OpenAI stream events keep the wire shape: nulls the server sent (`finish_reason: null`,
  `content: null`) are kept, so `chunk["choices"][0]["finish_reason"]` no longer raises
  `KeyError`; `partial_text` counts only choice 0 (R3ST-10).
- A stream caller waits `stream_idle_secs` + `stream_flush_ms` + `caller_margin_secs` between
  chunks (was `stream_idle_secs`), so the worker's own idle error, with `partial_text`, arrives
  first; before, the two timers raced and the kind flipped between `provider_error` and
  `deadline_exceeded` (R3ST-1). Every `CallFailed` a stream raises itself carries the text
  already yielded.
- A second `stream()` with the same `request_id` in one process while the first runs fails
  with `CallFailed("invalid_request")`; both streams starved before (R3ST-8).
- A stream cut mid-answer keeps its input and the output written so far charged against TPM and
  gives the rest of the `max_tokens` reservation back; it kept all of it, also when the cause was
  ours (`StreamTooLarge`, the store) (R3ST-13).
- **Breaking: a sync call on the queue's own event loop is refused** (Q1, R3AP-1).
  `converse_sync` / `chat_sync` / `embed_sync` (and LangChain's `invoke`) on the thread that
  runs a started queue's loop would deadlock; it raises `LLMQueueError` naming the fix.
  **Migration:** in async code await the call: `await llmq.converse(...)` (or `chat` /
  `embed`), and in LangChain `await chat.ainvoke(...)` / `await chat.astream(...)`. Sync code
  on another thread (or a process with no started loop) is unchanged.
- **Delivery is at least once** (D1). A caller whose call record the store lost (restart
  without persistence, flush, a promotion that lost writes) sends it again: the result sweep
  finds records gone, and an epoch or `master_replid` change runs it at once (R3FM-4, R3RC-2,
  R3FM-8). A rare second provider run is the accepted cost.
- **Only a success is replayed for an app request id** (Q2): a retry of an id whose last run
  ended failed, expired or cancelled runs again.
- **Cancel wins for a shared id** (Q3, Q5, R3RC-1): any caller's cancel (an explicit
  `cancel()` or a cancelled awaiting task) stops the call for every caller of the id, in one
  atomic script; an attach while the cancel is pending gets `cancelled`. Every cancel is logged
  (`call_cancel`, with its `source`). Two local waiters on one id are no longer exempt.
- **The caller liveness key lives two heartbeats at least** (`max(LLMQ_CALLER_TTL_SECS,
  2 x LLMQ_LEASE_SECS / 3 + 5)`, 45 s with the defaults) and every send and attach refreshes
  it; the heartbeat renews it inside RENEW (Q4, R3FM-1, R3RC-3, FL-21).
- **An app's `worker_id` is a prefix**: a random part is always added (Q6, R3RC-9).
- **Store refusals count as the store down** (Q12, R3FM-7): `NOREPLICAS`, `OOM`, `MISCONF`,
  `BUSY` (and `LOADING`, `MASTERDOWN`, `TRYAGAIN`) follow the model's `store_down` rule, an
  answer in hand rides them out, a waiting caller keeps waiting, and a stored result is read
  with a plain `GET` when its drop is refused. With `store_down: pause` an out-of-memory store
  now holds a call up to its deadline instead of failing it at once.
- **An answer in hand is kept until the call's deadline** during a store outage, not one lease
  (Q7).
- **`fail_fast` / `static_cap` answer at once during a known outage** (Q11, R3FM-3): a store
  outage a call saw is remembered `outage_memo_secs` (3 s; a good write probe clears it). A
  separate queue store found down is remembered the same way: calls then run direct at once,
  and each send try is bounded by `LLMQ_QUEUE_FAILOVER_SECS`. A blackholed queue store cost
  every call the client's full retry budget (about 21 s) before the direct path.
- **`LLMQ_QUEUE_WAIT_REPLICAS` covers call sends, job sends and registry live edits** (Q8, Q51),
  each switchable with `LLMQ_QUEUE_WAIT_SCOPES` (`all`, `none`, or a list of `calls`, `jobs`,
  `registry`; all by default). Too few replicas no longer runs the call direct: it is sent
  anyway, warned at most once a minute (`replicas_short`) and counted
  (`llmq_send_not_replicated_total{scope}`) (Q9). The WAIT is skipped while the store reports
  no replica (R3FM-11).
- **Strict mode fails readiness on an evicting store policy** (Q10, R3FM-8): `health_check()`
  answers `ready: false` with `not_ready_reason`; the policy is re-read each minute.
- **A full store connection pool is busy, not down** (R3PF-6): `CallFailed("rate_limited")`
  with a 1 s `retry_after`, counted (`llmq_store_pool_busy_total`); no `store_down` path.
- **`timeout_secs` and `default_output_tokens` are read at their most specific level**
  (account, deployment) (Q39); before the deployment is known, the largest over the model's
  deployments is used.
- **A live `max_concurrency` / `worker_slots` edit resizes the Bedrock pools** on the next
  heartbeat, through `ClientPool.plan` (Q43).
- `converse_api` is passed only to providers whose `call` accepts it (Q33).
- A call's send carries its queue's own `retention_secs` (Q27).
- Performance: no back-to-back 0.1.0b1 / 0.1.0b2 benchmark was run for this release (owner
  decision); each change's own before / after numbers are in its entry under Performance. The
  final checks on one Windows machine (store behind Docker Desktop): every scenario, failover
  drill and gate passed; B34's ceiling was about 300-400 calls/s with the queue path adding
  under 20 ms at p95.
- **Embedding families come from one codec table.** Each family (`titan`, `cohere`, `openai`)
  is an `EmbeddingCodec` holding its batch limit, longest text, request body, reply parser and
  health-probe options; a model's codec is its registry `family`, else its provider (`openai`),
  else the first codec whose id pattern matches (`titan-embed`, `cohere.embed`, any case).
  Batch size, the text-length check, the request, the reply, the health probe and the registry
  `inference` fields the worker fills all use it (the probe ignored `family`, so a Cohere model
  behind an ARN was probed with Titan's body; the worker told the families apart by looking
  for `inputText`).
  An embedding id no codec matches is now a registry load error ("cannot tell the embedding
  family of <id>; set `family` (one of ...)"); it used to be sent Titan's body (one text,
  `inputText`) and every call failed at the provider. `family` is allowed on any embedding
  model. `llm_queue.providers` no longer has `titan_body`, `cohere_body`, `embedding_body`,
  `max_batch_size` or `parse_embeddings` (the codecs' `build_request` / `parse_reply` replace
  them). Request and reply shapes are unchanged.

### Added

- **Token streaming in the LangChain chat adapters** (owner Q49): `queue_streaming=True` on
  `QueuedChatBedrockConverse` and `QueuedChatOpenAI`, **off by default** (one whole message, as
  before). When on, `astream()`, `astream_events()` and LangGraph's `stream_mode="messages"`
  go through `LLMQueue.converse_stream` / `chat_stream`: text deltas and `tool_call_chunks` as
  the parent class parses them, `usage_metadata` once on the last chunk with `finish_reason`
  (Bedrock's `stopReason` mapped) and how the queue served the call, `CallFailed` with
  `partial_text` on a mid-stream failure, and the queued call cancelled when the consumer stops
  (aclosing). Sync `stream()` still yields one whole message; a sync call on the queue's own
  loop keeps the queue's `LLMQueueError` (Q1), which `QueuedChatOpenAI` now raises as such
  instead of the SDK's `InternalServerError`. See [LangChain](https://techiewonk.github.io/llm-queue/samples/langchain/).
- **Per-tenant fairness** (owner Q28, Q29, Q50): registry setting `fairness` (defaults or per
  model, *merged*), **off by default** (admission is unchanged when off). The tenant is the
  call's `tenant_id`, else its `user_id` (calls with neither share one tenant); the store keys
  hold only its keyed id hash, and no metric carries it. Modes: `share` (each active tenant
  gets `weight / active weights` of the model's rpm, tpm and slots; an over-share tenant
  borrows unless a tenant within its share is waiting), `quota` (hard per-tenant `rpm` / `tpm`
  caps, `quotas` per tenant; a refusal is `CallFailed("rate_limited")` with
  `retry_after_secs`), `fair_queue` (waiting calls served round robin between tenants). Each
  admission is O(log n) in tenants, every new key (`{m:ALIAS}:tf:*`) has an expiry, and the
  scripts stay not cluster-safe like the rest of admission. New metric
  `llmq_tenant_refusals_total{model, mode}`, refusal reason `tenant`, `status()` field
  `fairness` (counts only), registry checks. See
  [Tenant fairness](https://techiewonk.github.io/llm-queue/explanation/fairness/).
  Measured 2026-11-15 on the local Valkey 8.1 (6,000 PICK_TAKE + FINISH cycles per variant,
  interleaved, 100 tenants; server time per script call from `INFO commandstats`): before the
  feature 287.0 us, **off 288.0 us (+0.3 %, within noise)**, `fair_queue` 312.2 us (+9 %),
  `quota` 325.3 us (+13 %), `share` 353.4 us (+23 %). Client round trips on this loaded
  Windows box (median about 4 ms per cycle) did not separate the variants.
- **`register_embedding_family(name, codec)` and `EmbeddingCodec`**: an app adds an embedding
  family (Cohere v4, Nova, ...) without a library release; the registry's `family` accepts its
  name, and its id patterns are tried before the built-in ones. Register it in every process
  before the registry loads (see docs/how-to/call.md, "Embedding families").
- **Monitoring** (R3OB-2, -3, -4, -5, -14, -15, -16, -17, -19, -20; owner Q5, Q9, Q10, Q11,
  Q15, Q25): [Monitor](https://techiewonk.github.io/llm-queue/how-to/monitor/) lists every metric with kind, labels, unit and
  emitter (from `llm_queue.engine.metrics.METRICS`; a test fails on an unlisted name), with
  CloudWatch and Prometheus alarms, Logs Insights queries and a page per incident. New:
  histograms `llmq_call_seconds`, `llmq_provider_seconds`, `llmq_first_token_seconds`,
  `llmq_admission_wait_seconds` (the wait for budget included; `llmq_wait_seconds` stops when a
  worker takes the call), with buckets up to 600 s; counters `llmq_attempts_total{result}`,
  `llmq_caller_failures_total`, `llmq_swept_total`, `llmq_reclaimed_total`,
  `llmq_stuck_calls_total`, `llmq_store_resets_total`, `llmq_credentials_failed_total`,
  `llmq_loop_died_total`, `llmq_cancels_total{source}`, `llmq_outage_memo_set_total`; cluster
  gauges `llmq_held_waiting`, `llmq_oldest_waiting_seconds`, `llmq_rpm_limit`,
  `llmq_tpm_limit`, `llmq_concurrency_limit`, `llmq_deployment_usable`, `llmq_kill_switch`,
  `llmq_region_blocked`, `llmq_job_dead_letters`, `llmq_job_waiting`,
  `llmq_alerts_evaluated_timestamp_seconds`; pod gauges `llmq_build_info`, `llmq_store_up`,
  `llmq_held_calls`, `llmq_heartbeat_age_seconds`, `llmq_ready`, `llmq_outage_memo_active`,
  `llmq_store_eviction_unsafe`. Status reports `waiting_for_grant` and `oldest_waiting_secs`
  per model; health reports `worker`, `version`, `registry_version`, `held`,
  `heartbeat_age_secs` and `live_reason`. A background loop that dies is logged at once
  (`background_loop_died`).
- **Alerts `breaker_open`, `kill_switch_on` and `queue_stalled`** (R3OB-17), with the registry
  threshold `alerts.queue_age_secs` (default 600). An alert whose model or deployment left the
  registry is logged off.
- **`status_router(..., metrics_route=True)`** serves `GET /metrics` (Prometheus exposition),
  and `llm_queue.engine.metrics.prometheus_text(sink)` for an app's own route (R3OB-14).
  Prometheus metrics have help text from the table, not their name.
- **`llm-queue cancel ID ... --yes|--dry-run`** (owner Q5, R3SE-5): cancels calls for every
  caller of each id, logs `call_cancel` with `source=cli` and writes an audit entry (`registry
  log`) with the actor.
- **`llm_queue.Kind`**, a `Literal` of every `CallFailed` kind (owner Q38); `CallFailed.kind` is
  typed with it and `KINDS` is built from it.
- **`DeploymentStatus`, `RateStatus` and `ConcurrencyStatus` are exported** (R3AP-3), and
  `DeploymentStatus` declares `account_enabled` and `blocked` (returned but not declared, so
  `mypy` refused reading them); `RequestStatus` declares the `*_at` state times and says every
  value is text (R3AP-4).
- **`register_probe` has a typed contract** (`Probe = Callable[[str, str], Mapping | None]`,
  FO-22, R3AP-12); the docs say hooks run on the event loop and must not block.
- `chat_stream` is its own method (it was an alias of `converse_stream`) and the `call`,
  `converse` and `chat` docstrings say when to use each and list every per-call option (owner
  Q36 / Q37, R3AP-10, R3AP-15).
- `http_router()` with no `dependencies` logs `admin_routes_unauthenticated` (FO-14): the read
  routes, `/registry` included, are open then. Ids and edit bodies are bounded (FO-15).
- Registry warnings (R3CF-11, R3CF-16): a model with no usable confirmed deployment, queue
  retention below the timeout, a retry base above its cap, `probe_after_secs` above
  `probe_max_secs`, duplicate fallback causes or models, a `fallback` rule with no fallback
  regions, settings the model's type or provider never reads, locked parameters of the other
  model type.
- The model setting `supports_tools` (default true; false drops a call's tool fields when that
  model serves it as a fallback) (R3AP-17).

- `converse_stream(..., meta_event=True)` (and `chat_stream`) ends a complete stream with one
  library event, `{"llmq": {"usage": {...}, "meta": {...}}}`: input, output and cache tokens,
  the served model, deployment, fallback, attempts and worker. A stream caller never sees the
  call's `Reply`, so after a model fallback it could not tell which model answered (R3ST-9).
- Jobs: `current_job()` (`JobContext`: name, `job_id`, attempt, `max_attempts`) inside a job
  (Q13, R3JE-1); `LLMQueue.job_dead_letters(name)` and `redrive_jobs(name)`, `llm-queue dlq
  list|show|redrive|purge --job NAME`, job queues in `llm-queue queues list`, a `jobs` entry in
  `status()` and the `job_dead_letters` alert (Q15, R3JE-4); `llmq_jobs_total{job,outcome}`;
  `register_job(..., concurrency=N)`; the `LLMQ_JOB_*` settings and `JobSettings` (see
  docs/reference/configuration.md). New pages: docs/how-to/jobs.md (guarantees and limits,
  R3JE-14, -16) and docs/how-to/large-results.md (IAM per process, lifecycle rule, R3JE-34).
- Embeddings: `EmbedResult.meta.tokens_in`, `EmbedResult.batches` and `EmbedResult.request_ids`
  (Q21, R3JE-18); the registry's `embed_max_chars` (Q20) and a Bedrock embedding model's
  `family: titan | cohere` for ids that do not name it, such as ARNs (Q22, R3JE-21).
- `S3Offload(sse=None)` leaves the encryption header out, `bucket_key=True` sets
  `BucketKeyEnabled`, `aws:kms` without a key id warns (R3JE-32); `start()` warns
  `offload_no_lifecycle_rule` when the bucket has no expiry rule for the prefix (Q23).
- `Provider`, `Route`, `JobFn`, `Secrets` and `PublishResult` are importable from `llm_queue`
  (Q35). `PublishResult.carried` lists the live routing blocks and kill switch a publish or
  rollback kept (Q41).
- `CredentialsRefreshSlow` (a `CredentialsUnavailable`) in `llm_queue.clients`.
- Warnings `store_pool_small` (the store pool is below the worker slots plus a margin, R3PF-8)
  and `store_pool_busy`; `StorePoolBusy` names a saturated store pool (R3PF-6).
- **Client hub:** one SDK client per (event loop, credential identity, region, options) per
  process, shared by every queue and the sync path, reference-counted; clients for every
  deployment built concurrently at start; warm connections (unsigned `HEAD` requests, refreshed
  every 10 s); pool size per client from the registry's concurrency slots (at least 10, no cap);
  credentials refreshed 5 minutes before expiry; `Tuning(openai=OpenAIOptions(...),
  warm=WarmOptions(...))`, both importable from `llm_queue` (`warm.connections` or
  `warm.refresh_secs` 0 turns warming or its repeats off).
- **OpenAI-compatible calls use the SDK's aiohttp client by default** (`OpenAIOptions(http=...)`,
  `"aiohttp"` or `"httpx"`; `DefaultAioHttpClient`) instead of httpx, with the same pool limits, keepalive and timeouts.
  The `openai` extra adds `httpx-aiohttp` (needed on openai 1.x / 2.x; 3.x vendors it); without
  it the client falls back to httpx and logs `openai_aiohttp_unavailable`. Measured on fakellm (4 alternating rounds, 200
  concurrent calls): 258 -> 352 calls/s, p99 -31 %, client CPU per call -26 %.
  A connection reset or refused connect classifies the same on both (aiohttp's transport reports
  them as timeouts: `APIConnectionError`, and a reset gets its one quick retry).
- **One `LLMQueue` per process guards:** a second started queue logs `second_llm_queue_started`
  (the first stays the LangChain default); calls from a forked child or another event loop
  raise a clear `LLMQueueError`. Sync calls run on the started queue's loop, else on one private
  loop per process with a 20-connection store pool.
- **Images and documents** in Converse messages (bytes travel encoded in the message), the
  `max_message_bytes` setting (4 MiB) and `CallFailed("invalid_request")` above it; binary
  blocks cost a fixed 1,000 tokens in the estimate.
- **Shared throttle window** per deployment: `max(Retry-After, jittered backoff 1-2 s doubling to
  30 s)`, skipped by every pod; calls wait for it when no other deployment is left.
- **Store checks at start:** cluster mode refused; a `maxmemory-policy` other than `noeviction`
  warned about; `health()` fields `eviction_policy` (state store, `unknown` when unreadable)
  and `queue_eviction_policy` (only with a separate queue store).
- **Registry:** a top-level `live_edit` field (the document's live-edit mode); publishes keep
  live routing blocks and the kill switch; strict checks on the paths a live edit adds (typos
  refused); an invalid or newer stored registry no longer stops a pod (it runs on its own
  document); `min_lib_version` keeps older pods up and off the stored registry; a model added by a newer
  registry gets a receive loop on the next heartbeat (every lease / 3), a removed model's loop
  ends; gauges `llmq_registry_invalid_reads`, `llmq_registry_stored_refused`,
  `llmq_registry_lib_too_old` and `llmq_registry_on_own_config` on every worker pod (every
  30 s).
- **Embeddings:** `embed(partial=True)` and `EmbedResult.errors`; one request id per batch
  (`X:n`); empty texts refused; at most 16 batches in flight.
- **Jobs:** `register_job(name, fn, *, timeout_secs=None)`, a 10,000-job queue limit
  (`send_job` then raises `CallFailed("queue_full")`, retry after 5 s), a `job_no_consumer` warning and
  metric, acks retried apart from the job, a job stopped by shutdown returned instead of failed.
- **Errors:** kinds `invalid_request`, `context_length_exceeded`, `worker_shutdown`;
  `RegistryConflict` and `LiveEditRefused` exported. The OpenAI HTTP transport maps request
  faults to 400 and exhausted retries to 502 / 503 with `retry-after` (the codes that changed
  are under Changed).
- **Health and metrics:** readiness from a cached background store probe (every 5 s; refusals
  are `down`, never a 500); liveness false when a background loop died or the heartbeat stalled;
  typed `health()` / `status()` results; status rows show `account_enabled` and `blocked`;
  `llmq_grants_lost_total`, `llmq_retries_total`, `llmq_store_errors_total`,
  `llmq_newer_messages_total` and per-deployment gauges.
- **S3 offload:** keys `{prefix}{key_prefix}/{sha256(id)}-{uuid}`, objects deleted after a
  successful read, `sse` / `kms_key_id` / `expected_bucket_owner` options, foreign references
  refused, one client per loop.
- Optional `uvloop` extra and `llm_queue.run(main)`.
- Docs: environment variables, clients and connections, Bedrock APIs and IAM, securing a
  deployment, throttling, shutdown on Kubernetes, multimodal calls, token estimators for Indic
  and CJK text; `scripts/bench.py` with the 0.1.0b1 baseline.
- Tests: fakellm fault knobs and counters, `--seed` for scenarios, gates and soak, a gates
  perf baseline (`--record-baseline`), Hypothesis property tests for admission, a nightly
  workflow, pytest-timeout and a coverage floor of 89 %.

### Fixed

- Engine audit (FE-*, RV-*, R3RC-*, R3FM-*): a given-back call releases its claim, so another
  pod runs it at once (FE-1, FL-1); a call received while `stop()` runs is returned (FE-2);
  `stop(drain_secs=0)` still returns waiting calls (FE-6); a hold is dropped, not cancelled
  inside its admission, and an admission that failed without an answer undoes its possible
  grant (FE-4, RV-N1); a hold cancelled before its first step frees its class (RV-N2); paused
  calls resume only when the store takes writes (FE-7); a direct run that raised stores its
  failure (FE-5), and one whose process died (killed, out of memory) is taken over by the next
  caller of its id once its liveness key and heartbeat lapse, where callers waited to their
  deadline for up to an hour (AMEND FE-5); a caller-only process stops its direct run on cancel (FE-17); an unreadable
  message whose return or fail errs is returned (FE-9); a retry admits against the current
  view (FE-13); a stale ack is logged and counted, `llmq_stale_acks_total` (FE-14); one model's
  failing sweep skips only itself (FE-15); `_free_stuck` cannot end the heartbeat (FE-18);
  background tasks are named (FE-19); `ring_again`'s error reply no longer ends a model loop
  (FE-20); a receive cancelled in flight is reclaimed, also by a full worker and at `stop()`
  (FE-8, FL-3); no canceller starts after `stop()` (RV-N3); the probe lock goes back to its
  5 s spacing (RV-N6); `InferenceRejected` on the `static_cap` path is
  `CallFailed("invalid_request")` (RV-EN14).
- An attached caller checks the record after the takeover grace and takes over a send that
  never happened (R3RC-4); a retry whose result is stored reads it at once (R3RC-5);
  `static_cap` stores its answer and `fail_fast` cancels behind, so a queued copy of an
  ambiguous send does not run twice (R3FM-2); job sends wait for replicas and a job ack is
  retried for a visibility time (R3FM-5).
- A slow `TIME` read keeps the old store-clock offset and a clock jump is logged,
  `store_clock_jump` (R3RC-7, R3FM-10); a subscriber is pinged and reset when silent, or when
  the sweep keeps finding results it never announced, `llmq_pubsub_resubscribes_total`
  (R3RC-6, R3FM-12); a duplicate goes back when the claim lapses: PICK_TAKE's `duplicate`
  reply carries the claim's time left (`ms`), where it went back after a fixed half lease (R3RC-8).
- A job delivered again while it still ran in the same process ran twice (R3JE-9); a failed job
  was logged without its traceback or id (R3JE-11) and any `TimeoutError` as the job's own
  limit (now `job_timeout` only when `timeout_secs` ran out: R3JE-13).
- The health probe of an embedding model tested `"cohere"` case-sensitively and sent the Titan
  body to a mixed-case Cohere id (R3JE-21). An OpenAI reply with a missing or repeated `index`
  put vectors on the wrong texts; one with base64 vectors became lists of characters (R3JE-22).
- A process without the offload read an offloaded result as JSON, raised a raw error and
  deleted the result; it now fails `result_expired`, logs `offload_missing` and keeps it
  (R3JE-26). A failed offload read (access denied, throttling) read as `result_expired` with no
  log; it is logged with its S3 code, tried twice more, then fails `queue_unavailable` with the
  result kept (R3JE-28). An object uploaded for a result already stored is deleted (R3JE-29).
  The S3 client used botocore's 60 s timeouts: now 3 s connect, 10 s read, 2 attempts, and an
  upload over 15 s falls back inline (R3JE-30).
- Lua audit (FL-*): UNDO now releases the call's claim, so another pod runs a given-back call at
  once instead of answering `duplicate` for a lease (FL-1; `undo(keep_claim=True)` keeps it,
  `Admission.release_claims` does it for calls given back without a grant). RESULT_TAKE no longer
  deletes a library id's result in the read: a replay after a dropped reply found nothing and
  the caller waited to its deadline; the result now expires 10 s after the read (FL-2). A
  receive's claim keeps its dead letters, so a replayed receive still reports them (FL-4).
  RETURN (no delay) and RECLAIM ring the queue's doorbell (FL-5). A held grant is judged alive by
  its own lease, not only by the job record's heartbeat (a missing record let a live call run
  twice, FL-6). Lease, in-progress, waiter, limiter, breaker, health and rotation keys get a
  sliding expiry, and publishing without a model deletes its state keys (FL-7). Fractional costs
  and refunds are rounded before HINCRBY, which raised after earlier writes stayed (FL-9). RENEW
  extends the claim's TTL and writes only its own claim's heartbeat (FL-12). REREGISTER restores
  the claim, the grant's budgets, window and account, and skips a cancelled call (FL-13). The
  weighted rotation moves only with a grant (FL-17). A class share of 0 never fits instead of
  having no cap (FL-18). REDRIVE stops at an optional `max_waiting` (FL-23). SWEEP waits half a
  lease before removing a lapsed grant, so a slightly late heartbeat still renews it (FL-24).
  REGISTER_SEND keeps the record when a takeover's send finds the queue full (FL-25).
- `RequestStore.cancel` marks and announces a batch of calls in one script, skipping calls
  already final: cancel wins for a shared id (Q3, R3RC-1; the runtime's read-then-write of the
  callers set never cancelled a shared id).
- The aiohttp Bedrock transport ignored `HTTPS_PROXY` / `HTTP_PROXY` / `NO_PROXY` and
  `REQUESTS_CA_BUNDLE`: a pod behind an egress proxy timed out on every call, and one behind a
  TLS-inspecting gateway failed every certificate check, while STS worked (FC-1, R3SE-12).
- A slow credentials refresh cut by the attempt timer dropped the client and its shared refresh,
  and every call it cut doubled the account back-off (5 s to 60 s at once). The client is kept,
  one refresh takes one back-off step, and a cut while the keys are still valid is a retryable
  timeout, not `CredentialsUnavailable` (FC-2).
- With `LLMQ_STORE_PASSWORD` set, a store URL with a user (an ACL user) failed every connection
  (redis-py refuses a URL user or password with a credential provider); the URL's user is kept
  and the rotating password used (R3SE-2).
- A store pool wait that ran out read as the store being down; it waits again up to one read
  timeout, then raises `StorePoolBusy` (R3PF-6).
- An assumed role's external id was read once, so a rotated one failed every later refresh; it
  is read on every AssumeRole (R3SE-11).
- `openai._constants` failing to import was reported as `MissingExtra("openai")` (R3SE-13).
- Streamed answers: partial tool JSON cut by `max_tokens`, a text and a tool delta on one index,
  chunks without `delta` (Azure content filters), tool calls without `index`, and stream frames
  whose JSON is not an object each failed a call (FATAL) whose whole stream had already reached
  the caller (R3ST-4, R3ST-5, R3ST-11, R3ST-14).
- `stop()` during a slow `start()` left a queue stopped and started at once (FC-3); a sync call
  during the drain built a second private engine (FC-4); a private sync engine whose start
  failed leaked its clients (FC-6); `converse_stream` closed early left its inner generator to
  the finalizer (FC-12); a `KeyboardInterrupt` in a waiting sync thread left the call running,
  and a loop between two `run_until_complete` calls was taken for stopped (FC-13).
- Two concurrent `ClientPool.aclose()` released every client twice, and a cancel skipped the
  rest (FC-7); `credentials=` / `secrets=` callables had no time limit (FC-8); an OpenAI
  account edit left the old client open (FC-10); `credentials=` returning an expired key failed
  `fatal` instead of trying the next account (FC-11); a dead loop's build lock was kept (FC-14);
  a failing client close was not logged (FC-15).
- The EMF writer thread was not restarted in a forked child, whose metric lines piled up and
  were dropped (FC-9).
- `RegistryStore.rollback` documents its real retry bound: `edit_retries x (edit_retries + 2)`
  publishes (FC-16).
- Clients audit (CL-*): the aiohttp Bedrock transport followed redirects and re-sent the signed
  body and session token to the new host (it never follows now; aiobotocore still does); TLS,
  proxy and bad-URL failures on the aiohttp transport were retried on every attempt and charged
  breakers (they are fatal, as on botocore); a garbled status line got no quick retry there; a
  corrupt stream frame or a non-JSON reply was fatal (retryable now).
- `credentials=` keys other than the AWS keys raised `ConfigError` on every aiohttp call (that
  account now uses the botocore transport, with one warning); a missing `api_key_ref` secret
  ended the call as `provider_error` (it is `CredentialsUnavailable` now, so the next deployment
  is tried, and `warm` lists missing secrets at start); an empty credential chain cached a client
  that failed until restart (no client is cached; the chain is read again).
- A slow STS refresh inside the 2-minute mandatory window was cancelled by each caller's attempt
  timer and never landed (one shared refresh task now; STS has a 10 s read timeout and two
  attempts); the `credentials=` and `secrets=` callables ran on the event loop.
- A client retired by `forget()` or an account edit was closed after a fixed 310 s grace, cutting
  longer healthy answers; it is closed when its calls end now. `aclose()` could release a retired
  client twice and close it under another queue; a cancelled `release` lost its cancellation.
- A stream cut after the answer began got the quick retry meant for stale sockets, so the
  generation ran (and was billed) twice. A sink that raised held the stream's connection until
  garbage collection.
- Bedrock pool size is the slots routed to the client (at least 10) without the 200 cap, under
  which callers queued for a socket inside the first-response timer and charged the breaker.
- `BedrockHTTP.open` takes `read_timeout_secs` / `connect_timeout_secs`; `ClientPool.bedrock()`
  returns a `BedrockClient` protocol; the client hub's at-fork hook is registered on first use,
  not at import (the fork guard in `core.py` is still registered at import, the one exception
  CONTRIBUTING names).
- A redelivery to the pod already running a call started a second run and leaked its slot.
- After a store reset only the first pod re-registered its calls; `RENEW` losses went unnoticed.
- The per-minute retries key had no TTL on a minute without a retry.
- Refunds were not capped at the charge, and used the current config row instead of the grant's.
- A deployment marked slow never recovered; an in-flight success closed an open breaker early;
  expired waiters blocked model fallback; a dead owner's breaker probe stayed set.
- A live-edit typo was accepted and did nothing; models added by a newer registry got no worker
  loop on running pods; reload storms on `stale_config`; a torn registry read.
- OpenAI-compatible calls with unknown params raised `TypeError` (now sent as `extra_body`);
  `n > 1`, reasoning and logprobs were lost; cached tokens were counted twice.
- Throttles retried at once and ignored Retry-After; a hung region never tripped its breaker;
  `on_exceed: reject` looped the message to the dead-letter queue; a mid-stream OpenAI error was
  fatal; request and store errors charged the provider's breaker.
- A stream could end "ok" with chunks missing after a failed flush; a slow consumer got
  `deadline_exceeded` on data already buffered; read streams stayed for minutes; one bad entry
  stopped the shared reader.
- An offloaded result was deleted before its S3 read; the `callers` set and the job claim had no
  TTL.
- Jobs: shutdown counted as a failure; a successful job re-ran when its ack failed; no timeout;
  unbounded queues for unregistered names.
- `embed(request_id=X)` rejected every batch after the first as a duplicate; one failed batch
  lost all; 100,000 texts created every task up front.
- Bytes in Converse messages broke the message encoding and were estimated by their base64 size.
- Credential backoff never reset after a success; signature and clock-skew errors were not
  treated as credential errors; a dropped connection charged the breaker.
- Readiness could answer 500 and take up to 3 s; liveness was always true; `health()` raised raw
  errors; the Prometheus sink could be registered twice.
- The sweeper stopped with an interval below 1 s; bad messages used default queue settings.
- Cancel keys are re-checked on pub/sub reconnect and every heartbeat.
- A message from a newer library was taken again every 5 s with a log line each time; it now
  goes back for 15 s with no failure counted, a metric and one warning a minute.
- The message size cap is checked in the engine, so sync calls, streams, embeddings, jobs and
  the LangChain adapters all meet it; `converse_stream` on an embedding model is refused.
- A stream's open and its first event share one first-response budget (a slow open gave the
  first event a whole budget again); a Bedrock stream cut mid-body (`ClientPayloadError`)
  counts as a connection reset.
- `result_dedupe_secs > 0` with an S3 offload that deletes on read is refused (`ConfigError`):
  the second reader found the object gone.
- Docs: eight statements that did not match the code (readiness probe, invalid stored registry,
  older services, the `all` live-edit mode, account kinds, `ConfigError` timing, the exception
  tree).
- `LLMQueue.stop()` skipped every later close step when one failed, and a second `stop()` did
  nothing, so store pools, provider sessions and the S3 client leaked; every step now runs and
  the first failure is raised at the end. `start()` is serialised (two concurrent starts doubled
  every background task) and a start that fails part-way stops what it started.
- Calls, streams, jobs and sync calls made once `stop()` has drained its jobs and running calls
  raise a clear `LLMQueueError` (they failed on a closed pool, and a sync call built a new
  private engine that nothing closed). Calls made during the drain are still served: a running
  job's next call and an app's follow-up call in a graceful shutdown. A sync call cancelled by
  `close_sync()` raises `CallFailed("cancelled")`, not `concurrent.futures.CancelledError`; a
  sync call to a loop that was stopped but not closed fails with `CallFailed("worker_shutdown")`
  instead of blocking forever, and one whose call raised a `TimeoutError` (such as
  `AttemptTimeout`) gets that error instead of waiting forever. Offload and sync-engine
  close failures are logged instead of swallowed.
- The registry reload loop ended for good, silently, on any error other than a store or config
  error (a corrupt change counter); it now logs it and backs off, up to 30 s.
- A failing registry restore on the call path raised a config error into the call; it is logged
  and the next reload retries. Publishes and live edits that lose a race retry with a jittered,
  doubling backoff (concurrent editors used up their tries together), and `RegistryConflict`
  says how many tries ran.
- Rollback did nothing when a deploy published between its version read and its publish; it now
  retries. A long actor no longer goes past the actor limit, and a corrupt history entry or
  audit entry raises `ConfigError` or is skipped instead of a JSON error.
- An edit followed by a flushed store raised `AssertionError`; it raises `StoreUnavailable`.
- A publish deletes the resolved numbers of models the deploy removed (they stayed forever) and
  no longer sends the stored registry back on every pod boot (reply 3682 -> 12 bytes).
- Deploy-only and protected registry settings are read from the `SETTINGS` table
  (`Spec.deploy_only` / `Spec.protected` were never read); `debug_capture` and `model_fallback`
  are marked there. `refused_edits` raises `ConfigError` for an unknown mode (was `ValueError`).
- `block()` of an unknown account or region lists the known names and a close match; reading the
  registry before it was loaded says to call `LLMQueue.start()` first.
- A log field value with a newline printed as two lines (a forged entry); such values are
  rendered with `repr()`. `Tuning` choice fields are checked by `section.field`.
- `llmq_grants_lost_total` was counted with labels `(model, step)` on finish but `(step)` on
  the heartbeat; with the Prometheus sink the second form raised, after the provider had
  answered. Both now use `(model, step)`, and `PrometheusSink` logs and drops a point whose
  labels do not fit instead of raising into a call (EN-1).
- `Admission.undo` skips (returns False, logs `undo_skipped`) when the store is unreachable,
  like `finish`, instead of raising (EN-16). A re-settled owner no longer stops the pruning of
  the settled-owner memory (EN-17).
- A custom `token_estimator` that raises falls back to characters / 4 with one `hook_failed`
  warning, and a request with `"inferenceConfig": None` (or `additionalModelRequestFields:
  None`) gets the defaults filled in. Both used to raise into the worker loop, which returned the
  message at once with no failure counted, so it spun until its deadline (EN-2). A
  `token_estimator` name that is not registered now warns once (EN-15).
- A large result is offloaded to S3 once even when its store write is retried through an
  outage (each 0.5 s retry uploaded it again, up to ~120 orphaned objects). An offload write
  that fails keeps the result inline (up to 64 MB) instead of raising a botocore error, which
  released the message and ran a call the provider had already answered a second time (EN-3).
- A store blip after an offloaded result was read no longer loses it: the read-marker cleanup
  is skipped with a warning, where the caller used to read again, find the object deleted and
  get `result_expired` (EN-4).
- `S3Offload.get` reads a result as gone only on S3's `NoSuchKey` / `404` error code; any error
  whose text held "404" (a request id, a host) was reported as `result_expired` (EN-11).
- An embedding reply the parser cannot read is a `CallFailed("provider_error")`; a raw
  `KeyError` / `ValueError` aborted every batch of `embed_partial` and reached the app outside
  `LLMQueueError` (EN-6).
- Job runner: `stop()` lets each loop finish a receive in flight before cancelling it (a cancel
  inside RECEIVE hid the message it took for the visibility time and charged it a failure,
  EN-8); a cancelled `stop()` now raises `CancelledError` instead of returning normally, and
  task errors are logged (EN-7); an idle loop sleeps on the jobs queue's doorbell instead of
  polling RECEIVE about 7 times a second (EN-29); a job whose ack failed every try logs
  `job_ack_gave_up` and counts `llmq_job_ack_failed_total` (EN-32).
- Chunk streams: stopping a writer's flush timer or closing a reader no longer swallows the
  caller's own cancellation (EN-7); a failed background flush is logged when it happens
  (EN-19); during a store outage the reader logs the first failure and the recovery and backs
  off to 5 s (it warned every 0.5 s), and `aclose` also cancels pending wake-ups (EN-18).
- Chunk streams no longer lose a chunk or the end of a stream when a write is cut. `end` and
  `error` cancelled the flush timer inside its write, and an `add` could be cancelled there too
  (a deadline, a cancel, shutdown): the batch was dropped, or, when the store had taken it, the
  next entry reused its seq and the reader skipped it, so the caller never saw `end` / `error`
  and failed `worker_lost` on a complete answer (FE-3, R3ST-7). An entry in doubt is now sent
  again under its own seq, and stopping the timer waits for a flush in flight.
- The call's deadline passing mid-stream ends the stream `deadline_exceeded` ("the call's
  deadline passed mid-stream") without charging the deployment's breaker; it was
  `provider_error` with an empty message, counted as a provider failure (R3ST-2).
- A stream cancelled with `cancel(request_id)` gets an error entry with `partial_text` at once
  and its flush timer is stopped; the caller waited out `stream_idle_secs` (30 s) and got
  `cancelled` with no text. A stream whose call failed after its last entry ends after a short
  grace, not the idle time (R3ST-3).
- An app request id streamed again after its chunks expired (30 s after `end`) while its
  result is still kept replays the kept reply as events (ConverseStream events, or one chat
  chunk plus a usage chunk); it ended with no event and no error (R3ST-6).
- Every event up to the first with answer content is written at once: the first text delta
  waited up to `stream_flush_ms` behind `messageStart` (R3ST-12). The `end` entry is not counted
  against `stream_max_bytes`; an answer that fit failed `StreamTooLarge` on its last entry
  (R3ST-13).
- `llm-queue redrive` drops (and reports) a dead letter without a body instead of failing
  part-way with some letters already moved, rings the queue's doorbell so an idle worker takes
  the redriven calls at once, and trims the stream like a send (EN-12). `ring_again` sets the
  doorbell's cap and TTL; it could create a bell with no TTL (EN-13).
- A failed health probe doubles the last wait (starting from the model's `probe_after_secs`)
  instead of jumping to at least 60 s; `Health.probed(probe_after_secs=)` sets the floor
  (EN-23).
- The alert loop writes its change markers with one pipelined `SET ... EX GET` per alert (500
  per pipeline) instead of `GETSET` then `EXPIRE`: 2 x N serial round trips per sample became
  N / 500 (about 1,840 alerts at 20 models x 30 deployments; time saved unverified) (EN-9).
  `Alert` is frozen (EN-25).
- The HTTP status, request, registry and write routes answer 503 `state store unavailable`
  during a store outage; redis-py's `ConnectionError` reached FastAPI as a 500 (EN-10).
- `llm-queue`: a store error prints one line and exits 1 instead of a traceback; both store
  clients are closed even when the first close raises; `registry gc` escapes glob characters of
  `LLMQ_KEY_PREFIX` in its SCAN pattern and deletes with pipelined `UNLINK` (EN-30).
- The store checks log (debug) why the eviction or cluster check could not run (EN-22).
  `FreshPassword` reads the secret in a worker thread on the asyncio client, so a slow lookup
  no longer blocks the event loop on every reconnect; `secrets` should still cache (EN-31).
- The store client retries a command whose connection dropped, even when the server already
  ran it. FAIL now ends its receipt (generation + 1, like RETURN), so a replay is `stale` and
  one failure is counted, not two. A replayed RECEIVE still takes a second batch (the first is
  hidden for the visibility time and charged one failure) and a replayed plain SEND appends
  twice; both are rare and bounded, and are documented in `store_client.py` (EN-5). The worker's
  receive is now safe: see S11 below.
- A receive whose reply was lost (a connection cut after the store ran it) hid its messages
  for the whole visibility timeout (60 s), and a replayed one took a second batch. The worker's
  receive records its batch under a nonce: a replay hands back the same messages, and the next
  round returns a lost batch to the queue at once, receipt-checked (S11, EN-5).
- A direct call (kill switch, `queue_pct`, `force_direct`, queue-store failover) whose run
  failed (a store refusal, missing admission numbers) waited until its deadline and got
  `deadline_exceeded`; the caller now gets the real error at once (RT-1).
- Calls cancelled by the app each started a cleanup task with three store calls and no
  timeout: many cancels during a store outage made a connection storm. One canceller per
  process now cancels them in pipelined batches, bounded by the probe timer and logged (RT-3).
- Closing a stream swallowed the consumer's own cancel, and a cancelled stream left a pending
  chunk getter (RT-4, RT-25).
- `stop()` returns held calls together under the drain deadline and stops their hold tasks
  (one more round could run a call already handed back), waits for the drain's give-backs
  before the clients close, cancels the registry reload, can itself be cancelled, logs a loop
  that died, and closes each part even when one fails. `aclose()` closes every client and
  raises the first failure (RT-5, RT-6, RT-7, CO-1).
- A call let go while its admission was in flight (`held_call_stuck`) could still start a run
  whose grant nobody renewed; the grant now goes straight back. SWEEP also removes leases past
  their end whose in-progress entry already expired: they stayed for good on a deployment no
  call picked again (scenario L4).
- One failed dead-letter reply skipped the whole received batch, and one failed return
  stranded the rest (RT-8). A failed abandon after a failed send replaced the send's error (a
  cancel became a store error) (RT-9). A failed status write after a model-fallback grant left
  the grant unrun (RT-10). A refused FINISH, or a failed debug-capture write, after a success
  ran the call again (RT-11, RT-12).
- `start()` is bounded on a dead store (it waited connect timeouts times retries) and never
  raises a raw store refusal (RT-13). Calls paused on `store_down` resume spread over half a
  second, not in one tick (RT-14). Store retries use full jitter (0.1 s doubling to 1 s; the
  effect on reconnect storms is unverified) instead of fixed 0.25 / 0.5 s pauses (RT-15).
- A reload removing a granted deployment raised `StopIteration` mid-call (RT-16); an
  `expired` id found on re-registration waited out its deadline (RT-17); caller errors carry
  the request id (RT-18); `store_down: static_cap` classifies provider errors and stops on a
  request error with its own kind (RT-19); any error building a call's admission request fails
  it as `invalid_request` instead of re-running it (EN-2); a corrupt registry counter during a
  call's reload is logged, never raised into the call (CO-9).
- A live `worker_slots` edit is warned about once (it needs a restart) (RT-23); the probe lock
  is held for each probe's budget, so two pods no longer probe one deployment at once (RT-27);
  heartbeat steps fail on their own (RT-33); silent `suppress(Exception)` blocks now log
  (RT-24); the `model_unavailable` diagnosis reads every breaker in one round trip (RT-31);
  registry reload failures are counted (`llmq_registry_reload_errors_total`, CO-4); OpenAI
  routes are checked for missing secrets at start (CL-6).

### Performance

- Log values are checked for control characters with one compiled regex (R3PF-5). Python
  3.12, Windows, 2026-11-15, micro-benchmark of `_value`: the 13 fields of `llmq_call_finished`
  20.2 -> 5.6 us, a 2,000-character error 362 -> 19 us; same output.
- EMF writes one line per label set per second instead of one per point (R3PF-14, R3OB-11): a
  successful call wrote 4-5 lines (17.7 us, 1,322 bytes measured in the audit), now its points
  join the label set's next line; the alert tick wrote one line per gauge, now one per
  deployment. Dropped lines are warned about once a minute with a count (once per process
  before). Unverified end to end: no CloudWatch run.
- OpenAI embeddings are asked for as base64 float32 and decoded once by the reader (R3PF-10).
  A 96 x 1024 reply, Python 3.12.13, Windows, 2026-11-15, micro-benchmark of `Reply.dumps` and
  of `orjson.loads` + validate + `vectors_of`: stored value 1.95 MB -> 0.53 MB, worker write
  8.6 ms -> 0.6 ms, caller read and decode 22.5 ms -> 10.5 ms.
- A Bedrock embedding batch sent by `embed()` is stored as its vectors in binary (float32 when
  every value is exactly one, else float64: lossless) instead of base64 of the JSON body; a raw
  InvokeModel call keeps its body. Gate B97 (300 Cohere calls of 96 texts, local Valkey 8.1,
  Windows, 2026-11-15): a stored result 3.1 MB -> 1.05 MB, store memory peak +102 / +107 MB ->
  +38 / +41 MB (budget +64 MB).
- Lua audit, measured on the local Valkey 8.1, Python 3.12.13, 2026-11-15, pipelines of 50 script
  calls, 20 rounds A/B against af2fec1 (per call, median): a receive of one message behind 1000
  returned ones 32,957 us -> 1,705 us (the oldest few due ids are picked in one pass instead of
  sorting the window with two string matches per comparison, FL-8); a refused PICK_TAKE behind
  400 waiters 4,050 us -> 3,413 us (one HMGET, FL-10); a granted PICK_TAKE over 6 candidates with
  rpm, tpm and account budgets 888 us -> 784 us (each budget hash read once, FL-11); the
  sweeper's two dead-letter trims per model 3,956 us -> 1,932 us (one call, 300 sequential runs,
  FL-20). The receive claim is one JSON string (one SET, one GET) instead of a list (FL-16):
  157-199 us -> 175-212 us a receive of one, no gain at batch 1 (fewer commands replicated).
  RENEW can set the caller key and return the epoch (FL-21); the heartbeat does not use it yet.
- PUBLISH and EDIT test the registry document with EXISTS instead of reading it into Lua (FL-19).
- Streamed answers are folded into the reply as they arrive, joined once: `_converse_body`
  4,000 deltas 5.6 -> 1.9 ms, 16,000 74 -> 10 ms, 64,000 2,808 -> 49 ms (it was quadratic,
  R3PF-1), and no list of every event is kept (2.7 MB for a 60 KB Bedrock answer, 2.4 KB per
  OpenAI chunk, R3PF-4) (Python 3.12.13, Windows, 2026-11-15).
- Bedrock text blocks skip the blob walk: request scan at 40 messages 629 -> 60 us, per streamed
  delta 6.9 -> 0.5 us (FC-5; Python 3.12.13, Windows, 2026-11-15).
- The default credentials and secrets lookups run inline instead of in a thread (FC-18).
- The CloudWatch (EMF) sink writes its lines from a background thread instead of on the event
  loop: with a stdout that takes 5 ms per line (a backed-up log pipe), a metric point held the
  loop p50 5,346 us / p99 6,544 us; queued it takes p50 3.8 us / p99 30 us (Python 3.12,
  Windows, 2026-11-15). Lines past 10,000 waiting are dropped with one warning (EN-20).
- Log calls below the logger's level are dropped before the message is built: a disabled `info`
  call 9.2 -> 5.3 us (Python 3.12.13, Windows, 2026-11-15).
- `alias_of(model_id)` is one dict lookup instead of a scan of every model: 2674 -> 241 ns with
  55 models (Python 3.12.13, 2026-11-15).
- Setting values (`inference`, `params`, `tags` ...) are shared by every call and not
  deep-frozen; nothing may change one. Freezing them is left out: they go into provider request
  bodies, and botocore's parameter check and the OpenAI SDK's JSON encoder refuse
  `MappingProxyType`, so each call would have to copy them back to plain dicts. Measured on one
  Converse request (inference with `stop` and `extra`, a guardrail in `params`): a memoised
  settings read stays 232 ns (freezing happens once per view), but the request build goes
  13.6 -> 21.1 us (+55 %) with the copy (Python 3.12.13, Windows, 2026-11-15).
- The caller's shared chunk read packs each open stream's key once and joins the command,
  instead of redis-py rebuilding and packing every key and id on each read: 0.30 -> 0.05 ms per
  read at 100 open streams, 3.1 -> 0.42 ms at 1,000, 16.0 -> 2.5 ms at 5,000. With eight writers
  at 33 entries/s per stream on the local Valkey, reader CPU per entry 41.8 -> 24.7 us (100
  streams), 15.8 -> 12.8 us (1,000) (R3PF-2; Python 3.12.13, Windows, 2026-11-15). A
  per-caller inbox stream (one key per read) is not done.
- `stream()` reads each chunk entry with one queue get; a Task and an `asyncio.wait` per entry
  cost 42 us each (R3PF-11, caller side; the worker still flushes one pipeline per stream).
- A queued call that succeeded ends in one script when the queues share the state store
  (`SETTLE_DONE`: result, status, ack, then FINISH): one round trip instead of two (RESULT_PUT,
  then FINISH and ACK together), and no window where the result is stored while the slot
  waits (FL-22). A separate queue store keeps the old path. Local Valkey 8.1, 1,000 calls per
  side, interleaved, 2 KB answer: 4.4-4.6 ms -> 2.2-2.3 ms median, p90 11.0-11.4 -> 5.0-5.5 ms
  (Python 3.12.13, Windows, 2026-11-15).
- A call's params are encoded once: the size cap's orjson bytes are spliced into the message
  (`orjson.Fragment`) instead of `Envelope.dumps` encoding them again (FO-21). Size cap plus
  message, median: a 2 MB image request 16.3 -> 9.4 ms, a 200 KB text request 70 -> 47 us, a
  4 KB chat 10.0 -> 9.3 us (micro-benchmark, Python 3.12.13, Windows, 2026-11-15).

### Packaging and CI

- Dependencies: `aiohttp>=3.10,<4` and `yarl>=1.12,<2` are declared (the default Bedrock
  transport imports them directly); `uuid-utils` now allows 1.x (`<2`, tested on 1.2.0); the
  floors are raised to the versions CI's lowest-versions job really tests (`aiobotocore>=2.24.3`,
  `openai>=1.109`); `uvloop` gained an upper bound (`<1`).
- Python 3.14 is supported (unit and integration tests run on 3.11-3.14 in CI).
- The sdist include list is anchored (a stray `samples/README.md` no longer ships).
- CI builds the package on every pull request (build, `twine check --strict`, install the wheel
  in a clean venv, import, `llm-queue --help`), every job has a timeout, and a new push cancels
  the old pull-request run. Docs deploys share one group, so an older build never lands last.
- The publish workflow no longer freezes a version after a TestPyPI dry run: TestPyPI keeps the
  first upload of a version, so when that copy is older than the tag's build the smoke test
  installs the built wheel instead (with a warning) and PyPI gets the new build.
- Renovate also updates the tool pins inside workflow steps and refreshes the lock file.

### Security

- Strict mode by default (see Changed); debug capture refused in every non-dev environment.
- TLS check bypass with repeated URL parameters closed; certificate and host-name checks written
  into the store URL explicitly.
- `base_url`: user info and keys in the URL refused; IPs read in every form a resolver accepts;
  link-local, `fd00:ec2::/32`, unspecified and multicast addresses refused; allowed hosts
  required when strict.
- Account secret references limited to the `LLMQ_ACCOUNT_` prefix.
- HTTP write routes: app auth required, JSON bodies and the `X-LLMQ-Admin` header (no cross-site
  posts); audit actors checked for length and control characters.
- Safe mode protects `model_fallback` (residency could be routed around) and `inference.extra`
  (an allow-list replaces any field, such as `store: true`).
- Cached clients follow account changes in the registry (keyed on the account's fields).
- S3 offload keys hashed and namespaced, objects deleted after read, KMS and bucket-owner
  options.
- Dead letters (which hold prompts) are trimmed; tenant ids hashed in logs; the id salt hidden
  from `repr()`; release build tools pinned.

## [0.1.0b1] - 2026-11-14

First beta, for test runs (`pip install --pre llm-queue`).

### Added

- **Queued LLM calls on Valkey / Redis.** `LLMQueue.from_env()` runs calls to Amazon Bedrock
  and OpenAI-compatible servers (vLLM) through a queue per model (a normal and a priority
  stream) on one Redis-protocol store. Every pod runs the same code as caller and worker
  (`LLMQ_MODE`: `both`, `caller`, `worker`); budgets, waiter order and health live in the
  store, so one pod and fifty behave the same.
- **Budgets per deployment** (model x account x region): RPM and TPM (`smooth` GCRA or `fixed`
  per minute), account budgets shared by models, concurrency slots learned from throttles,
  priority reserve and normal minimum share. Admission is one atomic store script: choose a
  deployment (`ordered`, `round_robin`, `weighted`, `headroom`, `custom:`), take every budget
  or none, keep waiters in arrival order.
- **Routing and recovery:** fallback regions and fallback models by cause (`exhausted`,
  `broken`, `error`) after a wait; retries with backoff, a per-model retry budget (moving to a
  deployment the call has not tried is free) and one deadline per call; circuit breaker per
  deployment (calls wait for its probe); health with auto-disable, probes and a last-healthy
  safeguard; slow deployments become last choices; credentials judged per account and region.
- **Registry** (`LLMQ_REGISTRY`): accounts (AWS by pod identity or assumed role,
  OpenAI-compatible endpoints), models with confirmed and fallback deployments, residency,
  routing (kill switch, `queue_pct`, blocked regions), every setting at four levels with
  types, ranges and defaults, validated with every problem listed. Stored in the store with a
  version rule, live edits with audit, rollback and restore after a store reset.
- **Calls:** `converse` / `chat`, `converse_stream` (events through a chunk stream;
  `CallFailed.partial_text` on a cut), `embed` (batched per model, vectors in order), sync
  calls (`*_sync`, `close_sync`), jobs (`register_job` / `send_job`, at least once),
  `request_context` (priority, session, user, tenant), duplicate request ids run once,
  cancel, large results in S3. LangChain: `QueuedChatBedrockConverse`,
  `QueuedBedrockEmbeddings`, `QueuedChatOpenAI`, `QueuedOpenAIEmbeddings`.
- **Errors:** every failed call is `CallFailed` with a `kind` (`rate_limited`,
  `deadline_exceeded`, `model_unavailable`, `queue_unavailable`, ...) and `retry_after_secs`
  where a retry can work; admin operations raise `StoreUnavailable`; bad configuration is
  `ConfigError` (`UnknownModel`, `MissingExtra`). Everything raised is an `LLMQueueError`.
- **Store resilience:** reconnect on `READONLY` and dropped connections (failover), a read
  timeout that is never retried, a blocking connection pool, `store_down` rules (`pause`,
  `static_cap`, `fail_fast`), callers kept alive through a store outage, calls held past their
  deadline let go, optional separate queue store with failover to the direct path, optional
  `WAIT` for replicas.
- **Tuning in code:** `Tuning` (`StoreOptions`, `BedrockOptions`, `EngineTimings`,
  `RegistryOptions`) passed to `LLMQueue.from_env(tuning=...)`: every timeout, pool size and
  pacing value (admission scripts included) with a measured default, validated at start-up.
- **Operations:** status and request status, alerts, metrics (CloudWatch EMF, Prometheus), one
  `llmq_call_finished` record per call, `health()` with per-store state, `pause_receiving()`
  for a preStop hook and a bounded `stop(drain_secs=)`, the `llm-queue` CLI (status, registry,
  settings, block, drain, dead letters, queues) and a FastAPI router (`http` extra).
- **Testing:** fakellm (a fake Bedrock / OpenAI server), `scripts/check.py` (lint, types, tests
  per step), fault scenarios by call count (`scripts/scenarios.py`: worker, store, provider,
  registry, caller and multi-client faults under three loads), load and chaos gates, a soak
  with chaos, a store failover drill, and runnable samples on fakellm or real Bedrock.
- **Docs site** (Zensical on GitHub Pages): tutorial, how-to guides, explanation with
  diagrams, an API reference from the docstrings.

[Unreleased]: https://github.com/techiewonk/llm-queue/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/techiewonk/llm-queue/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/techiewonk/llm-queue/compare/v0.1.0b3...v0.1.1
[0.1.0b3]: https://github.com/techiewonk/llm-queue/compare/v0.1.0b2...v0.1.0b3
[0.1.0b2]: https://github.com/techiewonk/llm-queue/compare/v0.1.0b1...v0.1.0b2
[0.1.0b1]: https://github.com/techiewonk/llm-queue/releases/tag/v0.1.0b1
