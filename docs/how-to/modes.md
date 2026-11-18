# Queue and local modes

An app makes the same calls whichever way they run: `llmq.converse`, `chat`, `call`,
`converse_stream`, `chat_stream`, `embed` and the `*_sync` methods, the LangChain queued
adapters and `llm_queue.fastapi.install()`. How they run is a deploy-time choice, from the
shared queue to plain in-process calls, so an app needs no second, direct code path to fall
back on.

## The switch ladder

From the normal setup to the furthest step away from the store, each step undone by reversing
it:

| Step | How | What changes | Undo |
|---|---|---|---|
| 1. Queue (normal) | `LLMQ_EXECUTION_MODE=queue` (the default) | Calls go through the store's queues; every pod shares the budgets, breakers, fairness and request records | - |
| 2. Canary | Registry `queue_pct` below 100 on a model (live edit) | That share of calls (by request id) runs direct in the calling pod, still under the shared budgets in the store | Set `queue_pct` back to 100 |
| 3. Kill switch | Registry `routing.kill_switch: true` (live edit) | Every call runs direct in the calling pod; the budgets, breakers and request records are still the store's | Set it back to false |
| 4. Local | `LLMQ_EXECUTION_MODE=local` and a restart | Every call runs in its own process against the provider; the store is never contacted (no client is built) | Set `queue` (or remove it) and restart |
| 5. Off | `LLMQ_ENABLED=false` and a restart | The library does nothing: the factories return None, so the app runs its own direct path (`chat_model(...) or ChatBedrockConverse(...)`); a direct `llmq.converse()` raises `QueueDisabled`; `install()` mounts the two probes only (`enabled: false`); the drain file still fails readiness | Remove it (or set `true`) and restart |

`LLMQ_ENABLED` is read from the environment, then `defaults=`, never from a secret and before
any secret: step 5 builds and reads no secrets source, so it works when Secrets Manager is the
problem. `LLMQ_CONFIG_ERROR_MODE=disable` takes step 5 on its own when `from_env()` meets a
`ConfigError` (a bad setting, a secret that cannot be read): it logs `llmq_config_failed` once
and returns the same disabled queue, with `disabled_reason: config_error` (step 5 by hand says
`switch`) in `health()` and `describe()`. The pod starts and serves the app's direct path instead
of crashing; fix the setting and restart. `disable` is the default; `raise` stops the start
instead (a pod that must not run without its queue).

Steps 2 and 3 need the store: they move where a call runs, not where its budget lives. Step 4
is for when the store itself is the problem (lost, being migrated, not yet provisioned) or for
a service that does not want one (a small tool, a test environment, a single-process job).

When to use which:

- A worker problem (receive loops stalled, a bad deploy of the worker side): step 3. The
  calling pods keep serving with the shared budgets.
- Trying the queue on part of the traffic, or ramping it up: step 2.
- The store is down for long or must be replaced: step 4, then back to step 1 once a store is
  there again.
- The library itself is in doubt (a bug in it, an incident you want it out of): step 5. Only an
  app that keeps its own direct path behind the factories (`chat_model(...) or <direct>`) can
  take it; code that calls `llmq.converse()` directly gets `QueueDisabled` instead. A model's `store_down: static_cap` does the same for short outages on its own,
  per call, with the same local executor.

## Local mode

```bash
LLMQ_EXECUTION_MODE=local
LLMQ_LOCAL_PROCESS_COUNT=8     # pods x worker processes sharing the provider quota (a ceiling)
LLMQ_LOCAL_CONCURRENCY=10      # calls in flight per model in each process
LLMQ_REGISTRY_FILE=/etc/app/registry.yaml
```

`LLMQ_STORE_URL` is not needed (when set it is neither checked nor contacted), and `LLMQ_ROLE`
does not apply (`local_mode_started` logs it once when set). What a call gets, per process:

- **Budgets.** Each deployment's `rpm` / `tpm` (and an account's `shared_rpm` / `shared_tpm`)
  divided by `LLMQ_LOCAL_PROCESS_COUNT`, kept as token buckets with the registry's
  `rate_window` (`smooth`: GCRA with `rate_burst`; `fixed`: per minute) and settled with the
  same refund rule (the real usage after an answer, everything back after a throttle). Set the
  count to the number of processes that share the quota, or the pods together may exceed it.
  The count is a fixed ceiling read at start, not discovered: with autoscaling, set it to the
  most processes that may run at once (fewer running leave part of the quota unused, more go
  over it). `local_mode_started` logs `budgets`, the rpm / tpm per model this process takes.
  A share below one unit a minute (`rpm: 4` over 10 processes) still lets one call through,
  and one call estimated above the share (`tpm` over the process count) is never refused for
  that alone: with `fixed` minutes what went over is carried into the next minutes as a debt
  (the share is paid off each minute), as `smooth` does, so each process stays within its
  share on average (rpm 4 over 10 processes: one call every 3 minutes per process).
- **Concurrency.** At most `LLMQ_LOCAL_CONCURRENCY` calls per model in flight in the process,
  async and sync calls together (the sync calls' private engine shares the cap, the budgets and
  the breakers); a waiting priority call (`priority=True`, `request_context(priority=True)`)
  takes the next free slot.
- **Attempts.** The engine's rules: `attempt_timeout_secs`, `timeout_secs`, `max_wait_secs`
  (`rate_limited` when no budget comes in time), retries with `retry_base_secs` backoff,
  `max_attempts`, the retry budget, `throttle_retry_attempts` with the throttle backoff,
  region fallback (`fallback`), model fallback (`model_fallback`, `allow_fallback`, Converse to
  OpenAI shape conversion), sticky sessions and the `balance` strategies.
- **Health.** Breakers, throttle backoff, target-error auto-disable and failed credentials per
  deployment, in this process's memory. A half-open breaker lets one probe through per
  process; a probe that fails before it reaches the provider (the request cannot be built)
  ends the probe and gives its budget back.
- **Streams and embeddings.** `converse_stream` / `chat_stream` yield the provider's events
  as they arrive (`meta_event=True` ends with the usual `{"llmq": ...}` event); `embed` batches
  by the model's codec and `embed_batch_size`.
- **Ids and metrics.** Request ids in every log line, `Meta` with `provider_request_id`,
  `llmq_call_finished` and `llmq_calls_total` with `path="local"`.

Not available, because they live in the store: request records and status by id, the
`llmq.admin` operations on requests, tenants (`tenants.list`, a tenant's status), purges,
registry versions, live edits, publish, rollback and the audit, models / deployments / blocks /
queues views, jobs and dead letters, and fairness across pods. Each raises `LocalModeRefused`
(a `ConfigError`, category `config_error`); over HTTP the route answers 503 with
`"reason": "local_mode"`, on the CLI exit 1. `llmq.admin.status.get`, `ops.index`,
`describe()`, `drain.*`, `registry.get` / `diff` / `schema` and `tenants.hash` answer from
this process. To change a model in local mode, change the registry document and restart.

## Off (`LLMQ_ENABLED=false`)

`LLMQueue.from_env()` returns a `DisabledQueue` (`llmq.enabled` is False) and reads nothing else
but `LLMQ_ENVIRONMENT`, `LLMQ_DRAIN_FILE` and `LLMQ_DRAIN_POLL_SECS`. On each surface:

| Surface | With the library off |
|---|---|
| Python | `start()` / `stop()` run the drain file watcher only and log `llmq_disabled` once; `converse`, `chat`, `call`, the streams, `embed`, the `*_sync` calls, jobs, `status()` and `request_status()` raise `QueueDisabled`; `health()` and `describe()` say `enabled: false`; `ready` and `queue_ready()` are true unless draining; `llmq.admin` serves `describe`, `ops`, `registry.schema` and `drain.*`, every other operation raises `QueueDisabled` |
| LangChain | `chat_model()` / `embeddings()` return None (with no queue given they read `LLMQ_ENABLED` from the environment) |
| HTTP | `install()` / `mount()` mount `GET /health/live` (`{"live": true, "enabled": false, "disabled_reason": "switch"}`, or `"config_error"`) and `GET /health/ready` (200, or 503 while draining) only, whatever the router options; the context middleware is still added |
| CLI | `llm-queue describe` answers `enabled: false`; every store command exits 1 with the `QueueDisabled` message |

## HTTP routes and probes

`install()` and `http_router()` mount every route in both modes. In local mode:

| Route | Answer |
|---|---|
| `/health/live`, `/health/ready` | 200 with `"mode": "local"` (ready: started, registry loaded, not draining) |
| `/status`, `/status?model=` | This process's view: per model the slots in use and waiting, per deployment its local `rpm` / `tpm` (limit, used, left), calls in flight, breaker, latency, throttles |
| `/` | The admin index: `library.execution_mode` is `local`, `process` from this process, per model its slots and usable deployments; `stores`, `jobs` and `purges` are null |
| `/describe` | `execution_mode: local`, the local settings; no store fields |
| `/registry`, `/registry/diff`, `/registry/schema`, `POST /tenants/hash` | The document (secrets hidden); `live_edits` is empty |
| `/drain`, `POST /drain`, `POST /resume` | As in queue mode: a dry run unless `{"dry_run": false}` (not audited: the audit is in the store) |
| `/requests*`, `/tenants`, `/purge/*`, `/registry/versions`, `POST /registry*`, `/audit`, `/models*`, `/blocks*`, `/queues`, `/jobs`, `/dlq*` | 503 `{"reason": "local_mode"}` |

A route's guards answer before the local-mode refusal, as in queue mode: a request without a
valid token is 403; a write route is mounted only with `allow_writes=True` (404 otherwise),
and with `dependencies` alone a write without the `X-LLMQ-Admin` header is 403. A request that
passes them gets the 503 above.

## Drain

Unchanged: `LLMQ_DRAIN_FILE` (or `POST /drain`) fails readiness, so the load balancer stops
sending; calls in flight finish, and new calls from them are still served. `stop()` and
`drain()` wait up to `LLMQ_DRAIN_SECS` for the calls in flight (each call, not the request
handler that made it: a handler that goes on streaming its answer does not hold the drain),
then `stop()` cancels the calls still running with `worker_shutdown`. A call its own caller
cancels (a client that went away) stays a plain cancel, during a shutdown too.
