# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[semantic versioning](https://semver.org/); while it is 0.x, a minor release may break the
public API (see STABILITY.md).

## [Unreleased]

## [0.1.0b2] - unreleased

Fixes from three code reviews and a six-part rescan of 0.1.0b1, a process-level client hub, and
behaviour decisions for production. No backward compatibility is kept with 0.1.0b1 (beta).

### Changed (behaviour)

- **Bedrock calls go over aiohttp by default** (`BedrockOptions(transport=...)`: `"aiohttp"`, or
  `"botocore"` for the aiobotocore client). The body is built and the reply parsed with orjson
  and sent on one aiohttp session per client; botocore still resolves and refreshes
  credentials, signs (SigV4) and decodes stream frames. Measured on fakellm (three runs of 3-4
  alternating rounds): Converse at 20 / 200 concurrent calls +78-104 % calls/s, client CPU per
  call -55-61 %, p99 -27-44 %; embeddings +12-25 % calls/s; stream first event -18-23 %; memory
  per client 669 -> 87 KiB. All nine samples pass on real Bedrock with it. Differences: Converse
  fields botocore does not know are sent (Bedrock refuses them) instead of dropped; FIPS
  endpoints and `AWS_ENDPOINT_URL_*` are not read (`AWS_CA_BUNDLE` and proxy variables are).
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
  below one call's reservation, fallback models across providers, fallback cycles of any length
  (unless every step requires headroom), fallback targets outside the source's residency and
  `breaker_probe_secs` below `attempt_timeout_secs` are refused; `queue.retention_secs` and
  `dlq_retention_secs` are capped at 14 days; `inference.extra` takes an allow-listed set of
  names only; `min_lib_version` must be a version.
- Alert settings `quota_used_mins`, `spill_pct`, `spill_window_secs` and `waiting_age_secs` are
  removed (they did nothing); `quota_used_pct` is 1-100 and also checks TPM; `dlq_messages` is
  at least 1.
- CLI: `dlq purge`, `dlq redrive`, `drain` and `registry gc` need `--yes` (or `--dry-run`);
  `registry gc` also removes the queues of removed models.
- Safe live-edit mode also protects `model_fallback` and `inference.extra`.
- Performance: <!-- filled at release -->

### Added

- **Client hub:** one SDK client per (event loop, credential identity, region, options) per
  process, shared by every queue and the sync path, reference-counted; clients for every
  deployment built concurrently at start; warm connections (unsigned `HEAD` requests, refreshed
  every 10 s); pool size per client from the registry's concurrency slots (10 to 200);
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
  faults to 400 and exhausted retries to 502 / 503 with `retry-after`.
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
  the LangChain adapters all meet it; `converse_stream` on an embedding model raises
  `ConfigError`.
- A stream's open and its first event share one first-response budget (a slow open gave the
  first event a whole budget again); a Bedrock stream cut mid-body (`ClientPayloadError`)
  counts as a connection reset.
- `result_dedupe_secs > 0` with an S3 offload that deletes on read is refused (`ConfigError`):
  the second reader found the object gone.
- Docs: eight statements that did not match the code (readiness probe, invalid stored registry,
  older services, the `all` live-edit mode, account kinds, `ConfigError` timing, the exception
  tree).
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

[Unreleased]: https://github.com/techiewonk/llm-queue/compare/v0.1.0b2...HEAD
[0.1.0b2]: https://github.com/techiewonk/llm-queue/compare/v0.1.0b1...v0.1.0b2
[0.1.0b1]: https://github.com/techiewonk/llm-queue/releases/tag/v0.1.0b1
