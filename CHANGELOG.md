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
  returns a `BedrockClient` protocol; the at-fork hook is registered on first use, not at import.
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
- `LLMQueue.stop()` skipped every later close step when one failed, and a second `stop()` did
  nothing, so store pools, provider sessions and the S3 client leaked; every step now runs and
  the first failure is raised at the end. `start()` is serialised (two concurrent starts doubled
  every background task) and a start that fails part-way stops what it started.
- Calls, streams, jobs and sync calls after `stop()` raise a clear `LLMQueueError` (they failed
  on a closed pool, and a sync call built a new private engine that nothing closed). A sync
  call cancelled by `close_sync()` raises `CallFailed("cancelled")`, not
  `concurrent.futures.CancelledError`; a sync call to a loop that was stopped but not closed
  fails with `CallFailed("worker_shutdown")` instead of blocking forever. Offload and sync-engine
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
  twice; both are rare and bounded, and are documented in `store_client.py` (EN-5).

### Performance

- The CloudWatch (EMF) sink writes its lines from a background thread instead of on the event
  loop: with a stdout that takes 5 ms per line (a backed-up log pipe), a metric point held the
  loop p50 5,346 us / p99 6,544 us; queued it takes p50 3.8 us / p99 30 us (Python 3.12,
  Windows, 2026-11-15). Lines past 10,000 waiting are dropped with one warning (EN-20).
- Log calls below the logger's level are dropped before the message is built: a disabled `info`
  call 9.2 -> 5.3 us (Python 3.12.13, Windows, 2026-11-15).
- `alias_of(model_id)` is one dict lookup instead of a scan of every model: 2674 -> 241 ns with
  55 models (Python 3.12.13, 2026-11-15).

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
