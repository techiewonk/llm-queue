# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[semantic versioning](https://semver.org/); while it is 0.x, a minor release may break the
public API (see STABILITY.md).

## [Unreleased]

## [0.1.0b2] - unreleased

Fixes from three code reviews and a six-part rescan of 0.1.0b1, a process-level client hub, and
behaviour decisions for production. No backward compatibility is kept with 0.1.0b1 (beta).

### Upgrading from 0.1.0b1

Every change an app pinned to 0.1.0b1 may need, as a checklist; each item is described in full
below, and [Upgrade from 0.1.0b1](docs/how-to/upgrade.md) has the same list with tables.

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
   `RateLimitError` / `BadRequestError`); the adapters refuse the Responses API and do not
   stream.

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
  `LLMQ_STORE_MAX_CONNECTIONS` 4-100000, `LLMQ_STATUS_TTL_SECS` at least 60,
  `LLMQ_QUEUE_WAIT_REPLICAS` at most 16, TTLs within 2147483647 s, and others listed in
  `docs/reference/environment.md`; an empty value counts as unset; `LLMQ_DRAIN_SECS=0` is valid.
  Whole-number variables say "must be a whole number" for `200.0` (R3CF-17).
- **Strict mode wants an `LLMQ_ID_HASH_SALT` of at least 16 characters** (FO-13): a
  one-character key does not stop brute-forcing hashed phone numbers.
- **`LLMQ_KEY_PREFIX` and `LLMQ_QUEUE_PREFIX` are plain names** (FO-20): letters, digits, `_`,
  `.`, `-` (`registry gc` split keys on `{m:` / `{q:` and mis-attributed them). Entries of
  `LLMQ_BASE_URL_ALLOWED_HOSTS` with a scheme, port or path are refused by name (R3CF-13).
- **Registry checks added** (R3CF-10, R3CF-12): a fallback `after_secs` that could never fire
  inside `timeout_secs` is an error; the weight-0 check covers `balance_priority` and counts
  usable deployments only. Cross-field messages show values and levels and use the dotted
  override path `settings set` takes (R3CF-17).
- **A fallback model of another provider is allowed** (owner Q40, R3CF-6): the changelog and
  docs said it was refused; it never was. The request is not converted between shapes yet, so
  the validator warns. A disabled fallback target and unknown `blocked_regions` stay warnings.
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
- Performance: <!-- filled at release -->
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
  [Tenant fairness](docs/explanation/fairness.md).
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
  Q15, Q25): [Monitor](docs/how-to/monitor.md) lists every metric with kind, labels, unit and
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
  failure (FE-5); a caller-only process stops its direct run on cancel (FE-17); an unreadable
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

[Unreleased]: https://github.com/techiewonk/llm-queue/compare/v0.1.0b2...HEAD
[0.1.0b2]: https://github.com/techiewonk/llm-queue/compare/v0.1.0b1...v0.1.0b2
[0.1.0b1]: https://github.com/techiewonk/llm-queue/releases/tag/v0.1.0b1
