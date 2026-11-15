# Upgrade from 0.1.0b1

0.1.0b2 keeps no backward compatibility with 0.1.0b1 (both are betas). This page lists every
change an app pinned to 0.1.0b1 may have to make, in the order to make them. The full list of
changes is in [CHANGELOG.md](../project/changelog.md).

## 1. Drain every 0.1.0b1 worker first

Mixed fleets are **not supported**: 0.1.0b1 and 0.1.0b2 pods must not share a queue. A message
carrying bytes (images, documents) is written in a form 0.1.0b1 does not decode, and 0.1.0b1
sends it to Bedrock as is. Before the first 0.1.0b2 pod starts:

1. Stop sending to 0.1.0b1 (or `llm-queue drain M --yes --wait` each model).
2. Let the queues empty, then stop the 0.1.0b1 pods.
3. Deploy 0.1.0b2, then set the models back on (`llm-queue registry set --model M enabled=true`).

## 2. Environment

| Change | What to do |
|---|---|
| `LLMQ_ENVIRONMENT` unset is strict (was dev) | Set `LLMQ_ENVIRONMENT=dev` on laptops and in CI |
| Strict mode requires `LLMQ_KEY_PREFIX` | Set one per app and environment (`myapp-prod`): 1-64 letters, digits, `_`, `.`, `-` |
| Strict mode requires `LLMQ_ID_HASH_SALT` of at least 16 characters | Use a random secret of 32 characters or more |
| Strict mode requires `https` for `LLMQ_BEDROCK_ENDPOINT_URL` / `LLMQ_STS_ENDPOINT_URL` | Use `https` overrides, or none |
| `LLMQ_BASE_URL_ALLOWED_HOSTS` takes bare host names | Drop schemes, ports and paths (`api.openai.com`) |
| Every `LLMQ_*` number has a range; whole-number variables refuse `200.0` | Fix any value the start-up error names; see [Environment variables](../reference/environment.md) |
| `LLMQ_DRAIN_SECS` default 25 (was 45) | Keep `terminationGracePeriodSeconds` above it |
| `LLMQ_CALLER_TTL_SECS` minimum | At least `LLMQ_LEASE_SECS / 3 + max(5, LLMQ_LEASE_SECS / 6)`; the defaults pass |

## 3. Registry document

| Change | What to do |
|---|---|
| `api_key_ref` / `external_id_ref` must start with `LLMQ_ACCOUNT_` | Rename the secrets (and the references) |
| Numbers are bounded: 2**53 - 1 for all, one day for timeouts and waits, tighter per setting | Fix any value the load error names ([Configuration](../reference/configuration.md#registry-settings)) |
| Names (aliases, accounts, regions) are 1-64 characters with no trailing newline; `queue.name` is 1-71 plain characters, not starting `jobs-` | Rename the ones the load error names |
| `queue.retention_secs` defaults to one day (was four) and is applied on every send | Set it on models whose calls may wait longer |
| Removed alerts `quota_used_mins`, `spill_pct`, `spill_window_secs`, `waiting_age_secs` | Delete them; they load with a deprecation warning until 0.2 |
| `inference.extra` takes plain lowercase names only, never SDK arguments | Move refused names into the request |
| `inference.embedding_types` takes only `["float"]` | Drop other types |
| Checks that were warnings are errors (residency leaving no deployment, a fallback `after_secs` that can never fire, ...) | Fix what the load error names |
| A `role_arn` must be an IAM role ARN; padded `model_id` / `profile_id` are refused | Fix the values |
| `min_lib_version` must be a version such as `0.1.0b2` | Fix it or remove it |

## 4. Code

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

## 5. HTTP, CLI and operations

| Change | What to do |
|---|---|
| `GET /health/live` answers 503 when a background loop died or the heartbeat stalled (was always 200) | Keep the liveness probe's `failureThreshold` at 3 or more |
| In strict mode an evicting `maxmemory-policy` fails readiness | Set `noeviction` |
| `http_router(allow_writes=True)` needs `dependencies=`; writes take JSON bodies and the `X-LLMQ-Admin` header | Update admin clients |
| A lost write race answers 409 (was 400); routes hit before the registry loads answer 503 (was 500) | Retry on 409 after `Retry-After` |
| `dlq purge`, `dlq redrive`, `drain`, `registry gc`, `cancel` need `--yes` or `--dry-run` | Add `--yes` to scripts |
| `llm-queue request ID` exits 1 for an unknown id; `drain --wait` exits 3 on timeout (both were 0) | Check exit codes in deploy scripts |
| A `/drain` preStop route must answer GET | Use `@app.api_route("/drain", methods=["GET", "POST"])` as in sample 09 |

## 6. OpenAI transport and LangChain

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
- The adapters do not stream: `astream()` yields the whole answer as one chunk. Use
  `LLMQueue.converse_stream` for tokens as they arrive.
- The first started queue stays the LangChain default (0.1.0b1: the last one).
