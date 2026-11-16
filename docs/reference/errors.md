# Errors

Everything raised to callers is an `LLMQueueError`. A failed call raises `CallFailed` with a
`kind`; bad configuration raises `ConfigError` (or one of its subclasses); an admin operation the
store cannot serve raises `StoreUnavailable`.

```text
LLMQueueError
├── CallFailed            a call ended without a reply; branch on .kind
├── ConfigError           invalid configuration or a call that can never work; do not retry
│   ├── RegistryError     an invalid registry document; .problems lists every problem
│   ├── RegistryConflict  a live edit or publish lost the race with other writes every time
│   ├── LiveEditRefused   a live edit touched fields that change only with a deploy
│   ├── UnknownModel      a model name not in the registry (also a KeyError)
│   └── MissingExtra      an optional feature's extra is not installed (also an ImportError)
└── StoreUnavailable      the store cannot serve an admin operation (also a ConnectionError)
```

A plain `LLMQueueError` is raised for a queue object used where it cannot work: a call from a
forked child of the process that started the queue, an async call from another event loop, a
sync call on the queue's own event-loop thread, or `start()` on a stopped queue. See
[Clients and connections](../explanation/clients.md#one-llmqueue-per-process).

## CallFailed

```python
class CallFailed(LLMQueueError):
    kind: Kind                     # llm_queue.Kind, a Literal of every kind (KINDS as a set)
    retry_after_secs: float | None # set where a later retry can work
    partial_text: str | None       # a stream cut mid-answer
    request_id: str | None
```

| Kind | Meaning | What a caller should do |
|---|---|---|
| `rate_limited` | No budget before `max_wait_secs` or the deadline, the provider kept throttling past `throttle_retry_attempts` (or the retry budget ran out with no untried deployment left), a fallback model was busy, the tenant's `fairness` quota is used up, or the store connection pool is full (`retry_after_secs` 1) | Retry after `retry_after_secs`, or tell the user to wait |
| `queue_full` | The model's queue already holds `max_waiting` (50,000) calls, or `send_job` found 10,000 jobs of that name waiting; `retry_after_secs` is 5 | Retry later; shed load |
| `deadline_exceeded` | No reply within the call's timeout (also: the store did not answer in time, or retries ran out of time) | Retry once, or raise `timeout_secs` for long answers |
| `invalid_request` | The request itself is wrong: an embedding text is empty, not a string or longer than `embed_max_chars` (with its index), a bad Cohere `input_type`, the provider refused the request as invalid, a `max_tokens` / `max_completion_tokens` is not a whole number on a model with a cap, `on_exceed: reject` refused its inference values, OpenAI `params` / `extra_body` / `extra_headers` touch a protected field (`model`, `max_tokens`, `stream`, `Authorization`, ...), a second `stream()` on the same id, a streaming call to an embedding model, or a `send_job` payload that is not JSON | Fix the request; retrying will not help |
| `context_length_exceeded` | The input does not fit the model's context window | Shorten the input |
| `request_too_large` | The call is too big: its input estimate is above every candidate deployment's `tpm`, or its message is above the model's `max_message_bytes` (4 MB), or a `send_job` payload is above `LLMQ_JOB_MAX_PAYLOAD_BYTES` (1 MB) | Shorten the input or split it, or raise `max_message_bytes`; retrying will not help |
| `no_deployment` | The call's own filters (`allowed_regions`, `allowed_accounts`) leave no deployment | Fix the filters or the registry |
| `model_unavailable` | No usable deployment: all disabled, blocked, unhealthy or outside residency; `retry_after_secs` is the time to the next probe | Use another feature or model, or retry after `retry_after_secs` |
| `model_disabled` | The model is disabled and has no fallback model to use | Use another model; an operator turned it off |
| `queue_unavailable` | The store is unreachable, read-only, out of memory, or the model's admission numbers are missing (`store_down: fail_fast`, or `pause` that ran out of time); a stream needs the store | Retry later; check the store |
| `credentials_unavailable` | Only accounts whose credentials were refused are left; `retry_after_secs` is 60 | Check the account's credentials / role; retry later |
| `retry_budget_exhausted` | Provider errors on every deployment, and the model's retry budget is spent | Retry later; the provider is failing |
| `provider_error` | A fatal provider error, or `max_attempts` reached (also an embedding reply with no body) | Show an error; check the request if it repeats |
| `request_id_conflict` | Your `request_id` was used before with other params or another user | Use a new id |
| `result_expired` | Your `request_id` finished earlier and its result is gone, or a result offloaded to S3 could not be read back | Use a new id, or keep results longer (`LLMQ_RESULT_TTL_SECS`) |
| `dead_lettered` | The call failed `max_receives` (10) times (workers died holding it) and went to the dead-letter queue | Report it; see `llm-queue dlq list` |
| `worker_lost` | A stream ended without its end entry or had a gap (its worker was lost) | Retry; `partial_text` holds the text so far |
| `worker_shutdown` | A stream had already sent chunks when its worker's drain ran out (a call that has sent nothing is given back to the queue instead and does not fail) | Retry; `partial_text` holds the text so far |
| `cancelled` | The call was cancelled: `cancel()` by any caller of its id (cancel wins for every caller, and an attach after a cancel is cancelled too), `llm-queue cancel`, or its caller process was gone | Nothing, unless you did not cancel it |
| `rejected` | The app's `admission` hook rejected the call before it was sent, or the hook raised, or the registry names one this process has not registered (fails closed) | Handle as your hook intends; for a failing hook, check `llmq_hook_failed_total` |

The set of kinds is `llm_queue.KINDS`. Over the OpenAI-compatible HTTP transport the LangChain
OpenAI adapters use, kinds map to status codes. Only the request's own faults are 4xx; failures
after the queue gave up are 5xx (with `retry-after` when known), so the SDK and LangChain treat
them as server-side, not as a bad request:

| Status | Kinds |
|---|---|
| 400 | `invalid_request`, `context_length_exceeded`, `request_too_large` |
| 403 / 404 / 409 / 410 / 499 | `rejected` / `model_disabled` / `request_id_conflict` / `result_expired` / `cancelled` |
| 429 | `rate_limited`, `queue_full` |
| 502 | `provider_error`, `worker_lost`, `dead_lettered` |
| 503 | `retry_budget_exhausted`, `no_deployment`, `queue_unavailable`, `model_unavailable`, `credentials_unavailable`, `worker_shutdown` |
| 504 | `deadline_exceeded` |
| 500 | any other kind |

## StoreUnavailable

Raised by `cancel`, `status`, `request_status`, `send_job` and the registry's reads and edits
when the state store is unreachable, read-only (after a failover) or full. Calls never raise it;
they raise `CallFailed` (`queue_unavailable`). It is also a `ConnectionError`. Retry after the
store is back; see [Operate](../how-to/operate.md#store-failover).

## ConfigError

Invalid configuration, or a call that can never work as written. Fix the configuration or the
call; retrying will not help. The message lists every problem at once. It is raised:

- by `from_env`: bad `LLMQ_*` settings or `Tuning`, an unreadable registry file or document;
- by `start()`: an invalid registry, a store in cluster mode;
- by an admin action: a refused registry edit or publish, `http_router(allow_writes=True)`
  without `dependencies`, an empty or malformed audit `actor`;
- by a call, before anything is queued: a model name not in the registry (`UnknownModel`) or a
  feature whose extra is missing (`MissingExtra`).

A call that was accepted never ends in a `ConfigError`; whatever goes wrong after that is a
`CallFailed`.

Subclasses:

- **`RegistryError`**: an invalid registry document; `.problems` and `.warnings` list each one
  with its path. A live edit is checked strictly on the paths it adds or changes, so a typo such
  as `rmp=400` is refused instead of being stored and ignored.
- **`RegistryConflict`**: a live edit (or a publish) raced other writers and lost every retry
  (`RegistryOptions.edit_retries`, 3). Nothing was written; run it again.
- **`LiveEditRefused`**: a live edit changed fields the live-edit mode does not allow (see
  [Live edits](../how-to/configure.md#live-edits)), or live edits are `off`. The message lists
  each refused path and why.
- **`UnknownModel`**: a model name that is neither an alias nor a model id in the registry. Also a
  `KeyError` (the HTTP router answers 404).
- **`MissingExtra`**: an optional feature's dependencies are not installed. The message names the
  extra: `pip install 'llm-queue[<extra>]'`. Also an `ImportError`.
