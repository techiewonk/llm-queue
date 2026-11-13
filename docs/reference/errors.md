# Errors

Everything raised to callers is an `LLMQueueError`. A failed call raises `CallFailed` with a
`kind`; bad configuration raises `ConfigError`; an admin operation the store cannot serve raises
`StoreUnavailable`.

```text
LLMQueueError
├── CallFailed            a call ended without a reply; branch on .kind
├── ConfigError           invalid configuration (start-up or a refused edit, never mid-call)
│   ├── RegistryError     an invalid registry document; .problems lists every problem
│   ├── UnknownModel      a model name not in the registry (also a KeyError)
│   └── MissingExtra      an optional feature's extra is not installed (also an ImportError)
└── StoreUnavailable      the store cannot serve an admin operation (also a ConnectionError)
```

## CallFailed

```python
class CallFailed(LLMQueueError):
    kind: str                      # one of llm_queue.KINDS
    retry_after_secs: float | None # set where a later retry can work
    partial_text: str | None       # a stream cut mid-answer
    request_id: str | None
```

| Kind | Meaning | What a caller should do |
|---|---|---|
| `rate_limited` | No budget before `max_wait_secs` or the deadline, the provider kept throttling past `throttle_retry_attempts`, or a fallback model was busy | Retry after `retry_after_secs`, or tell the user to wait |
| `queue_full` | The model's queue already holds `max_waiting` (50,000) calls; `retry_after_secs` is 5 | Retry later; shed load |
| `deadline_exceeded` | No reply within the call's timeout (also: the store did not answer in time, or retries ran out of time) | Retry once, or raise `timeout_secs` for long answers |
| `request_too_large` | The input estimate is above every candidate deployment's `tpm` | Shorten the input or split it; retrying will not help |
| `no_deployment` | The call's own filters (`allowed_regions`, `allowed_accounts`) leave no deployment | Fix the filters or the registry |
| `model_unavailable` | No usable deployment: all disabled, unhealthy or outside residency; `retry_after_secs` is the time to the next probe | Use another feature or model, or retry after `retry_after_secs` |
| `model_disabled` | The model is disabled and has no fallback model to use | Use another model; an operator turned it off |
| `queue_unavailable` | The store is unreachable, read-only, out of memory, or the model's admission numbers are missing (`store_down: fail_fast`, or `pause` that ran out of time); a stream needs the store | Retry later; check the store |
| `credentials_unavailable` | Only accounts whose credentials were refused are left; `retry_after_secs` is 60 | Check the account's credentials / role; retry later |
| `retry_budget_exhausted` | Provider errors on every deployment, and the model's retry budget is spent | Retry later; the provider is failing |
| `provider_error` | A fatal provider error, or `max_attempts` reached (also an embedding reply with no body) | Show an error; check the request if it repeats |
| `request_id_conflict` | Your `request_id` was used before with other params or another user | Use a new id |
| `result_expired` | Your `request_id` finished earlier and its result is gone | Use a new id, or keep results longer (`LLMQ_RESULT_TTL_SECS`) |
| `dead_lettered` | The call failed `max_receives` (10) times (workers died holding it) and went to the dead-letter queue | Report it; see `llm-queue dlq list` |
| `worker_lost` | A stream ended without its end entry or had a gap (its worker was lost) | Retry; `partial_text` holds the text so far |
| `cancelled` | The call was cancelled (`cancel()`, or its caller process was gone) | Nothing, unless you did not cancel it |
| `rejected` | The app's `admission` hook rejected the call before it was sent | Handle as your hook intends |

The set of kinds is `llm_queue.KINDS`. Over the OpenAI-compatible HTTP transport the LangChain
OpenAI adapters use, kinds map to status codes (for example `rate_limited`, `queue_full` and
`retry_budget_exhausted` to 429, `deadline_exceeded` to 504).

## StoreUnavailable

Raised by `cancel`, `status`, `request_status`, `send_job` and the registry's reads and edits
when the state store is unreachable, read-only (after a failover) or full. Calls never raise it;
they raise `CallFailed` (`queue_unavailable`). It is also a `ConnectionError`. Retry after the
store is back; see [Operate](../how-to/operate.md#store-failover).

## ConfigError

Invalid configuration: bad `LLMQ_*` settings or `Tuning` at `from_env`, an invalid registry at
start-up, or a refused registry edit. Raised at start-up or on the edit, never in the middle of a
call. The message lists every problem at once. Fix the configuration; retrying will not help.

- **`RegistryError`**: an invalid registry document; `.problems` and `.warnings` list each one
  with its path.
- **`UnknownModel`**: a model name that is neither an alias nor a model id in the registry. Also a
  `KeyError` (the HTTP router answers 404).
- **`MissingExtra`**: an optional feature's dependencies are not installed. The message names the
  extra: `pip install 'llm-queue[<extra>]'`. Also an `ImportError`.
