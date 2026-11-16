# What's new in 0.1.0b2

0.1.0b2 fixes what three code reviews and a rescan of 0.1.0b1 found, and settles how the queue
behaves in production. It keeps no backward compatibility with 0.1.0b1 (a beta): read
[Upgrade from 0.1.0b1](how-to/upgrade.md) before you deploy it, and drain every 0.1.0b1 worker
first (mixed fleets are not supported). It needs Python 3.12 or newer. The full list is in the [Changelog](project/changelog.md).

## Behaviour you can rely on

| Topic | What 0.1.0b2 does |
|---|---|
| Delivery | **At least once** for calls and jobs: a call whose record the store lost is sent again, and jobs survive a store failover. Make side effects idempotent |
| Request ids | Retrying an id after a failed end **runs the call again**; only a success is replayed |
| Cancel | **Cancel wins**: any caller's cancel stops the call for every caller sharing its id, and an attach after a cancel is cancelled too. Anyone may cancel any id; every cancel is logged with its source, and `llm-queue cancel ID --yes` is an audited admin cancel |
| Sync calls | A `*_sync` call (or LangChain `invoke`) on the queue's own event loop is **refused** with an `LLMQueueError` that names the fix (`await llmq.converse(...)`, `await chat.ainvoke(...)`) |
| Store outages | An answer in hand is kept and retried until the call's deadline. `fail_fast` and `static_cap` answer within about 100 ms of a known outage. `NOREPLICAS`, `OOM`, `MISCONF` and `BUSY` count as the store being down |
| Strict mode | `LLMQ_ENVIRONMENT` **unset is strict** (TLS store, a 16-character `LLMQ_ID_HASH_SALT`, an explicit `LLMQ_KEY_PREFIX`, `https` endpoint overrides). Set `dev`, `local`, `test` or `ci` to relax it. An evicting store fails readiness |
| Replica waits | `LLMQ_QUEUE_WAIT_REPLICAS` covers call sends, job sends and registry live edits, each switchable with `LLMQ_QUEUE_WAIT_SCOPES` (all on by default). Too few replicas: the write goes anyway, with a warning at most once a minute and a metric |

## New features

| Feature | Where to read |
|---|---|
| **Per-tenant fairness**, off by default: weighted shares with lending, hard per-tenant quotas, or round robin between tenants | [Tenant fairness](explanation/fairness.md), registry setting `fairness` |
| **Background jobs** with a context (`current_job()`: job id and attempt), deduplicated `send_job(job_id=...)` for 24 h, exponential retries for about an hour, dead letters kept 7 days (`job_dead_letters`, `redrive_jobs`), per-name `register_job(..., concurrency=N)` | [Run background jobs](how-to/jobs.md) |
| **Embedding codecs**: `register_embedding_family` for a new model family, a per-model `family` field, `embed_max_chars`, a refused batch split so only the bad text fails | [Make calls](how-to/call.md#embedding-families) |
| **LangChain streaming**: `queue_streaming=True` on the chat adapters (off by default) streams text, tool-call deltas and usage | [LangChain sample](samples/langchain.md) |
| **Monitoring**: histograms in seconds (`llmq_provider_latency_seconds` replaces `llmq_provider_latency_ms`), new counters and gauges, alerts `breaker_open`, `kill_switch_on`, `queue_stalled`, `GET /metrics` from `llmq.http_router(metrics_route=True)` | [Monitor](how-to/monitor.md) |
| **Every log event listed** with its level and module | [Log events](reference/log-events.md) |
| **Error kinds as a type**: `llm_queue.Kind`; oversize calls fail `request_too_large` | [Errors](reference/errors.md) |
| **Cross-provider model fallback**: a Bedrock model may fall back to an OpenAI-compatible one and back; requests and replies are converted | [Configuration](reference/configuration.md#fallback-to-a-model-of-another-provider) |
| **Registry JSON Schema**: `llm-queue registry schema`, and a page on the document's shape with a complete example | [Registry document](reference/registry.md) |
| **Faster transports**: Bedrock over aiohttp by default (measured +78-104 % calls/s against botocore), OpenAI-compatible calls through the SDK's aiohttp client | [Tune](how-to/tune.md), [Clients and connections](explanation/clients.md) |

## Safer configuration

- Every registry number is bounded (at most 2**53 - 1; timeouts and waits at most a day), and
  `NaN`, trailing newlines and malformed role ARNs are refused with their path.
- Queued messages, which hold the prompt, are kept **one day** by default (`queue.retention_secs`,
  at most two weeks).
- Protected registry fields in the stored document must match each pod's deploy config, or the
  stored document is refused.
- `timeout_secs` and `default_output_tokens` are honoured at account (and deployment) level.
- Every setting, with its default and range, is in [Configuration](reference/configuration.md)
  and [Environment variables](reference/environment.md); a test keeps those pages in step with
  the code.
