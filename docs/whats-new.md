# What's new

## 0.2.0 (unreleased): one admin API

Every admin operation is one method under `llmq.admin`; the `llm-queue` CLI and the HTTP routes
are generated from the same table, so an operation exists on every surface or is documented as
missing. Every write takes an actor, is audited, and is a dry run until you say otherwise.

```python
await llmq.admin.models.disable("gpt120b", actor="ops")                  # the diff it would make
await llmq.admin.models.disable("gpt120b", actor="ops", dry_run=False)   # done, audited
await llmq.admin.dlq.redrive(model="gpt120b", actor="ops", dry_run=False)
print(await llmq.admin.audit.list(count=5))
```

```bash
llm-queue models disable gpt120b          # dry run
llm-queue --actor ops models disable gpt120b --yes
curl -H "X-LLMQ-Token: $READ_TOKEN" https://app/llmq/   # every route and a live summary
```

`LLMQ_ADMIN_READ_TOKEN` opens the read routes, `LLMQ_ADMIN_TOKEN` reads and writes. Old names
(`llmq.purge`, `llmq.registry`, `llmq.cancel`, `job_dead_letters`, `redrive_jobs`,
`PurgeRefused`, CLI `request` / `cancel` / `block` / `drain` / `registry gc` ...) are gone: the
mapping is in [Upgrade](how-to/upgrade.md#5-the-admin-api). The examples in the
0.1.0b3 section below use the names of that release. Read [Admin API](reference/admin-api.md).

Calls in flight are indexed by model and tenant (`LLMQ_REQUEST_INDEX_ENABLED`, on), so one
tenant's calls can be listed, counted, cancelled, moved to the other class or given more time
without touching another tenant's:

```bash
llm-queue requests list gpt120b --tenant acme --state waiting
llm-queue --actor ops requests cancel --model gpt120b --tenant acme --yes   # waiting calls
llm-queue status --model gpt120b --tenant acme
```

Messages carry only keyed hashes of tenant and user ids.

## 0.1.0b3

0.1.0b3 lets llm-queue share a cache store that evicts, and deletes the store state of what you
removed. Pods of 0.1.0b2 and 0.1.0b3 can share a queue on a `noeviction` store; read
[Upgrade](how-to/upgrade.md#from-010b2-to-010b3) before you deploy it on a `volatile-*` store or
roll back. The full list is in the [Changelog](project/changelog.md).

### Key expiry modes: run on a shared `volatile-*` cache

ElastiCache's default policy, `volatile-lru`, evicts keys that have a TTL. With key expiry
`swept`, none of the keys llm-queue needs has one: each expiry is a member of the zset `P:ttl`
and a sweep in every process deletes due keys (up to 10 s late, 50000 keys per 10 s by
default). The default `LLMQ_KEY_EXPIRY=auto` picks `swept` on any policy but `noeviction`, and
the process is ready in strict mode. Switching modes loses nothing: keys move to the new mode on
their next write.

```bash
LLMQ_KEY_EXPIRY=auto      # default: native on noeviction, swept on volatile-*
```

```python
health = await llmq.health()
assert health["key_expiry"] == "swept" and health["ready"]
```

Read [Run on a shared cache store](how-to/shared-cache-store.md) and
[Key expiry modes](explanation/storage.md#key-expiry-modes).

### Purge removed models, deployments and tenants

`PurgeService` (`llmq.purge`, or `PurgeService.from_env()` without a queue) deletes the store
state of a removed model, a removed deployment or a tenant, with a dry run first. Every real
purge is audited. When a publish drops a model or deployment, worker processes purge it
automatically after `LLMQ_PURGE_GRACE_SECS` (900) unless it comes back (`LLMQ_AUTO_PURGE`, on
by default).

```python
plan = await llmq.purge.plan_tenant("acme")                  # what would go
report = await llmq.purge.tenant("acme", actor="ops@example.com")
print(report.as_dict())
```

The same purges from the CLI and HTTP (`POST /purge/tenant` with `allow_writes=True`; `dry_run`
defaults to true):

```bash
llm-queue purge deployment gpt20b aws-main/us-east-1 --dry-run
llm-queue --actor ops purge tenant acme --yes
llm-queue --actor ops registry gc --yes [--no-wait]      # everything the registry no longer has
```

A purge that would delete state still in use raises `PurgeRefused` (HTTP 409). Read
[Purge store state](how-to/purge.md) and the [purge sample](samples/purge.md).

### Store keys reference

Every key, stream and channel llm-queue keeps in the store, with its fields, writers, readers,
lifetime in each key expiry mode, what is lost if it is evicted under each policy, and code
references; also as JSON for tooling.

```bash
python -c "import json; print(len(json.load(open('docs/reference/store-keys.json'))))"
```

Read [Store keys](reference/store-keys.md).

### Eviction drill

`scripts/evict_drill.py` starts a throwaway Valkey with a small `maxmemory`, fills it with "app
cache" keys under queue traffic, and checks that with `volatile-lru` every call answers and not
one llm-queue key is evicted, and that with `noeviction` every call ends by itself.

```bash
python scripts/evict_drill.py                         # both policies, about 15 minutes
python scripts/evict_drill.py volatile-lru --calls 500
```

See [CONTRIBUTING](project/contributing.md).

## 0.1.0b2

0.1.0b2 fixes what three code reviews and a rescan of 0.1.0b1 found, and settles how the queue
behaves in production. It keeps no backward compatibility with 0.1.0b1 (a beta): read
[Upgrade from 0.1.0b1](how-to/upgrade.md) before you deploy it, and drain every 0.1.0b1 worker
first (mixed fleets are not supported). It needs Python 3.12 or newer. The full list is in the [Changelog](project/changelog.md).

### Behaviour you can rely on

| Topic | What 0.1.0b2 does |
|---|---|
| Delivery | **At least once** for calls and jobs: a call whose record the store lost is sent again, and jobs survive a store failover. Make side effects idempotent |
| Request ids | Retrying an id after a failed end **runs the call again**; only a success is replayed |
| Cancel | **Cancel wins**: any caller's cancel stops the call for every caller sharing its id, and an attach after a cancel is cancelled too. Anyone may cancel any id; every cancel is logged with its source, and `llm-queue cancel ID --yes` is an audited admin cancel |
| Sync calls | A `*_sync` call (or LangChain `invoke`) on the queue's own event loop is **refused** with an `LLMQueueError` that names the fix (`await llmq.converse(...)`, `await chat.ainvoke(...)`) |
| Store outages | An answer in hand is kept and retried until the call's deadline. `fail_fast` and `static_cap` answer within about 100 ms of a known outage. `NOREPLICAS`, `OOM`, `MISCONF` and `BUSY` count as the store being down |
| Strict mode | `LLMQ_ENVIRONMENT` **unset is strict** (TLS store, a 16-character `LLMQ_ID_HASH_SALT`, an explicit `LLMQ_KEY_PREFIX`, `https` endpoint overrides). Set `dev`, `local`, `test` or `ci` to relax it. An evicting store fails readiness |
| Replica waits | `LLMQ_QUEUE_WAIT_REPLICAS` covers call sends, job sends and registry live edits, each switchable with `LLMQ_QUEUE_WAIT_SCOPES` (all on by default). Too few replicas: the write goes anyway, with a warning at most once a minute and a metric |

### New features

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

### Safer configuration

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
