# Run background jobs

A job is work that should not block a request: ingestion, re-embedding, a nightly report. You
register an async function under a name, send it a JSON payload, and a worker process runs it.

```python
from llm_queue import current_job

async def index_documents(payload: dict) -> None:
    job = current_job()                      # name, job_id, attempt, max_attempts
    result = await llmq.embed("cohere", payload["texts"], input_type="search_document")
    await save_vectors(job.job_id, payload["ids"], result.vectors)   # upsert by job id

llmq.register_job("index_documents", index_documents, timeout_secs=600)   # before start();
# timeout_secs: default none (no limit); concurrency: default LLMQ_JOB_CONCURRENCY (4)
await llmq.start()
job_id = await llmq.send_job("index_documents", {"ids": ids, "texts": texts}, job_id=batch_id)
```

## Guarantees and limits

| | |
|---|---|
| Delivery | **At least once.** A job can run twice (a worker killed mid-job, an ack lost, a send retried). Make it idempotent: write by `current_job().job_id` |
| Result | **Fire and forget.** The return value is dropped; there is no job status (`request_status(job_id)` is empty). Write the outcome where the app can read it |
| Where it runs | Processes in `both` or `worker` mode that registered the name. `register_job` in `caller` mode, or after `start()`, raises `ConfigError` |
| The function | Must be `async def` (or a partial of one, or an object with an async `__call__`): a plain `def` is refused at `register_job`. Never block the event loop: put blocking SDK calls and CPU work in `asyncio.to_thread`, or the job's heartbeat stops and the job is handed to another worker |
| Names | 1-64 letters, digits, `_` or `-` |
| Payload | JSON only, at most 1 MB (`LLMQ_JOB_MAX_PAYLOAD_BYTES`). A larger one fails `request_too_large`; one orjson cannot write (a `Decimal`, a `set`) fails `invalid_request`. Tuples arrive as lists, `datetime` and `UUID` as strings. Keep large inputs in your own storage and send a reference |
| Dedupe | `send_job(job_id=X)` again within 24 h (`LLMQ_JOB_DEDUPE_SECS`) queues nothing and returns `X`. Without `job_id` every send is a new job |
| Order | Not kept: up to 4 jobs of a name run at once per process, and a retried job goes back in later |
| Concurrency | 4 per job name per process (`register_job(..., concurrency=N)` or `LLMQ_JOB_CONCURRENCY`), 32 per process over all names (`LLMQ_JOB_PROCESS_CONCURRENCY`) |
| Waiting | At most 10,000 waiting jobs per name (`CallFailed("queue_full")`, `retry_after_secs` 5); a job waits at most the queue retention (1 day) before it is trimmed |
| Durability | With `LLMQ_QUEUE_WAIT_REPLICAS` (default 0, off; scope `jobs` in `LLMQ_QUEUE_WAIT_SCOPES`, default `all`), a send waits for the replicas like a call does; with too few it is sent anyway, warned (`replicas_short`) and counted |
| Context | A job's LLM calls run at normal priority with the sender's `user_id` and `tenant_id` (from `request_context` at `send_job`), not with the context of the process that called `start()`. The job message carries only their keyed hashes (`ctx.user_hash`, `ctx.tenant_hash`): inside the job, `request_context` holds the hashes (`llm_queue.ids.HashedId`), which its calls use as they are |
| LLM budget | Jobs take none of their own; their LLM calls do |

## Failures, retries and dead jobs

A job that raises is retried after 5 s, then 10, 20, 40 ... up to 5 minutes between tries, for
about an hour in total (`LLMQ_JOB_RETRY_BASE_SECS`, `LLMQ_JOB_RETRY_MAX_SECS`,
`LLMQ_JOB_RETRY_FOR_SECS`); a `CallFailed` with `retry_after_secs` waits at least that long. Then
it is **dead**: moved to the job's dead letters, kept 7 days (`LLMQ_JOB_DEAD_RETENTION_SECS`),
logged `job_failed outcome=dead` at error level with the traceback, and counted in
`llmq_jobs_total{outcome="dead"}`.

- **`timeout_secs`**: a run longer than this is cancelled and counts as a failure (logged with
  `kind=job_timeout`). Without it a hung job holds its worker slot for good. It is the limit of
  the process that runs the job: during a rolling deploy that changes it, old and new pods use
  their own values.
- **Shutdown**: a job still running when the drain ends is returned to the queue, not failed.
  A job returned 5 times (`LLMQ_JOB_MAX_RETURNS`) is dead-lettered: a job longer than the drain
  otherwise restarted from zero at every deploy. Checkpoint long jobs (save progress under the
  job id and skip what is done).
- **No consumer**: `send_job` to a name no running process consumes still queues the job, but
  logs `job_no_consumer` (once a minute per name) and counts `llmq_jobs_no_consumer_total`.

Dead jobs:

```python
dlq = llmq.admin.dlq
dead = await dlq.list(job="index_documents")                  # {queue, letters: [id, src, fails]}
await dlq.redrive(job="index_documents", actor="ops", dry_run=False)  # run them again
```

```bash
llm-queue dlq list --job index_documents
llm-queue dlq redrive --job index_documents --yes
llm-queue queues list                                         # job queues included
llm-queue jobs list                                           # per job: waiting, held, dead
```

`dlq show ENTRY_ID --job J --unsafe` prints one dead job with its payload (bodies are left out
otherwise).

`status()` lists every job name a worker consumed lately (`jobs`: waiting and dead letters), and
the `job_dead_letters` alert turns on while a job name has any dead job.
