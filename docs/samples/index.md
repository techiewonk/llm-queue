# End-to-end samples

Ten runnable programs, one per way an app uses llm-queue. Each is plain app code: it builds
the queue with `LLMQueue.from_env()` and reads nothing else, so you can copy it into a service
as is. Every page shows the full code and its output: from fakellm (the fake Bedrock the
checks run on), and from a live run on Amazon Bedrock where that run's output still matches the
code.

!!! note "Last live run: 2026-11-14; samples changed since"
    On 2026-11-14 all nine samples, as they were then, passed on real Bedrock in `us-east-1`,
    with an ElastiCache Valkey 8.2 (TLS) as the store, in strict mode (`LLMQ_ENVIRONMENT`
    unset): `python scripts/samples.py --live --store env`. The run used its own key prefix and
    left no keys behind.

    Since then samples 02, 03, 06, 07, 08 and 09 show more of the 0.1.0b2 API and have not run
    on real Bedrock yet: their pages show fakellm output from 2026-11-16, and their live output
    will be refreshed on the next live run. Samples 01, 04 and 05 print what they printed then,
    so their pages keep the live output. Sample 10 (purge) is new in 0.1.0b3 and has not run
    on real Bedrock yet. All ten pass on fakellm (2026-11-16), as
    `python scripts/check.py samples` checks.

## What a sample run does

```mermaid
flowchart LR
  subgraph runner["scripts/samples.py"]
    env["sets LLMQ_* env<br/>(store, prefix, registry)"]
  end
  subgraph proc["Sample process (your app code)"]
    q["LLMQueue.from_env()<br/>start()"]
    call["converse / stream / embed<br/>jobs / status / router"]
    w["worker loop<br/>(same process)"]
  end
  store[("Valkey / Redis<br/>queues, budgets, health")]
  llm["Amazon Bedrock<br/>(or fakellm)"]
  env --> q --> call
  call -- "admit + enqueue" --> store
  store -- "pick" --> w
  w -- "Converse / InvokeModel" --> llm
  w -- "result" --> store
  store -- "reply" --> call
```

The runner only sets the environment and starts each sample as its own process. Inside, the
sample starts the queue; the same process acts as caller and worker (`LLMQ_MODE=both`), just as
an app pod does. At the end the runner deletes every key under the run's prefix.

## The samples

| Sample | Shows | Use it in |
|---|---|---|
| [01 Converse](converse.md) | `converse` with `request_context`; reply text and `meta` | Async request handlers, agent steps, anything user-facing |
| [02 Stream](stream.md) | `converse_stream` events as they arrive; `meta_event=True`; `partial_text` on a cut; `chat_stream` for OpenAI-compatible models | Chat UIs and server-sent events |
| [03 Embed](embed.md) | `embed` on Titan (1 text per request) and Cohere (96, `input_type`); `EmbedResult` usage; `register_embedding_family` | Indexing documents and search queries |
| [04 Sync](sync.md) | `converse_sync` / `embed_sync` on the direct path | Scripts, Celery tasks, sync libraries |
| [05 Errors and fallback](errors-and-fallback.md) | Model fallback; `CallFailed.kind`, typed `Kind`, to branch on | Every caller |
| [06 Jobs](jobs.md) | `register_job(concurrency=)` / `send_job`, LLM calls inside a job; dead letters and `redrive_jobs` | Ingestion, nightly re-embedding, batch work |
| [07 Status and registry](status-and-registry.md) | `status`, `request_status`, live `registry.set`, audit `log`, `health`; tenant `fairness` set live | Admin pages, runbooks, readiness checks |
| [08 LangChain](langchain.md) | `QueuedChatBedrockConverse` (with tools and `queue_streaming=True`), `QueuedBedrockEmbeddings` | LangChain / LangGraph chains, agents, RAG |
| [09 HTTP router](http-router.md) | `http_router` on FastAPI: probes, admin routes, `GET /metrics`, `/drain` | Admin API, Kubernetes probes, the preStop hook |
| [10 Purge](purge.md) | `llmq.purge`: a tenant purge planned then run, a deployment purge refused then planned with `force`; `PurgeReport.as_dict()` | Admin jobs, tenant offboarding, cleanup after a registry change |

## The registry they share

`samples/registry.json` defines four models on one AWS account: chat `gpt120b` (falls back to
`gpt20b`) and `gpt20b`, and embeddings `titan` and `cohere`. The quotas are small sample
numbers; use your Service Quotas numbers in a real registry. The runner sets every deployment
and the residency list to the run's region.

```json
--8<-- "samples/registry.json"
```

## Run them

Start the local stack once (Valkey, plus fakellm as a fake Bedrock), then run from the library
root:

```bash
python scripts/stack.py up

python scripts/samples.py                              # all, on fakellm (no real LLM, no cost)
python scripts/samples.py 03_embed 08_langchain        # some
python scripts/samples.py --live --env-file ../../.env # real Bedrock, AWS keys from that file
python scripts/samples.py --live --store env --env-file ../../.env  # and that file's store
```

- **What `--live` reads:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
  and `AWS_MODEL_REGION` from the env file. It never prints them.
- **Cost:** a full live run makes about 20 small calls, well under a cent.
- **`--store env`:** uses the env file's `LLMQ_STORE_URL`. Only the run's own prefix is touched
  and deleted, so a shared store (ElastiCache) is safe.
- **In CI:** `python scripts/check.py samples` runs the fakellm pass with lint and type checks;
  the integration jobs run every sample and attach the results as the `samples-<python>`
  artifact of the run.
- **Results:** each run prints every sample's output and saves it, with `summary.md` and
  `summary.json`, under `reports/samples-<fakellm|live>-<time>/` (`--out` picks the folder,
  `--no-save` writes nothing). Live runs are not part of CI: they need AWS keys.

## Use them in an app

An app sets the same environment the runner sets, from its own secrets:

| Variable | Runner value | In an app |
|---|---|---|
| `LLMQ_ENVIRONMENT` | `test` with the local Valkey; unset with `--store env` | unset or your environment's name (strict) |
| `LLMQ_ID_HASH_SALT` | a random one per run with `--store env` | from your secrets (required when strict) |
| `LLMQ_STORE_URL` | `redis://:localdev@127.0.0.1:6379/0` | `rediss://...` of the ElastiCache Valkey |
| `LLMQ_KEY_PREFIX`, `LLMQ_QUEUE_PREFIX` | per run | per environment, e.g. `llmq:prod` |
| `LLMQ_REGISTRY` | `samples/registry.json`, region set | the app's registry document |
| `LLMQ_METRICS_SINK` | `none` | `emf` (CloudWatch) or `prometheus` |
| AWS credentials | from `.env` (or fake for fakellm) | the pod's role; `role_arn` accounts assume roles |

Start the queue once per process in the app's startup hook, and stop it on shutdown:

```python
@asynccontextmanager
async def lifespan(app):
    await llmq.start()              # a failure here should fail the pod: queued models need it
    yield
    llmq.pause_receiving()          # not ready; no new queued work (also from a preStop /drain)
    await llmq.stop()               # LLMQ_DRAIN_SECS (25 s) for running jobs and calls together
```

Unless `LLMQ_ENVIRONMENT` is `dev`, `local`, `test` or `ci` (unset counts as strict),
`LLMQ_STORE_URL` must be `rediss://` and `LLMQ_ID_HASH_SALT` must be set.

## Measured on real Bedrock

| What | Result |
|---|---|
| Models | `gpt-oss-120b`, `gpt-oss-20b`, Titan v2 and Cohere v3 all answered through the queue (`us-east-1` and `ap-south-1`) |
| Embeddings, similar / unrelated pair | Titan v2: 0.73 / 0.07; Cohere v3: 0.83 / 0.37 (1024 dimensions each) |
| Streaming overhead | a warm `gpt-oss-20b` stream took 0.83 s through the queue against 0.7 s called directly |
| Queue wait (2026-11-14 run) | 48 to 213 ms for a priority call on an idle queue over TLS ElastiCache |
| Fallback | with `gpt120b` off, the call went to `gpt20b` (`meta.fallback == "model"`) |
| Tool calls | LangChain returned `stock_price({"symbol": "RELIANCE"})` |
