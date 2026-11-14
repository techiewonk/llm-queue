# Make calls

Every call goes through one `LLMQueue`, built once per process (after any fork) and started in
the app's startup hook on the event loop that makes the calls (see
[Clients and connections](../explanation/clients.md)):

```python
from llm_queue import LLMQueue

llmq = LLMQueue.from_env()
await llmq.start()      # publish / load the registry, run the workers
...
await llmq.stop()
```

A model is named by its registry alias (or its provider model id).

## Chat: `converse` and `chat`

```python
reply = await llmq.converse("gpt120b", {"messages": [...]})
reply.response          # the Bedrock Converse body
reply.meta.deployment   # "aws-main/ap-south-1"
reply.meta.served_model # the alias that served it (another one after a model fallback)
```

`converse` is a Bedrock Converse call; `chat` is an OpenAI-compatible chat completion. `call`
runs any operation of a model. Each takes these keyword arguments:

| Argument | Meaning |
|---|---|
| `priority` | Priority calls are read before normal ones (default from `request_context`, else normal) |
| `timeout_secs` | The call's time budget (default the model's `timeout_secs`, 120; capped by `timeout_max_secs`, 900) |
| `max_wait_secs` | The longest the call may wait for budget before it fails `rate_limited` |
| `request_id` | Your own id; a repeated id runs once and returns the same answer |
| `session_id`, `user_id`, `tenant_id` | Ids of the caller (default from `request_context`); user and session ids are stored only as a keyed hash (`LLMQ_ID_HASH_SALT`) |
| `allow_fallback` | `"all"` (default) lets a disabled model fall back to its `model_fallback` model |
| `allowed_regions`, `allowed_accounts` | Limit the deployments this call may use |

### Images and documents

Converse content blocks with bytes (images, documents) are sent inline, as you would call
Bedrock directly:

```python
with open("chart.png", "rb") as f:
    image = f.read()

request = {"messages": [{"role": "user", "content": [
    {"text": "What does this chart show?"},
    {"image": {"format": "png", "source": {"bytes": image}}},
]}]}
reply = await llmq.converse("claude-sonnet", request)
```

- Bytes travel through the queue encoded (`{"__b64__": ...}`) and reach the provider as bytes.
- The whole message, bytes included, may be at most the model's `max_message_bytes` (default
  4 MB). A larger one fails `CallFailed("invalid_request")` at once, before it is queued. For
  larger files, use a smaller image or an S3 location the model can read.
- The token estimate does not read the bytes: each binary block costs a fixed 1,000 tokens until
  the real usage corrects the budget after the call (see
  [Token estimates](../explanation/attempts.md#token-estimates)).

### Token estimates for other scripts

The TPM estimate is characters / 4, which is low for Hindi, other Indic scripts and CJK text. For
a model that serves them, register a tokenizer-based estimator and name it in the model's
`token_estimator` setting; see [Token estimates](../explanation/attempts.md#token-estimates).

## Streaming: `converse_stream`

```python
async for event in llmq.converse_stream("gpt20b", request):
    delta = event.get("contentBlockDelta", {}).get("delta", {})
    if "text" in delta:
        send_to_client(delta["text"])
```

Bedrock models yield ConverseStream events, OpenAI-compatible ones chat completion chunks
(`chat_stream` is the same method). A failure after events were sent raises `CallFailed` with
`partial_text`; the call never switches to another deployment half-way. See
[Streaming](../explanation/streaming.md).

## Embeddings: `embed`

```python
result = await llmq.embed("embed-cohere", texts, input_type="search_document")
result.vectors          # one vector per text, in order
```

Texts are batched by the model's `embed_batch_size` (capped by what the provider accepts: Titan
1, Cohere 96, OpenAI 2048), at most 16 batches in flight at a time.

- **Empty texts** (or only whitespace) are refused at once with `CallFailed("invalid_request")`.
- **`request_id=X`** names batch *n* `X:n`, so each batch is its own call and a repeat of the
  same `embed` returns the same vectors.
- **Partial results:** by default one failed batch fails the call. With `partial=True` the call
  returns what succeeded; a text whose batch failed has the vector `[]` and its error in
  `result.errors` (by text index):

```python
result = await llmq.embed("embed-cohere", texts, partial=True)
retry = [texts[i] for i in result.errors]      # texts to send again
```

## Sync code: `converse_sync`, `embed_sync`

For scripts, Celery tasks and sync libraries only:

```python
reply = llmq.converse_sync("gpt120b", request)
vectors = llmq.embed_sync("embed-cohere", texts).vectors
llmq.close_sync()       # in sync-only processes; stop() does it too
```

Sync calls run on the direct path: same budgets and order, no queue. When the queue is started,
they run on its event loop (from your thread), with its clients and connections. Otherwise they
run on one private event loop per process, with a caller-only engine and a small store pool.
Async code should use `converse` / `embed`: a sync call on the queue's own loop thread is refused
(it would deadlock), and one from any other running loop blocks it and logs
`sync_call_on_event_loop` once. Celery: see
[Clients and connections](../explanation/clients.md#celery).

## Jobs: `register_job`, `send_job`

Work that should not block a request, such as ingestion or re-embedding:

```python
async def index_documents(payload: dict) -> None:
    result = await llmq.embed("cohere", payload["texts"], input_type="search_document")
    ...

llmq.register_job("index_documents", index_documents, timeout_secs=600)   # before start()
await llmq.start()
job_id = await llmq.send_job("index_documents", {"ids": ids, "texts": texts})
```

Jobs run at least once, on processes in `both` or `worker` mode. They have no LLM budget of
their own; the LLM calls inside them do.

- **`timeout_secs`** (`register_job(name, fn, *, timeout_secs=None)`): a run longer than this is cancelled and counts as a failure (retried, then
  dead-lettered after 5 failures). Without it a hung job holds a worker slot for good.
- **Queue limit**: a jobs queue holds at most 10,000 waiting jobs; `send_job` beyond that raises
  `CallFailed("queue_full")` with `retry_after_secs` 5 instead of filling the store.
- **No consumer**: `send_job` to a name no running process has registered still queues the job,
  but logs `job_no_consumer` (once a minute per name) and counts `llmq_jobs_no_consumer_total`,
  so a typo or a missing worker deployment shows up at once.
- **Shutdown**: a job still running when the drain ends is returned to the queue, not failed.
  A job whose acknowledgement failed after it finished is logged as a duplicate run.

## Request context

`request_context` sets priority and ids for every call inside it, including calls made by
LangChain adapters:

```python
from llm_queue import request_context

with request_context(priority=True, session_id=sid, user_id=uid, tenant_id=tid):
    reply = await llmq.converse("gpt120b", request)
```

An argument passed to a call overrides the context.

## LangChain

```python
from llm_queue.langchain import QueuedBedrockEmbeddings, QueuedChatBedrockConverse

chat = QueuedChatBedrockConverse(model_alias="gpt120b", max_tokens=400)
message = await chat.ainvoke("In one line, what is an index fund?")
emb = QueuedBedrockEmbeddings(model_alias="titan")
```

`QueuedChatBedrockConverse`, `QueuedBedrockEmbeddings`, `QueuedChatOpenAI` and
`QueuedOpenAIEmbeddings` are drop-in replacements for the LangChain classes (tools work too).
They find the started queue on their own. Install the `langchain-aws` or `langchain-openai`
extra.

## Handle failures

A failed call raises `CallFailed`; branch on its `kind`:

```python
from llm_queue import CallFailed

try:
    reply = await llmq.converse("gpt120b", request, max_wait_secs=5)
except CallFailed as exc:
    if exc.kind in ("rate_limited", "queue_full"):
        ...  # busy: retry after exc.retry_after_secs, or ask the user to wait
    elif exc.kind in ("model_unavailable", "model_disabled", "no_deployment"):
        ...  # nothing can serve the model now
    else:
        ...  # show an error
```

`CallFailed` also carries `retry_after_secs` (where a later retry can work), `partial_text` (a
stream cut mid-answer) and `request_id`. Every kind and what to do is in
[Errors](../reference/errors.md). Everything raised to callers is an `LLMQueueError`.

## Cancel and look up a call

```python
await llmq.cancel(request_id)           # waiting: never starts; running: the attempt is cancelled
await llmq.request_status(request_id)   # status of one call
```

Both raise `StoreUnavailable` when the store cannot serve them.
