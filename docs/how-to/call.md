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
reply.meta.provider_request_id  # the provider's own id of the answering attempt
```

`provider_request_id` is Bedrock's `x-amzn-RequestId` or the OpenAI-compatible server's
`x-request-id` (empty when the server sent none): the id to quote to the provider's support. A
stream's `meta_event=True` last event carries it too.

`converse`, `chat` and `call` are one call under three names, all supported:

| Method | Use it for |
|---|---|
| `converse` | Bedrock models, with a Converse request (`messages`, `inferenceConfig`, `toolConfig`) |
| `chat` | OpenAI-compatible models (vLLM, OpenAI), with a chat completions request (`messages`, `max_tokens`, `tools`) |
| `call` | Code that handles any model alike, and the other operations (`invoke_model`, `embeddings`) |

The model's provider decides the request shape, not the method name: `chat` on a Bedrock model
sends a Converse request. Pick the name that matches the model so the code reads right. The
streaming pair is `converse_stream` / `chat_stream`, the sync pair `converse_sync` /
`chat_sync`.

Each takes these keyword arguments (keyword only; an unknown name raises `TypeError`):

| Argument | Meaning |
|---|---|
| `priority` | Priority calls are read before normal ones (default from `request_context`, else normal). Only trusted code should set it |
| `timeout_secs` | The call's time budget (default the model's `timeout_secs`, 120; capped by `timeout_max_secs`, 900) |
| `max_wait_secs` | The longest the call may wait for budget before it fails `rate_limited` (default the model's `max_wait_secs`; none: until the deadline) |
| `request_id` | Your own id; a repeat while it runs attaches to the same call, a repeat after a success returns the same answer, and a repeat after a failure runs the call again |
| `session_id`, `user_id`, `tenant_id` | Ids of the caller (default from `request_context`); user and session ids are stored only as a keyed hash (`LLMQ_ID_HASH_SALT`) |
| `allow_fallback` | `"all"` (default): fallback regions and fallback models (also when the model is disabled); `"regions"`: fallback regions only; `"none"`: neither |
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

- Bytes travel through the queue encoded (`{"__llmq_b64__": ...}`) and reach the provider as bytes.
- The whole message, bytes included, may be at most the model's `max_message_bytes` (default
  4 MB). A larger one fails `CallFailed("request_too_large")` at once, before it is queued. For
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
(`chat_stream` is the same call under the OpenAI name). A failure after events were sent raises `CallFailed` with
`partial_text`; the call never switches to another deployment half-way. See
[Streaming](../explanation/streaming.md).

## Embeddings: `embed`

```python
result = await llmq.embed("embed-cohere", texts, input_type="search_document")
result.vectors          # one vector per text, in order

vector = await llmq.embed_one("embed-cohere", query, input_type="search_query")
vector = llmq.embed_one_sync("embed-cohere", query)   # sync code, direct path
```

`embed_one(model, text, **options)` is `embed(model, [text], ...).vectors[0]`: one text, its
vector, the same options; `embed_one_sync` is the sync form (like `embed_sync`).

Texts are batched by the model's `embed_batch_size` (capped by what the provider accepts: Titan
1, Cohere 96, OpenAI 2048; the default is 1, so set it, for example 96 for Cohere and 256 to
2048 for OpenAI), at most `concurrency=16` batches in flight at a time.

- **Checked before any call** (`CallFailed("invalid_request")`, naming the text's index): a
  `str` instead of a list, a text that is not a string, empty or only whitespace, a text longer
  than the model's limit (Titan 50,000 characters, Cohere 2,048; the registry's
  `embed_max_chars` overrides it, 0 turns it off; OpenAI is left to the provider), and a Cohere
  `input_type` other than `search_document`, `search_query`, `classification`, `clustering`.
- **A refused batch is split**: when the provider refuses a batch for its content, the batch is
  split in two and sent again until only the bad text fails; its error names the text's index.
- **`timeout_secs` is per batch**, not for the whole `embed`: 100,000 Titan texts are 100,000
  calls. The vectors are Python floats (about 32 bytes each: 100,000 x 1,024 is about 3 GB);
  embed a large set in slices.
- **Usage**: `result.meta.tokens_in` (the input tokens the replies reported), `result.batches`
  (provider calls, splits included) and `result.request_ids`.
- **Options**: only `input_type` is per call; `dimensions`, `normalize`, `truncate` come from
  the registry's `inference`. Titan v2 normalises by default, Titan v1 and Cohere do not.
  `inference.embedding_types` takes only `["float"]`. The body shape is the model's family
  (see below).
- **`request_id=X`** names batch *n* `X:n`, so each batch is its own call and a repeat of the
  same `embed` returns the same vectors.
- **Partial results:** by default one failed batch fails the call. With `partial=True` the call
  returns what succeeded; a text whose batch failed has the vector `[]` and its error in
  `result.errors` (by text index):

```python
result = await llmq.embed("embed-cohere", texts, partial=True)
retry = [texts[i] for i in result.errors]      # texts to send again
```

### Embedding families

A family is one request and reply shape, held in an `EmbeddingCodec`: texts per call
(`max_batch`), the longest text (`max_chars`), the request body and the reply parser. Three are
built in:

| Family | Texts per call | Longest text | Request | Reply |
|---|---|---|---|---|
| `titan` | 1 | 50,000 | `inputText` (`dimensions`, `normalize`) | `embedding` |
| `cohere` | 96 | 2,048 | `texts`, `input_type` | `embeddings` (or `embeddings.float`) |
| `openai` | 2,048 | (server's) | `input`, base64 vectors | `data[i].embedding` |

A model's family is its registry `family` if set, else `openai` for the `openai` provider,
else the first family whose pattern is in the model id, whatever its case (`titan-embed`,
`cohere.embed`). A model id no family matches (an application inference profile ARN, a new
model) fails the registry load: `cannot tell the embedding family of <id>; set family (one
of ...)`. Set `"family": "cohere"` (or another name) on that model.

Add a family for a model the library does not know, without waiting for a release:

```python
from llm_queue import EmbeddingCodec, register_embedding_family


def nova_request(texts, options):
    return {"taskType": "SINGLE_EMBEDDING",
            "singleEmbeddingParams": {"text": {"value": texts[0]}}}


def nova_reply(body):
    return [item["embedding"] for item in body["embeddings"]]


register_embedding_family("nova", EmbeddingCodec(
    name="nova",
    build_request=nova_request,      # (texts, options) -> the model's JSON body
    parse_reply=nova_reply,          # decoded reply body -> one vector per text, in order
    max_batch=1,
    max_chars=50_000,
    patterns=("amazon.nova-2-multimodal-embeddings",),  # optional: ids that name it
))
```

Register it in every process (callers and workers) **before** the registry loads; a registry
that names a family this process has not registered is refused. Patterns of the newest
registration are tried first, so a narrower one (`cohere.embed-v4`) wins over a built-in one.
`options` carries the per-call `input_type`; `input_types=` closes the set of values it may
take, `probe_options=` go into the health probe's request, and `inference_names=` names the
registry `inference` fields the body takes (left out: the worker leaves the body as built).
`parse_reply` raising `ValueError`, `TypeError` or `KeyError` fails the batch as
`provider_error`; `llm_queue.providers.vector` turns a list of numbers or base64 float32 into
floats with the built-in checks. The request shapes above are an example: check the model's
own documentation.

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
Async code should use `converse` / `embed`: a sync call on the queue's own loop thread is
**refused** with an `LLMQueueError` that names the fix (`await llmq.converse(...)`, in
LangChain `await chat.ainvoke(...)`), since it would deadlock. Without a started queue, a
sync call from a running event loop blocks that loop and logs `sync_call_on_event_loop`
once. Celery: see
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

Jobs run at least once and are fire and forget. Retries, dead jobs, limits and what a job can
rely on are in [Run background jobs](jobs.md).

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

**Cancel wins:** a cancel stops the call for every caller waiting on that id, and a caller
that attaches to the id afterwards gets `cancelled` too. Any code (or `llm-queue cancel ID
--yes`, audited) may cancel any id; each cancel is logged and counted with its source.

**Request ids:** a call sent again with the id of a call that **succeeded** gets the same
answer without running again (while the result is kept). An id whose last run failed,
expired or was cancelled **runs again**.

Both raise `StoreUnavailable` when the store cannot serve them.
