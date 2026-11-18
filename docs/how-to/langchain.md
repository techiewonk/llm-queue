# Use LangChain models

The queued adapters (`QueuedChatBedrockConverse`, `QueuedChatOpenAI`, `QueuedBedrockEmbeddings`,
`QueuedOpenAIEmbeddings`) are drop-in LangChain classes; see the
[LangChain sample](../samples/langchain.md) for what they do. This page is about choosing
between them and the app's own direct models with the model factories.

## The factories

```python
from langchain_aws import ChatBedrockConverse
from llm_queue.langchain import chat_model, embeddings

llm = chat_model(model_id, temperature=0.2, max_tokens=4096) or ChatBedrockConverse(
    model=model_id, temperature=0.2, max_tokens=4096, client=bedrock_client
)
embedder = embeddings(embed_id, model_kwargs={"input_type": "search_query"}) or BedrockEmbeddings(
    model_id=embed_id, client=bedrock_client
)
```

`chat_model(model_id, *, registry=None, **kw)` and
`embeddings(model_id, *, registry=None, **kw)` return:

- **the queued class** whenever the boot registry knows `model_id`:
  `QueuedChatBedrockConverse` / `QueuedBedrockEmbeddings` for a `bedrock` model,
  `QueuedChatOpenAI` / `QueuedOpenAIEmbeddings` for an `openai` one, with `model_alias` and the
  model id set from the registry;
- **None** when the model is not in the boot registry: a model id outside the registry stays
  on the caller's own path, so `or` falls back to the app's direct model.

| Argument | Meaning |
|---|---|
| `model_id` | A registry alias (`gpt120b`) or a provider model id (`openai.gpt-oss-120b-1:0`). A model id maps to its alias; a registry gives each model id one alias (a stored document with a repeat: the first alias listed wins) |
| `registry` | Where the boot registry comes from (below) |
| `**kw` | Passed to the class's constructor and wins over the factory's values (`temperature`, `max_tokens`, `queue_streaming`, `priority`, even `model_id`) |

An embedding model asked for with `chat_model` (or the other way round) and a provider with no
LangChain adapter raise `ConfigError`: those are code errors, not a reason to run direct.
`aliases(registry=None)` returns the model id -> alias map of the same registry.

For a model outside the registry, neither langchain, boto3 nor httpx is imported by the
factories: only the app's direct path loads them.

## The boot registry

The factories read the registry the process **starts** with, without a store:

| `registry=` | Read from |
|---|---|
| None (default) | The started queue (`LLMQueue.start()` or `install()` registers it), as for `registry=llmq`. Before any queue has started: `LLMQ_REGISTRY` or `LLMQ_REGISTRY_FILE` in the environment, placeholders expanded from the environment, `.yaml` files as YAML |
| a mapping, or JSON text | That document |
| a path (`str` or `Path`) | That file |
| an `LLMQueue` | The boot document its settings read: secrets, the environment and `defaults=`, placeholders expanded (and the queue is passed to the adapter as `llmq`) |

The started queue also decides the off switch: a `DisabledQueue` (`LLMQ_ENABLED=false`, or
`LLMQ_CONFIG_ERROR_MODE=disable` after a configuration error) makes every factory return None,
so the app takes its direct path. Before any queue has started, `LLMQ_ENABLED` is read from
the environment.

The registry is parsed once: once per queue, and before start once per file version (the
file's path, modification time and size, and the placeholder values read). A ConfigMap updated
in place does not change the registry of a started queue.

What that means for live edits:

- a model **added** by a live edit is not in the boot registry: its factory call returns None
  and the app keeps calling it directly until the next restart;
- a model **removed** by a live edit is still built as a queued class: its calls raise
  `UnknownModel` until the next restart (or until it is added back).

Pass `registry=llmq` when a factory runs before the queue starts (at import time, for example)
and the app builds its queue with `defaults=`, `secrets=` or `LLMQ_SECRETS_SOURCE`: the
environment alone may not hold the registry's location or the off switch then.
