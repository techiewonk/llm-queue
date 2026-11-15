# 08 LangChain

Drop-in LangChain chat model and embeddings whose calls go through the queue. Use them in
LangChain / LangGraph chains, agents and RAG pipelines.

## Key points

- `QueuedChatBedrockConverse` is a `ChatBedrockConverse`. Tools and structured output work as
  usual. Streaming does not: `astream()`, `astream_events()` and LangGraph's
  `stream_mode="messages"` yield the whole answer as one chunk (the adapters turn streaming off).
  For tokens as they arrive, call `LLMQueue.converse_stream` (see [Stream](stream.md)).
- `QueuedBedrockEmbeddings` is a `BedrockEmbeddings`.
- Both find the started `LLMQueue` on their own. `model_alias` names a registry model, and the
  `model_id` comes from the registry.
- They work with LangChain 1.x; CI also checks the 0.3 line.

## Code

```python title="samples/08_langchain.py"
--8<-- "samples/08_langchain.py"
```

## Output (live, 2026-11-14)

```text
chat: 'An index fund is a passively managed mutual fund or ETF that seeks to replicate the performance of a specific market ind' via aws-main/us-east-1
tool calls: [('stock_price', {'symbol': 'RELIANCE'})]
embeddings: 2 docs, query of 1024 dims
```
