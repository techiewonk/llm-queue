# 08 LangChain

Drop-in LangChain chat model and embeddings whose calls go through the queue. Use them in
LangChain / LangGraph chains, agents and RAG pipelines.

## Key points

- `QueuedChatBedrockConverse` is a `ChatBedrockConverse`. Tools and structured output work as
  usual.
- Streaming is off by default: `astream()`, `astream_events()` and LangGraph's
  `stream_mode="messages"` yield the whole answer as one chunk. Pass `queue_streaming=True`
  (on `QueuedChatBedrockConverse` or `QueuedChatOpenAI`) to get tokens as they arrive, through
  `LLMQueue.converse_stream` / `chat_stream` (see [Stream](stream.md)):
    - chunks are parsed by the installed langchain-aws / langchain-openai, so text deltas and
      `tool_call_chunks` (index, id, name, argument pieces) look as from the parent class;
    - `usage_metadata` arrives once, on the last chunk, with the stop reason (`finish_reason`;
      Bedrock's `stopReason` too) and how the queue served the call (`served_model`,
      `deployment`, `attempts`, ...) in `response_metadata`;
    - a failure after tokens were sent raises `CallFailed` with `partial_text`;
    - a consumer that stops reading cancels the queued call;
    - sync `stream()` still yields one whole message (the queue has no sync stream), and a sync
      call on the queue's own event loop raises the queue's `LLMQueueError` unchanged: use
      `await chat.ainvoke(...)` / `astream(...)` there;
    - `disable_streaming="tool_calling"` is kept; langchain-aws 0.2 turns streaming off for a
      Bedrock model id it does not know (pass `disable_streaming=False` to stream anyway);
    - a `QueuedChatOpenAI` structured-output request (`response_format`) is answered whole.
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
