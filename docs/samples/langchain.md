# 08 LangChain

Drop-in LangChain chat model and embeddings whose calls go through the queue. Use them in
LangChain / LangGraph chains, agents and RAG pipelines.

## Key points

- `QueuedChatBedrockConverse` is a `ChatBedrockConverse`. Tools and structured output work as
  usual.
- Every chat reply (and a stream's last chunk) has how the queue served it in
  `response_metadata["llmq"]` (the `Meta`: `served_model`, `deployment`, `fallback`,
  `attempts`, `queue_wait_ms`, `provider_request_id`, ...) and boto's
  `response_metadata["ResponseMetadata"]` (`RequestId` = the provider's request id,
  `HTTPStatusCode` 200, `RetryAttempts` = the queue's attempts after the first; no
  `HTTPHeaders`), on both the Bedrock and the OpenAI-compatible adapters.
- Streaming is off by default: `astream()`, `astream_events()` and LangGraph's
  `stream_mode="messages"` yield the whole answer as one chunk. Pass `queue_streaming=True`
  (on `QueuedChatBedrockConverse` or `QueuedChatOpenAI`) to get tokens as they arrive, through
  `LLMQueue.converse_stream` / `chat_stream` (see [Stream](stream.md)):
    - chunks are parsed by the installed langchain-aws / langchain-openai, so text deltas and
      `tool_call_chunks` (index, id, name, argument pieces) look as from the parent class;
    - `usage_metadata` arrives once, on the last chunk, with the stop reason (`finish_reason`;
      Bedrock's `stopReason` too) and how the queue served the call in `response_metadata`
      (see below);
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
- `chat_model(model_id)` / `embeddings(model_id)` build the right class from the registry, or
  return None when the model is not in the registry, for
  `chat_model(...) or <direct model>`: see [Use LangChain models](../how-to/langchain.md).

## Code

```python title="samples/08_langchain.py"
--8<-- "samples/08_langchain.py"
```

## Output (fakellm, 2026-11-16)

```text
chat: 'fakellm reply 1e23b81a404afcd9' via aws-main/us-east-1
tool calls: [('stock_price', {'symbol': 'Use the stock_price tool to get the price of RELIANCE.'})]
streamed: 5 chunks, 'fakellm reply 305b7948f2bb67e3', finish_reason=stop, output_tokens=8
embeddings: 2 docs, query of 1024 dims
```

fakellm answers with a fixed text and fills a tool's arguments with the prompt, so the replies
mean nothing; the lines show what the code prints. On real Bedrock (2026-11-14) the tool call
came back as `stock_price({'symbol': 'RELIANCE'})`. The live output will be refreshed on the
next live run (`python scripts/samples.py --live`); the last one, on 2026-11-14, ran this sample
before its current code.
