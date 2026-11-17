"""LangChain: drop-in chat model and embeddings that go through the queue (chains, agents, RAG).

`QueuedChatBedrockConverse` is a `ChatBedrockConverse` whose calls go through the queue (tools
and structured output work as usual; `astream()` yields the whole answer as one chunk unless
`queue_streaming=True`, which streams tokens through `LLMQueue.converse_stream`);
`QueuedBedrockEmbeddings` is a `BedrockEmbeddings`. They find the started `LLMQueue` on their own.

    python scripts/samples.py 08_langchain [--live]
"""

from __future__ import annotations

import asyncio

from langchain_core.messages import AIMessage, AIMessageChunk
from langchain_core.tools import tool

from llm_queue import LLMQueue
from llm_queue.langchain import QueuedBedrockEmbeddings, QueuedChatBedrockConverse


@tool
def stock_price(symbol: str) -> str:
    """Latest price of an NSE stock symbol."""
    return f"{symbol}: 2,950.10"


async def main() -> None:
    llmq = LLMQueue.from_env()
    await llmq.start()
    try:
        # model_id comes from the registry of the started queue
        chat = QueuedChatBedrockConverse(model_alias="gpt120b", max_tokens=400)  # type: ignore[call-arg]
        message = await chat.ainvoke("In one line, what is an index fund?")
        served = message.response_metadata["llmq"]
        print(f"chat: {message.text()[:120]!r} via {served['deployment']}")

        with_tools = chat.bind_tools([stock_price])
        called = await with_tools.ainvoke("Use the stock_price tool to get the price of RELIANCE.")
        assert isinstance(called, AIMessage)
        print(f"tool calls: {[(c['name'], c['args']) for c in called.tool_calls]}")
        assert called.tool_calls
        assert called.tool_calls[0]["name"] == "stock_price"

        # queue_streaming=True: astream() yields tokens as they arrive (through converse_stream)
        streaming = QueuedChatBedrockConverse(  # type: ignore[call-arg]
            model_alias="gpt120b", max_tokens=400, queue_streaming=True
        )
        chunks: list[AIMessageChunk] = []
        async for chunk in streaming.astream("Name three Nifty 50 companies, comma separated."):
            chunks.append(chunk)
        streamed = sum(chunks[1:], chunks[0])
        usage = streamed.usage_metadata  # once, on the last chunk
        assert usage is not None
        print(
            f"streamed: {len(chunks)} chunks, {streamed.text()[:80]!r}, "
            f"finish_reason={streamed.response_metadata.get('finish_reason')}, "
            f"output_tokens={usage['output_tokens']}"
        )

        emb = QueuedBedrockEmbeddings(model_alias="titan")
        docs = await emb.aembed_documents(["first", "second"])
        query = await emb.aembed_query("which one")
        print(f"embeddings: {len(docs)} docs, query of {len(query)} dims")
        assert len(docs) == 2
        assert query
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
