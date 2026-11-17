"""Stream an answer as it is written (chat UIs, server-sent events to the browser).

`converse_stream` yields Bedrock ConverseStream events in order. With `meta_event=True` a
complete stream ends with one more event of the library's own, `{"llmq": {"usage", "meta"}}`:
tokens and how the call was served (model, deployment, fallback, attempts). If the stream
breaks after some text was sent, `CallFailed` carries `partial_text` and the call is not
retried, so the user never sees the answer start over.

    python scripts/samples.py 02_stream [--live]
"""

from __future__ import annotations

import asyncio

from llm_queue import CallFailed, LLMQueue, request_context


async def main() -> None:
    llmq = LLMQueue.from_env()
    await llmq.start()
    try:
        request = {
            "messages": [
                {"role": "user", "content": [{"text": "Explain a P/E ratio in two lines."}]}
            ],
            "inferenceConfig": {"maxTokens": 400},
        }
        pieces: list[str] = []
        events = 0
        served = None
        try:
            with request_context(priority=True):
                async for event in llmq.converse_stream("gpt20b", request, meta_event=True):
                    if "llmq" in event:  # the library's last event: not sent to the client
                        served = event["llmq"]["meta"]
                        continue
                    events += 1
                    delta = event.get("contentBlockDelta", {}).get("delta", {})
                    if "text" in delta:
                        pieces.append(delta["text"])
                        print(delta["text"], end="", flush=True)  # send to the client here
                    if "metadata" in event:
                        usage = event["metadata"]["usage"]
                        tokens = f"{usage['inputTokens']}/{usage['outputTokens']}"
                        print(f"\n  [{events} events, tokens {tokens}]")
        except CallFailed as exc:
            print(f"\n  stream failed: {exc.kind}; text so far: {exc.partial_text!r}")
            raise
        assert served is not None, "no llmq event at the end of the stream"
        print(
            f"  served by {served['served_model']} on {served['deployment']}, "
            f"attempts {served['attempts']}, fallback={served['fallback']}"
        )
        assert "".join(pieces).strip(), "the stream had no text"

        # An OpenAI-compatible model (provider "openai", such as vLLM) streams the same way
        # under its own name: `chat_stream` yields chat completion chunks, with the text in
        # `chunk["choices"][0]["delta"]["content"]`. samples/registry.json has only Bedrock
        # models, so this runs only when the registry names one.
        models = llmq.admin.registry.current.registry.models
        openai_chat = [a for a, m in models.items() if m.provider == "openai" and m.type == "chat"]
        for alias in openai_chat[:1]:
            chat_request = {"messages": [{"role": "user", "content": "Say hi."}]}
            async for chunk in llmq.chat_stream(alias, chat_request, meta_event=True):
                for choice in chunk.get("choices", []):
                    print(choice.get("delta", {}).get("content") or "", end="", flush=True)
            print(f"\n  ({alias}: chat_stream)")
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
