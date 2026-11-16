"""Handle failures: one exception type (`CallFailed`) with a `kind` to branch on.

`kind` is typed as the public `Kind` (a `Literal` of every kind a caller can get), so a type
checker flags a misspelt kind in `exc.kind == "..."` or in a set of kinds.

Turns a deployment off with a live registry edit (what `llm-queue registry disable` does) to
show two things: a model with a fallback model is served by it (`meta.fallback == "model"`),
and a model with nowhere to go fails fast with `CallFailed`. Both edits are undone at the end.

    python scripts/samples.py 05_errors_and_fallback [--live]
"""

from __future__ import annotations

import asyncio

from llm_queue import CallFailed, Kind, LLMQueue

REQUEST = {
    "messages": [{"role": "user", "content": [{"text": "Say hi."}]}],
    "inferenceConfig": {"maxTokens": 200},
}
# Busy: retry after `retry_after_secs`. Unavailable: nothing can serve the model now.
BUSY: frozenset[Kind] = frozenset({"rate_limited", "queue_full"})
UNAVAILABLE: frozenset[Kind] = frozenset({"model_unavailable", "model_disabled", "no_deployment"})


async def main() -> None:
    llmq = LLMQueue.from_env()
    await llmq.start()
    view = llmq.registry.current
    big = view.deployments("gpt120b")[0].key  # e.g. "aws-main/us-east-1"
    small = view.deployments("gpt20b")[0].key
    try:
        # 1. gpt120b is off: its `model_fallback` sends the call to gpt20b.
        await llmq.registry.set_enabled("gpt120b", big, False, actor="sample")
        reply = await llmq.converse("gpt120b", REQUEST, max_wait_secs=30)
        print(
            f"gpt120b off -> served by {reply.meta.served_model} (fallback={reply.meta.fallback})"
        )
        assert reply.meta.served_model == "gpt20b"

        # 2. gpt20b off as well: nothing can serve it, the call fails with a kind.
        await llmq.registry.set_enabled("gpt20b", small, False, actor="sample")
        kind: Kind | None = None
        try:
            await llmq.converse("gpt20b", REQUEST, max_wait_secs=3)
        except CallFailed as exc:
            kind = exc.kind
            print(f"gpt20b off -> CallFailed kind={kind!r} retry_after={exc.retry_after_secs}")
            if kind in BUSY:
                pass  # retry after exc.retry_after_secs, or tell the user to wait
            elif kind in UNAVAILABLE:
                pass  # show an error or use another feature
            elif kind == "deadline_exceeded":
                pass  # the call ran out of time: a shorter prompt or a later retry
        assert kind in UNAVAILABLE, f"expected an unavailable kind, got {kind!r}"
    finally:
        await llmq.registry.set_enabled("gpt120b", big, True, actor="sample")
        await llmq.registry.set_enabled("gpt20b", small, True, actor="sample")
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
