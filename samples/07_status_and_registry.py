"""Operate: status of models and calls, and live registry edits (admin pages, runbooks).

`status(model)` shows waiting calls, calls in progress and each deployment's RPM / TPM used
and left this minute. `request_status(id)` follows one call. `registry.set` changes a safe
setting live in every pod (audited); `registry.log` lists the changes. The CLI `llm-queue`
and the HTTP router (`llmq.http_router()`, extra `[http]`) expose the same.

Per-tenant fairness is a registry setting too, off by default: `"quota"` caps each tenant's
rpm / tpm, `"share"` splits the budget by weight and lends what is unused, `"fair_queue"`
serves waiting tenants round robin. A call's tenant is its `tenant_id` (else its `user_id`)
from `request_context`. While it is on, `status(model)` has a `fairness` entry (counts only,
never a tenant id). The sample turns a quota on live, makes a call as a tenant, and turns it
back off.

    python scripts/samples.py 07_status_and_registry [--live]
"""

from __future__ import annotations

import asyncio

from llm_queue import LLMQueue, request_context

REQUEST = {
    "messages": [{"role": "user", "content": [{"text": "One word: ok"}]}],
    "inferenceConfig": {"maxTokens": 100},
}


async def main() -> None:
    llmq = LLMQueue.from_env()
    await llmq.start()
    try:
        reply = await llmq.converse("gpt20b", REQUEST)
        call = await llmq.request_status(reply.request_id)
        print(f"request {reply.request_id}: state={call['state']}")

        model = await llmq.status("gpt20b")
        for dep in model["deployments"]:
            print(
                f"{dep['name']}: rpm {dep['rpm']['used']}/{dep['rpm']['limit']} "
                f"tpm {dep['tpm']['used']}/{dep['tpm']['limit']} healthy={dep['healthy']} "
                f"breaker={dep['breaker']}"
            )
        print(f"waiting={model['waiting']} in_progress={model['in_progress']}")

        before = llmq.registry.current.value("gpt20b", "rpm")
        await llmq.registry.set({"rpm": 45}, model="gpt20b", actor="sample")
        print(f"gpt20b rpm {before} -> {llmq.registry.current.value('gpt20b', 'rpm')} (live)")
        for entry in await llmq.registry.log(count=3):
            print(f"  audit: {entry}")
        await llmq.registry.set({"rpm": before}, model="gpt20b", actor="sample")

        # Fairness, live: at most 30 calls a minute per tenant, then off again (None deletes
        # the model's own setting, back to the registry default "off").
        await llmq.registry.set(
            {"fairness": {"mode": "quota", "rpm": 30}}, model="gpt20b", actor="sample"
        )
        try:
            with request_context(tenant_id="acme"):
                await llmq.converse("gpt20b", REQUEST)
            print(f"fairness on: {(await llmq.status('gpt20b'))['fairness']}")
        finally:
            await llmq.registry.set({"fairness": None}, model="gpt20b", actor="sample")
        mode = llmq.registry.current.value("gpt20b", "fairness")["mode"]
        print(f"fairness {mode}: in status: {'fairness' in await llmq.status('gpt20b')}")
        assert call["state"] == "done"
        print(f"health: {await llmq.health()}")
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
