"""Purge store state: a tenant that left, and a deployment's plan (admin jobs, runbooks).

`llmq.purge` is the `PurgeService` of a started queue (`PurgeService.from_env()` builds one for
an admin worker without a queue). Each purge has a dry run (`plan_*`, or `dry_run=True`) that
reports what it would delete and changes nothing, and returns a `PurgeReport`;
`report.as_dict()` is JSON-ready for an admin API. A real purge is audited (`registry.log`).

A purge that would delete state still in use raises `PurgeRefused` (HTTP 409 on the built-in
routes): here, a deployment the registry still has. `force=True` overrides it; a plan with
`force=True` only shows what would go. The sample gives a tenant some fairness state, plans and
runs its purge, then plans a deployment purge.

    python scripts/samples.py 10_purge [--live]
"""

from __future__ import annotations

import asyncio
import json

from llm_queue import LLMQueue, PurgeRefused, request_context

REQUEST = {
    "messages": [{"role": "user", "content": [{"text": "One word: ok"}]}],
    "inferenceConfig": {"maxTokens": 100},
}


def show(title: str, report: dict[str, object]) -> None:
    """One report, as an admin API would return it (without the wall time)."""
    print(f"{title}: {json.dumps({k: v for k, v in report.items() if k != 'at'})}")


async def main() -> None:
    llmq = LLMQueue.from_env()
    await llmq.start()
    try:
        # A tenant's fairness state: a quota turned on live, one call as tenant "acme".
        await llmq.registry.set(
            {"fairness": {"mode": "quota", "rpm": 30}}, model="gpt20b", actor="sample"
        )
        try:
            with request_context(tenant_id="acme"):
                await llmq.converse("gpt20b", REQUEST)

            plan = await llmq.purge.plan_tenant("acme")  # dry run: nothing is deleted
            show("plan tenant", plan.as_dict())
            assert plan.dry_run
            assert plan.deleted_keys > 0

            done = await llmq.purge.tenant("acme", actor="sample")
            show("purge tenant", done.as_dict())
            assert not done.dry_run
            assert done.deleted_keys == plan.deleted_keys
            entry = (await llmq.registry.log(count=1))[0]  # the purge's audit entry
            print(f"audit: op={entry['op']} actor={entry['actor']}")
            assert entry["op"] == "purge_tenant"

            again = await llmq.purge.plan_tenant("acme")  # nothing left to delete
            print(f"plan tenant again: deleted_keys={again.deleted_keys}")
            assert again.deleted_keys == 0
        finally:
            await llmq.registry.set({"fairness": None}, model="gpt20b", actor="sample")

        # A deployment still in the registry: refused, unless forced; plan only.
        deployment = (await llmq.status("gpt20b"))["deployments"][0]["name"]
        try:
            await llmq.purge.plan_deployment("gpt20b", deployment)
        except PurgeRefused as exc:
            print(f"plan deployment {deployment}: refused ({exc})")
        else:
            raise AssertionError("a live deployment's purge should be refused")
        forced = await llmq.purge.plan_deployment("gpt20b", deployment, force=True)
        show("plan deployment (force)", forced.as_dict())
        assert forced.dry_run
        assert forced.deleted_keys > 0
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
