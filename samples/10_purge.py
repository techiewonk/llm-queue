"""Purge store state: a tenant that left, and a deployment's plan (admin jobs, runbooks).

`llmq.admin.purge` purges a started queue's environment (`AdminServices.from_env()` builds the
same services for an admin worker without a queue). Every purge is a dry run unless
`dry_run=False`: the dry run reports what it would delete and changes nothing. The answer is
the purge report as JSON, ready for an admin API. A real purge is audited
(`llmq.admin.audit.list`).

A purge that would delete state still in use raises `AdminRefused` (HTTP 409 on the built-in
routes): here, a deployment the registry still has. `force=True` overrides it; a dry run with
`force=True` only shows what would go. The sample gives a tenant some fairness state, plans and
runs its purge, then plans a deployment purge.

    python scripts/samples.py 10_purge [--live]
"""

from __future__ import annotations

import asyncio
import json

from llm_queue import AdminRefused, LLMQueue, request_context

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
    admin = llmq.admin
    try:
        # A tenant's fairness state: a quota turned on live, one call as tenant "acme".
        await admin.registry.set(
            {"fairness": {"mode": "quota", "rpm": 30}}, model="gpt20b", actor="sample",
            dry_run=False,
        )  # fmt: skip
        try:
            with request_context(tenant_id="acme"):
                await llmq.converse("gpt20b", REQUEST)

            plan = await admin.purge.tenant("acme", actor="sample")  # dry run: nothing deleted
            show("plan tenant", plan)
            assert plan["dry_run"]
            assert plan["deleted_keys"] > 0

            done = await admin.purge.tenant("acme", actor="sample", dry_run=False)
            show("purge tenant", done)
            assert not done["dry_run"]
            assert done["deleted_keys"] == plan["deleted_keys"]
            entry = (await admin.audit.list(1))["entries"][0]  # the purge's audit entry
            print(f"audit: op={entry['op']} actor={entry['actor']}")
            assert entry["op"] == "purge_tenant"

            again = await admin.purge.tenant("acme", actor="sample")  # nothing left to delete
            print(f"plan tenant again: deleted_keys={again['deleted_keys']}")
            assert again["deleted_keys"] == 0
        finally:
            await admin.registry.set(
                {"fairness": None}, model="gpt20b", actor="sample", dry_run=False
            )

        # A deployment still in the registry: refused, unless forced; plan only.
        deployment = (await llmq.status("gpt20b"))["deployments"][0]["name"]
        try:
            await admin.purge.deployment("gpt20b", deployment, wait=False, actor="sample")
        except AdminRefused as exc:
            print(f"plan deployment {deployment}: refused ({exc})")
        else:
            raise AssertionError("a live deployment's purge should be refused")
        forced = await admin.purge.deployment(
            "gpt20b", deployment, force=True, wait=False, actor="sample"
        )
        show("plan deployment (force)", forced)
        assert forced["dry_run"]
        assert forced["deleted_keys"] > 0
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
