"""Background jobs (ingestion, nightly re-embedding): queued work that runs at least once.

Register the job function in every worker process before `start()`, with `concurrency` to cap
its runs per process; any process can send a job. A job that raises is retried with a growing
delay, so make it safe to run twice (upsert by `current_job().job_id`); when the retries run
out it is dead-lettered. `llmq.admin.dlq.list(job=...)` lists the dead jobs and
`llmq.admin.dlq.redrive(job=...)` sends them back once the cause is fixed. LLM calls made
inside a job go through the same queue and budgets as user calls, at normal priority. Jobs
return nothing: this sample learns "done" through an in-process `asyncio.Queue`, which works
only because sender and worker are one process.

The retry settings here are cut to a second so the dead letter shows quickly; an app keeps
the defaults (5 s doubling to 5 min, for about an hour).

    python scripts/samples.py 06_jobs [--live]
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from llm_queue import LLMQueue, current_job

done: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
FAST_RETRIES = {
    "LLMQ_JOB_RETRY_BASE_SECS": "0.2",
    "LLMQ_JOB_RETRY_MAX_SECS": "0.2",
    "LLMQ_JOB_RETRY_FOR_SECS": "0.5",
}
feed = {"down": True}  # the outside service the second job needs; fixed before the redrive


async def main() -> None:
    llmq = LLMQueue.from_env({**os.environ, **FAST_RETRIES})

    async def index_documents(payload: dict[str, Any]) -> None:
        result = await llmq.embed("cohere", payload["texts"], input_type="search_document")
        # write result.vectors to the vector store here, keyed by payload["ids"]
        await done.put({"ids": payload["ids"], "vectors": len(result.vectors)})

    async def sync_feed(payload: dict[str, Any]) -> None:
        job = current_job()
        assert job is not None
        if feed["down"]:
            raise ConnectionError(f"feed down (attempt {job.attempt})")
        await done.put({"feed": payload["feed"], "attempt": job.attempt})

    llmq.register_job("index_documents", index_documents, concurrency=2)
    llmq.register_job("sync_feed", sync_feed)
    await llmq.start()
    try:
        job_id = await llmq.send_job(
            "index_documents", {"ids": ["d1", "d2"], "texts": ["first report", "second report"]}
        )
        print(f"sent job {job_id}")
        finished = await asyncio.wait_for(done.get(), timeout=60)
        print(f"job finished: {finished}")
        assert finished["vectors"] == 2

        # A job that keeps failing: retried, then dead-lettered.
        await llmq.send_job("sync_feed", {"feed": "nse"}, job_id="feed-nse")
        dead: list[dict[str, Any]] = []
        async with asyncio.timeout(30):
            while not dead:
                await asyncio.sleep(0.2)
                dead = (await llmq.admin.dlq.list(job="sync_feed"))["letters"]
        print(f"dead letter: job {dead[0]['job_id']}, fails {dead[0]['fails']}")

        feed["down"] = False  # the cause is fixed: run the dead jobs again
        redrive = await llmq.admin.dlq.redrive(job="sync_feed", actor="sample", dry_run=False)
        print(f"redriven: {redrive['moved']}")
        rerun = await asyncio.wait_for(done.get(), timeout=30)
        print(f"after redrive: {rerun}")
        assert rerun["feed"] == "nse"
        assert not (await llmq.admin.dlq.list(job="sync_feed"))["letters"]
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
