# Purge store state

When a model or deployment leaves the registry, or a tenant leaves your app, its state stays
in the store: budgets, limiter, breaker, health, sticky sessions, queues, tenant clocks. A
purge deletes it. One service does every purge, `PurgeService`; the CLI, the built-in HTTP
routes and your own API routes all call it.

Every purge can run as a **dry run** first (it reports what it would delete and changes
nothing). A real purge writes an entry to the registry audit (`llm-queue registry log`), logs
`llmq_purge` and counts `llmq_purged_keys_total{kind}`. Each returns a `PurgeReport`;
`report.as_dict()` is JSON-ready:

| Field | Holds |
|---|---|
| `target`, `kind` | What was purged: `alias`, `alias/account/name`, `tenant:<hash>` or `*`; `model`, `deployment`, `tenant` or `removed` |
| `deleted_keys`, `deleted_fields` | Keys deleted; fields of shared hashes removed (health, rotation, tenant weights) |
| `queues` | Queues deleted |
| `skipped` | `(thing, why)` pairs: a queue still holding messages, a deployment still running calls |
| `cancelled` | Calls a tenant purge cancelled |
| `dry_run`, `actor`, `at` | As given; wall time of the report |

## From Python

```python
report = await llmq.purge.plan_tenant("acme")                # dry run: what would go
report = await llmq.purge.tenant("acme", actor="ops@example.com")
report = await llmq.purge.deployment("gpt20b", "aws-main/us-east-1", actor="ops")
report = await llmq.purge.model("old-model", actor="ops")
report = await llmq.purge.removed(actor="ops")               # all the registry no longer has
pending = await llmq.purge.scheduled()                       # auto purges in their grace
await llmq.purge.cancel_scheduled("gpt20b/aws-main/us-east-1")  # keep that state
```

`llmq.purge` works on a started queue. For an admin worker without a queue, build one from the
same `LLMQ_*` settings and close it when done:

```python
from llm_queue import PurgeService

async with PurgeService.from_env() as purge:
    print((await purge.removed(actor="nightly", dry_run=True)).as_dict())
```

Errors are `LLMQueueError`s: `PurgeRefused` when a purge would delete state still in use
(nothing is deleted), `StoreUnavailable` when the store cannot serve it, `ConfigError` for a
bad argument.

## Use it from your own API

Put the purge behind your own route and your own auth; the library needs nothing else. A dry
run by default, as the built-in routes do:

```python
from fastapi import Depends, FastAPI, HTTPException

from llm_queue import PurgeRefused

app = FastAPI()


@app.post("/admin/tenants/{tenant_id}/purge")
async def purge_tenant(tenant_id: str, dry_run: bool = True, user=Depends(admin_user)):
    try:
        report = await llmq.purge.tenant(tenant_id, actor=user.name, dry_run=dry_run)
    except PurgeRefused as exc:
        raise HTTPException(409, str(exc)) from exc
    return report.as_dict()
```

## What each purge deletes

| Purge | Deletes | Refused (`PurgeRefused`) |
|---|---|---|
| `model(alias, force=False)` | Every `P:{m:alias}:*` key; its queues (`llm-<alias>`, its `queue.name` now or in a kept registry version, and `-priority`) and doorbells. A queue still holding messages is skipped unless `force` | While the registry still has the model, unless `force` |
| `deployment(alias, "account/name", force=False, wait=None, wait_secs=None)` | `rate:D`, `ac:D`, `cb:D`, `lease:D:p\|n`, `terr:D`, `thr:D:*`; fields `D\|*` of `health` and `D`, `rr\|D`, `cw\|D` of `rot`; `sticky:*` keys pointing at it | While the registry still has the deployment, unless `force` |
| `tenant(tenant_id, remove=None, models=None)` | In every model (or `models`): `tf:t:<h>`, `tf:s:<h>`, the tenant in `tf:act` and `tf:w` (the weight sum `~sum` is corrected in the same script, so other tenants' shares are right at once) | `remove` with `queued` / `running` while `LLMQ_TENANT_INDEX` is off |
| `removed(force=False, wait=None)` | `model` for every model with state in the store (or queues in a kept registry version) that the registry no longer has, and `deployment` for every such deployment of a live model | Never |

Every deleted key is also removed from the expiry index `P:ttl`. Keys go with `UNLINK` in
pipelined batches of `LLMQ_PURGE_BATCH` (500).

### Running calls on a deployment

A deployment purge waits for its running calls first (`wait`, default `LLMQ_PURGE_WAIT=true`)
for at most `wait_secs` (default `LLMQ_PURGE_WAIT_SECS=300`). When calls are still running then,
nothing is deleted and the report's `skipped` says how many. `wait=False` deletes at once; keys a
finishing call writes back are cleaned by the next purge or expire on their own.

### Tenant options

The tenant id is the raw id your app passes as `tenant_id` (or `user_id` when there is no
tenant). It is hashed with `LLMQ_ID_HASH_SALT` exactly as admission hashes it; the report and
the audit name only the hash.

| `remove` | Does |
|---|---|
| `keys` (default, `LLMQ_TENANT_PURGE_DEFAULT`) | Deletes the tenant's fairness state. A tenant that calls again starts fresh |
| `queued` | Cancels the tenant's calls still waiting (not running) |
| `running` | Cancels every call of the tenant, running ones included |

`queued` and `running` need the per-tenant request index: `LLMQ_TENANT_INDEX=true` makes each
new call add its id to `P:tenant:<hash>:reqs` (one extra write per call). Only calls made after
it was turned on are indexed. Without it, asking for them raises `PurgeRefused` naming the
setting. Cancels go through the normal cancel path (every caller gets `cancelled`) and are
logged `call_cancel` with `source=purge`.

## Automatic purge

With `LLMQ_AUTO_PURGE=true` (the default), when a publish or reload drops a model or a
deployment, one worker (lock `P:lock:purge`) records it in `P:purge` with a due time of now
plus `LLMQ_PURGE_GRACE_SECS` (900). The worker sweep loop then purges each due target **that is
still absent** from the registry; a rollback within the grace keeps the state (the entry is
dropped, logged `purge_unscheduled`). A dropped model's purge covers its deployments. Auto
purges are audited with the actor `auto`.

- **Workers only.** Auto purge runs in worker processes; a caller-only process (`workers=0`)
  never schedules or runs one.
- **The first tick is a baseline.** Each worker compares the registry with the one it saw at
  its previous tick. Its first tick after start only records what it sees, so starting a pod
  never schedules a purge. A drop made while no worker ran is not noticed: clean it with
  `llm-queue registry gc` (or `purge.removed()`).
- **Strict registry read.** Before it schedules or runs a purge, the worker reads the registry
  as stored (not its own cached copy). If that read fails or the document is refused (for
  example it does not validate), it skips the purge and tries again next tick: a stale or
  fallback copy never deletes a live model's state.
- **No wait for running calls.** An auto deployment purge does not wait for running calls
  (`wait=False`). The grace covers them: by default `LLMQ_PURGE_GRACE_SECS` (900) is as long as
  the longest call the registry allows (`timeout_max_secs`, 900), and a dropped deployment gets
  no new calls. If you raise `timeout_max_secs`, raise the grace to at least the same.

Set `LLMQ_AUTO_PURGE=false` to purge only by hand (the 0.1.0b2 behaviour).

## CLI

```bash
llm-queue purge model old-model --dry-run
llm-queue --actor ops purge model old-model --yes [--force]
llm-queue --actor ops purge deployment gpt20b aws-main/us-east-1 --yes [--no-wait] [--wait-secs 60]
llm-queue --actor ops purge tenant acme --yes [--remove keys,queued] [--models gpt20b,gpt120b]
llm-queue --actor ops registry gc --yes [--force] [--no-wait]   # every removed model and deployment
```

Each needs `--yes` or `--dry-run` and prints the report. `--no-wait` (on `purge deployment`
and `registry gc`) deletes a deployment's state without waiting for its running calls. See
[CLI and HTTP](../reference/cli-http.md).

## HTTP

With `http_router(allow_writes=True, dependencies=[...], actor=...)`, the router also mounts
`POST /purge/model`, `/purge/deployment`, `/purge/tenant`, `/registry/gc`, `GET
/purge/scheduled` and `POST /purge/scheduled/cancel`. They need the `X-LLMQ-Admin` header like
the other writes, take JSON bodies and record `actor(request)`. **`dry_run` defaults to true**:
send `"dry_run": false` to purge.

```bash
curl -X POST https://app/llmq/purge/tenant -H 'X-LLMQ-Admin: 1' \
  -H 'Content-Type: application/json' -d '{"tenant": "acme", "dry_run": false}'
```

A refused purge answers 409 (no `Retry-After`), a bad body 400, a store outage 503.

An HTTP request should not hang for minutes, so a purge over HTTP that waits for running calls
(`/purge/deployment`, `/registry/gc`) waits at most `LLMQ_HTTP_PURGE_WAIT_SECS` (60): a larger
or omitted `wait_secs` is capped to it. Calls still running then are reported in `skipped` and
nothing of that deployment is deleted; purge again later, or send `"wait": false`.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `LLMQ_AUTO_PURGE` | true | Auto purge after a registry drop |
| `LLMQ_PURGE_GRACE_SECS` | 900 | Grace before an auto purge |
| `LLMQ_PURGE_WAIT` | true | A deployment purge waits for its running calls |
| `LLMQ_PURGE_WAIT_SECS` | 300 | ... at most this long |
| `LLMQ_HTTP_PURGE_WAIT_SECS` | 60 | The longest an HTTP purge (deployment, gc) waits for running calls |
| `LLMQ_PURGE_BATCH` | 500 | Keys per `UNLINK` batch |
| `LLMQ_TENANT_PURGE_DEFAULT` | `keys` | What a tenant purge removes |
| `LLMQ_TENANT_INDEX` | false | Index each call by tenant (needed for `queued` / `running`) |

Ranges are in [Environment variables](../reference/environment.md#purge).
