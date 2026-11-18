# Purge store state

When a model or deployment leaves the registry, or a tenant leaves your app, its state stays
in the store: budgets, limiter, breaker, health, sticky sessions, queues, tenant clocks. A
purge deletes it. One service does every purge, reached as `llmq.admin.purge`;
the CLI, the built-in HTTP routes and your own API routes all call it (see
[Admin API](../reference/admin-api.md)).

Every purge is a **dry run** unless you pass `dry_run=False` (`--yes` on the CLI,
`"dry_run": false` over HTTP): it reports what it would delete and changes nothing. A real purge
writes an entry to the audit (`llm-queue audit list`), logs `llmq_purge` and counts
`llmq_purged_keys_total{kind}`. `llmq.admin.purge.*` returns the report as a JSON-ready dict
(the internal service returns a `PurgeReport`; `report.as_dict()` is the same dict):

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
purge = llmq.admin.purge
plan = await purge.tenant("acme", actor="ops@example.com")   # dry run: what would go
done = await purge.tenant("acme", actor="ops@example.com", dry_run=False)
await purge.deployment("gpt20b", "aws-main/us-east-1", actor="ops", dry_run=False)
await purge.model("old-model", actor="ops", dry_run=False)
await purge.removed(actor="ops", dry_run=False)              # all the registry no longer has
pending = await purge.scheduled()                            # auto purges in their grace
await purge.unschedule("gpt20b/aws-main/us-east-1", actor="ops", dry_run=False)  # keep it
```

For an admin worker without a queue, build the same services from the `LLMQ_*` settings and
close them when done:

```python
from llm_queue import AdminServices

async with AdminServices.from_env() as admin:
    print(await admin.purge.removed(actor="nightly"))  # a dry run
```

`AdminServices.from_env()` builds the same services without a queue (`.purge`). Errors are `LLMQueueError`s: `AdminRefused` when a purge would delete state still in use
(nothing is deleted), `StoreUnavailable` when the store cannot serve it, `ConfigError` for a
bad argument.

## Use it from your own API

Put the purge behind your own route and your own auth; the library needs nothing else. A dry
run by default, as the built-in routes do:

```python
from fastapi import Depends, FastAPI, HTTPException

from llm_queue import AdminRefused

app = FastAPI()


@app.post("/admin/tenants/{tenant_id}/purge")
async def purge_tenant(tenant_id: str, dry_run: bool = True, user=Depends(admin_user)):
    try:
        return await llmq.admin.purge.tenant(tenant_id, actor=user.name, dry_run=dry_run)
    except AdminRefused as exc:
        raise HTTPException(409, str(exc)) from exc
```

## What each purge deletes

| Purge | Deletes | Refused (`AdminRefused`) |
|---|---|---|
| `model(model, force=False)` | Every `P:{m:alias}:*` key; its queues (`llm-<alias>`, its `queue.name` now or in a kept registry version, and `-priority`) and doorbells. A queue still holding messages is skipped unless `force` | While the registry still has the model, unless `force` |
| `deployment(model, "account/name", force=False, wait=None, wait_secs=None)` | `rate:D`, `ac:D`, `cb:D`, `lease:D:p\|n`, `terr:D`, `thr:D:*`; fields `D\|*` of `health` and `D`, `rr\|D`, `cw\|D` of `rot`; `sticky:*` keys pointing at it | While the registry still has the deployment, unless `force` |
| `tenant(tenant, remove=None, models=None)` | In every model (or `models`): `tf:t:<h>`, `tf:s:<h>`, the tenant in `tf:act` and `tf:w` (the weight sum `~sum` is corrected in the same script, so other tenants' shares are right at once) | `remove` with `queued` / `running` while the request index is off (`LLMQ_REQUESTS_INDEX_ENABLED=false`) |
| `removed(force=False, wait=None)` | `model` for every model with state in the store (or queues in a kept registry version) that the registry no longer has, and `deployment` for every such deployment of a live model | Never |
| `queue(model, tenant=None)` | Cancels a model's queued calls (of one tenant) | While the request index is off (`LLMQ_REQUESTS_INDEX_ENABLED=false`) |

Every deleted key is also removed from the expiry index `P:ttl`. Keys go with `UNLINK` in
pipelined batches of `LLMQ_ADMIN_BATCH` (500).

### Running calls on a deployment

A deployment purge waits for its running calls first (`wait`, default `LLMQ_PURGE_WAIT_ENABLED=true`)
for at most `wait_secs` (default `LLMQ_PURGE_WAIT_SECS=300`). When calls are still running then,
nothing is deleted and the report's `skipped` says how many. `wait=False` deletes at once; keys a
finishing call writes back are cleaned by the next purge or expire on their own.

### Tenant options

The tenant id is the raw id your app passes as `tenant_id` (or `user_id` when there is no
tenant). It is hashed with `LLMQ_ID_HASH_SALT` exactly as admission hashes it; the report and
the audit name only the hash.

| `remove` | Does |
|---|---|
| `keys` (default, `LLMQ_PURGE_TENANT_REMOVE`) | Deletes the tenant's fairness state and its name beside its hash (`tqn`, kept only with `LLMQ_TENANT_NAMES_ENABLED`). A tenant that calls again starts fresh |
| `queued` | Cancels the tenant's calls still waiting (not running); their callers get `cancelled` at once, as with `requests cancel` |
| `running` | Cancels every call of the tenant, running ones included |

`queued` and `running` read the request index of each model (`P:{m:ALIAS}:tq:<hash>`, on by
default: `LLMQ_REQUESTS_INDEX_ENABLED`), written by the scripts that send and end each call
(no extra round trip). With the index off, asking for them raises `AdminRefused` naming the
setting. The index itself is not deleted: each cancelled call leaves it with its final write.
Cancels go through the normal cancel path (every caller gets `cancelled`) and are
logged `call_cancel` with `source=purge`.

## Automatic purge

With `LLMQ_PURGE_AUTO_ENABLED=true` (the default), when a publish or reload drops a model or a
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
  `llm-queue purge removed --yes` (or `llmq.admin.purge.removed(..., dry_run=False)`).
- **Strict registry read.** Before it schedules or runs a purge, the worker reads the registry
  as stored (not its own cached copy). If that read fails or the document is refused (for
  example it does not validate), it skips the purge and tries again next tick: a stale or
  fallback copy never deletes a live model's state.
- **No wait for running calls.** An auto deployment purge does not wait for running calls
  (`wait=False`). The grace covers them: by default `LLMQ_PURGE_GRACE_SECS` (900) is as long as
  the longest call the registry allows (`timeout_max_secs`, 900), and a dropped deployment gets
  no new calls. If you raise `timeout_max_secs`, raise the grace to at least the same.

Set `LLMQ_PURGE_AUTO_ENABLED=false` to purge only by hand (the 0.1.0b2 behaviour).

## CLI

```bash
llm-queue purge model old-model                    # a dry run: the report of what would go
llm-queue --actor ops purge model old-model --yes [--force]
llm-queue --actor ops purge deployment gpt20b aws-main/us-east-1 --yes [--no-wait] [--wait-secs 60]
llm-queue --actor ops purge tenant acme --yes [--remove keys,queued] [--models gpt20b,gpt120b]
llm-queue --actor ops purge removed --yes [--force] [--no-wait]   # every removed model and deployment
llm-queue purge scheduled
llm-queue --actor ops purge unschedule gpt20b/aws-main/us-east-1 --yes
```

Without `--yes` each prints the dry-run report. `--no-wait` (on `purge deployment` and `purge
removed`) deletes a deployment's state without waiting for its running calls. See
[Admin API](../reference/admin-api.md).

## HTTP

`GET /purge/scheduled` is a read. With `http_router(allow_writes=True)` (or
`LLMQ_ADMIN_WRITES_ENABLED=true`) the router also mounts `POST /purge/model`,
`/purge/deployment`, `/purge/tenant`, `/purge/queue`, `/purge/removed` and `/purge/unschedule`.
They are guarded like the other writes (the admin token, or `dependencies` plus the
`X-LLMQ-Admin` header), take JSON bodies and are audited under the actor. **`dry_run` defaults
to true**: send `"dry_run": false` to purge.

```bash
curl -X POST https://app/llmq/purge/tenant -H "X-LLMQ-Token: $LLMQ_ADMIN_TOKEN" \
  -H 'X-LLMQ-Actor: ops' -H 'Content-Type: application/json' \
  -d '{"tenant": "acme", "dry_run": false}'
```

A refused purge answers 409 (no `Retry-After`), a bad body 400, a store outage 503.

An HTTP request should not hang for minutes, so a purge over HTTP that waits for running calls
(`/purge/deployment`, `/purge/removed`) waits at most `LLMQ_ADMIN_HTTP_WAIT_SECS` (60): a larger
or omitted `wait_secs` is capped to it. Calls still running then are reported in `skipped` and
nothing of that deployment is deleted; purge again later, or send `"wait": false`.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `LLMQ_PURGE_AUTO_ENABLED` | true | Auto purge after a registry drop |
| `LLMQ_PURGE_GRACE_SECS` | 900 | Grace before an auto purge |
| `LLMQ_PURGE_WAIT_ENABLED` | true | A deployment purge waits for its running calls |
| `LLMQ_PURGE_WAIT_SECS` | 300 | ... at most this long |
| `LLMQ_ADMIN_HTTP_WAIT_SECS` | 60 | The longest an HTTP purge (deployment, removed) or model drain waits |
| `LLMQ_ADMIN_BATCH` | 500 | Keys per `UNLINK` batch (and ids per admin pipeline) |
| `LLMQ_PURGE_TENANT_REMOVE` | `keys` | What a tenant purge removes |
| `LLMQ_REQUESTS_INDEX_ENABLED` | true | Index each call by model and tenant (needed for `queued` / `running`) |

Ranges are in [Environment variables](../reference/environment.md#purge).
