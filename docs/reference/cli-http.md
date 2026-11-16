# CLI and HTTP

## `llm-queue` command

Reads the same `LLMQ_*` settings as the app. `--actor NAME` (before the command) records who
made a change in the audit; without it the OS user is used, looked up only by commands that
write (`uid:<n>` when the user has no passwd entry, as in a container run under a bare UID).

| Command | Does |
|---|---|
| `llm-queue status [--model M]` | Status of every model, or of one |
| `llm-queue request ID` | Status of one call (exit 1 when unknown or expired) |
| `llm-queue settings get --model M [--deployment D]` | Effective value of every setting and the level it came from |
| `llm-queue registry get [--model M]` | The live registry (secrets redacted) |
| `llm-queue registry set [--model M] path=value ...` | Live edit (JSON values) |
| `llm-queue registry enable M D` / `disable M D` | Turn one deployment of a model on or off. `D` is `account/name`, where `name` is the region or the inference profile's name as listed under `confirmed` / `fallback`; the edit is written to `deployments.<account>.overrides.<name>` |
| `llm-queue registry diff` | What is live but not in this process's config |
| `llm-queue registry log [--count N] [--path P]` | The audit trail (default 20 entries): publishes, edits and CLI cancels |
| `llm-queue registry publish [--from-version N]` | Publish `LLMQ_REGISTRY`, or roll back to version N |
| `llm-queue registry gc --yes\|--dry-run [--force] [--no-wait]` | Purge the store state of every model and deployment the registry no longer has (`PurgeService.removed`); queues only when empty unless `--force`; `--no-wait` does not wait for a removed deployment's running calls. Prints the purge report |
| `llm-queue purge model M --yes\|--dry-run [--force]` | Purge a removed model: its keys, queues (empty ones unless `--force`) and doorbells. Refused while the registry has it, unless `--force` |
| `llm-queue purge deployment M D --yes\|--dry-run [--force] [--no-wait] [--wait-secs S]` | Purge a removed deployment (`account/name`) of model M; waits for its running calls (`LLMQ_PURGE_WAIT`, `LLMQ_PURGE_WAIT_SECS`) |
| `llm-queue purge tenant T --yes\|--dry-run [--remove keys,queued,running] [--models M1,M2]` | Purge a tenant's fairness state (default `LLMQ_TENANT_PURGE_DEFAULT`); `queued` / `running` cancel its calls and need `LLMQ_TENANT_INDEX` |
| `llm-queue registry schema` | The registry document's JSON Schema ([Registry document](registry.md#json-schema)); needs no store and no `LLMQ_*` settings |
| `llm-queue block --region R` / `--account A` | Stop using a region or an account |
| `llm-queue unblock --region R` / `--account A` | Use it again |
| `llm-queue cancel ID [ID ...] --yes\|--dry-run` | Cancel calls for every caller of each id (at most 1000 ids). Anyone with store access may cancel any id; each cancel is logged (`call_cancel`, `source=cli`) and written to the audit trail with the actor |
| `llm-queue drain M --yes\|--dry-run [--wait] [--timeout S]` | Disable a model for new calls; serve waiting ones (`--wait` until its queues are empty, default timeout 600 s; exit 3 when it times out) |
| `llm-queue dlq list\|show\|redrive\|purge --model M [--priority] [--count N]` | Dead letters of a model's queue (`show --unsafe` prints message bodies; `redrive` and `purge` need `--yes` or `--dry-run`) |
| `llm-queue dlq list\|show\|redrive\|purge --job J [--count N]` | Dead jobs of a job name (instead of `--model`), kept `LLMQ_JOB_DEAD_RETENTION_SECS` (7 days) |
| `llm-queue queues list` | The queues |

Commands that delete or move data (`dlq purge`, `dlq redrive`, `registry gc`, `purge`,
`drain`, `cancel`) refuse to run without `--yes`; `--dry-run` shows what they would do. The CLI reads the
store password like the app does. `--actor` is 1-200 characters without control characters.

**`drain` stays until it is undone.** It is a live edit (`enabled: false` on the model), so the
model takes no new calls until someone sets it back:
`llm-queue registry set --model M enabled=true`. It is refused when live edits are off
(`LLMQ_REGISTRY_LIVE_EDIT=off`); then change `enabled` in the registry and publish.

**Exit codes.** 0 done; 1 an error (a bad name or edit, an unknown or expired request id, a
store outage: one line on stderr, no traceback); 3 a `drain --wait` that ran out of time with
calls still waiting.

## HTTP routes

`llmq.http_router(dependencies=[...], allow_writes=False, actor=None)` returns a FastAPI router
(extra `http`):

```python
app.include_router(llmq.http_router(dependencies=[Depends(require_admin)]), prefix="/llmq")
```

| Route | Auth | Answers |
|---|---|---|
| `GET /health/live` | none | 200 while the library's background loops run and the heartbeat makes progress; 503 when a loop died or the heartbeat stalled for a lease (stays 200 once `stop()` begins). Reads the process's own state, never the store |
| `GET /health/ready` | none | 200 when the registry is loaded and not draining, else 503 (also 503 in strict mode with an evicting store, with `not_ready_reason`); each store as `ok`, `read_only` or `down` (from a probe cached every 5 s; never a 500) |
| `GET /metrics` | none (`metrics_route=True`, off by default) | The Prometheus exposition of a `LLMQ_METRICS_SINK=prometheus` process (extra `metrics`) |
| `GET /status?model=M` | `dependencies` | Status of every model or one |
| `GET /requests/{id}` | `dependencies` | Status of one call |
| `GET /registry` | `dependencies` | The live registry (role ARNs and secret references redacted) |
| `POST /registry?model=M` | `dependencies`, `allow_writes=True`, header `X-LLMQ-Admin` | Live edit: a JSON body of `path: value` (at most 100 paths), audited under `actor(request)` |
| `POST /registry/block` | `dependencies`, `allow_writes=True`, header `X-LLMQ-Admin` | Block or unblock: a JSON body `{"region": R}` or `{"account": A}`, with `"on": true\|false` (default true), audited |
| `POST /purge/model` | as above (the purge routes exist only with `allow_writes=True`) | `{"model": M, "force"?: bool, "dry_run"?: bool}`; the purge report. **`dry_run` defaults to true**: send `false` to purge |
| `POST /purge/deployment` | as above | `{"model": M, "deployment": D, "force"?, "wait"?, "wait_secs"?, "dry_run"?}`; waits at most `LLMQ_HTTP_PURGE_WAIT_SECS` (60): a larger or omitted `wait_secs` is capped to it |
| `POST /purge/tenant` | as above | `{"tenant": T, "remove"?: ["keys", "queued", "running"], "models"?: [M], "dry_run"?}` |
| `POST /registry/gc` | as above | `{"force"?, "dry_run"?}`: everything the registry no longer has; waits for running calls at most `LLMQ_HTTP_PURGE_WAIT_SECS` (60) |
| `GET /purge/scheduled` | as above | Auto purges waiting for their grace |
| `POST /purge/scheduled/cancel` | as above | `{"target": "alias" or "alias/account/name"}`: keep that state |

Status codes of the admin routes (every route except the two health routes):

| Code | When |
|---|---|
| 200 | Done |
| 400 | A refused write: a live edit of a protected or deploy-only field, an invalid value, a bad block body, more than 100 paths, an id or name longer than 256 characters |
| 403 | A write while `allow_writes` is off, or without the `X-LLMQ-Admin` header |
| 404 | An unknown model (`/status`), an unknown or expired request id (`/requests/{id}`) |
| 409 | A write lost the compare-and-set race with other writes on every try; the request is valid, retry after `Retry-After` (1 s). A purge refused (`PurgeRefused`: still in the registry, or the tenant index is off), with no `Retry-After` |
| 422 | A write body that is not a JSON object (FastAPI's own validation) |
| 503 | The state store is unreachable, or the registry is not loaded yet (the router was hit before `start()` finished) |

`allow_writes=True` needs `dependencies=` (else `ConfigError`) and `actor=` (a function from the
request to the authenticated name). With no `dependencies` the read routes are open to anyone
who reaches the app (`/registry` lists accounts, regions, model ids and base URLs), and the
router logs `admin_routes_unauthenticated` when it is built. See
[Secure a deployment](../how-to/secure.md#4-protect-the-http-write-routes).

The `/drain` route used by a preStop hook is the app's own (see `samples/09_http_router.py`);
the router has none. It must answer GET, which is what `preStop: httpGet` sends.
