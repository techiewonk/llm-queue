# Admin API

Every admin operation is one service method under `llmq.admin`, and the same method is reached
from the command line (`llm-queue ...`) and over HTTP (`llmq.http_router()`). One table in the
code, `llm_queue/engine/admin/ops.py`, generates the CLI sub-commands and the HTTP routes, and a
test compares the three surfaces with it, so an operation cannot ship on one surface only.

Transport rules (authentication, the CSRF header, status codes and exit codes) are in
[CLI and HTTP](cli-http.md). To wire the routes into an app, see
[Integrate with an app](../how-to/integrate.md).

## Rules every operation follows

- **Reads** change nothing and write no audit entry.
- **Writes** take `actor` (who made the change) and are a **dry run by default** on every
  surface: Python `dry_run=True`, CLI without `--yes`, HTTP without `"dry_run": false`. A dry
  run runs every check the real change runs and reports what it would do; nothing is written.
- Every real write is **audited**: one entry in the registry's audit stream (`audit list`,
  `GET /audit`) with `actor`, `op`, the path it touched and its counts (never a tenant name),
  and one `llmq_admin` log line (`kind`: `requests`, `purge`, `dlq`, `drain`, `models`), all
  through one writer (`engine/audit.py`); a purge also logs `llmq_purge` with what it deleted.
  Registry edits and publishes are audited by the registry's own script (`op` `edit` /
  `publish`).
- The actor is 1-128 characters without control characters, on every surface; every write
  checks it before it acts (`ConfigError`, HTTP 400), dry runs too.
- The actor is `--actor` (default the OS user) on the CLI, `http:<actor(request)>` with the
  app's `actor=` function, or `token:<X-LLMQ-Actor>` with the admin token and no `actor=`
  (`token:unknown` without the header: a claimed name, marked as such).
- Process-local operations (drain, health, metrics) act on the process that serves them;
  outside a running queue (`AdminServices.from_env()`) they refuse with `AdminRefused`.

```python
from llm_queue import LLMQueue

llmq = LLMQueue.from_env()
await llmq.start()
plan = await llmq.admin.models.disable("sonnet", actor="ann")  # dry run: the diff it would make
done = await llmq.admin.models.disable("sonnet", actor="ann", dry_run=False)
```

Without a running queue (a script, an admin worker):

```python
from llm_queue import AdminServices

async with AdminServices.from_env() as admin:
    print(await admin.queues.list())
```

## Operations

Guard: `open` needs nothing; `read` needs the read token, the admin token or the app's
`dependencies`; `write` needs the admin token or `dependencies` (plus the `X-LLMQ-Admin`
header), `allow_writes=True` (`LLMQ_ADMIN_WRITES_ENABLED`) and an actor. Over HTTP, a `GET`
takes its arguments from the query string and a `POST` from a JSON object body; unknown or
mistyped fields are a 400.

| Op | Verb | Python | CLI | HTTP | Guard | Audit op | Dry run first |
|---|---|---|---|---|---|---|---|
| `ops.index` | read | `await llmq.admin.ops.index()` | `llm-queue ops` | `GET /` | read |  |  |
| `health.live` | read | `await llmq.health()` | process-local | `GET /health/live` | open |  |  |
| `health.ready` | read | `await llmq.health()` | process-local | `GET /health/ready` | open |  |  |
| `metrics.get` | read | the metrics sink (`LLMQ_METRICS_SINK`) | process-local | `GET /metrics` | open |  |  |
| `process.describe` | read | `llmq.admin.describe()` | `llm-queue describe` | `GET /describe` | read |  |  |
| `drain.get` | read | `llmq.admin.drain.get()` | process-local | `GET /drain` | read |  |  |
| `drain.start` | update | `await llmq.admin.drain.start(actor="ann")` | process-local (pods use `LLMQ_DRAIN_FILE`) | `POST /drain` | write | `drain` | no |
| `drain.resume` | update | `await llmq.admin.drain.resume(actor="ann")` | process-local | `POST /resume` | write | `resume` | no |
| `status.get` | read | `await llmq.admin.status.get("sonnet", tenant="acme")` | `llm-queue status [--model M] [--tenant T]` | `GET /status?model=M&tenant_hash=H` | read |  |  |
| `status.health` | read | `await llmq.admin.status.health()` | `llm-queue health [--model M]` | `GET /status/health?model=M` | read |  |  |
| `registry.get` | read | `await llmq.admin.registry.get()` | `llm-queue registry get [--model M]` | `GET /registry` | read |  |  |
| `registry.versions` | read | `await llmq.admin.registry.versions()` | `llm-queue registry versions` | `GET /registry/versions` | read |  |  |
| `registry.diff` | read | `await llmq.admin.registry.diff()` | `llm-queue registry diff` | `GET /registry/diff` | read |  |  |
| `registry.schema` | read | `llmq.admin.registry.schema()` | `llm-queue registry schema` | `GET /registry/schema` | read |  |  |
| `registry.set` | update | `await llmq.admin.registry.set({"rpm": 400}, model="sonnet", actor="ann", dry_run=False)` | `llm-queue registry set --model M rpm=400 [--yes]` | `POST /registry` | write | `edit` | yes |
| `registry.publish` | create | `await llmq.admin.registry.publish(actor="ann", dry_run=False)` | `llm-queue registry publish [--yes]` | `POST /registry/publish` | write | `publish` | yes |
| `registry.rollback` | update | `await llmq.admin.registry.rollback(41, actor="ann", dry_run=False)` | `llm-queue registry rollback VERSION [--yes]` | `POST /registry/rollback` | write | `publish` | yes |
| `audit.list` | read | `await llmq.admin.audit.list(20, op="purge_tenant")` | `llm-queue audit list [--count N] [--path P] [--op O]` | `GET /audit` | read |  |  |
| `models.list` | read | `await llmq.admin.models.list()` | `llm-queue models list` | `GET /models` | read |  |  |
| `models.get` | read | `await llmq.admin.models.get("sonnet")` | `llm-queue models get MODEL` | `GET /models/{model}` | read |  |  |
| `models.enable` | update | `await llmq.admin.models.enable("sonnet", actor="ann", dry_run=False)` | `llm-queue models enable MODEL [--yes]` | `POST /models/{model}/enable` | write | `edit` | yes |
| `models.disable` | update | `await llmq.admin.models.disable("sonnet", actor="ann", dry_run=False)` | `llm-queue models disable MODEL [--yes]` | `POST /models/{model}/disable` | write | `edit` | yes |
| `models.drain` | update | `await llmq.admin.models.drain("sonnet", wait_secs=30, actor="ann", dry_run=False)` | `llm-queue models drain MODEL [--wait-secs S] [--yes]` | `POST /models/{model}/drain` | write | `model_drain` | yes |
| `deployments.list` | read | `await llmq.admin.deployments.list("sonnet")` | `llm-queue deployments list MODEL` | `GET /models/{model}/deployments` | read |  |  |
| `deployments.get` | read | `await llmq.admin.deployments.get("sonnet", "acct-a/us-east-1")` | `llm-queue deployments get MODEL ACCOUNT/NAME` | `GET /models/{model}/deployments/{account}/{name}` | read |  |  |
| `deployments.enable` | update | `await llmq.admin.deployments.enable("sonnet", "acct-a/us-east-1", actor="ann", dry_run=False)` | `llm-queue deployments enable MODEL ACCOUNT/NAME [--yes]` | `POST /models/{model}/deployments/{account}/{name}/enable` | write | `edit` | yes |
| `deployments.disable` | update | `await llmq.admin.deployments.disable("sonnet", "acct-a/us-east-1", actor="ann", dry_run=False)` | `llm-queue deployments disable MODEL ACCOUNT/NAME [--yes]` | `POST /models/{model}/deployments/{account}/{name}/disable` | write | `edit` | yes |
| `blocks.list` | read | `await llmq.admin.blocks.list()` | `llm-queue blocks list` | `GET /blocks` | read |  |  |
| `blocks.add` | create | `await llmq.admin.blocks.add(region="us-east-1", actor="ann", dry_run=False)` | `llm-queue blocks add --region R [--yes]` (or `--account A`) | `POST /blocks` | write | `edit` | yes |
| `blocks.remove` | delete | `await llmq.admin.blocks.remove(region="us-east-1", actor="ann", dry_run=False)` | `llm-queue blocks remove --region R [--yes]` (or `--account A`) | `POST /blocks/remove` | write | `edit` | yes |
| `blocks.kill_switch` | update | `await llmq.admin.blocks.kill_switch(True, actor="ann", dry_run=False)` | `llm-queue blocks kill-switch on [--yes]` | `POST /blocks/kill-switch` | write | `edit` | yes |
| `requests.list` | read | `await llmq.admin.requests.list("sonnet", tenant="acme")` | `llm-queue requests list MODEL [--tenant T] [--state S]` | `GET /requests?model=M` | read |  |  |
| `requests.count` | read | `await llmq.admin.requests.count("sonnet")` | `llm-queue requests count MODEL` | `GET /requests/count?model=M` | read |  |  |
| `requests.get` | read | `await llmq.admin.requests.get("r-1")` | `llm-queue requests get REQUEST_ID` | `GET /requests/{request_id}` | read |  |  |
| `requests.cancel` | delete | `await llmq.admin.requests.cancel(request_ids=["r-1"], actor="ann", dry_run=False)` | `llm-queue requests cancel --id ID [--id ID] [--state all] [--yes]` | `POST /requests/cancel` | write | `requests_cancel` | yes |
| `requests.reprioritize` | update | `await llmq.admin.requests.reprioritize("sonnet", priority=True, actor="ann", dry_run=False)` | `llm-queue requests reprioritize MODEL [--priority] [--yes]` | `POST /requests/reprioritize` | write | `requests_reprioritize` | yes |
| `requests.extend` | update | `await llmq.admin.requests.extend("r-1", timeout_secs=120, actor="ann", dry_run=False)` | `llm-queue requests extend REQUEST_ID --timeout-secs S [--yes]` | `POST /requests/{request_id}/extend` | write | `requests_extend` | yes |
| `tenants.list` | read | `await llmq.admin.tenants.list("sonnet")` | `llm-queue tenants list MODEL` | `GET /tenants?model=M` | read |  |  |
| `tenants.hash` | read | `llmq.admin.tenants.hash("acme")` | `llm-queue tenants hash TENANT` | `POST /tenants/hash` | write |  |  |
| `queues.list` | read | `await llmq.admin.queues.list()` | `llm-queue queues list` | `GET /queues` | read |  |  |
| `dlq.list` | read | `await llmq.admin.dlq.list(model="sonnet")` | `llm-queue dlq list --model M` (or `--job J`) | `GET /dlq?model=M` | read |  |  |
| `dlq.show` | read | `await llmq.admin.dlq.show("1700000000000-0", model="sonnet")` | `llm-queue dlq show ENTRY_ID --model M [--unsafe]` | `GET /dlq/{entry_id}?model=M` | read |  |  |
| `dlq.redrive` | update | `await llmq.admin.dlq.redrive(model="sonnet", actor="ann", dry_run=False)` | `llm-queue dlq redrive --model M [--yes]` | `POST /dlq/redrive` | write | `dlq_redrive` | yes |
| `dlq.purge` | delete | `await llmq.admin.dlq.purge(model="sonnet", actor="ann", dry_run=False)` | `llm-queue dlq purge --model M [--id E] [--yes]` | `POST /dlq/purge` | write | `dlq_purge` | yes |
| `jobs.list` | read | `await llmq.admin.jobs.list()` | `llm-queue jobs list` | `GET /jobs` | read |  |  |
| `purge.model` | delete | `await llmq.admin.purge.model("old-model", actor="ann", dry_run=False)` | `llm-queue purge model MODEL [--force] [--yes]` | `POST /purge/model` | write | `purge_model` | yes |
| `purge.deployment` | delete | `await llmq.admin.purge.deployment("sonnet", "acct-a/us-east-1", actor="ann", dry_run=False)` | `llm-queue purge deployment MODEL ACCOUNT/NAME [--no-wait] [--yes]` | `POST /purge/deployment` | write | `purge_deployment` | yes |
| `purge.tenant` | delete | `await llmq.admin.purge.tenant("acme", remove=["keys"], actor="ann", dry_run=False)` | `llm-queue purge tenant TENANT [--remove keys,queued] [--yes]` | `POST /purge/tenant` | write | `purge_tenant` | yes |
| `purge.queue` | delete | `await llmq.admin.purge.queue("sonnet", tenant="acme", actor="ann", dry_run=False)` | `llm-queue purge queue MODEL [--tenant T] [--yes]` | `POST /purge/queue` | write | `purge_queue` | yes |
| `purge.removed` | delete | `await llmq.admin.purge.removed(actor="ann", dry_run=False)` | `llm-queue purge removed [--force] [--yes]` | `POST /purge/removed` | write | `purge_removed` | yes |
| `purge.scheduled` | read | `await llmq.admin.purge.scheduled()` | `llm-queue purge scheduled` | `GET /purge/scheduled` | read |  |  |
| `purge.unschedule` | delete | `await llmq.admin.purge.unschedule("old-model", actor="ann", dry_run=False)` | `llm-queue purge unschedule TARGET [--yes]` | `POST /purge/unschedule` | write | `purge_unschedule` | yes |

Models are created and deleted by publishing the registry (`registry.publish`) and purging
what it dropped (`purge.model`, `purge.removed`); tenants' weights and quotas are registry
settings (`registry.set`); jobs are sent by the app (`llmq.send_job`), their dead letters are
`dlq.*` with `job=`.

## Parameters and answers

| Op | Parameters (default) | Answer |
|---|---|---|
| `process.describe` | | `{version, worker_id, enabled, execution_mode, secrets, mode, environment, store, separate_queue_store, key_prefix, queue_prefix, key_expiry_mode, store_max_connections, metrics_sink, tenant_default_hash, admin_writes_enabled, registry_version, models}` (no password, salt, token or registry body; `tenant_default` too only with `LLMQ_TENANT_NAMES_ENABLED`); logged once at start as `llmq_configured`. Off: `{version, enabled: false, environment, drain_file}` |
| `status.get` | `model` (all), `tenant` / `tenant_hash` (with a model) | `StatusReport`, or `ModelStatus` of one model (with `tenants` and `request_index`); with a tenant also `tenant: {tenant, waiting, running, by_stage, oldest_wait_secs, fairness}` |
| `status.health` | `model` (all) | `{health, registry_version, models: [{model, state, reasons, budget_used_pct, waiting, oldest_waiting_secs, usable_deployments, deployments}]}`; `state` is `healthy`, `warning` or `critical` (`health` the worst), against the model's `health_summary` thresholds. Critical: `no_usable_deployment` (an enabled model), `budget_used`, `waiting`, `oldest_wait` at their critical mark; warning: those at their warning mark, `breaker_open`, `deployment_disabled`, `dead_letters` (`alerts.dlq_messages`), `kill_switch`, `model_disabled`. Local mode: this process's view and budget shares. The same summary is each model's `health` in `status.get` and `GET /` |
| `registry.get` | `model` (whole document) | `{registry, changes, live_edits}`, secrets shown as `[redacted]` |
| `registry.versions` | | `{versions: [int], current: int}` |
| `registry.set` | `values` (`{path: value}`, at most 100; `null` deletes), `model` | `{diff, version, dry_run}` |
| `registry.publish` | | `{action, version, carried, diff, dry_run}`; `action` by the version rule: `published`, `kept`, `ignored`, `not_bumped` |
| `registry.rollback` | `version` | as publish; the live routing blocks and kill switch are kept |
| `audit.list` | `count` (20, at most 1,000), `path` (prefix), `op` | `{entries: [{id, actor, op, version, diff, ts}]}`, newest first |
| `models.list` | | `{models: [{model, type, provider, enabled, queue, waiting, in_progress, dead_letters, deployments, usable_deployments}]}` |
| `models.get` | `model` | `{model, type, provider, enabled, settings: {name: {value, level}}, live}` |
| `models.enable` / `disable` | `model` | `{model, enabled, diff, version, dry_run}` |
| `models.drain` | `model`, `wait_secs` (0; capped over HTTP) | `{model, enabled: false, waiting, drained}`; `drained` null when not waited for |
| `deployments.list` | `model` | `{model, deployments: [DeploymentStatus + usable]}` |
| `deployments.get` | `model`, `deployment` (`account/name`) | the row and its `settings`; 404 when the model has no such deployment |
| `blocks.list` | | `{regions: [{name, since, by}], accounts: [...], kill_switch: {on, since, by}}` (`since` / `by` from the audit, best effort) |
| `blocks.add` / `remove` | `region` or `account` | `{blocked_regions, blocked_accounts, diff, version, dry_run}` |
| `requests.list` | `model`, `tenant` / `tenant_hash` (`-`: no tenant), `state` (`all`), `limit` (100, at most `LLMQ_REQUESTS_LIST_MAX`), `after` (the previous page's `next`) | `{items: [{request_id, model, tenant, state, stage, priority, age_secs, deadline_in_secs, deployment, user, cancelling, message_id, tenant_name}], next, scanned}`, oldest deadline first; `tenant` / `user` are hashes |
| `requests.count` | `model`, `tenant` / `tenant_hash` | `{model, tenant, total, by_state, by_stage, cancelling, oldest_wait_secs, truncated}` |
| `requests.get` | `request_id` | `{summary, status, final, callers, result_ready}` (`summary` null once final); 404 when unknown or expired |
| `requests.cancel` | `request_ids` (1-1,000), or `model` with `tenant` / `tenant_hash`; `state` (`waiting`) | `{op, model, tenant, matched, changed, skipped: {reason: n}, by_state, sample_ids, dry_run, actor, at}`; `skipped` reasons: `other_state` (not in `state`), `final` (ended or unknown), `already_cancelling`, `started` (granted between the read and the cancel, `state=waiting`), `not_running` |
| `requests.reprioritize` | `model`, `priority`, `tenant` / `tenant_hash`, `limit` (all) | the same report; waiting calls only, each keeps its deadline and place |
| `requests.extend` | `request_id`, `timeout_secs` (from the send; at most the model's `timeout_max_secs` and its queue's `retention_secs`, else `ConfigError`) | the same report for one call; `skipped` says `final`, `gone`, `already` (not later than the current deadline, the call's own timeout included), `past` or `unknown_model`. A waiting call's `max_wait_secs` no longer applies; a running attempt and a stream run on to the new deadline. Needs the request index (`AdminRefused` when off) |
| `tenants.list` | `model` | `{model, tenants: [{tenant, calls, next_deadline_in_secs, tenant_name?}], truncated}`, most calls first; `tenant_name` only with `LLMQ_TENANT_NAMES_ENABLED` |
| `tenants.hash` | `tenant` | `{tenant_hash}`: the keyed hash (`LLMQ_ID_HASH_SALT`) admin views show. Write-guarded over HTTP: it confirms which tenant a hash is |
| `queues.list` | | `{queues: [{queue, model, job, class, messages, waiting, held, dead_letters, oldest_wait_secs, consumer}]}` |
| `dlq.list` | `model` or `job`, `priority` (false), `count` (10) | `{queue, letters: [{id, src, fails, request_id, tenant}]}`; `tenant` is a hash, bodies never listed |
| `dlq.show` | `entry_id`, `model` or `job`, `priority`, `unsafe` (false) | one letter; `body` only with `unsafe`, and over HTTP only with `LLMQ_ADMIN_DEAD_LETTER_BODIES_ENABLED=true` |
| `dlq.redrive` | `model` or `job`, `priority`, `count` (1,000) | `{queue, moved, dead_letters, dry_run}` |
| `dlq.purge` | `model` or `job`, `priority`, `entry_ids` (all) | `{queue, deleted, dry_run}` |
| `jobs.list` | | `{jobs: [{job, waiting, held, dead_letters, consumer}]}` |
| `purge.*` | see [Purge store state](../how-to/purge.md) | the purge report (`PurgeReport.as_dict()`) |
| `purge.unschedule` | `target` (`alias` or `alias/account/name`) | `{target, cancelled, dry_run}` |
| `drain.get` / `start` / `resume` | | `{draining, since, held, running}` |

### Requests by model and tenant

`requests.*`, `tenants.list`, `purge.queue` and `status.get` with a tenant are served by
`RequestsAdmin` (`engine/requests_admin.py`) over the per-model request index
(`P:{m:ALIAS}:tq:H`, 0.2.0 T1): the engine's own in a running queue, one over the same stores
under `AdminServices.from_env()` and the CLI. A tenant is given by name (`tenant`, hashed with
`LLMQ_ID_HASH_SALT`, never stored) or by hash (`tenant_hash`, 16 hex, or `-` for calls with no
tenant). `GET` routes take `tenant_hash` only (`?tenant=` is a 400: a name in a URL reaches
access logs); run `llm-queue tenants hash NAME` first. `describe()` shows
`tenant_default_hash`, and the name of `LLMQ_TENANT_DEFAULT` only with
`LLMQ_TENANT_NAMES_ENABLED`. With `LLMQ_REQUEST_INDEX_ENABLED=false`, looking calls up by model and tenant answers
`AdminRefused` (HTTP 409, CLI exit 1), `requests.count` and `requests.extend` included (a count
of zeros would read as "nothing waiting"); `requests.get` and `requests.cancel` by request ids
still work. A cancel logs `call_cancel` only for the calls it cancelled, and answers the waiting
ones in one pipelined round trip per `LLMQ_ADMIN_BATCH`. A reprioritize stopped by a full target
counts every call it did not reach in `skipped.full`. An operation that would read more than
`LLMQ_ADMIN_SCAN_MAX` index entries is refused the same way.

## `GET /` and `llm-queue ops`

One call says what the pod serves and how it is: library version, worker, role, environment;
process (`live`, `ready`, `not_ready_reason`, `draining`, `held`, heartbeat age); stores (state,
key expiry, eviction policy); registry (version, changes, live edits, kill switch, blocks);
per model waiting, waiting for a grant, in progress, oldest wait, dead letters, deployments,
usable deployments and open breakers; jobs; scheduled purges; and `routes`, every route this
router mounted with its method, path, op id, guard and whether it writes. A part that cannot
be read (the store is down) is `null`. The summary is reused for
`LLMQ_ADMIN_INDEX_CACHE_SECS` (2 s) so a dashboard poll cannot load the store. The CLI's `ops`
prints the same summary without the process part and lists every operation.
