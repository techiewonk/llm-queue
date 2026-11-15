# Operate

How to deploy, watch and change a running system.

## Start and stop with the app

Start the queue once per process in the app's startup hook and stop it on shutdown:

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    await llmq.start()              # a failure here should fail the pod: queued models need it
    yield
    llmq.pause_receiving()          # not ready; no new queued work
    await llmq.stop()               # drain: LLMQ_DRAIN_SECS (25 s) for jobs and calls together
```

Start one queue per process, after any fork (in each gunicorn / uvicorn worker, in Celery's
`worker_process_init`); see [Clients and connections](../explanation/clients.md#one-llmqueue-per-process).

## Deploys and restarts

1. A **preStop hook** calls `llmq.pause_receiving()` through a `/drain` endpoint of the app (the
   library's router has none), as in `samples/09_http_router.py`. It must answer **GET**:
   `preStop: httpGet` sends a GET, and a POST-only route answers 405 and nothing drains. Readiness fails, and the process takes no new calls or jobs;
   running work goes on.
2. **Shutdown** is `await llmq.stop(drain_secs=...)`. Held messages go back to the queue at once;
   running calls and this pod's waiting callers get up to `drain_secs` (default
   `LLMQ_DRAIN_SECS`, **25 s**) to finish. Jobs drain first, then calls get what is left.
3. **When the drain runs out**, a call still running is **given back, not failed**: its grants are
   refunded and its message returned to the queue at once, so another pod runs it and no failure
   is counted toward `max_receives`. A stream that already sent chunks to its caller cannot move
   and ends `worker_shutdown` (with `partial_text`). A job still running is returned the same way.

```mermaid
sequenceDiagram
  participant K as Kubernetes
  participant P as Pod (app + llm-queue)
  participant Q as Queue
  K->>P: preStop: GET /drain
  P->>P: pause_receiving(): not ready, receive nothing
  K->>P: SIGTERM
  P->>Q: return held (not started) messages
  P->>P: running calls finish (up to 25 s)
  P->>Q: still running: refund grants, return message
  Q-->>K: another pod takes them
  Note over K,P: terminationGracePeriodSeconds 30 > drain 25 s
```

Kubernetes settings that fit the defaults:

```yaml
spec:
  terminationGracePeriodSeconds: 30     # the default; keep it above LLMQ_DRAIN_SECS (25)
  containers:
    - name: app
      lifecycle:
        preStop:
          httpGet: {path: /drain, port: 8000}   # calls llmq.pause_receiving()
      readinessProbe:
        httpGet: {path: /llmq/health/ready, port: 8000}
      livenessProbe:
        httpGet: {path: /llmq/health/live, port: 8000}
```

The preStop hook's time counts against the grace period, so keep the `/drain` handler instant
(it only flips a flag). If you raise `LLMQ_DRAIN_SECS` for long answers, raise
`terminationGracePeriodSeconds` with it (drain + 5 s or more); a pod killed before its drain ends
loses nothing either, its calls reappear after `visibility_secs`, but they then count one
failure each.

Measured: 4 rolling restarts, 0 calls lost (gate B33).

## Health and readiness

| Route / call | Answers |
|---|---|
| `GET /health/live` | 200 while the library's background loops run and the heartbeat makes progress; **503** when a loop died or the heartbeat stalled for a lease. Once `stop()` begins it stays 200, so a draining pod is not restarted. It reads the process's own state only, never the store |
| `GET /health/ready` | 200 when the registry is loaded and the process is not draining, else 503 |
| `await llmq.health()` | `{"live", "ready", "stores": {"state", "queue"}, "eviction_policy"}`, plus `queue_eviction_policy` with a separate queue store |

A liveness probe on `/health/live` restarts a pod whose loops died (0.1.0b1 always answered
200): keep its `failureThreshold` at 3 or more so one slow beat does not restart a healthy pod.

Readiness never depends on the stores' reachability or the providers, and answers at once from
cached state (one exception: in strict mode a store whose `maxmemory-policy` may evict keys makes
the process not ready, with `not_ready_reason` in the body; the policy is read again every
minute):
a background task probes each store every 5 s with a short write bounded by
`EngineTimings.health_probe_secs` (3 s) and reports it `ok`, `read_only` (a failover left it a
replica) or `down` (unreachable, or refusing writes, such as out of memory). A store error never
turns the readiness route into a 500. Liveness is false when one of the library's background
loops died or the heartbeat made no progress for a lease (`LLMQ_LEASE_SECS`).

At `start()` the store's own setup is checked: a cluster-mode store is refused (`ConfigError`),
and a `maxmemory-policy` other than `noeviction` logs `store_eviction_policy` with a warning and,
when strict, fails readiness (see
[Store requirements](../explanation/storage.md#store-requirements)).

## Status

```bash
llm-queue status                # every model
llm-queue status --model M      # one model: deployments, budgets used, health, queues, blocks
llm-queue request ID            # one call
```

The same from code: `await llmq.status(model)`, `await llmq.request_status(request_id)`; over
HTTP: `GET /status`, `GET /requests/{id}`.

## Alerts

Every 30 s one process (the holder of the alert lock, which it keeps while it runs) evaluates
alerts from the status report. Each alert is a gauge `llmq_alert_<name>`, and a log line
`llmq_alert` is written once when it turns on or off; an alert whose model or deployment left
the registry is logged off. Thresholds come from the model's `alerts` setting. Alarm and query
examples: [Monitor](monitor.md).

| Alert | On when |
|---|---|
| `model_unavailable` | No usable deployment is left: disabled, unhealthy, outside residency, blocked, on a disabled account, or behind an open breaker |
| `deployment_disabled` | A deployment was auto-disabled by health checks |
| `quota_near_limit` | A deployment used `alerts.quota_used_pct` (90) % of its RPM or TPM (off after two clear samples) |
| `quota_above_observed` | The provider throttled (this or the last minute) while the deployment had budget left: the real quota is below the registry's number (off after two clear samples) |
| `dead_letters` | A model's dead-letter queues (priority and normal together) hold `alerts.dlq_messages` (1) or more messages: `dlq list --model M` and `dlq list --model M --priority` |
| `breaker_open` | A deployment's breaker is open |
| `kill_switch_on` | The routing kill switch is on (one per model) |
| `queue_stalled` | A model's oldest queued call is older than `alerts.queue_age_secs` (600) |
| `job_dead_letters` | A job name has dead jobs |

## Change things at run time

```bash
llm-queue registry set --model M rpm=400    # live edit (audited)
llm-queue registry disable M aws-main/ap-south-1
llm-queue block --region R                  # stop using a region (or --account A)
llm-queue unblock --region R
llm-queue drain M --yes --wait              # disable a model for new calls; serve waiting ones
llm-queue dlq list --model M                # dead letters; also show, redrive, purge
```

Commands that delete or move data (`drain`, `dlq redrive`, `dlq purge`, `registry gc`) need
`--yes`; `--dry-run` shows what they would do.

`--actor NAME` records who made a change. Live edits and their limits are in
[Configure](configure.md#live-edits). Every command is in [CLI and HTTP](../reference/cli-http.md).

## HTTP router

With the `http` extra:

```python
from fastapi import Depends

app.include_router(
    llmq.http_router(dependencies=[Depends(require_admin)]),
    prefix="/llmq",
)
```

This gives `/status`, `/requests/{id}` and `/registry` behind your dependencies, and
`/health/live` and `/health/ready` without them. `allow_writes=True, actor=fn` adds
`POST /registry` and `POST /registry/block`, audited under `actor(request)`. Write routes need
`dependencies` (else `ConfigError`), a JSON body and the `X-LLMQ-Admin` header; see
[Secure a deployment](secure.md#4-protect-the-http-write-routes).

## Store failover

The store is one primary with replicas (cluster mode is not supported).

- Clients reconnect on `READONLY` after a failover. The failover drill (`scripts/failover.py`)
  loses no call on `failover` and `restart`; with DNS lagging 6 s, at most 5 % of calls in
  flight may fail.
- `LLMQ_QUEUE_WAIT_REPLICAS` / `LLMQ_QUEUE_WAIT_MS`: a send waits for this many replicas to
  acknowledge it. Too few acknowledgements make the call run direct under the same record.
- With a separate queue store (`LLMQ_QUEUE_STORE_URL`), errors for `LLMQ_QUEUE_FAILOVER_SECS`
  (5) make calls run direct under the same budgets.
- A state store outage: callers already waiting keep waiting until their deadline; new calls
  follow the model's `store_down` rule (`pause`, the default; `static_cap`; `fail_fast`).
  Measured: a 45 s outage with 500 calls waiting dropped none.

Details: [Failures and recovery](../explanation/resilience.md).

## StoreUnavailable

Admin operations (`cancel`, `status`, `request_status`, `send_job`, and the registry's reads and
edits) raise `StoreUnavailable` when the store is unreachable, read-only or full. It is also a
`ConnectionError`. Calls raise `CallFailed` (`queue_unavailable`) instead.

## Logs and metrics

The library logs with structlog and never configures logging; the app does. Log records are
named events, for example:

| Event | Meaning |
|---|---|
| `llmq_call_finished` | One per call (every path, failures too): request id, model, outcome, account, region, deployment, attempts, waits, duration, tokens, worker, hashed user and tenant; no prompt text |
| `llmq_alert` | An alert turned on or off |
| `llmq_registry_audit` | A registry publish or edit, with actor and diff (secrets redacted) |
| `registry_older_ignored` | This process's registry is older than the stored one, which is used |
| `registry_invalid_replaced`, `registry_needs_newer_library` | The stored registry was invalid and replaced; or it needs a newer library and this pod keeps its own |
| `second_llm_queue_started` | A second `LLMQueue` was started in one process |
| `store_eviction_policy` | The store may evict keys (not `noeviction`) |
| `job_no_consumer` | A job was sent to a name no running process consumes |
| `store_down` | The state store failed before a call was sent; the `store_down` rule applies |
| `deployment_disabled`, `deployment_recovered` | Health checks turned a deployment off or back on |

Metrics go to `LLMQ_METRICS_SINK`: `emf` (CloudWatch embedded metrics, default, with the
`environment` dimension), `prometheus` (needs the `metrics` extra; `status_router(...,
metrics_route=True)` serves `GET /metrics`) or `none`; one sink per process. Per call: calls by
outcome and by caller-seen failure, attempts by result, tokens, and histograms of the queue
wait, the wait for a grant, the call and each provider attempt. Every 30 s the alert-lock
holder sets the cluster gauges (queues, held calls, oldest call, budgets and their limits,
concurrency, breakers, usable deployments, routing switches, dead jobs, alerts), and every pod
sets its own (ready, stores up, held calls, heartbeat age, outage memo, registry state, build
info). [Monitor](monitor.md) lists every metric with its labels, unit and emitter.
