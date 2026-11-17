# Integrate with an app

Which APIs an app must, should and may use, and the few to avoid. The rest of the library is
for operators ([Admin API](../reference/admin-api.md)) or for special cases.

## What you need

- `LLMQ_*` settings in the environment ([Environment variables](../reference/environment.md))
  and a registry file (`LLMQ_REGISTRY_FILE`, [Registry document](../reference/registry.md)).
- `LLMQ_ID_HASH_SALT` and `LLMQ_KEY_PREFIX`, one prefix per app and environment
  (`myapp-prod`); a TLS store URL outside dev. The process refuses to start without them when
  strict.
- The `http` extra for FastAPI: `pip install "llm-queue[http]"`.

## The app in ten lines

```python
from fastapi import Depends, FastAPI
from llm_queue import request_context
from llm_queue.fastapi import install
from llm_queue.langchain import chat_model

app = FastAPI()
llmq = install(app)          # LLMQueue.from_env(): lifespan, context middleware, admin and probe routes
chat = chat_model("anthropic.claude-sonnet-4", registry=llmq) or make_direct_chat()

@app.post("/ask")
async def ask(q: str, user=Depends(current_user)):
    with request_context(user_id=user.id, tenant_id=user.org_id, priority=True):
        return {"answer": (await chat.ainvoke(q)).content}
```

`install()` starts the queue before the app's own startup and stops it after its shutdown,
adds the request context middleware, and mounts the admin, probe and drain routes under
`/internal/llm-queue` ([Use with FastAPI](fastapi.md)). The admin routes need
`LLMQ_ADMIN_TOKEN` (or `LLMQ_ADMIN_READ_TOKEN` for a read-only dashboard) or your own
`dependencies=`; without either only the probes are mounted.

## Must, should, may, avoid

| Level | API | Why |
|---|---|---|
| **Must** | `LLMQueue.from_env(...)` with `LLMQ_*` settings and a registry file, and `llm_queue.fastapi.install(app, llmq)` (or `start()` / `stop()` in your lifespan) | The queue does nothing until started; stop drains running calls |
| **Must** | Readiness and liveness probes on `/health/ready` and `/health/live` | Kubernetes must not send traffic to a pod whose queue is not ready or is draining |
| **Must** | `LLMQ_DRAIN_FILE` touched by the preStop hook (several processes per pod) or `POST /drain` (one process) | Without it a rolling deploy cuts calls off instead of handing them to other pods |
| **Must** | Queued model classes: `chat_model(...)` / `embeddings(...)` (or `QueuedChatBedrockConverse` / `QueuedBedrockEmbeddings`) | Only queued calls are admitted against the shared budgets |
| **Must** | `LLMQ_ID_HASH_SALT`, `LLMQ_KEY_PREFIX` per app and environment, a TLS store URL (strict environments) | Refused at start otherwise; keeps environments apart |
| **Should** | `request_context(user_id=, session_id=, tenant_id=, priority=)` per request (or `RequestContextMiddleware`, `LLMQ_DEFAULT_TENANT`) | Priority for interactive work, sticky sessions, tenant fairness and the per-tenant views |
| **Should** | `llm_queue.background()` / `@background_priority` for background work | Background work never takes budget from waiting users |
| **Should** | `error.category` / `error.retryable` in your error handling | Do not retry what the queue already retried |
| **Should** | The admin routes with `LLMQ_ADMIN_TOKEN` (writes) and `LLMQ_ADMIN_READ_TOKEN` (dashboards); `GET /` and `/status` on your dashboard; a metrics sink (`emf` or `prometheus`) | Live status and operations without shell access |
| **Should** | `describe()` logged at start | One line that says how the pod is configured |
| **May** | `llmq.admin.requests` (cancel by id; list / cancel / reprioritize by model and tenant), `llmq.admin.purge`, `llmq.admin.dlq`, registry live edits, blocks | Operations and incidents: every change is a dry run until asked for, and audited |
| **May** | Tenant fairness (`fairness` share / quota / fair_queue), `LLMQ_METRICS_TENANTS` | When several tenants share models |
| **Avoid** | Sync calls (`converse_sync`, `embed_sync`) on the event-loop thread | Refused with a clear error: they would deadlock; use the async calls there |
| **Avoid** | `LLMQ_ADMIN_WRITES_ENABLED=true` without a token or `dependencies`, or `insecure_reads=True` outside local development | The admin routes list accounts, regions and model ids and can change them |

## Request context

Set it per request: `request_context(...)` around the work, or `RequestContextMiddleware`
(added by `install()`), with callables when the values come from the request. A single-tenant
app sets `LLMQ_DEFAULT_TENANT` once. Background work runs under `llm_queue.background()` or
`@background_priority` (normal priority); `create_background_task` and `run_in_thread` carry
the context into a task or a thread at background priority. See
[Make calls](call.md#request-context).

## Model factories

`chat_model(...)` and `embeddings(...)` return None when the queue is off or the model is
unknown: keep the direct client as the fallback (`or make_direct_chat()`), so one flag turns
the queue off without a deploy.

## Errors

```python
from llm_queue import LLMQueueError

try:
    answer = await chat.ainvoke(prompt)
except LLMQueueError as e:
    logger.warning("llm_failed", category=e.category, retryable=e.retryable)
    if not e.retryable:
        raise
```

The queue already retried what it could; a caller retry helps only when `retryable` is true
([Errors](../reference/errors.md)).

## Kubernetes

```yaml
spec:
  terminationGracePeriodSeconds: 40            # >= LLMQ_DRAIN_SECS (25) + margin
  containers:
    - name: api
      env:
        - {name: LLMQ_DRAIN_FILE, value: /tmp/llmq-drain}
        - {name: LLMQ_ADMIN_TOKEN, valueFrom: {secretKeyRef: {name: llmq, key: admin_token}}}
        - {name: LLMQ_ADMIN_READ_TOKEN, valueFrom: {secretKeyRef: {name: llmq, key: read_token}}}
        - {name: LLMQ_ID_HASH_SALT, valueFrom: {secretKeyRef: {name: llmq, key: id_hash_salt}}}
      lifecycle:
        preStop: {exec: {command: ["sh", "-c", "touch /tmp/llmq-drain && sleep 5"]}}
      readinessProbe: {httpGet: {path: /internal/llm-queue/health/ready, port: 8000}, periodSeconds: 5}
      livenessProbe: {httpGet: {path: /internal/llm-queue/health/live, port: 8000}, periodSeconds: 10}
```

A pod with one process may call `POST /internal/llm-queue/drain` instead; several processes
need the file ([Drain a pod](operate.md#drain-a-pod)).

## Operate

- `GET /internal/llm-queue/` with the read token: every route this pod serves and a live
  summary (models, waiting, in progress, dead letters, usable deployments, scheduled purges).
- A metrics sink: `LLMQ_METRICS_SINK=emf` (CloudWatch) or `prometheus` with `metrics_route=True`.
- `describe()` is safe to log at start (no password, salt, token or registry body).
- Incidents: cancel calls (`requests cancel`), purge what a tenant or a removed model left
  (`purge`), redrive or delete dead letters (`dlq`), block a region or account (`blocks`), turn
  a model off (`models disable` / `models drain`). Every one is a dry run until `--yes` /
  `"dry_run": false`, and audited ([Admin API](../reference/admin-api.md)).

## Checklist before go-live

- [ ] Readiness and liveness probes point at `/internal/llm-queue/health/ready` and `/live`.
- [ ] The preStop hook touches `LLMQ_DRAIN_FILE`; `terminationGracePeriodSeconds` covers
      `LLMQ_DRAIN_SECS` plus a margin.
- [ ] `LLMQ_ADMIN_TOKEN` (and `LLMQ_ADMIN_READ_TOKEN` for dashboards) come from a secret;
      `LLMQ_ADMIN_WRITES_ENABLED` is on only where operators change things over HTTP.
- [ ] `LLMQ_ID_HASH_SALT` from a secret, the same in every service sharing the store.
- [ ] `LLMQ_KEY_PREFIX` names the app and environment.
- [ ] A metrics sink is set and `describe()` is logged at start.
