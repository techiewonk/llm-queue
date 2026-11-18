# Use with FastAPI

`llm_queue.fastapi` (extra `http`: `pip install "llm-queue[http]"`) wires a queue into a
FastAPI app: its lifespan, the per-request context and the admin, probe and drain routes.

## FastAPI in 5 lines

```python
from fastapi import FastAPI
from llm_queue.fastapi import install

app = FastAPI(lifespan=my_lifespan)        # your own lifespan, if any, is kept
llmq = install(app, admin_token=token)     # LLMQueue.from_env(), or install(app, llmq)
# probes: /internal/llm-queue/health/ready and /health/live
```

`install()` does four things:

| Step | What it does |
|---|---|
| Lifespan | Wraps the app's lifespan (`app.router.lifespan_context`): the queue starts **before** your startup and stops **after** your shutdown, so your producers stop first and running calls then drain for `drain_secs` (default `LLMQ_DRAIN_SECS`, 25 s). The queue is also `app.state.llm_queue` |
| Middleware | Adds `RequestContextMiddleware(**context)`: each HTTP and websocket request runs in its own [request context](call.md#request-context) |
| Routes | Mounts `llmq.http_router(**router_options)` under `prefix` (default `/internal/llm-queue`): `/health/live`, `/health/ready`, `GET /` and every admin operation ([Admin API](../reference/admin-api.md)); with no token and no `dependencies` only the probes |
| Drain | Mounts `GET /drain`, `POST /drain` and `POST /resume` when they can be guarded as writes (the admin token, or `dependencies` with `actor`). The `LLMQ_DRAIN_FILE` watcher is started by `LLMQueue.start()` itself (see [Drain a pod](operate.md#drain-a-pod)) |

Its parameters, all optional:

| Parameter | Default | Meaning |
|---|---|---|
| `llmq` | None | The queue; None builds one with `LLMQueue.from_env()` (a bad setting raises here, at import) |
| `prefix` | `/internal/llm-queue` | Where the routes are mounted |
| `on_start_error` | `"raise"` | `"raise"`: a failed start fails the app's startup. `"log"`: logs `llmq_start_failed` and keeps the app up; the queue is stopped, so queued calls fail at once with `LLMQueueError` and readiness stays false |
| `drain_secs` | None | The bound of the stop at shutdown; None is `LLMQ_DRAIN_SECS` |
| `mount_admin` | True | False mounts no routes |
| `context_middleware` | True | False adds no middleware |
| `context` | None | Keyword arguments of `RequestContextMiddleware` (`tenant_id`, `user_id`, `session_id`, `priority`) |
| `**router_options` | | Passed to `llmq.http_router()`: `admin_token`, `admin_read_token`, `admin_header`, `dependencies`, `allow_writes`, `actor`, `metrics_route`, `drain_routes` (an explicit value wins over the rule above), `insecure_reads` |

A second `install()` on one app raises `ConfigError`. Point the Kubernetes probes at
`/internal/llm-queue/health/ready` and `/health/live`.

## Your own lifespan order: `serve()` and `mount()`

When the queue must start at a set point in your own lifespan, use the two halves:

```python
from contextlib import asynccontextmanager
from llm_queue import RequestContextMiddleware
from llm_queue.fastapi import mount, serve

@asynccontextmanager
async def lifespan(app):
    await open_db()
    async with serve(app, llmq, on_start_error="log"):   # start; stop at the end of the block
        yield
    await close_db()

app = FastAPI(lifespan=lifespan)
mount(app, llmq, admin_token=lambda: secrets.get("llmq_admin_token"))
app.add_middleware(RequestContextMiddleware, tenant_id="acme")
```

Mount routes while building the app, not inside the lifespan: routes added after startup are
missing from an OpenAPI schema already served.

## Request context per request

`RequestContextMiddleware` (in `llm_queue.asgi`, pure ASGI: no FastAPI import, works for
websockets) runs each request inside `request_context(...)`. Each value is a constant or a
callable of the ASGI scope (sync or async); None leaves the field inherited, so
`LLMQ_TENANT_DEFAULT` still applies:

```python
from llm_queue import RequestContextMiddleware

def tenant(scope):
    return dict(scope["headers"]).get(b"x-tenant", b"").decode() or None

app.add_middleware(RequestContextMiddleware, tenant_id=tenant, priority=True)
```

Authentication usually runs later, in a route dependency, so set per-user ids in the route:

```python
with request_context(user_id=user.id, session_id=session_id, priority=True):
    answer = await chat.ainvoke(question)
```

## Background work

Background work should never take budget from users waiting on a reply. Each helper keeps the
ids (so the calls still count for their tenant) and drops the priority:

| Helper | Use |
|---|---|
| `with llm_queue.background():` | A block: `request_context(priority=False)` |
| `@llm_queue.background_priority` | A function or coroutine function (generator functions are refused) |
| `llm_queue.create_background_task(coro, name=None)` | `asyncio.create_task` in a copy of the context, kept referenced until done (a fire-and-forget task is never collected mid-run); a failure is logged as `background_task_failed` |
| `llm_queue.run_in_thread(fn, *args, **kwargs)` | A daemon thread running `fn` in a copy of the context; returns the started thread; a failure is logged as `background_thread_failed` |

```python
from llm_queue import background_priority, create_background_task

@background_priority
async def summarise_memory(session_id: str) -> None:
    ...

@app.post("/turn")
async def turn(...):
    with request_context(user_id=user.id, priority=True):
        reply = await chat.ainvoke(text)
        create_background_task(summarise_memory(session_id), name="memory")
    return reply
```

## Readiness of your own

`llmq.ready` is a sync, cheap check: started, registry loaded, not draining, and (strict mode)
no store whose eviction policy may drop queue keys. Unlike `health()["ready"]` it is also false
before `start()` and after `stop()`. `llm_queue.queue_ready(llmq=None)` folds it into an app's
own probe: the queue's `ready` (default: the started default queue; False when none is
started). With the library off (`LLMQ_ENABLED=false`) the queue is ready unless draining, so
the app needs no flag of its own.

```python
from llm_queue import queue_ready

@app.get("/ready")
async def ready():
    ok = queue_ready() and db.ready
    return Response(status_code=200 if ok else 503)
```
