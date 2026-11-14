# Many services, one model

Several services, each with many pods, can call the same models through the same queues and
budgets. Budgets, slots, waiter order and health live in the store, so the limits hold across
every process that shares it.

```mermaid
flowchart LR
  subgraph A["Service A (pods 1..N)"]
    a1["caller + worker"]
  end
  subgraph B["Service B (pods 1..M)"]
    b1["caller + worker"]
  end
  A & B <--> S[("One store<br/>same LLMQ_KEY_PREFIX / LLMQ_QUEUE_PREFIX")]
  S --> M["Model gpt120b<br/>one RPM / TPM budget per deployment"]
```

## What the services must share

| Must be the same | Why |
|---|---|
| The store (`LLMQ_STORE_URL`, and `LLMQ_QUEUE_STORE_URL` if used) | Queues, budgets and results live there |
| `LLMQ_KEY_PREFIX` and `LLMQ_QUEUE_PREFIX` | They name the keys and queues; a different prefix is a separate system with its own budgets |
| The registry document | Every service ships the **same** document (`LLMQ_REGISTRY` or `LLMQ_REGISTRY_FILE`) |

A service in `caller` mode only makes calls; the workers of any service in `both` or `worker`
mode run them.

## The registry version rule

Each process publishes its registry at `start()` by `registry_version`:

| This process's version vs the store's | What happens |
|---|---|
| Higher | It is published and replaces the stored one; live routing blocks and the kill switch of the stored one are carried over |
| Equal, same content | The stored one is kept, with its live edits |
| Equal, other content | The stored one is kept; the process logs `registry_version_not_bumped` |
| Lower | The stored one is used; the process logs `registry_older_ignored` |
| Stored one invalid for this pod, its version not above this one | This pod's document replaces it (`registry_invalid_replaced`) |
| Stored one needs a newer library (`min_lib_version`) | Never replaced; this pod keeps its own document and logs `registry_needs_newer_library` |

So during a rolling deploy of one service, its new pods publish the new version and the old pods
of every service pick it up on their next reload (`LLMQ_CFG_RELOAD_SECS`, 30 s). Raise
`registry_version` with every deployed change, and ship the new document to every service.

What a service on an older document gets from a newer stored one:

- **Changed settings and deployments** of models it knows: used at the next reload.
- **New models**: running pods start a receive loop for them at the next reload, so their calls
  are served; the service's own code can call them by alias once it knows the name.
- **Settings it does not know** (written by a newer library): read as warnings and ignored by
  this pod; the newer pods apply them.
- **A `min_lib_version` above its library**: the pod stays up on its own registry, never
  publishes over the stored one, and warns. Upgrade the library.

Emergency routing blocks (`llm-queue block`) and the kill switch are carried into every publish:
a deploy never undoes them; only an explicit unblock does.

<!-- verify after merge: running pods start receive loops for models added by a newer registry (RESCAN R3); behaviour for removed models. -->

```mermaid
flowchart TD
  S([start / reload]) --> R{Stored registry}
  R -- "missing" --> P[Publish own document]
  R -- "needs newer library" --> K[Keep own / last good<br/>never publish, warn]
  R -- "invalid for this pod" --> V{Own version >= stored?}
  V -- yes --> P
  V -- no --> K
  R -- "valid" --> C{Own version vs stored}
  C -- higher --> P2[Publish, carry live blocks]
  C -- "equal / lower" --> U[Use the stored one]
```

## Fairness between clients

Calls are served in arrival order within a class (priority, then normal). There is no fair share
per client or per service: one client that floods a model delays every other client of that
model.

To keep latency-sensitive callers moving:

- mark user-facing calls `priority=True` (or with `request_context(priority=True)`); priority
  calls are read first, and `priority_reserve` keeps part of each budget for them;
- give callers that should not wait long a `max_wait_secs`, so they fail `rate_limited` (with
  `retry_after_secs`) instead of waiting behind the flood.

## Library versions

Services may run different llm-queue versions for a while. Queue messages carry a format
version; a worker that gets a message newer than it understands passes it on to the queue,
never drops it (and does not spin on it), so a newer worker can take it. Set the registry's
`min_lib_version` when a registry change needs a newer library: older pods then stay up but
never publish over that registry. See the [stability policy](../project/stability.md).

<!-- verify after merge: "does not spin on it" — PLAN C5: newer-envelope messages are passed on without the 5 s hot loop. -->

Each service also needs its own process settings right: the same `LLMQ_ENVIRONMENT` class
(strict or dev) everywhere, since a strict process refuses plain-TCP stores and a missing salt
([Environment variables](../reference/environment.md)).
