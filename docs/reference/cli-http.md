# CLI and HTTP

Both surfaces are generated from one operations table and call the same `llmq.admin` methods.
Every command and route, with its Python call, guard, audit entry and an example, is listed in
[Admin API](admin-api.md). This page has the transport rules.

## `llm-queue` command

```text
llm-queue [--actor NAME] RESOURCE ACTION [ARGS] [--yes]
llm-queue models list
llm-queue requests cancel --id r-1 --id r-2            # a dry run: what it would cancel
llm-queue --actor ann requests cancel --id r-1 --yes   # cancels, audited as ann
```

- Reads the same `LLMQ_*` settings as the app (`AdminServices.from_env`) and talks to the
  stores directly; the store password is read like the app reads it. `registry schema` needs no
  settings and no store.
- `--actor NAME` (before the resource) records who made a change in the audit; without it the
  OS user is used, looked up only by commands that write (`uid:<n>` when the user has no passwd
  entry, as in a container run under a bare UID). 1-128 characters without control characters,
  checked by every write before it acts (registry, requests, dead letters, purges, drains).
- A command that changes something is a **dry run unless given `--yes`**: it prints what it
  would do. There is no `--dry-run` flag.
- Values of `registry set path=value` are JSON when they parse (`400`, `true`, `["a"]`), else
  text. Lists such as `purge tenant --remove keys,queued` are comma lists; repeated ids are
  repeated options (`--id A --id B`).
- Output is one JSON document.

**Exit codes.** 0 done; 1 an error (a bad name or edit, an unknown or expired id, a refused
operation, a store outage: one line on stderr, no traceback); 3 a `models drain --wait-secs S`
that ran out of time with calls still waiting.

## HTTP routes

`llmq.http_router()` returns a FastAPI router (extra `http`):

```python
app.include_router(llmq.http_router(), prefix="/internal/llm-queue")
```

| Argument | Default | Does |
|---|---|---|
| `admin_token` | `LLMQ_ADMIN_TOKEN` | Guards writes, and reads too. A value or a callable read on every request (a rotated secret needs no restart) |
| `admin_read_token` | `LLMQ_ADMIN_READ_TOKEN` | Guards reads only (`GET` routes) |
| `admin_header` | `LLMQ_ADMIN_HEADER` (`X-LLMQ-Token`) | The header that carries either token, compared in constant time |
| `dependencies` | none | The app's own admin check (FastAPI dependencies), on every route but the probes and `/metrics`, with or without tokens |
| `allow_writes` | `LLMQ_ADMIN_WRITES_ENABLED` (false) | Mount the write routes |
| `actor` | none | `actor(request) -> str`: the authenticated name a write is audited under (`http:<name>`). Without it, the token guard audits `token:<X-LLMQ-Actor>` (`LLMQ_ADMIN_ACTOR_HEADER`), `token:unknown` without the header |
| `drain_routes` | false | `GET /drain`, `POST /drain`, `POST /resume` for this process (a pod with several processes drains through `LLMQ_DRAIN_FILE`, see [Drain a pod](../how-to/operate.md#drain-a-pod)) |
| `metrics_route` | false | An open `GET /metrics`: the Prometheus exposition of a `LLMQ_METRICS_SINK=prometheus` process (extra `metrics`) |
| `insecure_reads` | false | Mount the read routes with no token and no `dependencies` (local development only) |

A tenant is named by its hash on every `GET` route (`?tenant_hash=H`, `-` for calls with no
tenant): a name in a URL reaches proxy and access logs, so `?tenant=NAME` is refused with 400.
Get the hash first with `llm-queue tenants hash NAME` (or `POST /tenants/hash`, write guard);
the CLI and Python take `tenant=NAME` and hash it locally.

Guards:

| Guard | Routes | Needs |
|---|---|---|
| open | `GET /health/live`, `GET /health/ready`, `GET /metrics` | nothing |
| read | every other `GET` route | the read token or the admin token in `admin_header`, or `dependencies` (both when both are set) |
| write | every other `POST` route, `POST /tenants/hash` (it writes nothing, but with the hashes the read routes show it would tell a read token which tenant a hash is; it is never mounted without a write guard) | the admin token, or `dependencies` plus the `X-LLMQ-Admin` header (a browser cannot send either cross-site without a CORS preflight, so a page an admin visits cannot post with the admin's cookies); `allow_writes=True`; an actor |

The router refuses to be built (`ConfigError`) with no token and no `dependencies` (unless
`insecure_reads=True`), with write routes but neither the admin token nor `dependencies`, and
with write routes but no actor (`actor=` or the admin token). The read token never writes.

A `GET` takes its arguments from the query string, a `POST` from a JSON object body; path
parameters are in the route. Unknown or mistyped fields are refused. A write's body takes
`"dry_run": false` to act; without it the answer is the dry run. Every `wait_secs` an HTTP
client asks for (purges, `models drain`) is capped at `LLMQ_HTTP_PURGE_WAIT_SECS` (60); an
omitted one on a purge waits that long at most. Dead-letter bodies (`GET /dlq/{id}?unsafe=true`)
need `LLMQ_ADMIN_DLQ_BODIES_ENABLED=true`.

Status codes of the admin routes:

| Code | When |
|---|---|
| 200 | Done (or the dry run's report) |
| 400 | Bad input: an unknown, missing or mistyped field, a body that is not a JSON object, an id or name longer than 256 characters, more than 100 edit paths or list items; a refused edit (a protected or deploy-only field, an invalid value) |
| 403 | No or a wrong token; the read token on a write; a dependency-guarded write without `X-LLMQ-Admin`; dead-letter bodies with the setting off |
| 404 | An unknown model; an unknown or expired request, dead letter or deployment |
| 404, 405 | A route that is not mounted: a write with `allow_writes` off (405 where a `GET` shares the path), a read with no guard |
| 409 | A refused operation (`AdminRefused`: a purge of state still in use, a service not available, a stopped queue), no `Retry-After`; or a write that lost the compare-and-set race with other writes on every try: valid, retry after `Retry-After` (1 s) |
| 503 | The state store is unreachable, or the registry is not loaded yet (the router was hit before `start()` finished) |
