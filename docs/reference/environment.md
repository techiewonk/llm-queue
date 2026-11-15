# Environment variables

Every process setting (`LLMQ_*`) that `LLMQueue.from_env()` reads, with its default and purpose.
Model settings live in the registry document, and timeouts and pool sizes in `Tuning` (see
[Configuration](configuration.md)).

## Where values come from

```python
llmq = LLMQueue.from_env()                                    # os.environ
llmq = LLMQueue.from_env(secrets=lookup)                      # lookup(name) first, then os.environ
llmq = LLMQueue.from_env(environ={"LLMQ_STORE_URL": "..."},   # no environment variables at all
                         secrets=lookup)
```

- `environ=` replaces `os.environ` with a mapping you build (from a config file, Secrets
  Manager, a test fixture). Nothing is read from the process environment then.
- `secrets=` is a function from a name to a value or `None`; each `LLMQ_*` name is looked up
  there first, then in `environ`. `LLMQ_STORE_PASSWORD` and `LLMQ_QUEUE_STORE_PASSWORD` are read
  through it on every new store connection, so a rotated password needs no restart. Account
  secrets (`api_key_ref`, `external_id_ref`) are looked up the same way.

Every value is checked at `from_env`; all problems are raised together as one `ConfigError`.

## `LLMQ_ENVIRONMENT`: strict or dev

| Value | Mode | What it means |
|---|---|---|
| `dev`, `local`, `test`, `ci` | dev | Plain `redis://` stores allowed, no id salt needed, any `base_url` host, `LLMQ_DEBUG_CAPTURE_FROM_CONFIG` allowed |
| anything else (`prod`, `staging`, ...) | strict | The checks below |
| **unset** | **strict** | Same as any other name (since 0.1.0b2; before, unset meant dev) |

`production` is read as `prod`. Strict mode requires:

- store URLs with `rediss://` (or `valkeys://`) and the certificate and host name checked; a
  localhost URL may stay plain. `ssl_cert_reqs=required` and `ssl_check_hostname=true` are
  written into the URL so every client built from it verifies explicitly; a repeated query
  parameter is refused;
- `LLMQ_ID_HASH_SALT`, at least 16 characters (it keys the hash of ids such as phone numbers;
  a short key does not stop brute force);
- `LLMQ_KEY_PREFIX`;
- `LLMQ_BASE_URL_ALLOWED_HOSTS` when the registry has an `openai` account;
- `LLMQ_DEBUG_CAPTURE_FROM_CONFIG` off (it stores prompt text).

```mermaid
flowchart LR
  E{LLMQ_ENVIRONMENT} -- "dev / local / test / ci" --> D[dev: checks relaxed]
  E -- "unset or any other name" --> S[strict: TLS, salt,<br/>allowed hosts, no debug capture]
```

Numbers have a range: whole-number variables refuse `200.0` or `1e3`, TTLs stay within
2147483647 seconds (what `EXPIRE` takes), and every bound is listed below. An empty value
(`LLMQ_LEASE_SECS=`, as compose's `${VAR:-}` writes) counts as unset.

## Stores

| Variable | Default | Meaning |
|---|---|---|
| `LLMQ_STORE_URL` | required | The state store: `redis://`, `rediss://`, `valkey://` or `valkeys://` |
| `LLMQ_STORE_PASSWORD` | | Optional; read through `secrets=` on every new connection (rotation) |
| `LLMQ_QUEUE_STORE_URL` | the state store | Optional separate store for queues and doorbells (same TLS rule) |
| `LLMQ_QUEUE_STORE_PASSWORD` | | As `LLMQ_STORE_PASSWORD`, for the queue store |
| `LLMQ_STORE_MAX_CONNECTIONS` | 200 | Store connection pool per engine (worker slots + callers + 3), 4 to 100000. Sync calls without a started queue use at most 20 |
| `LLMQ_QUEUE_FAILOVER_SECS` | 5 | With a separate queue store: send errors this long before calls run direct (0 to 3600) |
| `LLMQ_QUEUE_WAIT_REPLICAS` | 0 (off) | A send waits for this many replicas to acknowledge it (0 to 16) ... |
| `LLMQ_QUEUE_WAIT_MS` | 100 | ... for at most this long, up to 60000 (must stay below the store read timeout, see [Tune](../how-to/tune.md)) |

## Environment, names and process

| Variable | Default | Meaning |
|---|---|---|
| `LLMQ_ENVIRONMENT` | unset (strict) | See above |
| `LLMQ_ID_HASH_SALT` | | Key of the user / session id hash (HMAC); required when strict, at least 16 characters. Use the same value in every service that shares the store |
| `LLMQ_KEY_PREFIX` | `llmq` in dev and test; **required** in strict mode | Prefix of this environment's store keys: name it per app and environment (`myapp-prod`). 1-64 letters, digits, `_`, `.` or `-` (no braces, colons or glob characters: it is part of every key) |
| `LLMQ_QUEUE_PREFIX` | empty | Prefix of this environment's queue names; 0-64 of the same characters |
| `LLMQ_MODE` | `both` | `both` (caller and worker), `caller` or `worker` |
| `LLMQ_METRICS_SINK` | `emf` | `emf` (CloudWatch embedded metrics), `prometheus` (extra `metrics`) or `none`. One sink per process |

## Registry

| Variable | Default | Meaning |
|---|---|---|
| `LLMQ_REGISTRY` | | The registry document (JSON) |
| `LLMQ_REGISTRY_FILE` | | A file holding it (set one of the two, not both). A repeated JSON key is refused |
| `LLMQ_REGISTRY_LIVE_EDIT` | `safe` | `safe`, `all` or `off`, when the document has no `live_edit` field (see [Live edits](../how-to/configure.md#live-edits)) |
| `LLMQ_CFG_RELOAD_SECS` | 30 | How often each process reloads the registry (1 to 3600) |
| `LLMQ_AUDIT_RETENTION_DAYS` | 90 | How long registry audit entries are kept (at most 3650) |
| `LLMQ_BASE_URL_ALLOWED_HOSTS` | empty | Comma-separated bare host names (no scheme, port or path: `api.openai.com`) an `openai` account's `base_url` may name. Empty: any host in dev; required when strict and the registry has an `openai` account |
| `LLMQ_DEBUG_CAPTURE_FROM_CONFIG` | false | Allows a model's `debug_capture` (prompt text in the store for an hour); refused when strict |

## Calls and lifecycle

| Variable | Default | Meaning |
|---|---|---|
| `LLMQ_LEASE_SECS` | 60 | Lease of a running call's grant; renewed by the heartbeat every third of it (at most 3600) |
| `LLMQ_CALLER_TTL_SECS` | 30 | How long a caller process counts as alive without a heartbeat. Must be at least `LLMQ_LEASE_SECS / 3` plus a margin (`max(5, beat / 2)`), checked at start |
| `LLMQ_RESULT_TTL_SECS` | 300 | How long a stored result lives |
| `LLMQ_RESULT_DEDUPE_SECS` | 0 | Keep a result with a library-made id this long after it was read (0: delete on read) |
| `LLMQ_STATUS_TTL_SECS` | 1800 | How long a call's status record lives (at least 60) |
| `LLMQ_DRAIN_SECS` | 25 | Default `drain_secs` of `stop()` (0 to 3600; 0 returns held work at once); fits the Kubernetes default grace period of 30 s (see [Operate](../how-to/operate.md#deploys-and-restarts)) |
| `LLMQ_LOCAL_LIMIT_CAP` | 10 | Calls per process and model run without the store under `store_down: static_cap` |

## Account secrets

| Variable | Meaning |
|---|---|
| `LLMQ_ACCOUNT_*` | Secrets named by a registry account's `api_key_ref` or `external_id_ref`. Only names with this prefix (upper case, digits, `_`) are accepted, so a registry can never name the store password or the pod's AWS keys |

## Tests only

| Variable | Meaning |
|---|---|
| `LLMQ_BEDROCK_ENDPOINT_URL`, `LLMQ_STS_ENDPOINT_URL` | Point Bedrock / STS at fakes (fakellm, moto); `https` only in strict mode |
