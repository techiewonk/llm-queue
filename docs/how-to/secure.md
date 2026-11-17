# Secure a deployment

The checks llm-queue makes so a production deployment holds no plain-text secrets, accepts no
unauthenticated writes and sends no prompt text where it should not. Most are on by default in
strict mode.

## 1. Run strict

Leave `LLMQ_ENVIRONMENT` unset or set it to your environment's name (`prod`, `staging`); only
`dev`, `local`, `test` and `ci` relax the checks. Strict mode requires TLS stores with
certificate and host-name checks, an `LLMQ_ID_HASH_SALT` of 16 characters or more, an explicit
`LLMQ_KEY_PREFIX` (so two environments on one store never share queues), allowed `base_url`
hosts and `https` for `LLMQ_BEDROCK_ENDPOINT_URL` / `LLMQ_STS_ENDPOINT_URL` (any host), and it
refuses debug capture. See [Environment variables](../reference/environment.md#llmq_environment-strict-or-dev).

`verify=False` (or `use_ssl=False`) in an account's `credentials=` is allowed in every mode, with a
`bedrock_tls_verify_off` warning: use it only against a local fake.

## 2. Keep account secrets out of the registry

The registry names secrets; it never holds them:

```json
"vllm": {"kind": "openai", "base_url": "https://vllm.internal.example.com/v1",
         "api_key_ref": "LLMQ_ACCOUNT_VLLM_API_KEY"}
```

- `api_key_ref` and `external_id_ref` must be upper-case names starting with `LLMQ_ACCOUNT_`.
  Anything else is refused, so a registry author (or a live edit) cannot send the store password
  or the pod's own AWS keys as a Bearer token.
- The value is looked up in the secrets (`from_env(secrets=)`, else `LLMQ_SECRETS_SOURCE`)
  first, then the environment.
- Audit entries and the HTTP router redact `role_arn`, `external_id_ref` and `api_key_ref`.

### Read secrets from a secret store, not the manifest

Set `LLMQ_SECRETS_SOURCE` (`aws-secretsmanager:<id>` on the pod role, or `file:<path>` on a
mounted Kubernetes secret) and `LLMQ_SECRET_NAMES`, so the store password, the id salt and the
admin token never sit in a manifest or the environment (see
[Configure: Secrets](configure.md#secrets)):

- grant the pod role `secretsmanager:GetSecretValue` on that one secret only;
- a source that cannot be read at start (denied, missing) stops the process with a
  `ConfigError` naming the source; values never appear in an error, a log line, `repr` or
  `describe()` (which shows the source kinds, secret ids and mapped names only);
- values are cached for `LLMQ_SECRETS_CACHE_SECS` (300), so a rotation reaches new store
  connections within that time without a restart; a failed refresh keeps the last values and
  logs `secrets_refresh_failed`.

## 3. Restrict `base_url`

An `openai` account's `base_url`:

- must be `https`, unless the host is local (`localhost`, `127.0.0.1`, `::1`,
  `host.docker.internal`);
- must not carry a user, password or key (`?api_key=`, `?token=`, ...);
- must not point at a link-local, metadata (`169.254.169.254`, `fd00:ec2::/32`), unspecified or
  multicast address, in any spelling (`0x7f.1`, `2130706433` and `::ffff:a.b.c.d` are read as
  the address they name);
- must name a host in `LLMQ_BASE_URL_ALLOWED_HOSTS`. When strict, the variable is required as
  soon as the registry has an `openai` account.

## 4. Protect the HTTP write routes

```python
from fastapi import Depends

app.include_router(
    llmq.http_router(
        dependencies=[Depends(require_admin)],   # or LLMQ_ADMIN_TOKEN
        allow_writes=True,
        actor=lambda request: request.state.user.email,
    ),
    prefix="/llmq",
)
```

- Two tokens, both optional and read on every request (rotation needs no restart), carried in
  `LLMQ_ADMIN_HEADER` (`X-LLMQ-Token`): `LLMQ_ADMIN_READ_TOKEN` opens the read routes only;
  `LLMQ_ADMIN_TOKEN` opens reads and writes. The app's `dependencies` apply on top of them.
- **Reads are guarded too.** `/registry` lists account names, regions, model ids, `base_url`s
  and routing blocks (only role ARNs and secret references are redacted). With no token and no
  `dependencies` the router mounts only the open routes and logs
  `admin_routes_unauthenticated`; `insecure_reads=True` mounts the reads open, for an
  internal-only port behind the app's own auth. Write routes with no write guard raise
  `ConfigError`. Only `/health/live`,
  `/health/ready` and `/metrics` are always open.
- Write routes are mounted only with `allow_writes=True` (default
  `LLMQ_ADMIN_WRITES_ENABLED=false`) and need the admin token or `dependencies`; the read token
  never writes. With `dependencies` alone, a write also needs the `X-LLMQ-Admin` header with any
  non-empty value. Every write takes a JSON body. A browser cannot send the token header, the
  `X-LLMQ-Admin` header or a JSON body cross-site without a CORS preflight, so a page an admin
  visits cannot post an edit with the admin's cookies.
- Every write is a dry run unless its body says `"dry_run": false`, and is audited under an
  actor: `actor(request)` (the authenticated name, never a client-supplied value; 1-200
  characters without control characters), else, with the token, `token:<X-LLMQ-Actor>`
  (`token:unknown` without the header). A shared token has no identity: the `X-LLMQ-Actor`
  value is a claimed name, labelled as such; apps with real auth pass `actor=`.
- `GET /dlq/{entry_id}?unsafe=true` returns message bodies (prompts) only with
  `LLMQ_ADMIN_DLQ_BODIES_ENABLED=true`.
- Bodies and ids are bounded before any store read: at most 100 paths per edit, ids and names
  of at most 256 characters.

```bash
curl -X POST https://app/llmq/blocks \
  -H "X-LLMQ-Token: $LLMQ_ADMIN_TOKEN" -H "X-LLMQ-Actor: ann@example.com" \
  -H "Content-Type: application/json" -d '{"region": "ap-south-2", "dry_run": false}'
```

## 5. Limit live edits

In the default `safe` live-edit mode, these change only with a deploy: residency,
`debug_capture`, `model_fallback` (where calls may go), `inference.extra` (fields sent to the
provider), account `kind` / `base_url` / secrets, region lists and deployment `params`. Put
`"live_edit": "safe"` (or `"off"`) in the registry document to make that the rule for every
process. `inference.extra` accepts only plain lowercase names and refuses SDK arguments and
request fields such as `extra_body`, `store`, `metadata` and `user`.

The stored document's protected fields (`role_arn`, `model_id`, residency, `live_edit`, ...) must
also match each pod's own deploy config: a stored document that differs is refused by that pod
(logged and counted), so whoever can write the store key cannot redirect prompts.

## 6. Who may set priority and cancel

- `priority=True` calls are served before every tenant's fair share. Only trusted code should set
  it; do not pass it through from a client request.
- Anyone who can call the queue (or reach the store) may cancel any request id, and a cancel
  stops the call for every caller of that id. Every cancel is logged (`call_cancel`) and counted
  (`llmq_cancels_total{source}`); an admin cancel (`llm-queue requests cancel --id ID --yes`,
  `POST /requests/cancel`, `llmq.admin.requests.cancel`) also writes the audit trail.

## 7. Prompt text

- No prompt text is logged; ids are hashed (user, session, and tenant in logs).
- `debug_capture` (prompt text in the store for an hour) needs
  `LLMQ_DEBUG_CAPTURE_FROM_CONFIG=true`, which is refused in every strict environment.
- Dead letters hold the prompt: `queue.dlq_retention_secs` is capped at two weeks and dead
  letters are trimmed on a timer.

## Large results in S3

Offloaded results are encrypted at rest (`sse`, default `AES256`, or a KMS key), their object
names hold no caller text and no request id, and a reference outside the configured prefix is
refused before any read. `expected_bucket_owner` makes every request fail when the bucket
changed owner. Every option, the IAM and KMS permissions per process and an example lifecycle
rule are in [Keep large results in S3](large-results.md).

