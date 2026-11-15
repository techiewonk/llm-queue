# Secure a deployment

The checks llm-queue makes so a production deployment holds no plain-text secrets, accepts no
unauthenticated writes and sends no prompt text where it should not. Most are on by default in
strict mode.

## 1. Run strict

Leave `LLMQ_ENVIRONMENT` unset or set it to your environment's name (`prod`, `staging`); only
`dev`, `local`, `test` and `ci` relax the checks. Strict mode requires TLS stores with
certificate and host-name checks, `LLMQ_ID_HASH_SALT`, allowed `base_url` hosts, and refuses
debug capture. See [Environment variables](../reference/environment.md#llmq_environment-strict-or-dev).

## 2. Keep account secrets out of the registry

The registry names secrets; it never holds them:

```json
"vllm": {"kind": "openai", "base_url": "https://vllm.internal.example.com/v1",
         "api_key_ref": "LLMQ_ACCOUNT_VLLM_API_KEY"}
```

- `api_key_ref` and `external_id_ref` must be upper-case names starting with `LLMQ_ACCOUNT_`.
  Anything else is refused, so a registry author (or a live edit) cannot send the store password
  or the pod's own AWS keys as a Bearer token.
- The value is looked up in `from_env(secrets=)` first, then the environment.
- Audit entries and the HTTP router redact `role_arn`, `external_id_ref` and `api_key_ref`.

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
        dependencies=[Depends(require_admin)],   # required with allow_writes
        allow_writes=True,
        actor=lambda request: request.state.user.email,
    ),
    prefix="/llmq",
)
```

- `allow_writes=True` without `dependencies` raises `ConfigError`: write routes are never
  mounted without the app's own auth.
- `POST /registry` and `POST /registry/block` take a JSON body (`/registry/block`:
  `{"region": ..., "account": ..., "on": true}`) and need the `X-LLMQ-Admin` header with any
  non-empty value. A browser cannot send either cross-site without a CORS preflight, so a page an
  admin visits cannot post an edit with the admin's cookies.
- `actor(request)` returns the authenticated name the audit records; never a client-supplied
  value. Actors are 1-200 characters without control characters.

```bash
curl -X POST https://app/llmq/registry/block \
  -H "Authorization: Bearer $TOKEN" -H "X-LLMQ-Admin: 1" \
  -H "Content-Type: application/json" -d '{"region": "ap-south-2", "on": true}'
```

## 5. Limit live edits

In the default `safe` live-edit mode, these change only with a deploy: residency,
`debug_capture`, `model_fallback` (where calls may go), `inference.extra` (fields sent to the
provider), account `kind` / `base_url` / secrets, region lists and deployment `params`. Put
`"live_edit": "safe"` (or `"off"`) in the registry document to make that the rule for every
process. `inference.extra` accepts only plain lowercase names and refuses SDK arguments and
request fields such as `extra_body`, `store`, `metadata` and `user`.

## 6. Prompt text

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

