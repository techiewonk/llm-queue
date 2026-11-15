# Configure

llm-queue takes its configuration from two places:

1. **Process settings** (`LLMQ_*`): the stores, prefixes, the registry's source and a few process
   limits. Read once by `LLMQueue.from_env()`.
2. **The registry document**: accounts, models, deployments and every model setting. Published to
   the store and shared by every process.

Timeouts, pool sizes and loop pacing are set in code; see [Tune](tune.md).

## Process settings

`LLMQueue.from_env()` reads `LLMQ_*` from the environment. With `secrets=` (a function from a
name to a value or `None`), each name is looked up there first, then in the environment:

```python
llmq = LLMQueue.from_env(secrets=my_secret_lookup)
```

`LLMQ_STORE_PASSWORD` and `LLMQ_QUEUE_STORE_PASSWORD` are read from `secrets=` on every new
connection, so a rotated password is picked up without a restart. To avoid environment
variables entirely, pass a mapping: `LLMQueue.from_env(environ={...}, secrets=...)`.

The settings you set in most deployments:

| Setting | Meaning |
|---|---|
| `LLMQ_STORE_URL` | The state store (`redis://` or `rediss://`). Required |
| `LLMQ_ENVIRONMENT` | `dev`, `local`, `test` or `ci` relax the checks; anything else, **unset included**, is strict: TLS stores, `LLMQ_ID_HASH_SALT`, allowed `base_url` hosts, no debug capture |
| `LLMQ_ID_HASH_SALT` | Key of the user / session id hash (HMAC); required when strict |
| `LLMQ_KEY_PREFIX`, `LLMQ_QUEUE_PREFIX` | Names of this environment's keys and queues |
| `LLMQ_REGISTRY` or `LLMQ_REGISTRY_FILE` | The registry document, inline or as a file (set one, not both) |
| `LLMQ_MODE` | `both` (default), `caller`, `worker` |
| `LLMQ_METRICS_SINK` | `emf` (CloudWatch, default), `prometheus`, `none` |

!!! warning "Unset `LLMQ_ENVIRONMENT` is strict"
    Since 0.1.0b2 a process with no `LLMQ_ENVIRONMENT` gets the production checks. On a laptop
    or in CI set `LLMQ_ENVIRONMENT=dev` (or `local`, `test`, `ci`); a forgotten variable in
    production can no longer switch TLS and the id salt off.

Every variable with its default is in [Environment variables](../reference/environment.md).
`from_env` checks them all at start-up and raises `ConfigError` with every problem listed at once.

## The registry document

```json
{
  "registry_version": 1,
  "residency": {"allowed_regions": ["ap-south-1", "ap-south-2"]},
  "defaults": {"timeout_secs": 120},
  "accounts": {"aws-main": {"kind": "aws"}},
  "models": {
    "gpt120b": {
      "type": "chat", "provider": "bedrock", "model_id": "openai.gpt-oss-120b-1:0",
      "rpm": 500, "tpm": 400000, "balance": "headroom", "balance_priority": "ordered",
      "inference": {"temperature": 0.2, "max_tokens": 4096},
      "fallback": {"on": ["exhausted", "broken"], "after_secs": 2},
      "deployments": {"aws-main": {"confirmed": ["ap-south-1", "ap-south-2"],
                                   "fallback": ["us-east-1"]}}
    },
    "embed-cohere": {
      "type": "embedding", "provider": "bedrock", "model_id": "cohere.embed-english-v3",
      "rpm": 2000, "embed_batch_size": 96,
      "deployments": {"aws-main": {"confirmed": ["ap-south-1"]}}
    }
  }
}
```

| Part | What it holds |
|---|---|
| `registry_version` | An integer of at least 1. Raise it with every deployed change (see [the version rule](share-models.md#the-registry-version-rule)) |
| `residency` | Regions calls may run in; a deployment outside them is not used |
| `defaults` | Setting values for every model |
| `accounts` | One entry per account, with account-level settings. `kind` is `aws` (serves `bedrock` models), `openai` (serves `openai` models: an OpenAI-compatible server such as vLLM), or a kind added with `register_account_kind` |
| `models` | One entry per alias: `type` (`chat` or `embedding`), `provider`, `model_id`, settings, and `deployments` |
| `models.*.deployments` | Per account: `confirmed` regions (used first) and `fallback` regions (used by the `fallback` rule), plus deployment-level settings |
| `live_edit` | Optional: `safe`, `all` or `off`; the live-edit mode of this document (deploy-only). Unset, each process's `LLMQ_REGISTRY_LIVE_EDIT` applies |
| `min_lib_version` | Optional: the oldest llm-queue that may use this document (for example `0.1.0b2`) |

Invalid documents are refused with every problem listed at once.

### Accounts

```json
"accounts": {
  "aws-main": {"kind": "aws"},
  "aws-other": {"kind": "aws", "role_arn": "arn:aws:iam::123456789012:role/llmq",
                "external_id_ref": "LLMQ_ACCOUNT_OTHER_EXTERNAL_ID"},
  "vllm": {"kind": "openai", "base_url": "https://vllm.internal.example.com/v1",
           "api_key_ref": "LLMQ_ACCOUNT_VLLM_API_KEY"}
}
```

| Kind | Fields | Notes |
|---|---|---|
| `aws` | `role_arn`, `external_id_ref` (both optional) | Without `role_arn` the pod's own identity is used (EKS pod identity, IRSA, instance role); with it the role is assumed per account. Takes no `base_url` / `api_key_ref` |
| `openai` | `base_url` (required), `api_key_ref` | `https` unless the host is local; no user, password or key in the URL; link-local and metadata addresses refused; the host must be in `LLMQ_BASE_URL_ALLOWED_HOSTS` when strict |

`api_key_ref` and `external_id_ref` name a secret, not hold it: an upper-case name that starts
with `LLMQ_ACCOUNT_`, looked up in `from_env(secrets=)` and then the environment. Any other name
is refused, so a registry edit can never point an account at the store password or the pod's
AWS keys. See [Secure a deployment](secure.md).

### Setting levels

A setting may be allowed at several levels. The most specific one wins:

**deployment > model > account > defaults**

Some settings exist at only some levels (for example `shared_rpm` / `shared_tpm` only on an
account, `model_fallback` only on a model). Every setting, its levels, type, range and default
are in the design plan (maintainers' notes in docs/internal, not published),
sections 3.9 and 3.19.

To see the effective value of each setting for a model and the level it came from:

```bash
llm-queue settings get --model gpt120b
```

## Live edits

The registry can be changed while the system runs, without a deploy. The document's `live_edit`
field (else `LLMQ_REGISTRY_LIVE_EDIT`, default `safe`) decides what may change live. Keeping the
mode in the document makes it one rule for the whole environment rather than per process.

| Value | Live edits allowed |
|---|---|
| `safe` (default) | Numbers and switches. Residency, `debug_capture`, `model_fallback`, `inference.extra`, account `kind` / `base_url` / secrets, region lists and deployment `params` change only with a deploy |
| `all` | Also the fields `safe` protects. It never adds or removes an account or a model, and never touches the always-deploy-only fields below |
| `off` | None |

Always deploy-only, in every mode: `registry_version`, `min_lib_version`, `live_edit`, adding or
removing a model **or an account**, a model's `type`, `provider`, `model_id`, `worker_slots` and
queue name.

A live edit is validated strictly on the paths it adds or changes: an unknown name (a typo such
as `rmp=400`) is refused with a `RegistryError` rather than stored and ignored. A refused field
raises `LiveEditRefused`; an edit that keeps losing a race with other edits raises
`RegistryConflict` (see [Errors](../reference/errors.md#configerror)).

Ways to edit:

```bash
llm-queue registry set --model gpt120b rpm=400       # path=value ... (JSON values)
llm-queue registry disable gpt120b aws-main/ap-south-1  # turn one deployment off
llm-queue registry diff                              # what is live but not in this config
llm-queue registry log --count 20                    # the audit trail
llm-queue registry publish --from-version N          # roll back to version N
```

From code, `llmq.registry` is the registry store (for example
`await llmq.registry.set_enabled(model, deployment, False, actor="me")`); over HTTP,
`POST /registry` when the router allows writes (see [Operate](operate.md#http-router)).

Every edit is audited under its actor (`--actor` on the CLI; `actor=` in code and HTTP) and kept
for `LLMQ_AUDIT_RETENTION_DAYS` (90). The last 20 versions are kept for rollback
(`RegistryOptions.history_size`). An edit refused by the mode is rejected with its reasons.

!!! note
    A live edit is kept until a newer `registry_version` is published. Publishing the same
    version again keeps the stored document and its live edits. Routing blocks
    (`blocked_regions`, `blocked_accounts`) and the kill switch set live survive even a newer
    version: a deploy never undoes an emergency block.

## Large results

Results above a size can go to S3 instead of the store:

```python
from llm_queue import S3Offload

llmq = LLMQueue.from_env(offload=S3Offload("my-bucket"), offload_bytes=256_000)
```

The store keeps a marker; the caller gets the full result, and the object is deleted once read.
Every process needs the same offload. Options, IAM and the bucket's lifecycle rule are in
[Keep large results in S3](large-results.md).

## Bedrock and other providers

Which Bedrock API each call uses, and the IAM permissions that follow, are in
[Bedrock APIs and IAM](bedrock.md).
