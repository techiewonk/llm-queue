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
connection, so a rotated password is picked up without a restart.

The settings you set in most deployments:

| Setting | Meaning |
|---|---|
| `LLMQ_STORE_URL` | The state store (`redis://` or `rediss://`). Required |
| `LLMQ_ENVIRONMENT` | `dev` (default), `local`, `test`, `ci`, or any other name; any other name requires TLS and `LLMQ_ID_HASH_SALT` |
| `LLMQ_ID_HASH_SALT` | Key of the user / session id hash (HMAC); required outside dev |
| `LLMQ_KEY_PREFIX`, `LLMQ_QUEUE_PREFIX` | Names of this environment's keys and queues |
| `LLMQ_REGISTRY` or `LLMQ_REGISTRY_FILE` | The registry document, inline or as a file (set one, not both) |
| `LLMQ_MODE` | `both` (default), `caller`, `worker` |
| `LLMQ_METRICS_SINK` | `emf` (CloudWatch, default), `prometheus`, `none` |

Every setting with its default is in [Configuration](../reference/configuration.md).
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
| `accounts` | One entry per account (`kind`: `aws`, or a kind added with `register_account_kind`), with account-level settings |
| `models` | One entry per alias: `type` (`chat` or `embedding`), `provider`, `model_id`, settings, and `deployments` |
| `models.*.deployments` | Per account: `confirmed` regions (used first) and `fallback` regions (used by the `fallback` rule), plus deployment-level settings |

Invalid documents are refused with every problem listed at once.

### Setting levels

A setting may be allowed at several levels. The most specific one wins:

**deployment > model > account > defaults**

Some settings exist at only some levels (for example `shared_rpm` / `shared_tpm` only on an
account, `model_fallback` only on a model). Every setting, its levels, type, range and default
are in the [design plan](https://github.com/techiewonk/llm-queue/blob/main/docs/ACCOUNT_ROUTING_PLAN.md),
sections 3.9 and 3.19.

To see the effective value of each setting for a model and the level it came from:

```bash
llm-queue settings get --model gpt120b
```

## Live edits

The registry can be changed while the system runs, without a deploy. `LLMQ_REGISTRY_LIVE_EDIT`
decides what may change live:

| Value | Live edits allowed |
|---|---|
| `safe` (default) | Numbers and switches. Residency, `debug_capture`, account `kind` / `base_url` / secrets, adding or removing an account, region lists and deployment `params` change only with a deploy |
| `all` | Everything except the deploy-only changes below |
| `off` | None |

Always deploy-only: `registry_version`, `min_lib_version`, adding or removing a model, a model's
`type`, `provider`, `model_id`, `worker_slots` and queue name.

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
    version again keeps the stored document and its live edits.

## Large results

Results above a size can go to S3 instead of the store:

```python
llmq = LLMQueue.from_env(offload=my_offload, offload_bytes=...)
```

The store keeps a marker; the caller gets the full result.
