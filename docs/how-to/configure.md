# Configure

llm-queue takes its configuration from two places:

1. **Process settings** (`LLMQ_*`): the stores, prefixes, the registry's source and a few process
   limits. Read once by `LLMQueue.from_env()`.
2. **The registry document**: accounts, models, deployments and every model setting. Published to
   the store and shared by every process.

Timeouts, pool sizes and loop pacing are set in code; see [Tune](tune.md).

## Process settings

`LLMQueue.from_env()` reads `LLMQ_*` from the environment. With a secrets source
(`LLMQ_SECRETS_SOURCE`, see [Secrets](#secrets)) or `secrets=` (a function from a name to a
value or `None`), each name is looked up there first, then in the environment:

```python
llmq = LLMQueue.from_env(secrets=my_secret_lookup)
```

`LLMQ_STORE_PASSWORD` and `LLMQ_QUEUE_STORE_PASSWORD` are read on every new connection (from
the secrets, else the environment), so a rotated password is picked up without a restart. To
avoid environment variables entirely, pass a mapping: `LLMQueue.from_env(environ={...},
secrets=...)`.

### Secrets

Where secrets come from is configuration, not code: another app with other secret names or
another secret store changes two variables. In a Kubernetes manifest:

```yaml
- {name: LLMQ_SECRETS_SOURCE, value: "aws-secretsmanager:${ENV}/researchassistant/secret"}
- {name: LLMQ_SECRETS_NAMES, value: '{"LLMQ_STORE_HOST":"valkey_host","LLMQ_STORE_PORT":"valkey_port","LLMQ_STORE_PASSWORD":"valkey_key","LLMQ_ID_HASH_SALT":"llmq_id_hash_salt"}'}
```

- `LLMQ_SECRETS_SOURCE` lists sources tried in order for each name:
  `aws-secretsmanager:<secret-id>` (one secret holding a JSON object; the pod role's
  credentials; region `LLMQ_SECRETS_REGION`, else `AWS_REGION`), `file:<path>` (a mounted
  secret volume with one file per key, a `.json` file or a `.env` file), `env` and `none` (the
  default). `${NAME}` and `${NAME:-default}` are expanded from the environment (`ENV` above).
- In a `.env` file, `#` starts a comment at the start of a line or after whitespace
  (`KEY=abc # note` reads `abc`); a `#` with no whitespace before it is part of the value
  (`KEY=a#b`). Quote a value that holds ` #`: `KEY="a # b"` is the text between the quotes,
  and only a comment may follow the closing quote. A line that breaks these rules is refused,
  naming the line, never its text.
- `LLMQ_SECRETS_NAMES` gives each setting's key in the secret; a setting named twice is
  refused. `secrets_names=` in code is merged over it (the argument wins per setting). Names
  and keys are stripped of spaces in both.
- Values are cached for `LLMQ_SECRETS_CACHE_SECS` (300) and read again after, so a rotated
  store password reaches the next new connection within that time. A source that cannot be read
  at start (missing, denied) stops `from_env` with a `ConfigError` naming it; a key it does not
  hold is unset, and the environment and `defaults=` are asked next.
- The same source serves the store passwords, `LLMQ_ADMIN_TOKEN`, `LLMQ_JOB_*`, the account
  secrets (`api_key_ref`, `external_id_ref`), registry placeholders, the `llm-queue` CLI,
  and `AdminServices.from_env` (it takes `secrets=`, `defaults=` and `secrets_names=` as
  `LLMQueue.from_env` does).
- `describe()` shows the sources (kind and secret id) and the mapped setting names, never a
  value.

Precedence: `secrets=` in code wins over `LLMQ_SECRETS_SOURCE` (which is then not read); then
the environment; then `defaults=`. To build the same lookup in code:

```python
from llm_queue.secrets import secrets_from_config

lookup = secrets_from_config("file:/etc/llmq,aws-secretsmanager:prod/app", cache_secs=60)
llmq = LLMQueue.from_env(secrets=lookup, secrets_names={"LLMQ_STORE_PASSWORD": "valkey_key"})
```

An app's own store is a class with `kind`, `label` and `load() -> Mapping[str, str]` (the
`llm_queue.secrets.SecretSource` protocol), wrapped in `SecretReader([source], cache_secs=...)`.
Whatever its `load()` raises, the reader treats it like a failed read: at start a `ConfigError`
naming the source and the exception's class (never its message, which may echo a value); on
a later refresh the last values are kept and `secrets_refresh_failed` is logged.

### Layers: secrets, environment, defaults

`from_env` reads every `LLMQ_*` name from three layers, highest first:

1. the secrets: `secrets=` (a function from a name to a value or `None`), else the
   `LLMQ_SECRETS_SOURCE` sources;
2. `environ=` (default `os.environ`);
3. `defaults=`: a mapping the app keeps in code, used for any name the two above leave unset.

```python
llmq = LLMQueue.from_env(
    secrets=my_secret_lookup,
    secrets_names={                       # the secrets' own names
        "LLMQ_STORE_HOST": "valkey_host",
        "LLMQ_STORE_PASSWORD": "valkey_password",
        "LLMQ_ADMIN_TOKEN": "llmq_admin_token",
    },
    defaults={                           # the app's defaults; a manifest or a secret wins
        "LLMQ_KEY_PREFIX": "research-dev",
        "LLMQ_REGISTRY_FILE": "llm_registry.yaml",
        "LLMQ_STORE_MAX_CONNECTIONS": "auto",
        "LLMQ_DRAIN_FILE": "/tmp/drain",
    },
)
```

- `secrets_names` (merged over `LLMQ_SECRETS_NAMES`) asks the secret under its own name instead
  of the `LLMQ_*` name; the order of the layers does not change. It covers every setting read through the layers: the store
  passwords, `LLMQ_ADMIN_TOKEN`, `LLMQ_JOB_*` and the account secrets.
- A secret wins over the environment, as in 0.1. For the environment to win over a secret,
  build the environment yourself: `environ=collections.ChainMap(os.environ, secret_map)`.
- Two settings that name one thing two ways form a group: `LLMQ_STORE_URL` against the store
  parts, and `LLMQ_REGISTRY` against `LLMQ_REGISTRY_FILE`. The highest layer that sets any
  name of the group picks the form and the other form is ignored in every layer, so a default
  `LLMQ_REGISTRY_FILE` in code gives way to a manifest's `LLMQ_REGISTRY`. One layer that sets
  both is refused.

### The store as parts

Instead of one URL, give the store as parts (often what a secret holds):

| Setting | Default |
|---|---|
| `LLMQ_STORE_HOST` | none (then `LLMQ_STORE_URL` is required) |
| `LLMQ_STORE_PORT` | 6379 |
| `LLMQ_STORE_DB` | 0 |
| `LLMQ_STORE_TLS_ENABLED` | true (`rediss://`) |
| `LLMQ_STORE_PASSWORD` | none |

The URL is built from them (the password percent-escaped, so `@`, `/` and `:` in it are safe)
and checked like `LLMQ_STORE_URL`: in strict mode a host that is not local needs TLS, and the
certificate and host name checks are written into it. Setting the parts and `LLMQ_STORE_URL` in
one layer is refused.

### Size the store pool from the registry

`LLMQ_STORE_MAX_CONNECTIONS=auto` sizes the pool as the sum of `worker_slots` over the boot
registry's models plus 10 (the receive loop, the result subscriber, heartbeats, status reads),
within 4 to 100,000. It is computed once by `from_env`: a live edit that raises `worker_slots`
does not resize it (`start()` warns when the pool is below the slots). A number (default 200)
works as before.

The settings you set in most deployments:

| Setting | Meaning |
|---|---|
| `LLMQ_STORE_URL` | The state store (`redis://` or `rediss://`). Required, unless the store is given as parts (`LLMQ_STORE_HOST`, above) |
| `LLMQ_ENVIRONMENT` | `dev`, `local`, `test` or `ci` relax the checks; anything else, **unset included**, is strict: TLS stores, `LLMQ_ID_HASH_SALT`, allowed `base_url` hosts, no debug capture |
| `LLMQ_ID_HASH_SALT` | Key of the user / session id hash (HMAC); required when strict |
| `LLMQ_KEY_PREFIX`, `LLMQ_QUEUE_PREFIX` | Names of this environment's keys and queues. The key prefix defaults to `llmq` in dev and is **required** in strict mode (name it per app and environment); the queue prefix defaults to empty |
| `LLMQ_REGISTRY` or `LLMQ_REGISTRY_FILE` | The registry document, inline or as a file (set one, not both) |
| `LLMQ_ROLE` | `both` (default), `caller`, `worker` |
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
| `live_edit` | Optional: `safe`, `all` or `off`; the live-edit mode of this document (deploy-only). Unset, each process's `LLMQ_REGISTRY_LIVE_EDIT_MODE` applies |
| `min_lib_version` | Optional: the oldest llm-queue that may use this document (for example `0.1.0b2`) |

Invalid documents are refused with every problem listed at once.

### A registry file with placeholders

Keep the registry beside the code and leave what changes per environment to variables. A file
ending `.yaml` takes comments (`pip install 'llm-queue[yaml]'`); JSON works the same way.

```yaml
# registry.yaml  (LLMQ_REGISTRY_FILE=registry.yaml)
registry_version: ${REGISTRY_VERSION:-1}
residency: {allowed_regions: auto}          # every region used below
accounts:
  aws-main: {kind: aws}
models:
  gpt120b:
    type: chat
    provider: bedrock
    model_id: ${CHAT_MODEL_ID:-openai.gpt-oss-120b-1:0}
    quota_rpm: 500                          # the account's quota ...
    quota_tpm: 400000
    budget_pct: 0.9                         # ... of which this app uses 90 %
    deployments:
      aws-main:
        confirmed: ${CHAT_REGIONS:-["ap-south-1"]}
  embed-cohere:
    type: embedding
    provider: bedrock
    model_id: cohere.embed-english-v3
    rpm: ${EMBED_RPM:-2000}
    embed_batch_size: 96
    deployments:
      aws-main:
        confirmed: ${EMBED_REGIONS:-["ap-south-1"]}
```

`${NAME}` reads `from_env(secrets=)` and then the environment; `${NAME:-default}` falls back to
the default; an unset name without one stops the start, naming the variable and where it is. A
value that is a whole placeholder and reads as a JSON list, object, number or boolean becomes
that value (`EMBED_REGIONS='["ap-south-1","ap-south-2"]'` is a list). In YAML, an unquoted
`${...}` is already a string. Only this boot document is expanded; live edits are stored as
written. Details: [Placeholders and YAML](../reference/registry.md#placeholders-and-yaml).

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
| `openai` | `base_url` (required), `api_key_ref` | `https` unless the host is local; no user, password or key in the URL; link-local and metadata addresses refused; the host must be in `LLMQ_HTTP_ALLOWED_HOSTS` when strict |

`api_key_ref` and `external_id_ref` name a secret, not hold it: an upper-case name that starts
with `LLMQ_ACCOUNT_`, looked up in `from_env(secrets=)` and then the environment. Any other name
is refused, so a registry edit can never point an account at the store password or the pod's
AWS keys. See [Secure a deployment](secure.md).

### Setting levels

A setting may be allowed at several levels. The most specific one wins:

**deployment > model > account > defaults**

Some settings exist at only some levels (for example `shared_rpm` / `shared_tpm` only on an
account, `model_fallback` only on a model). Every setting, its levels, range and default are
in [Configuration](../reference/configuration.md#registry-settings); the document's shape and
its JSON Schema are in [Registry document](../reference/registry.md).

To see the effective value of each setting for a model and the level it came from:

```bash
llm-queue models get gpt120b                        # every setting with its level
llm-queue deployments get gpt120b aws-main/us-east-1  # one deployment's settings and state
```

## Live edits

The registry can be changed while the system runs, without a deploy. The document's `live_edit`
field (else `LLMQ_REGISTRY_LIVE_EDIT_MODE`, default `safe`) decides what may change live. Keeping the
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
llm-queue registry set --model gpt120b rpm=400              # a dry run: the diff it would make
llm-queue registry set --model gpt120b rpm=400 --yes        # path=value ... (JSON values)
llm-queue deployments disable gpt120b aws-main/ap-south-1 --yes  # turn one deployment off
llm-queue registry diff                                     # what is live but not in this config
llm-queue audit list --count 20                             # the audit trail
llm-queue registry rollback N --yes                         # roll back to version N
```

Every change is a dry run (the diff it would make, after every check) until `--yes`. From code,
`llmq.admin.registry` and its neighbours do the same (for example
`await llmq.admin.deployments.disable(model, deployment, actor="me", dry_run=False)`;
`llmq.admin.registry.current` is the registry this process runs on); over HTTP, `POST /registry`
and the other write routes when the router allows writes (see
[Admin API](../reference/admin-api.md)).

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
