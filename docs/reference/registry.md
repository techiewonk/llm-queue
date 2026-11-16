# Registry document

The registry document (`LLMQ_REGISTRY`, or the file named by `LLMQ_REGISTRY_FILE`) names the
accounts, the models and where each model runs, and holds every model setting. This page is its
shape. Every setting, with its default and range, is in
[Configuration](configuration.md#registry-settings); how to write and change one is in
[Configure](../how-to/configure.md).

## JSON Schema

[`registry-schema.json`](registry-schema.json) is the document's JSON Schema (draft 2020-12). It
is generated from the same checks that validate a document, so it lists every field, type, range
and library default. Print the one for your installed version with:

```bash
llm-queue registry schema > registry-schema.json   # needs no store and no LLMQ_* settings
```

Point your editor at it (in VS Code, `json.schemas` in the settings) for completion and checks
while you write. The schema covers shape, types and ranges only. The rules that compare fields
(a TPM below one call's reservation, fallback cycles, residency, a breaker probe shorter than an
attempt) are checked when the library loads the document, and it lists every problem with its
path at once.

## A complete example

Every part of the document in one example (a unit test loads it, so it stays valid):

<!-- registry-example -->
```json
{
  "registry_version": 7,
  "min_lib_version": "0.1.0b2",
  "live_edit": "safe",
  "defaults": {
    "timeout_secs": 120,
    "attempt_timeout_secs": 60,
    "max_wait_secs": 30,
    "inference": {"temperature": 0.2},
    "queue": {"retention_secs": 86400},
    "alerts": {"queue_age_secs": 600}
  },
  "routing": {
    "kill_switch": false,
    "allow_users": [],
    "blocked_regions": [],
    "blocked_accounts": []
  },
  "residency": {"allowed_regions": ["ap-south-1", "ap-south-2", "dc1"]},
  "accounts": {
    "aws-main": {"kind": "aws", "settings": {"shared_rpm": 5000}},
    "aws-dr": {
      "kind": "aws",
      "role_arn": "arn:aws:iam::222222222222:role/llmq-bedrock",
      "external_id_ref": "LLMQ_ACCOUNT_DR_EXTERNAL_ID",
      "settings": {"attempt_timeout_secs": 45}
    },
    "vllm-dc1": {
      "kind": "openai",
      "base_url": "https://vllm.internal.example/v1",
      "api_key_ref": "LLMQ_ACCOUNT_VLLM_API_KEY"
    }
  },
  "models": {
    "gpt120b": {
      "type": "chat",
      "provider": "bedrock",
      "model_id": "openai.gpt-oss-120b-1:0",
      "rpm": 500,
      "tpm": 400000,
      "max_concurrency": 64,
      "priority_reserve": 0.2,
      "balance": "headroom",
      "balance_priority": "ordered",
      "inference": {"max_tokens": 4096},
      "inference_limits": {"max_tokens": 8192, "on_exceed": "clamp"},
      "fallback": {"on": ["exhausted", "broken"], "after_secs": 2},
      "model_fallback": {
        "on": ["exhausted", "broken"],
        "after_secs": 5,
        "models": ["gpt120b-vllm"]
      },
      "fairness": {"mode": "share", "weights": {"tenant-a": 3}, "default_weight": 1},
      "deployments": {
        "aws-main": {
          "confirmed": ["ap-south-1"],
          "fallback": [
            {
              "name": "apac",
              "region": "ap-south-1",
              "profile_id": "apac.openai.gpt-oss-120b-1:0",
              "routes_to": ["ap-south-1", "ap-south-2"],
              "rpm": 300
            }
          ],
          "overrides": {"ap-south-1": {"max_concurrency": 40}}
        },
        "aws-dr": {
          "confirmed": ["ap-south-2"],
          "overrides": {"ap-south-2": {"rpm": 200}}
        }
      }
    },
    "gpt120b-vllm": {
      "type": "chat",
      "provider": "openai",
      "model_id": "gpt-oss-120b",
      "rpm": 120,
      "tpm": 200000,
      "residency": {"allowed_regions": ["dc1"]},
      "deployments": {"vllm-dc1": {"confirmed": ["dc1"]}}
    },
    "embed-docs": {
      "type": "embedding",
      "provider": "bedrock",
      "model_id": "arn:aws:bedrock:ap-south-1:111111111111:application-inference-profile/abc123",
      "family": "cohere",
      "rpm": 2000,
      "embed_batch_size": 96,
      "embed_max_chars": 2048,
      "inference": {"input_type": "search_document", "embedding_types": ["float"]},
      "deployments": {"aws-main": {"confirmed": ["ap-south-1"]}}
    }
  }
}
```

What each part does:

| Path | What it does here |
|---|---|
| `registry_version` | 7. Raise it with every deployed change: a process never replaces a newer stored document with an older one (see [the version rule](../how-to/share-models.md#the-registry-version-rule)) |
| `min_lib_version` | A process older than 0.1.0b2 does not use this document and keeps its own |
| `live_edit` | `safe`: live edits may change budgets and switches, not protected fields |
| `defaults` | For every model: a 120 s deadline, 60 s per attempt, at most 30 s waiting for budget, temperature 0.2, queued messages kept one day, a `queue_stalled` alert after 10 minutes |
| `routing` | The switches operators flip live (below) |
| `residency` | Calls may run only in these three regions |
| `accounts.aws-main` | The pod's own AWS identity; one 5000 RPM budget shared by all its deployments |
| `accounts.aws-dr` | A second AWS account, reached by assuming a role; the external id is the secret `LLMQ_ACCOUNT_DR_EXTERNAL_ID` (through `secrets=`, then the environment); its attempts time out after 45 s |
| `accounts.vllm-dc1` | An OpenAI-compatible server; its key is the secret `LLMQ_ACCOUNT_VLLM_API_KEY`. Outside dev its host must be in `LLMQ_BASE_URL_ALLOWED_HOSTS` |
| `models.gpt120b` | A chat model with 500 RPM and 400,000 TPM per deployment, 64 calls in flight, 20 % kept for priority calls, balanced by free headroom (priority calls in order) |
| `...inference`, `...inference_limits` | 4096 output tokens unless the call asks for another number; a call asking for more than 8192 is clamped to 8192 |
| `...fallback` | When the confirmed regions are out of budget or broken for 2 s, the `fallback` entries are used |
| `...model_fallback` | After 5 s, calls spill to the vLLM model (converted to the OpenAI shape; streams do not fall back across shapes, and the load warns about it) |
| `...fairness` | Per-tenant shares: `tenant-a` gets three times the share of any other active tenant (see [Tenant fairness](../explanation/fairness.md)) |
| `...deployments.aws-main` | `ap-south-1` is confirmed; the cross-region inference profile `apac` is a fallback with its own 300 RPM |
| `...overrides` | Deployment-level values: 40 in flight in `ap-south-1` on `aws-main`, 200 RPM in `ap-south-2` on `aws-dr` |
| `models.gpt120b-vllm` | The fallback target, limited to the `dc1` region |
| `models.embed-docs` | An embedding model behind an application inference profile ARN. The ARN names no family, so `family` gives the request shape (Cohere); 96 texts per request, each at most 2048 characters |

## Top level

| Field | Required | Meaning |
|---|---|---|
| `registry_version` | yes | A whole number, 1 to 9007199254740991. Raise it with every deployed change |
| `min_lib_version` | no | The oldest llm-queue that may use the document, a version such as `0.1.0b2` |
| `live_edit` | no | `safe`, `all` or `off`; unset, each process's `LLMQ_REGISTRY_LIVE_EDIT` applies (deploy only) |
| `defaults` | no | Setting values for every model (the `defaults` level) |
| `routing` | no | Live switches, [below](#routing) |
| `residency` | no | `{"allowed_regions": [...]}`: the only regions any deployment may use |
| `accounts` | yes | At least one account, by name |
| `models` | yes | At least one model, by alias |

Names (account names, aliases, regions, profile names) are 1-64 lowercase letters, digits, `_`,
`.` or `-`, starting with a letter or digit. The process's own document is read strictly (an
unknown name is an error). A document read back from the store is read leniently (an unknown
name is a warning and is ignored), so an older pod still starts next to a newer one.

## Accounts

| Field | Meaning |
|---|---|
| `kind` | Required. `aws` (serves `bedrock` models), `openai` (serves `openai` models: an OpenAI-compatible server such as vLLM), or a kind added with `register_account_kind` |
| `enabled` | Default true. False: no new calls through the account |
| `role_arn` | `aws` only: an IAM role to assume (`arn:aws:iam::<12 digits>:role/<name>`); unset, the pod's own credentials are used |
| `external_id_ref` | `aws` with `role_arn` only: the name of the secret holding the external id |
| `base_url` | `openai` only, and required there: `https://` (plain `http://` only for a local host); no user, password or key in it |
| `api_key_ref` | `openai` only: the name of the secret holding the API key |
| `settings` | Account-level settings (the `account` level) |

A secret reference (`external_id_ref`, `api_key_ref`) is the name of a value starting with
`LLMQ_ACCOUNT_` (upper case, digits, `_`), looked up through `secrets=` and then the environment.
No other name is accepted, so a registry can never name the store password or the pod's own AWS
keys.

## Models

| Field | Meaning |
|---|---|
| `type` | Required. `chat` or `embedding` |
| `provider` | Required. `bedrock`, `openai`, or a provider added with `register_account_kind` |
| `model_id` | Required. The provider's model id (or an inference profile ARN), without padding |
| `deployments` | Required. Where the model runs, by account ([below](#deployments)); at least one confirmed region |
| `residency` | `{"allowed_regions": [...]}` for this model; it replaces the top-level list |
| `family` | Embedding models only: `titan`, `cohere`, `openai` or a family added with `register_embedding_family`. It is the request and reply shape. Unset: `openai` for the `openai` provider, else guessed from `model_id` (case-insensitive). Required when the id names no family, or the load fails |
| any setting | Model-level settings, written next to the fields above (the `model` level) |

## Deployments

`models.<alias>.deployments.<account>` holds:

| Field | Meaning |
|---|---|
| `confirmed` | Regions (or inference profiles) used first |
| `fallback` | Regions (or profiles) used only by the `fallback` rule |
| `overrides` | `{"<region or profile name>": {settings}}`: deployment-level settings of one entry |

An entry of `confirmed` or `fallback` is a region name, or an inference profile object:

| Field | Meaning |
|---|---|
| `name` | Required. The profile's own name; it may not equal a region name of the account |
| `region` | Required. The region the client calls |
| `profile_id` | Required. The inference profile id or ARN |
| `routes_to` | The regions the profile may send calls to (checked against residency) |
| any setting | Deployment-level settings of this profile |

An entry listed in both `confirmed` and `fallback`, or twice, is refused.

## Routing

| Field | Default | Meaning |
|---|---|---|
| `kill_switch` | false | True: calls skip the queue and run direct, except those of `allow_users` |
| `allow_users` | `[]` | Hashed user ids (`hash_id`, keyed by `LLMQ_ID_HASH_SALT`) whose calls are always queued, whatever `kill_switch` and `queue_pct` say |
| `blocked_regions` | `[]` | Regions no call may use (live) |
| `blocked_accounts` | `[]` | Accounts no call may use (live) |

A rollback (`llm-queue registry publish --from-version N`) keeps the live `routing` blocks and
kill switch, and its reply lists what it kept.

## Residency

| Field | Meaning |
|---|---|
| `allowed_regions` | The regions deployments may use. A deployment outside the list is not used, and a model left with no deployment is an error. An inference profile is allowed only when every `routes_to` region is |

## Defaults and fairness

`defaults` takes any setting allowed at the `defaults` level. `fairness` is one of them: a
model's `fairness` merges over the default one key by key. Both are listed with every default
and range in [Configuration](configuration.md#registry-settings).
