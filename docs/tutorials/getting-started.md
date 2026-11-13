# Getting started

This tutorial installs llm-queue, starts a local store and a fake Bedrock, describes two models
in a registry and makes a first call. Nothing here calls a real LLM.

You need Python 3.11+ and Docker.

## 1. Install

```bash
pip install "llm-queue[langchain-aws]"            # Bedrock + LangChain
pip install "llm-queue[openai,langchain-openai]"  # OpenAI-compatible servers
pip install "llm-queue[http]"                     # status and health routes for FastAPI
```

The base install covers Bedrock. Optional features sit behind extras (`openai`,
`langchain-aws`, `langchain-openai`, `metrics`, `http`); using one without its extra raises
`MissingExtra`, which names the extra to install.

## 2. Start a local stack

From a clone of the [repository](https://github.com/techiewonk/llm-queue):

```bash
python scripts/stack.py up        # Valkey, Redis, fakellm (fake Bedrock and OpenAI)
python scripts/stack.py status    # which services answer
```

| Service | Port | Used for |
|---|---|---|
| Valkey 8.1 | 6379 | the store (password `localdev`) |
| Redis 7.4 | 6380 | a second store engine for tests |
| fakellm | 8080 | a fake Bedrock / OpenAI-compatible server |

Any Valkey 8 or Redis 7.2+ works as the store. It must allow Lua scripts, pub/sub and blocking
list reads; cluster mode is not supported.

## 3. Write a registry

The registry describes accounts, models and their settings. Save this as `registry.json`:

```json
{
  "registry_version": 1,
  "residency": {"allowed_regions": ["us-east-1"]},
  "accounts": {"aws-main": {"kind": "aws"}},
  "models": {
    "gpt20b": {
      "type": "chat", "provider": "bedrock", "model_id": "openai.gpt-oss-20b-1:0",
      "rpm": 60, "tpm": 100000,
      "inference": {"max_tokens": 512},
      "deployments": {"aws-main": {"confirmed": ["us-east-1"]}}
    },
    "cohere": {
      "type": "embedding", "provider": "bedrock", "model_id": "cohere.embed-english-v3",
      "rpm": 120, "embed_batch_size": 96,
      "deployments": {"aws-main": {"confirmed": ["us-east-1"]}}
    }
  }
}
```

Each model has an alias (`gpt20b`), a provider model id and its budgets. Its deployments name
the account and the regions it runs in. Use your own Service Quotas numbers in a real registry.

## 4. Set the environment

The library reads its process settings from `LLMQ_*` variables. For the local stack and
fakellm:

```bash
export LLMQ_STORE_URL="redis://:localdev@127.0.0.1:6379/0"
export LLMQ_REGISTRY_FILE="registry.json"
export LLMQ_METRICS_SINK="none"
export LLMQ_BEDROCK_ENDPOINT_URL="http://127.0.0.1:8080"   # fakellm; tests only
export AWS_ACCESS_KEY_ID="aws-main" AWS_SECRET_ACCESS_KEY="fake"
```

The environment defaults to `dev`. Outside `dev` / `local` / `test` / `ci`, `LLMQ_STORE_URL` must
be `rediss://` with the certificate checked, and `LLMQ_ID_HASH_SALT` must be set. Every setting
is in [Configuration](../reference/configuration.md).

## 5. Make a first call

```python
import asyncio

from llm_queue import LLMQueue, request_context


async def main() -> None:
    llmq = LLMQueue.from_env()
    await llmq.start()                 # publish / load the registry, run the workers
    try:
        request = {
            "messages": [{"role": "user", "content": [{"text": "Name one stock index."}]}],
            "inferenceConfig": {"maxTokens": 300},
        }
        with request_context(priority=True, session_id="s-1", user_id="u-1"):
            reply = await llmq.converse("gpt20b", request)
        print(reply.response)          # the Bedrock Converse body
        print(reply.meta.deployment)   # "aws-main/us-east-1"

        vectors = (await llmq.embed("cohere", ["first", "second"],
                                    input_type="search_document")).vectors
        print(len(vectors))
    finally:
        await llmq.stop()


asyncio.run(main())
```

`start()` publishes the registry to the store (or loads a newer one already there) and runs the
worker side in this process. The call is put on the model's queue, admitted by a worker under
the model's budgets, and the reply comes back through the store. `meta` says where and how the
call was served.

## 6. Choose a run mode

`LLMQ_MODE` decides what `start()` runs in a process:

| Mode | Runs | Use |
|---|---|---|
| `both` (default) | the caller side and the worker side | every app pod runs the same code; no separate worker fleet |
| `caller` | the caller side only | processes that make calls but should not run them |
| `worker` | the worker side and jobs only | a separate worker deployment |

Budgets, slots, waiter order and health live in the store, so any number of processes in any
mode share the same limits.

## Next steps

- Runnable samples of streaming, sync calls, jobs, fallback, status and LangChain:
  [Samples](../how-to/samples.md).
- Every way to call: [Make calls](../how-to/call.md).
- Settings and live edits: [Configure](../how-to/configure.md).
- Deploying and running it: [Operate](../how-to/operate.md).
