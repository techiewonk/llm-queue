# llm-queue

LLM calls through a queue on Valkey or Redis, under RPM / TPM budgets per **deployment**
(model x account x region), balanced across regions and accounts, with fallback regions,
fallback models, health checks and status. Works with Amazon Bedrock and OpenAI-compatible
servers (vLLM), and with LangChain. The queue, the budgets and the results live in one store, so
admission (deployment, RPM, TPM, concurrency slot) is one atomic step, and 1 pod behaves the
same as 50.

## Install

```
pip install "llm-queue[langchain-aws]"            # Bedrock + LangChain
pip install "llm-queue[openai,langchain-openai]"  # OpenAI-compatible
pip install "llm-queue[http]"                     # status and health routes for FastAPI
```

## Example

```python
from llm_queue import LLMQueue, CallFailed, request_context

llmq = LLMQueue.from_env()               # LLMQ_STORE_URL, LLMQ_REGISTRY, ...
await llmq.start()                       # publish / load the registry, run the workers
try:
    with request_context(priority=True, user_id=uid):
        reply = await llmq.converse("gpt120b", {"messages": [...]}, max_wait_secs=5)
    print(reply.response, reply.meta.deployment)
except CallFailed as exc:                # exc.kind: rate_limited, model_unavailable, ...
    ...
await llmq.stop()
```

## Documentation

Full documentation: **https://techiewonk.github.io/llm-queue/**

- [Getting started](https://techiewonk.github.io/llm-queue/tutorials/getting-started/): install, local stack, a registry, the first call
- [How-to guides](https://techiewonk.github.io/llm-queue/how-to/configure/): configure, tune, call, operate, share models across services, test
- [Explanation](https://techiewonk.github.io/llm-queue/explanation/architecture/): architecture, the life of a call, admission, failures and recovery
- [Reference](https://techiewonk.github.io/llm-queue/reference/configuration/): settings, error kinds, CLI and HTTP, the Python API

Project: [CHANGELOG](https://github.com/techiewonk/llm-queue/blob/main/CHANGELOG.md),
[CONTRIBUTING](https://github.com/techiewonk/llm-queue/blob/main/CONTRIBUTING.md),
[STABILITY](https://github.com/techiewonk/llm-queue/blob/main/STABILITY.md),
[SECURITY](https://github.com/techiewonk/llm-queue/blob/main/SECURITY.md).
