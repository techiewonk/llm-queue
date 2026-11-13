# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[semantic versioning](https://semver.org/); while it is 0.x, a minor release may break the
public API (see STABILITY.md).

## [Unreleased]

## [0.1.0b1] - 2026-11-14

First beta, for test runs (`pip install --pre llm-queue`).

### Added

- **Queued LLM calls on Valkey / Redis.** `LLMQueue.from_env()` runs calls to Amazon Bedrock
  and OpenAI-compatible servers (vLLM) through a queue per model (a normal and a priority
  stream) on one Redis-protocol store. Every pod runs the same code as caller and worker
  (`LLMQ_MODE`: `both`, `caller`, `worker`); budgets, waiter order and health live in the
  store, so one pod and fifty behave the same.
- **Budgets per deployment** (model x account x region): RPM and TPM (`smooth` GCRA or `fixed`
  per minute), account budgets shared by models, concurrency slots learned from throttles,
  priority reserve and normal minimum share. Admission is one atomic store script: choose a
  deployment (`ordered`, `round_robin`, `weighted`, `headroom`, `custom:`), take every budget
  or none, keep waiters in arrival order.
- **Routing and recovery:** fallback regions and fallback models by cause (`exhausted`,
  `broken`, `error`) after a wait; retries with backoff, a per-model retry budget (moving to a
  deployment the call has not tried is free) and one deadline per call; circuit breaker per
  deployment (calls wait for its probe); health with auto-disable, probes and a last-healthy
  safeguard; slow deployments become last choices; credentials judged per account and region.
- **Registry** (`LLMQ_REGISTRY`): accounts (AWS by pod identity or assumed role,
  OpenAI-compatible endpoints), models with confirmed and fallback deployments, residency,
  routing (kill switch, `queue_pct`, blocked regions), every setting at four levels with
  types, ranges and defaults, validated with every problem listed. Stored in the store with a
  version rule, live edits with audit, rollback and restore after a store reset.
- **Calls:** `converse` / `chat`, `converse_stream` (events through a chunk stream;
  `CallFailed.partial_text` on a cut), `embed` (batched per model, vectors in order), sync
  calls (`*_sync`, `close_sync`), jobs (`register_job` / `send_job`, at least once),
  `request_context` (priority, session, user, tenant), duplicate request ids run once,
  cancel, large results in S3. LangChain: `QueuedChatBedrockConverse`,
  `QueuedBedrockEmbeddings`, `QueuedChatOpenAI`, `QueuedOpenAIEmbeddings`.
- **Errors:** every failed call is `CallFailed` with a `kind` (`rate_limited`,
  `deadline_exceeded`, `model_unavailable`, `queue_unavailable`, ...) and `retry_after_secs`
  where a retry can work; admin operations raise `StoreUnavailable`; bad configuration is
  `ConfigError` (`UnknownModel`, `MissingExtra`). Everything raised is an `LLMQueueError`.
- **Store resilience:** reconnect on `READONLY` and dropped connections (failover), a read
  timeout that is never retried, a blocking connection pool, `store_down` rules (`pause`,
  `static_cap`, `fail_fast`), callers kept alive through a store outage, calls held past their
  deadline let go, optional separate queue store with failover to the direct path, optional
  `WAIT` for replicas.
- **Tuning in code:** `Tuning` (`StoreOptions`, `BedrockOptions`, `EngineTimings`,
  `RegistryOptions`) passed to `LLMQueue.from_env(tuning=...)`: every timeout, pool size and
  pacing value (admission scripts included) with a measured default, validated at start-up.
- **Operations:** status and request status, alerts, metrics (CloudWatch EMF, Prometheus), one
  `llmq_call_finished` record per call, `health()` with per-store state, `pause_receiving()`
  for a preStop hook and a bounded `stop(drain_secs=)`, the `llm-queue` CLI (status, registry,
  settings, block, drain, dead letters, queues) and a FastAPI router (`http` extra).
- **Testing:** fakellm (a fake Bedrock / OpenAI server), `scripts/check.py` (lint, types, tests
  per step), fault scenarios by call count (`scripts/scenarios.py`: worker, store, provider,
  registry, caller and multi-client faults under three loads), load and chaos gates, a soak
  with chaos, a store failover drill, and runnable samples on fakellm or real Bedrock.
- **Docs site** (Zensical on GitHub Pages): tutorial, how-to guides, explanation with
  diagrams, an API reference from the docstrings.

[Unreleased]: https://github.com/techiewonk/llm-queue/compare/v0.1.0b1...HEAD
[0.1.0b1]: https://github.com/techiewonk/llm-queue/releases/tag/v0.1.0b1
