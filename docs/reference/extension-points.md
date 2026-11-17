# Extension points

Named hooks an app registers to change one decision without forking the library.

| Hook | Registered with | Chosen by | Contract |
|---|---|---|---|
| Balancer | `register_balancer(name, fn)` | `balance: "custom:<name>"` | `fn(views, ctx) -> ordered keys`; the script then takes the first that fits |
| Fallback policy | `register_fallback_policy(name, fn)` | `fallback_policy` | `fn(cause, waited, rule, ctx) -> bool` replaces the `on` / `after_secs` test |
| Admission | `register_admission(name, fn)` | `admission` | `fn(ctx)` returns a `Reject` or None, on the caller before sending |
| Sticky key | `register_sticky_key(name, fn)` | `sticky.key: "custom:<name>"` | `fn(ctx)` returns a key or None (hashed) |
| Probe | `register_probe(name, fn)` | `probe` | `fn(alias, op) -> params or None` (type `Probe`) for health probes; `op` is `converse`, `chat`, `embeddings` or `invoke_model`; None sends the built-in probe |
| Token estimator | `register_token_estimator(name, fn)` | `token_estimator` | `fn(op, params) -> input tokens` |
| Embedding family | `register_embedding_family(name, codec)` | model `family`, or the codec's id `patterns` | an `EmbeddingCodec`: `build_request(texts, options)`, `parse_reply(body)`, `max_batch`, `max_chars`; register before the registry loads ([Embedding families](../how-to/call.md#embedding-families)) |
| Provider, account kind | `register_provider(name, factory)`, `register_account_kind(kind, provider)` | model `provider`, account `kind` | a `Provider` with `call(op, params, route, timing, sink=None, *, converse_api="converse")` and `classify(exc)` |
| Secret source | `llm_queue.secrets`: `SecretReader([source], cache_secs=...)` or `secrets_from_config(spec)`, passed as `from_env(secrets=)` | `secrets=` (wins over `LLMQ_SECRETS_SOURCE`) | a `SecretSource`: `kind`, `label` (safe to log, no values) and `load() -> Mapping[str, str]`, raising `ConfigError` naming the source when it cannot be read; see [Configure: Secrets](../how-to/configure.md#secrets) |
| Others | `register_job`; `from_env(credentials=, secrets=, metrics=, offload=, offload_bytes=, worker_id=)` | | credentials per account, secret source, metrics sink, S3 offload |

Hooks run through `call_safely`. A balancer, fallback policy, sticky key, probe or token
estimator that raises or is not registered is logged once and the built-in behaviour is used,
so those hooks never fail a call.

**Admission fails closed.** An admission hook is a gate (tenants, entitlements), so one that
raises, or an `admission` name the registry uses that is not registered in this process,
refuses the call with `CallFailed("rejected")`. Every hook failure, of any kind, is logged once
per name and counted on every occurrence as `llmq_hook_failed_total{kind, hook}` (admission
hooks; the others are logged only).

**Hooks must not block.** They run synchronously on the caller's or worker's event loop: no
network or disk I/O and no `time.sleep` inside a hook, or every call in the process waits on
it. Load what a hook needs (entitlements, prices) in the background and read it from memory.

`CallContext` carries ids, never prompt text.
