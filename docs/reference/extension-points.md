# Extension points

Named hooks an app registers to change one decision without forking the library.

| Hook | Registered with | Chosen by | Contract |
|---|---|---|---|
| Balancer | `register_balancer(name, fn)` | `balance: "custom:<name>"` | `fn(views, ctx) -> ordered keys`; the script then takes the first that fits |
| Fallback policy | `register_fallback_policy(name, fn)` | `fallback_policy` | `fn(cause, waited, rule, ctx) -> bool` replaces the `on` / `after_secs` test |
| Admission | `register_admission(name, fn)` | `admission` | `fn(ctx)` returns a `Reject` or None, on the caller before sending |
| Sticky key | `register_sticky_key(name, fn)` | `sticky.key: "custom:<name>"` | `fn(ctx)` returns a key or None (hashed) |
| Probe | `register_probe(name, fn)` | `probe` | `fn(alias, op) -> params` for health probes |
| Token estimator | `register_token_estimator(name, fn)` | `token_estimator` | `fn(op, params) -> input tokens` |
| Provider, account kind | `register_provider(name, factory)`, `register_account_kind(kind, provider)` | model `provider`, account `kind` | a `Provider` with `call(op, params, route, timing, sink=)` and `classify(exc)` |
| Others | `register_job`; `from_env(credentials=, secrets=, metrics=, offload=, offload_bytes=, worker_id=)` | | credentials per account, secret source, metrics sink, S3 offload |

Hooks run through `call_safely`: one that raises or is not registered is logged once and the built-in
behaviour is used, so a hook never fails a call. `CallContext` carries ids, never prompt text.
