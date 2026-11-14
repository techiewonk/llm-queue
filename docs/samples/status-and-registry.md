# 07 Status and registry

Check the status of models and calls, and edit the registry live. Use it for admin pages,
runbooks and readiness checks.

## Key points

- `status(model)` shows waiting calls, calls in progress, and each deployment's RPM / TPM used
  and left this minute, its health and its breaker.
- `request_status(id)` follows a single call.
- `registry.set` changes a safe setting live in every pod. Each change is audited, and
  `registry.log` lists the changes.
- The same views are on the CLI (`llm-queue`) and the [HTTP router](http-router.md).

## Code

```python title="samples/07_status_and_registry.py"
--8<-- "samples/07_status_and_registry.py"
```

## Output (live, 2026-11-14)

```text
request 01a1d83ea22470e1acb8721297d2277b: state=done
aws-main/us-east-1: rpm 1/60 tpm 143/100000 healthy=True breaker=closed
waiting={'priority': 0, 'normal': 0} in_progress=0
gpt20b rpm 60 -> 45 (live)
  audit: {'actor': 'sample', 'op': 'edit', 'diff': [{'path': 'models.gpt20b.rpm', 'before': 60, 'after': 45}], ...}
  audit: {'actor': 'sample', 'op': 'edit', 'diff': [{'path': 'models.gpt20b.deployments.aws-main.overrides.us-east-1.enabled', 'before': False, 'after': None}], ...}
  audit: {'actor': 'sample', 'op': 'edit', 'diff': [{'path': 'models.gpt120b.deployments.aws-main.overrides.us-east-1.enabled', 'before': False, 'after': None}], ...}
health: {'live': True, 'ready': True, 'stores': {'state': 'ok', 'queue': 'ok'}}
```

The last two audit entries come from [sample 05](errors-and-fallback.md) turning its
deployments back on. The registry and its audit log are shared by every process using the same
prefix.

See also: [Operate](../how-to/operate.md), [CLI and HTTP](../reference/cli-http.md).
