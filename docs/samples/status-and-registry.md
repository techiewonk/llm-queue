# 07 Status and registry

Check the status of models and calls, and edit the registry live. Use it for admin pages,
runbooks and readiness checks.

## Key points

- `status(model)` shows waiting calls, calls in progress, and each deployment's RPM / TPM used
  and left this minute, its health and its breaker.
- `request_status(id)` follows a single call.
- `llmq.admin.registry.set(values, model=, actor=, dry_run=False)` changes a safe setting live
  in every pod (without `dry_run=False` it only returns the diff it would make). Each change is
  audited, and `llmq.admin.audit.list()` lists the changes.
- Per-tenant fairness (`fairness` in the registry, off by default) is a live setting too:
  `quota` caps each tenant's rpm / tpm, `share` splits the budget by weight and lends what is
  unused, `fair_queue` serves waiting tenants round robin. A call's tenant is its `tenant_id`,
  else its `user_id`, from `request_context`. While it is on, `status(model)` has a `fairness`
  entry with counts only, never a tenant id. Setting it to `None` deletes the model's own
  value, back to the default. See [Tenant fairness](../explanation/fairness.md).
- The same views are on the CLI (`llm-queue`) and the [HTTP router](http-router.md).

## Code

```python title="samples/07_status_and_registry.py"
--8<-- "samples/07_status_and_registry.py"
```

## Output (fakellm, 2026-11-17)

```text
request 01a1ea27bf6173668077caace10bcf66: state=done
aws-main/us-east-1: rpm 2/60 tpm 21/100000 healthy=True breaker=closed
waiting={'priority': 0, 'normal': 0} in_progress=0
plan: [{'path': 'models.gpt20b.rpm', 'before': 60, 'after': 45}]
gpt20b rpm 60 -> 45 (live)
  audit: {'id': '1794929838507-0', 'actor': 'sample', 'op': 'edit', 'version': '1', 'diff': [{'path': 'models.gpt20b.rpm', 'before': 60, 'after': 45}], 'ts': '1794929838.5072'}
  audit: {'id': '1794929832579-0', 'actor': 'sample', 'op': 'dlq_redrive', 'version': '', 'diff': [{'path': 'dlq.llmqsample-03c07bf1-jobs-sync_feed', 'before': {'dead_letters': 1}, 'after': {'moved': 1}}], 'ts': '1794929832.576724'}
  audit: {'id': '1794929827054-0', 'actor': 'sample', 'op': 'edit', 'version': '1', 'diff': [{'path': 'models.gpt20b.deployments.aws-main.overrides.us-east-1.enabled', 'before': False, 'after': None}], 'ts': '1794929827.055'}
fairness on: {'mode': 'quota', 'active_tenants': 1, 'waiting_within_share': 0}
fairness off: in status: False
health: {'live': True, 'ready': True, 'draining': False, 'draining_since': None, 'worker': 'api-7f9c:12-9bb228', 'version': '0.1.1', 'registry_version': 1, 'held': 0, 'heartbeat_age_secs': 2.4, 'stores': {'state': 'ok', 'queue': 'ok'}, 'eviction_policy': 'noeviction', 'key_expiry_mode': 'native'}
```

The second and third audit entries come from [sample 05](errors-and-fallback.md) turning its
deployments back on. The registry and its audit log are shared by every process using the same
prefix.

The live output will be refreshed on the next live run (`python scripts/samples.py --live`); the
last one, on 2026-11-14, ran this sample before its current code.

See also: [Operate](../how-to/operate.md), [CLI and HTTP](../reference/cli-http.md).
