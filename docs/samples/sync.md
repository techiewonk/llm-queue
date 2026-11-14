# 04 Sync

Call from sync code: scripts, Celery tasks, or the threads of a sync library. Async code should
use `converse` / `embed` instead.

## Key points

- `converse_sync` / `embed_sync` run on the **direct path** in a private event-loop thread. They
  share the same RPM / TPM budgets and health as queued calls, but have no queue wait.
- No `start()` is needed for sync calls.
- Call `close_sync()` when done, to stop the private loop and close the clients.

## Code

```python title="samples/04_sync.py"
--8<-- "samples/04_sync.py"
```

## Output (live, 2026-11-14)

```text
gpt20b (sync): 'ready' via aws-main/us-east-1
titan (sync): 1 vector of 1024 dims
```
