# CLI and HTTP

## `llm-queue` command

Reads the same `LLMQ_*` settings as the app. `--actor NAME` (before the command) records who
made a change in the audit.

| Command | Does |
|---|---|
| `llm-queue status [--model M]` | Status of every model, or of one |
| `llm-queue request ID` | Status of one call |
| `llm-queue settings get --model M [--deployment D]` | Effective value of every setting and the level it came from |
| `llm-queue registry get [--model M]` | The live registry (secrets redacted) |
| `llm-queue registry set [--model M] path=value ...` | Live edit (JSON values) |
| `llm-queue registry enable M D` / `disable M D` | Turn one deployment (`account/region`) of a model on or off |
| `llm-queue registry diff` | What is live but not in this process's config |
| `llm-queue registry log [--count N] [--path P]` | The audit trail (default 20 entries) |
| `llm-queue registry publish [--from-version N]` | Publish `LLMQ_REGISTRY`, or roll back to version N |
| `llm-queue registry gc` | Delete keys of models that no longer exist |
| `llm-queue block --region R` / `--account A` | Stop using a region or an account |
| `llm-queue unblock --region R` / `--account A` | Use it again |
| `llm-queue drain M [--wait] [--timeout S]` | Disable a model for new calls; serve waiting ones (`--wait` until its queues are empty, default timeout 600 s) |
| `llm-queue dlq list\|show\|redrive\|purge --model M [--priority] [--count N]` | Dead letters of a model's queue (`show --unsafe` prints message bodies) |
| `llm-queue queues list` | The queues |

## HTTP routes

`llmq.http_router(dependencies=[...], allow_writes=False, actor=None)` returns a FastAPI router
(extra `http`):

```python
app.include_router(llmq.http_router(dependencies=[Depends(require_admin)]), prefix="/llmq")
```

| Route | Auth | Answers |
|---|---|---|
| `GET /health/live` | none | 200 while the process runs |
| `GET /health/ready` | none | 200 when the registry is loaded and not draining, else 503; each store as `ok`, `read_only` or `down` |
| `GET /status?model=M` | `dependencies` | Status of every model or one (404 for an unknown model) |
| `GET /requests/{id}` | `dependencies` | Status of one call |
| `GET /registry` | `dependencies` | The live registry |
| `POST /registry?model=M` | `dependencies`, `allow_writes=True` | Live edit: a JSON body of `path: value`, audited under `actor(request)` |
| `POST /registry/block?region=R&account=A&on=true` | `dependencies`, `allow_writes=True` | Block or unblock a region or account, audited |

`allow_writes=True` needs `actor=` (a function from the request to the authenticated name).
