# Keep large results in S3

A result above `offload_bytes` can go to an S3 bucket instead of the store: the store keeps a
small marker, the caller reads the object and deletes it.

```python
from llm_queue import LLMQueue, S3Offload

offload = S3Offload(
    "my-results-bucket",
    prefix="llmq-results/",
    key_prefix="prod",                      # one namespace per environment
    sse="aws:kms", kms_key_id="arn:aws:kms:ap-south-1:123456789012:key/...", bucket_key=True,
    expected_bucket_owner="123456789012",
)
llmq = LLMQueue.from_env(offload=offload, offload_bytes=256_000)
```

`offload` and `offload_bytes` go together (one without the other is a `ConfigError`).

## What is offloaded, and what is not

- **Any** result above `offload_bytes`: bulk embedding batches most often, but long chat answers
  too. Results are written as they are (JSON, not compressed).
- Never: results of the direct path (`*_sync` without a started queue, a fallback when the
  queue store is down) and streams, which do not go through the result store.
- If the upload fails or takes more than 15 s, the result is kept in the store instead (up to
  64 MB; above that the caller gets `result_expired`). The upload is logged `offload_put_failed`.
- Picking `offload_bytes`: the store keeps everything below it for `LLMQ_RESULT_TTL_SECS`. 256 KB
  keeps chat answers in the store and sends embedding batches (a 96 x 1024 OpenAI batch is about
  0.5 MB, a Cohere one about 2 MB) to S3.

## Every process needs the same offload

Callers read what workers wrote, so every process of the fleet needs the same `S3Offload`
(bucket, prefix, `key_prefix`). The store records that the fleet offloads: a process started
without the offload then refuses to start (`ConfigError`); the record lapses 7 days after the
last offloaded write. A process that still meets an offloaded result without an offload fails
the read with `result_expired` and logs `offload_missing`; the result is left in place.

With `LLMQ_RESULT_DEDUPE_SECS` above 0 a result is read more than once: build the offload with
`delete_after_read=False` (the bucket rule removes the objects).

## Options

| Option | Default | Effect |
|---|---|---|
| Object key | | `{prefix}/{key_prefix}/{random id}`: no caller text and no request id in a key, no two writes on one name. `prefix` and `key_prefix` are normalised to one trailing `/` |
| `sse` | `AES256` | Server-side encryption header; `aws:kms` / `aws:kms:dsse` with `kms_key_id` for a customer key; `None` leaves it out (the bucket's default encryption applies). `aws:kms` without a key id uses the AWS-managed key (warned) |
| `bucket_key` | false | `BucketKeyEnabled`: one KMS request per bucket key instead of per object |
| `expected_bucket_owner` | | Every request fails when the bucket changed owner |
| `delete_after_read` | true | The object is deleted after a successful read (never before) |
| `client_kwargs` | | Passed to the S3 client. Without a `config`, the client gets 3 s connect / 10 s read timeouts and 2 attempts |

A reference outside the configured prefix is refused before any read. A read that fails for
another reason than "no such key" (access denied, throttling, a timeout) is logged
`offload_get_failed` with the S3 error code, tried twice more, then raised as
`queue_unavailable`: the result is kept and can be read again. A missing object is
`result_expired`.

## IAM and KMS

| Process | S3 | KMS (with `sse="aws:kms"`) |
|---|---|---|
| Workers (write results) | `s3:PutObject`, `s3:DeleteObject` (a duplicate run's object) | `kms:GenerateDataKey` |
| Callers (read results) | `s3:GetObject`, `s3:DeleteObject` | `kms:Decrypt` |
| Any process (start-up check) | `s3:GetLifecycleConfiguration` (optional: without it the check is skipped) | |

```json
{
  "Effect": "Allow",
  "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
  "Resource": "arn:aws:s3:::my-results-bucket/llmq-results/prod/*"
}
```

## Lifecycle rule

Objects are deleted when they are read. Results nobody reads (a caller that timed out, was
cancelled or died; a failed delete; `delete_after_read=False`) stay until a bucket rule removes
them. `start()` warns `offload_no_lifecycle_rule` when no enabled expiry rule covers the prefix.
S3 expiry counts whole days, so an object can live up to about two days. On a versioned bucket
a delete only adds a marker: expire noncurrent versions too.

```json
{
  "Rules": [
    {
      "ID": "llmq-results-expiry",
      "Status": "Enabled",
      "Filter": {"Prefix": "llmq-results/"},
      "Expiration": {"Days": 1},
      "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
      "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}
    }
  ]
}
```

```bash
aws s3api put-bucket-lifecycle-configuration --bucket my-results-bucket \
  --lifecycle-configuration file://lifecycle.json
```
