# 03 Embed

Embed texts with Titan and Cohere. Use it to index documents and to embed search queries.

## Key points

- `embed` returns an `EmbedResult`: one vector per text, in order, and what it cost:
  `meta.tokens_in` (input tokens the replies reported), `batches` (provider calls) and
  `request_ids` (one per answered batch), for cost tracking and support.
- The library splits the texts into batches of the model's `embed_batch_size`: Titan takes 1
  text per request, Cohere up to 96. Every batch goes through the queue and the model's RPM
  budget.
- Cohere needs `input_type`: `search_document` when indexing, `search_query` when searching.
- A family the library does not know (a new Cohere or Nova version, an OpenAI-compatible
  server with its own body) gets an `EmbeddingCodec` through `register_embedding_family`, in
  every process, before `start()`. A registry model opts in with `"family": "<name>"`. The
  sample registers one that no model uses, so the run is the same with or without it.

## Code

```python title="samples/03_embed.py"
--8<-- "samples/03_embed.py"
```

## Output (fakellm, 2026-11-16)

```text
titan: 3 vectors of 1024 dims; similar pair -0.043, unrelated pair 0.011
  30 input tokens in 3 batch(es), 3 request id(s)
cohere: 3 vectors of 1024 dims; similar pair -0.043, unrelated pair 0.011
  30 input tokens in 1 batch(es), 1 request id(s)
cohere query vector: 1024 dims
```

fakellm answers with a fixed text and random vectors, so its replies and similarity numbers mean
nothing; the lines show what the code prints. The live output will be refreshed on the next live
run (`python scripts/samples.py --live`); the last one, on 2026-11-14, ran this sample before
its current code.

On real Bedrock (2026-11-14), the two banking sentences scored close on both models (Titan
0.727, Cohere 0.829) and the monsoon sentence far (0.069, 0.365).
