# 03 Embed

Embed texts with Titan and Cohere. Use it to index documents and to embed search queries.

## Key points

- `embed` returns one vector per text, in order.
- The library splits the texts into batches of the model's `embed_batch_size`: Titan takes 1
  text per request, Cohere up to 96. Every batch goes through the queue and the model's RPM
  budget.
- Cohere needs `input_type`: `search_document` when indexing, `search_query` when searching.

## Code

```python title="samples/03_embed.py"
--8<-- "samples/03_embed.py"
```

## Output (live, 2026-11-14)

```text
titan: 3 vectors of 1024 dims; similar pair 0.727, unrelated pair 0.069
cohere: 3 vectors of 1024 dims; similar pair 0.829, unrelated pair 0.365
cohere query vector: 1024 dims
```

The two banking sentences score close on both models, and the monsoon sentence scores far.
Titan separates them more sharply.
