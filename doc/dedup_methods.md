# Dedup Method Registry

The benchmark framework lives under `scripts/dedup/`.

## Running a Method

```bash
python -m scripts.dedup.benchmark \
  --csv bdrc_data/nlm_merged.csv \
  --method minhash_os_jaccard \
  --options analyzer=tibetan-lenient shingle_size=1 bucket_count=512
```

Run the default comparison matrix:

```bash
python -m scripts.dedup.benchmark --all
```

Results are written to `data/benchmark_results/`. The `--all` run also writes `doc/dedup_methods_results.md`.

## Corpus Scope

The benchmark is intentionally not tied to a fixed set of texts. By default it uses rows from `bdrc_data/nlm_merged.csv` whose `mw_id` exists in `bec_texts`. Use `--allowlist` or `--denylist` with one `mw_id` per line to narrow or broaden the working corpus without changing any method code.

## Registered Methods

- `minhash_datasketch`: in-memory client-side MinHash/LSH bands.
- `minhash_os_query`: production OpenSearch path using stored `text_bo.min_hash_lenient` hashes.
- `minhash_os_jaccard`: offline proxy using OpenSearch analyzer tokens and local MinHash, useful for sweeping analyzer/shingle options without reindexing.
- `minhash_os_sidecar`: queryable sidecar OpenSearch path for the 3-gram MinHash config (`bec_texts_minhash_3`).
- `fasttext_embedding`: FastText document embedding + cosine/knn search.
- `chunked_minhash`: equal-syllable chunks with per-chunk MinHash.
- `chunked_embedding`: equal-syllable chunks with per-chunk FastText embeddings.

## FastText

Build a tokenized training corpus and train two CPU FastText models:

```bash
python -m scripts.dedup.embeddings.train_fasttext
```

Then backfill embeddings:

```bash
python -m scripts.backfill_embeddings --model-path data/fasttext/bo_skipgram_subword.bin
```

Training and encoding both use the `tibetan-lenient` analyzer. The analyzer name is saved in a manifest next to each model.

## 3-Gram MinHash Sidecar

Build the sidecar index for the same corpus scope as a benchmark smoke test:

```bash
python -m scripts.build_minhash_sidecar --from-csv --limit-groups 25
```

Then benchmark the queryable sidecar:

```bash
python -m scripts.dedup.benchmark \
  --method minhash_os_sidecar \
  --options msm_pct=0.02 \
  --limit-groups 25
```

The sidecar avoids changing the existing `bec_texts` index while letting us test the 3-gram signal as a real retrieval method.

