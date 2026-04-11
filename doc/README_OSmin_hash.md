# OpenSearch MinHash Token Filter Guide

### Understanding Jaccard Similarity and Shingling

At its core, MinHash estimates the **Jaccard Similarity** between two documents: the number of shared items (intersection) divided by the total number of unique items (union). However, standard Jaccard similarity evaluates texts as strict mathematical sets. It completely ignores **token frequency** (how many times a word appears) and **token order** (grammatical sequence). For example, the sentences "The dog bit the man" and "The man bit the dog" have the exact same vocabulary and yield a 100% Jaccard similarity if analyzed word-by-word. 

To solve this, MinHash pipelines must use **shingling** (n-grams). By grouping the text into overlapping sequential windows (e.g., 5-word chunks), we force the algorithm to evaluate the local structural sequence rather than an unstructured bag-of-words. Shingling ensures that a high Jaccard similarity actually represents identical text phrasing, not just a shared vocabulary.

## The OpenSearch LSH Paradigm

If you are coming from academic literature or Python libraries like `datasketch`, **OpenSearch implements Locality Sensitive Hashing (LSH) differently.**

Standard LSH concatenates hashes into "bands" at *index time* to enforce similarity thresholds. **OpenSearch does not do index-time banding.** Instead, it indexes individual, uncombined min-hashes as separate terms. You control the similarity threshold dynamically at *query time* using the `minimum_should_match` parameter in a boolean or terms query.

---

## Parameter Reference

The `min_hash` filter is placed at the end of an analyzer pipeline (typically after tokenization and shingling). It accepts the following configuration parameters:

### `hash_count` (Integer)
The number of independent, cryptographic hashing functions applied to each token in the document.
* **Default:** `1`
* **Impact:** Higher values distribute the hashing process across more mathematical permutations, but linearly increase the CPU load during indexing.

### `bucket_count` (Integer)
The number of discrete ranges (buckets) the output hash space is divided into. The filter tracks the minimum hash value observed *for each bucket independently*. 
* **Default:** `512`

### `hash_set_size` (Integer)
The number of minimum hash values to retain per bucket.
* **Default:** `1`
* **Impact:** Standard MinHash retains only the single absolute minimum hash (value `1`). Setting this higher is only recommended for extremely short texts (like single sentences) where standard MinHash fails to generate enough variance. 

### `with_rotation` (Boolean)
A mathematical safeguard for short documents.
* **Default:** `true`
* **Impact:** If a document is so short that it lacks enough unique tokens to fill the requested `hash_set_size`, OpenSearch will mathematically rotate (bitwise shift) the existing hashes to deterministically synthesize the missing values.

---

## Under the Hood: Hash Count vs. Bucket Count

Lucene provides two ways to generate a 512-token signature:

### The "Brute Force" Method (`hash_count: 512`, `bucket_count: 1`)
This is the standard academic approach. For every single token in the text, Lucene runs 512 entirely different hashing algorithms. 
* **Cost:** If a document has 10,000 tokens, the CPU performs 5,120,000 cryptographic hash operations. This causes massive indexing latency.

### The Lucene Optimization (`hash_count: 1`, `bucket_count: 512`)
To solve the CPU bottleneck, Lucene leverages the fact that a good hash function (MurmurHash3) distributes outputs uniformly across a massive 64-bit integer space. 
1. Lucene divides that massive output space into 512 equal segments (buckets) using modulo arithmetic (`hash_value % 512`).
2. For each token, Lucene runs the hash function **exactly once**.
3. It looks at the resulting number, calculates which of the 512 buckets it belongs to, and updates the minimum value *only* for that specific bucket.
* **Cost:** For a 10,000-token document, the CPU performs exactly 10,000 hash operations.

Statistically, both methods produce mathematically sound fingerprints of the text. The optimization method gives you identical LSH deduplication power while requiring 512x less CPU work during ingestion.

---

## The Index Bloat Formula (Critical)

The total number of synthetic tokens output to the inverted index **per document** is determined by this strict formula:

> **Tokens Output = `hash_count` × `bucket_count` × `hash_set_size`**

If you mistakenly configure `hash_count: 512` AND `bucket_count: 512`, OpenSearch will generate **262,144 unique tokens per document**, which will catastrophically crash your cluster.

### Recommended Production Configuration

Target a total output of **128 to 512 tokens** per document depending on the desired resolution of your similarity estimates. Maximize `bucket_count` and minimize `hash_count` for optimal indexing speed.

```json
"filter": {
  "my_minhash": {
    "type": "min_hash",
    "hash_count": 1,
    "bucket_count": 512, 
    "hash_set_size": 1
  }
}
```

---

## Querying and Thresholds

Because OpenSearch outputs individual min-hash tokens, you control the required Jaccard similarity dynamically at query time.

To find duplicates, pass the analyzed hashes of your target document into a `terms` query (or use a `more_like_this` query) against the `min_hash` field. Use `minimum_should_match` to enforce your threshold:

> **`minimum_should_match` = Target Similarity % × Total Tokens**

**Examples (Assuming a 512-token configuration):**
* **10% Similarity (Loose - Catches heavy OCR noise):** `minimum_should_match: 51`
* **50% Similarity (Moderate):** `minimum_should_match: 256`
* **90% Similarity (Strict - Near exact matches):** `minimum_should_match: 460`

This allows you to index the document once and dynamically query it for entirely different use cases without reindexing.


## Sources

- [Lucene source code](https://github.com/apache/lucene/blob/main/lucene/analysis/common/src/java/org/apache/lucene/analysis/minhash/MinHashFilter.java#L34)
- [OpenSearch doc](https://docs.opensearch.org/latest/analyzers/token-filters/min-hash/)