# Dedup Method Benchmark Results

| Method | Options | Docs | Groups | F1 | Precision | Recall | PR-AUC | Closed R@20 | Open R@50 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| minhash_os_jaccard | `{"analyzer": "tibetan-lenient", "bucket_count": 512, "shingle_size": 3}` | 258 | 36 | 0.828 | 0.854 | 0.804 | 0.826 | 0.854 | 0.000 |
| minhash_os_sidecar | `{"index": "bec_texts_minhash_3_test", "msm_pct": 0.02}` | 258 | 36 | 0.802 | 0.817 | 0.787 | 0.802 | 0.853 | 0.000 |
| minhash_os_sidecar | `{"index": "bec_texts_minhash_3_test", "msm_pct": 0.055}` | 258 | 36 | 0.802 | 0.817 | 0.787 | 0.802 | 0.853 | 0.757 |
| minhash_os_jaccard | `{"analyzer": "tibetan-lenient", "bucket_count": 512, "shingle_size": 1}` | 258 | 36 | 0.708 | 0.801 | 0.635 | 0.702 | 0.836 | 0.000 |
| chunked_minhash | `{"bucket_count": 256, "chunk_threshold": 0.6, "n_chunks": 20, "shingle_size": 1}` | 258 | 36 | 0.024 | 0.833 | 0.012 | 0.011 | 0.130 | 0.000 |
| minhash_datasketch | `{"bands": 20, "num_perm": 128, "shingle_size": 1}` | 258 | 36 | 0.021 | 0.867 | 0.011 | 0.010 | 0.121 | 0.000 |
| chunked_minhash | `{"bucket_count": 256, "chunk_threshold": 0.6, "n_chunks": 10, "shingle_size": 1}` | 258 | 36 | 0.014 | 0.818 | 0.007 | 0.006 | 0.110 | 0.000 |
| minhash_datasketch | `{"bands": 20, "num_perm": 128, "shingle_size": 3}` | 258 | 36 | 0.000 | 0.000 | 0.000 | 0.000 | 0.087 | 0.000 |
| minhash_os_query | `{"msm_pct": 0.1}` | 29 | 3 | 0.033 | 1.000 | 0.017 | 0.017 | 0.048 | 0.048 |
