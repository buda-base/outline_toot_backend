from __future__ import annotations

import argparse
import json
import logging
import time
from collections.abc import Iterable
from dataclasses import asdict, replace
from itertools import combinations
from pathlib import Path
from typing import Any

from scripts.dedup.corpus import CorpusConfig, groups_by_field, load_corpus, mw_id_subset_for_groups
from scripts.dedup.methods.base import DedupMethod, QueryMatch, QueryScope, TextDoc
from scripts.dedup.metrics import (
    best_f1,
    pair_scores_from_score_map,
    per_group_recall,
    pr_auc,
    recall_at_k,
    threshold_sweep,
)
from scripts.dedup.registry import available_methods, create_method, load_builtin_methods, options_hash

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_CSV = Path("bdrc_data/nlm_merged.csv")
DEFAULT_OUTPUT_DIR = Path("data/benchmark_results")
DEFAULT_KS = [5, 10, 20, 50]


def parse_options(values: list[str] | None) -> dict[str, Any]:
    options: dict[str, Any] = {}
    for value in values or []:
        if "=" not in value:
            msg = f"Option {value!r} must be key=value"
            raise ValueError(msg)
        key, raw = value.split("=", 1)
        options[key] = _parse_scalar(raw)
    return options


def _parse_scalar(value: str) -> bool | int | float | str:
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _score_map_from_pair_method(
    method: DedupMethod[Any],
    docs: list[TextDoc],
) -> tuple[dict[tuple[str, str], float], dict[str, float]]:
    timings: dict[str, float] = {}
    preload = getattr(method, "preload", None)
    if callable(preload):
        logger.info("Preloading pair fingerprints for %d docs...", len(docs))
        start = time.monotonic()
        preload(docs)
        timings["preload_ms"] = (time.monotonic() - start) * 1000

    logger.info("Computing fingerprints for %d docs...", len(docs))
    start = time.monotonic()
    fingerprints = {doc.mw_id: method.fingerprint(doc) for doc in docs}
    timings["fingerprint_ms"] = (time.monotonic() - start) * 1000

    logger.info("Scoring %d closed-set pairs...", len(docs) * (len(docs) - 1) // 2)
    start = time.monotonic()
    scores: dict[tuple[str, str], float] = {}
    for doc_a, doc_b in combinations(docs, 2):
        key = tuple(sorted((doc_a.mw_id, doc_b.mw_id)))
        scores[key] = float(method.pair_score(fingerprints[doc_a.mw_id], fingerprints[doc_b.mw_id]))
    timings["pair_score_ms"] = (time.monotonic() - start) * 1000
    return scores, timings


def _query_all(
    method: DedupMethod[Any],
    docs: list[TextDoc],
    *,
    top_k: int,
    scope: QueryScope,
) -> tuple[dict[str, list[QueryMatch]], float]:
    results: dict[str, list[QueryMatch]] = {}
    preload = getattr(method, "preload", None)
    if callable(preload):
        logger.info("Preloading query fingerprints for %d docs...", len(docs))
        preload(docs)
    start = time.monotonic()
    for index, doc in enumerate(docs):
        results[doc.mw_id] = method.query(doc, top_k=top_k, scope=scope)
        if (index + 1) % 50 == 0:
            logger.info("Queried %d/%d docs", index + 1, len(docs))
    return results, (time.monotonic() - start) * 1000


def _score_map_from_query_results(query_results: dict[str, list[QueryMatch]]) -> dict[tuple[str, str], float]:
    scores: dict[tuple[str, str], float] = {}
    for source_id, matches in query_results.items():
        for match in matches:
            key = tuple(sorted((source_id, match.mw_id)))
            scores[key] = max(scores.get(key, 0.0), match.score)
    return scores


def _ranking_from_scores(
    docs: list[TextDoc],
    scores: dict[tuple[str, str], float],
    *,
    top_k: int,
) -> dict[str, list[QueryMatch]]:
    doc_ids = [doc.mw_id for doc in docs]
    rankings: dict[str, list[QueryMatch]] = {}
    for source_id in doc_ids:
        matches: list[QueryMatch] = []
        for candidate_id in doc_ids:
            if candidate_id == source_id:
                continue
            key = tuple(sorted((source_id, candidate_id)))
            matches.append(QueryMatch(mw_id=candidate_id, score=scores.get(key, 0.0)))
        rankings[source_id] = sorted(matches, key=lambda match: match.score, reverse=True)[:top_k]
    return rankings


def _closed_eval(
    method: DedupMethod[Any],
    docs: list[TextDoc],
    *,
    groups: dict[str, set[str]],
    ks: list[int],
) -> dict[str, Any]:
    if method.supports_pair_score:
        scores, timings = _score_map_from_pair_method(method, docs)
        rankings = _ranking_from_scores(docs, scores, top_k=max(ks))
    elif method.supports_query:
        rankings, query_ms = _query_all(
            method,
            docs,
            top_k=len(docs) - 1,
            scope=QueryScope.closed_set({doc.mw_id for doc in docs}),
        )
        scores = _score_map_from_query_results(rankings)
        timings = {"closed_query_ms": query_ms}
    else:
        msg = f"{method.name} supports neither pair scoring nor query"
        raise RuntimeError(msg)

    missing_score = -1.0 if method.supports_query and not method.supports_pair_score else 0.0
    pair_scores = pair_scores_from_score_map(docs, scores, groups, missing_score=missing_score)
    sweep = threshold_sweep(pair_scores)
    best = best_f1(sweep)
    group_recall = per_group_recall(pair_scores)
    closed_rank = recall_at_k(docs, rankings, groups, ks=ks)
    return {
        "pairs": {
            "count": len(pair_scores),
            "positives": sum(1 for pair in pair_scores if pair.is_positive),
            "pr_auc": pr_auc(sweep),
            "best_f1": asdict(best),
            "per_group_recall_mean": sum(group_recall.values()) / len(group_recall) if group_recall else 0.0,
            "per_group_recall": group_recall,
        },
        "closed_rank": closed_rank,
        "timings": timings,
    }


def _open_eval(
    method: DedupMethod[Any],
    docs: list[TextDoc],
    *,
    groups: dict[str, set[str]],
    ks: list[int],
) -> dict[str, Any]:
    if not method.supports_query:
        return {"skipped": True, "reason": "method does not implement query()"}
    rankings, query_ms = _query_all(method, docs, top_k=max(ks), scope=QueryScope.open_set())
    return {
        "skipped": False,
        "recall": recall_at_k(docs, rankings, groups, ks=ks),
        "timings": {"open_query_ms": query_ms},
    }


def run_one(
    *,
    method_name: str,
    options: dict[str, Any],
    corpus_config: CorpusConfig,
    positive_field: str,
    ks: list[int],
    output_dir: Path,
    limit_groups: int = 0,
    skip_open: bool = False,
) -> dict[str, Any]:
    load_builtin_methods()
    method = create_method(method_name, options)
    corpus_config = replace(
        corpus_config,
        load_source_text=method.requires_source_text,
        mw_id_subset=mw_id_subset_for_groups(
            corpus_config,
            positive_field=positive_field,
            limit_groups=limit_groups,
        ),
    )

    docs, _rows = load_corpus(corpus_config)
    groups = groups_by_field(docs, positive_field)
    if not docs:
        msg = "No benchmark docs found"
        raise RuntimeError(msg)
    if not groups:
        msg = f"No positive groups for field {positive_field!r}"
        raise RuntimeError(msg)
    if limit_groups > 0:
        logger.info("Limited benchmark to %d positive groups / %d docs", len(groups), len(docs))

    logger.info(
        "Running %s on %d docs / %d positive groups (%s)",
        method.name,
        len(docs),
        len(groups),
        positive_field,
    )
    closed = _closed_eval(method, docs, groups=groups, ks=ks)
    open_set = (
        {"skipped": True, "reason": "--skip-open"}
        if skip_open
        else _open_eval(method, docs, groups=groups, ks=ks)
    )

    result = {
        "method": method.name,
        "options": options,
        "positive_field": positive_field,
        "corpus": {
            "csv_path": str(corpus_config.csv_path),
            "filter_in_index": corpus_config.filter_in_index,
            "allowlist_path": str(corpus_config.allowlist_path) if corpus_config.allowlist_path else None,
            "denylist_path": str(corpus_config.denylist_path) if corpus_config.denylist_path else None,
            "load_source_text": corpus_config.load_source_text,
            "doc_count": len(docs),
            "positive_group_count": len(groups),
            "limit_groups": limit_groups,
        },
        "closed": closed,
        "open": open_set,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{method.name}_{positive_field}_{options_hash(options)}.json"
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(result, fh, ensure_ascii=False, indent=2)
    logger.info("Wrote %s", out_path)
    return result


def default_method_matrix() -> Iterable[tuple[str, dict[str, Any]]]:
    yield "minhash_os_query", {"msm_pct": 0.10}
    yield "minhash_datasketch", {"shingle_size": 1, "num_perm": 128, "bands": 20}
    yield "minhash_datasketch", {"shingle_size": 3, "num_perm": 128, "bands": 20}
    yield "minhash_os_jaccard", {"analyzer": "tibetan-lenient", "shingle_size": 1, "bucket_count": 512}
    yield "minhash_os_jaccard", {"analyzer": "tibetan-lenient", "shingle_size": 3, "bucket_count": 512}
    yield "chunked_minhash", {"n_chunks": 10, "shingle_size": 1, "bucket_count": 256, "chunk_threshold": 0.6}
    yield "chunked_minhash", {"n_chunks": 20, "shingle_size": 1, "bucket_count": 256, "chunk_threshold": 0.6}


def _load_existing_results(output_dir: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for path in sorted(output_dir.glob("*.json")):
        with path.open(encoding="utf-8") as fh:
            result = json.load(fh)
        if isinstance(result, dict) and "closed" in result:
            result["_path"] = str(path)
            results.append(result)
    return results


def _result_sort_key(result: dict[str, Any]) -> tuple[int, float, str]:
    doc_count = int(result.get("corpus", {}).get("doc_count", 0))
    f1 = float(result.get("closed", {}).get("pairs", {}).get("best_f1", {}).get("f1", 0.0))
    return (-doc_count, -f1, str(result.get("method", "")))


def _write_markdown_report(results: list[dict[str, Any]], output_path: Path) -> None:
    lines = [
        "# Dedup Method Benchmark Results",
        "",
        "| Method | Options | Docs | Groups | Best score threshold | F1 | Precision | Recall | "
        "PR-AUC | Closed R@20 | Open R@50 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for result in sorted(results, key=_result_sort_key):
        options = json.dumps(result["options"], ensure_ascii=False, sort_keys=True)
        closed_pairs = result["closed"]["pairs"]
        best = closed_pairs["best_f1"]
        closed_rank = result["closed"].get("closed_rank", {})
        open_recall = result["open"].get("recall", {}) if not result["open"].get("skipped") else {}
        row_template = (
            "| {method} | `{options}` | {docs} | {groups} | {threshold:.3f} | {f1:.3f} | "
            "{precision:.3f} | {recall:.3f} | {pr_auc:.3f} | {closed_r20:.3f} | {open_r50:.3f} |"
        )
        lines.append(
            row_template.format(
                method=result["method"],
                options=options,
                docs=result["corpus"]["doc_count"],
                groups=result["corpus"]["positive_group_count"],
                threshold=best["threshold"],
                f1=best["f1"],
                precision=best["precision"],
                recall=best["recall"],
                pr_auc=closed_pairs["pr_auc"],
                closed_r20=closed_rank.get("recall@20", 0.0),
                open_r50=open_recall.get("recall@50", 0.0),
            )
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Wrote %s", output_path)


def main() -> None:
    load_builtin_methods()
    parser = argparse.ArgumentParser(description="Benchmark registered deduplication methods")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--allowlist", type=Path)
    parser.add_argument("--denylist", type=Path)
    parser.add_argument("--positive-field", choices=["d_id", "rkts_id", "wa_id_orig"], default="d_id")
    parser.add_argument("--method", choices=available_methods())
    parser.add_argument("--options", nargs="*", default=[])
    parser.add_argument("--all", action="store_true", help="Run the default comparison matrix")
    parser.add_argument("--ks", type=int, nargs="+", default=DEFAULT_KS)
    parser.add_argument("--limit-groups", type=int, default=0)
    parser.add_argument("--skip-open", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report", type=Path, default=Path("doc/dedup_methods_results.md"))
    parser.add_argument(
        "--summarize-existing",
        action="store_true",
        help="Write --report from JSON files in --output-dir without running benchmarks",
    )
    args = parser.parse_args()

    if args.summarize_existing:
        results = _load_existing_results(args.output_dir)
        if not results:
            parser.error(f"No benchmark result JSON files found in {args.output_dir}")
        _write_markdown_report(results, args.report)
        return

    corpus_config = CorpusConfig(
        csv_path=args.csv,
        filter_in_index=True,
        allowlist_path=args.allowlist,
        denylist_path=args.denylist,
    )

    matrix = list(default_method_matrix()) if args.all else []
    if args.method:
        matrix.append((args.method, parse_options(args.options)))
    if not matrix:
        parser.error("Provide --method or --all")

    results = [
        run_one(
            method_name=method_name,
            options=options,
            corpus_config=corpus_config,
            positive_field=args.positive_field,
            ks=args.ks,
            output_dir=args.output_dir,
            limit_groups=args.limit_groups,
            skip_open=args.skip_open,
        )
        for method_name, options in matrix
    ]

    if args.all:
        _write_markdown_report(results, args.report)


if __name__ == "__main__":
    main()

