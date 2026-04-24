"""
Mark all volumes in a batch CSV with a given batch_id.

Reads a CSV file (w_id, i_id, i_version, etext_source) — same format as
import_batch.py — and sets batch_id on every matching OpenSearch document.

For each volume the existing batch_id is checked first (via mget):
  - **document not in the index**   → skipped (logged; use import when the volume exists)
  - already set to the target value → skipped (no write needed)
  - set to a different value        → warned and skipped, unless --force
  - absent / null (doc exists)      → updated

Usage:
    python -m scripts.mark_batch path/to/batch.csv BATCH_ID [--dry-run] [--force]
"""

import argparse
import csv
import logging
from pathlib import Path

from api.config import index_name, opensearch_client
from api.services.volumes import _volume_doc_id

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_csv(path: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with Path(path).open(newline="") as fh:
        reader = csv.reader(fh)
        for row in reader:
            if not row or not row[0].strip():
                continue
            rows.append(
                {
                    "w_id": row[0].strip(),
                    "i_id": row[1].strip(),
                    "i_version": row[2].strip(),
                    "etext_source": row[3].strip(),
                }
            )
    return rows


def fetch_batch_id_state(
    doc_ids: list[str],
) -> tuple[dict[str, str | None], set[str]]:
    """
    Return (batch_id_by_doc_id, missing_doc_ids).

    For each **found** document, values are the current ``batch_id`` (``None`` if the field
    is absent or null). **Missing** documents (not in the index) are only listed in
    ``missing_doc_ids`` so we do not attempt an ``update`` on them.
    """
    if not doc_ids:
        return {}, set()
    response = opensearch_client.mget(
        body={"ids": doc_ids},
        index=index_name,
        _source_includes=["batch_id"],
    )
    batch_id_by_id: dict[str, str | None] = {}
    missing: set[str] = set()
    for doc in response["docs"]:
        _id = doc["_id"]
        if doc.get("found"):
            batch_id_by_id[_id] = doc.get("_source", {}).get("batch_id")
        else:
            missing.add(_id)
    return batch_id_by_id, missing


def bulk_update(doc_ids: list[str], batch_id: str) -> tuple[int, int]:
    """Send a single bulk request updating all doc_ids. Returns (updated, failed)."""
    body: list[dict] = []
    for doc_id in doc_ids:
        body.append({"update": {"_index": index_name, "_id": doc_id}})
        body.append({"doc": {"batch_id": batch_id}})

    response = opensearch_client.bulk(body=body)

    updated = 0
    failed = 0
    for item in response["items"]:
        result = item.get("update", {})
        if result.get("error"):
            logger.warning("Failed to update %s: %s", result["_id"], result["error"])
            failed += 1
        else:
            updated += 1
    return updated, failed


def main() -> None:
    parser = argparse.ArgumentParser(description="Tag volumes in a batch CSV with a batch_id")
    parser.add_argument("csv", help="Path to the batch CSV file (w_id,i_id,i_version,etext_source)")
    parser.add_argument("batch_id", help="Value to write into the batch_id field")
    parser.add_argument("--dry-run", action="store_true", help="Print what would happen without writing")
    parser.add_argument("--force", action="store_true", help="Overwrite documents that already have a different batch_id")
    parser.add_argument("--chunk-size", type=int, default=30, help="Number of documents per bulk request (default: 30)")
    args = parser.parse_args()

    rows = load_csv(args.csv)
    logger.info("Loaded %d rows from %s", len(rows), args.csv)

    all_doc_ids = [_volume_doc_id(r["w_id"], r["i_id"], r["i_version"], r["etext_source"]) for r in rows]

    # Fetch current batch_ids in chunks to avoid huge mget requests
    logger.info("Fetching existing batch_ids from OpenSearch…")
    batch_id_by_id: dict[str, str | None] = {}
    not_in_index: set[str] = set()
    for i in range(0, len(all_doc_ids), args.chunk_size):
        bmap, missing = fetch_batch_id_state(all_doc_ids[i : i + args.chunk_size])
        batch_id_by_id.update(bmap)
        not_in_index |= missing

    # Classify each document
    to_update: list[str] = []
    already_ok = 0
    conflict_skipped = 0

    for doc_id in all_doc_ids:
        if doc_id in not_in_index:
            continue

        current = batch_id_by_id[doc_id]
        if current == args.batch_id:
            logger.debug("Skipping %s (already batch_id=%r)", doc_id, args.batch_id)
            already_ok += 1
        elif current is not None:
            if args.force:
                logger.warning("Overwriting %s (batch_id %r → %r)", doc_id, current, args.batch_id)
                to_update.append(doc_id)
            else:
                logger.warning("Skipping %s (already has batch_id=%r, use --force to overwrite)", doc_id, current)
                conflict_skipped += 1
        else:
            to_update.append(doc_id)

    if not_in_index:
        n_missing = len(not_in_index)
        if n_missing <= 20:
            logger.info(
                "Not in index (%d skipped — import these volumes first): %s",
                n_missing,
                ", ".join(sorted(not_in_index)),
            )
        else:
            sample = list(sorted(not_in_index))[:20]
            logger.info(
                "Not in index (%d skipped — import first); e.g. %s, …",
                n_missing,
                ", ".join(sample),
            )

    logger.info(
        "Plan: %d to update, %d already correct, %d conflict-skipped, %d not in index",
        len(to_update), already_ok, conflict_skipped, len(not_in_index),
    )

    if args.dry_run:
        for doc_id in sorted(not_in_index):
            print(f"  skip (not in index) {doc_id}")  # noqa: T201
        for doc_id in to_update:
            print(f"  would update {doc_id} → batch_id={args.batch_id!r}")  # noqa: T201
        logger.info("Dry run complete.")
        return

    if not to_update:
        if not_in_index and not (already_ok or conflict_skipped):
            logger.info("No documents in the index to tag; run import for those rows first.")
        else:
            logger.info("Nothing to update.")
        return

    total_updated = 0
    total_failed = 0
    chunks = [to_update[i : i + args.chunk_size] for i in range(0, len(to_update), args.chunk_size)]

    for i, chunk in enumerate(chunks):
        logger.info("Bulk update chunk %d/%d (%d docs)…", i + 1, len(chunks), len(chunk))
        updated, failed = bulk_update(chunk, args.batch_id)
        total_updated += updated
        total_failed += failed

    logger.info("=" * 60)
    logger.info(
        "Done: %d updated, %d already correct, %d conflict-skipped, %d not in index, %d failed",
        total_updated, already_ok, conflict_skipped, len(not_in_index), total_failed,
    )


if __name__ == "__main__":
    main()
