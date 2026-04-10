"""
Sync text segments from the main bec index into the bec_texts shadow index.

Scrolls all segmented volume_etext documents from OpenSearch, extracts
the text content for each segment (segment_type == "text" only), and
bulk-indexes them into the bec_texts index with client-side MinHash LSH
band signatures (via datasketch) for deduplication.

Usage:
    python -m scripts.sync_texts [--dry-run] [--limit N] [--volume-id VOL_ID]

Requires the OpenSearch environment variables from .env.
"""

import argparse
import contextlib
import hashlib
import json
import logging
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from datasketch import MinHash
from opensearchpy.exceptions import NotFoundError as OSNotFoundError

from api.config import index_name, opensearch_client
from api.models import DocumentType, SegmentType
from api.services.matching import _extract_text_from_chunks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

TEXT_INDEX_NAME = "bec_texts"
MAPPINGS_PATH = Path(__file__).resolve().parent.parent / "doc" / "mappings_bec_texts.json"
SCROLL_SIZE = 50
SCROLL_TIMEOUT = "5m"
BULK_BATCH_SIZE = 200
MIN_TEXT_LENGTH = 50
NUM_PERM = 128
LSH_BANDS = 20
LSH_ROWS = NUM_PERM // LSH_BANDS  # 6 rows per band -> ~60% Jaccard threshold
SHINGLE_SIZE = 3
TSHEG_PATTERN = re.compile(r"[་།]+")


def _tibetan_shingles(text: str, shingle_size: int = SHINGLE_SIZE) -> set[str]:
    """Split Tibetan text into overlapping syllable n-grams (shingles)."""
    syllables = [s for s in TSHEG_PATTERN.split(text) if s.strip()]
    if len(syllables) < shingle_size:
        return {"_".join(syllables)} if syllables else set()
    return {"_".join(syllables[i : i + shingle_size]) for i in range(len(syllables) - shingle_size + 1)}


def compute_lsh_bands(text: str) -> list[str]:
    """Compute MinHash LSH band signatures for a Tibetan text.

    Returns a list of band signature strings like 'b0_<hex>', 'b1_<hex>', etc.
    Documents sharing any band value are dedup candidates.
    """
    shingles = _tibetan_shingles(text)
    if not shingles:
        return []

    mh = MinHash(num_perm=NUM_PERM)
    for s in shingles:
        mh.update(s.encode("utf-8"))

    hashvalues = mh.hashvalues
    bands: list[str] = []
    for band_idx in range(LSH_BANDS):
        start = band_idx * LSH_ROWS
        band_slice = hashvalues[start : start + LSH_ROWS]
        band_hash = hashlib.md5(band_slice.tobytes()).hexdigest()[:16]  # noqa: S324
        bands.append(f"b{band_idx}_{band_hash}")
    return bands


def ensure_index_exists() -> None:
    """Create the bec_texts index if it doesn't already exist."""
    if opensearch_client.indices.exists(index=TEXT_INDEX_NAME):
        logger.info("Index %s already exists", TEXT_INDEX_NAME)
        return

    with MAPPINGS_PATH.open() as f:
        mappings_body = json.load(f)

    opensearch_client.indices.create(index=TEXT_INDEX_NAME, body=mappings_body)
    logger.info("Created index %s", TEXT_INDEX_NAME)


def scroll_segmented_volumes(
    volume_id: str | None = None,
    limit: int = 0,
) -> list[dict[str, Any]]:
    """Scroll volume_etext documents that have segments.

    Args:
        volume_id: If provided, only fetch this specific volume.

    Returns:
        List of volume docs with id, vol_id, wa_id, mw_id, etext_source,
        segments, and chunks.
    """
    if volume_id is not None:
        try:
            response = opensearch_client.get(index=index_name, id=volume_id)
        except OSNotFoundError:
            logger.warning("Volume %s not found", volume_id)
            return []
        source = response["_source"]
        if not source.get("segments"):
            logger.warning("Volume %s has no segments — skipping", volume_id)
            return []
        return [{"id": response["_id"], **source}]

    filters: list[dict[str, Any]] = [
        {"term": {"type": DocumentType.VOLUME_ETEXT.value}},
        {"nested": {"path": "segments", "query": {"exists": {"field": "segments.cstart"}}}},
    ]
    query_body: dict[str, Any] = {"query": {"bool": {"filter": filters}}}

    volumes: list[dict[str, Any]] = []

    response = opensearch_client.search(
        index=index_name,
        body=query_body,
        size=SCROLL_SIZE,
        scroll=SCROLL_TIMEOUT,
        _source_includes=["vol_id", "vol_version", "rep_id", "wa_id", "mw_id", "etext_source", "segments", "chunks"],
    )

    scroll_id = response.get("_scroll_id")
    hits = response["hits"]["hits"]

    while hits:
        volumes.extend({"id": hit["_id"], **hit["_source"]} for hit in hits)

        if limit > 0 and len(volumes) >= limit:
            volumes = volumes[:limit]
            break

        response = opensearch_client.scroll(scroll_id=scroll_id, scroll=SCROLL_TIMEOUT)
        scroll_id = response.get("_scroll_id")
        hits = response["hits"]["hits"]

    if scroll_id:
        with contextlib.suppress(Exception):
            opensearch_client.clear_scroll(scroll_id=scroll_id)

    return volumes


def _get_existing_text_docs(doc_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Fetch existing documents from bec_texts to preserve wa_id_orig."""
    if not doc_ids:
        return {}
    response = opensearch_client.mget(
        body={"ids": doc_ids},
        index=TEXT_INDEX_NAME,
    )
    return {doc["_id"]: doc["_source"] for doc in response["docs"] if doc.get("found")}


def build_text_docs(volume: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Extract text documents from a volume's segments.

    Returns:
        List of (doc_id, body) tuples for each text segment.
    """
    volume_id = volume["id"]
    vol_id = volume.get("vol_id", "")
    vol_version = volume.get("vol_version", "")
    volume_wa_id = volume.get("wa_id")
    volume_mw_id = volume.get("mw_id")
    etext_source = volume.get("etext_source", "")
    segments = volume.get("segments", [])
    chunks = volume.get("chunks", [])

    results: list[tuple[str, dict[str, Any]]] = []

    for idx, seg in enumerate(segments):
        segment_type = seg.get("segment_type", SegmentType.TEXT.value)
        if segment_type != SegmentType.TEXT.value:
            continue

        cstart = seg.get("cstart", 0)
        cend = seg.get("cend", 0)
        if cend <= cstart:
            continue

        text = _extract_text_from_chunks(chunks, cstart, cend)
        if len(text.strip()) < MIN_TEXT_LENGTH:
            continue

        seg_wa_id = seg.get("wa_id")
        seg_mw_id = seg.get("mw_id") or volume_mw_id+"_S"+str(idx)

        title_bo = seg.get("title_bo")
        if isinstance(title_bo, list):
            title_bo = title_bo[0] if title_bo else None

        doc_id = seg_mw_id
        body: dict[str, Any] = {
            "volume_id": volume_id,
            "vol_id": vol_id,
            "vol_version": vol_version,
            "root_mw_id": volume_mw_id,
            "rep_id": volume.get("rep_id"),
            "mw_id": seg_mw_id,
            "etext_source": etext_source,
            "segment_idx": idx,
            "title_bo": title_bo,
            "wa_id_orig": seg_wa_id,
            "cstart": cstart,
            "cend": cend,
            "text_bo": text,
            "minhash_lsh": compute_lsh_bands(text),
            "text_length": len(text),
            "synced_at": datetime.now(UTC).isoformat(),
        }

        results.append((doc_id, body))

    return results


def bulk_upsert(
    docs: list[tuple[str, dict[str, Any]]],
    existing_map: dict[str, dict[str, Any]],
) -> int:
    """Bulk upsert text documents into bec_texts.

    Preserves wa_id_orig for documents that already exist.

    Returns:
        Number of documents indexed.
    """
    if not docs:
        return 0

    bulk_body: list[dict[str, Any]] = []
    for doc_id, body in docs:
        if doc_id in existing_map:
            existing_wa_id_orig = existing_map[doc_id].get("wa_id_orig")
            if existing_wa_id_orig is not None:
                body["wa_id_orig"] = existing_wa_id_orig

        bulk_body.append({"index": {"_index": TEXT_INDEX_NAME, "_id": doc_id}})
        bulk_body.append(body)

    response = opensearch_client.bulk(body=bulk_body, refresh=False)

    if response.get("errors"):
        error_count = sum(1 for item in response["items"] if item.get("index", {}).get("error"))
        logger.warning("Bulk upsert had %d errors", error_count)
        for item in response["items"]:
            error = item.get("index", {}).get("error")
            if error:
                logger.warning("  %s: %s", item["index"]["_id"], error.get("reason", error))

    return len(docs)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync text segments to bec_texts shadow index")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count volumes and segments without indexing",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit the number of volumes to process (0 = no limit)",
    )
    parser.add_argument(
        "--volume-id",
        default=None,
        help="Sync only this specific volume ID",
    )
    args = parser.parse_args()

    logger.info("Starting text sync to %s", TEXT_INDEX_NAME)

    if not args.dry_run:
        ensure_index_exists()

    logger.info("Scrolling segmented volumes...")
    t0 = time.monotonic()
    volumes = scroll_segmented_volumes(volume_id=args.volume_id, limit=args.limit)
    scroll_elapsed = time.monotonic() - t0
    logger.info("Found %d segmented volumes in %.1fs", len(volumes), scroll_elapsed)

    total_texts = 0
    total_skipped_editorial = 0
    total_skipped_short = 0
    volumes_processed = 0

    t_start = time.monotonic()
    pending_docs: list[tuple[str, dict[str, Any]]] = []

    for i, volume in enumerate(volumes):
        segments = volume.get("segments", [])
        text_docs = build_text_docs(volume)
        editorial_count = sum(
            1 for seg in segments if seg.get("segment_type", SegmentType.TEXT.value) != SegmentType.TEXT.value
        )
        short_count = len(segments) - editorial_count - len(text_docs)

        total_skipped_editorial += editorial_count
        total_skipped_short += short_count

        if args.dry_run:
            total_texts += len(text_docs)
            volumes_processed += 1
            if (i + 1) % 100 == 0:
                logger.info("[dry-run] [%d/%d] %d texts so far", i + 1, len(volumes), total_texts)
            continue

        pending_docs.extend(text_docs)

        if len(pending_docs) >= BULK_BATCH_SIZE or i == len(volumes) - 1:
            pending_ids = [doc_id for doc_id, _ in pending_docs]
            existing_map = _get_existing_text_docs(pending_ids)
            indexed = bulk_upsert(pending_docs, existing_map)
            total_texts += indexed
            pending_docs = []

        volumes_processed += 1

        if (i + 1) % 50 == 0:
            elapsed = time.monotonic() - t_start
            rate = volumes_processed / elapsed if elapsed > 0 else 0
            logger.info(
                "[%d/%d] %d texts indexed (%.1f vol/s)",
                i + 1,
                len(volumes),
                total_texts,
                rate,
            )

    if not args.dry_run:
        opensearch_client.indices.refresh(index=TEXT_INDEX_NAME)

    elapsed_total = time.monotonic() - t_start
    logger.info("=" * 60)
    logger.info(
        "Sync complete: %d volumes processed, %d texts %s, %d editorial skipped, %d short skipped in %.1fs",
        volumes_processed,
        total_texts,
        "found" if args.dry_run else "indexed",
        total_skipped_editorial,
        total_skipped_short,
        elapsed_total,
    )


if __name__ == "__main__":
    main()
