"""
Import NLM (National Library of Mongolia) volumes and their outline segments.

Phase 1: Download the CSV of (mw, w, i) tuples from eroux.fr, discover the
         google_vision vol_version from S3, and import OCR parquet files.
Phase 2: Build SPARQL lookup maps, parse outline .trig files, extract segments
         with image-coordinate-to-character-offset mapping, and store them on
         the volume documents.

Usage:
    # Full import (both phases)
    python -m scripts.import_nlm

    # Phase 1 only (OCR import)
    python -m scripts.import_nlm --phase ocr

    # Phase 2 only (segments from outlines, assumes OCR already imported)
    python -m scripts.import_nlm --phase segments

    # Dry run
    python -m scripts.import_nlm --dry-run

    # Resume from a specific row
    python -m scripts.import_nlm --start-from 100
"""

import argparse
import concurrent.futures
import csv
import gzip
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import boto3
import botocore.config
import botocore.exceptions
import pyewts
from rdflib import ConjunctiveGraph, Literal, Namespace, URIRef
from rdflib.term import Node

import requests
from api.config import Config, index_name, opensearch_client
from api.models import SegmentType
from api.services.ocr_import import import_ocr_from_s3
from api.services.os_client import get_document, refresh_index, update_document
from api.services.volumes import _volume_doc_id

# ruff: noqa: S603, S607

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _filter_os_404(record: logging.LogRecord) -> bool:
    """Suppress expected 404 warnings from opensearch client."""
    return "status:404" not in record.getMessage()


logging.getLogger("opensearch").addFilter(_filter_os_404)

# ── Constants ──────────────────────────────────────────────────────────────

NLM_CSV_URL = "https://eroux.fr/mw_w_i_nlm.csv.gz"
ETEXT_SOURCE = "google_vision"
BDRC_GITLAB_BASE = "https://gitlab.com/bdrc-data"
OUTLINES_REPO_NAME = "outlines-20220922"
DEFAULT_DATA_DIR = os.getenv("BDRC_DATA_DIR", "./bdrc_data")

EWTS_CONVERTER = pyewts.pyewts()

# RDF namespaces
BDO = Namespace("http://purl.bdrc.io/ontology/core/")
BDR = Namespace("http://purl.bdrc.io/resource/")
BDA = Namespace("http://purl.bdrc.io/admindata/")
BDG = Namespace("http://purl.bdrc.io/graph/")
ADM = Namespace("http://purl.bdrc.io/ontology/admin/")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")

PART_TYPE_TEXT = BDR.PartTypeText
PART_TYPE_EDITORIAL = BDR.PartTypeEditorial
PART_TYPE_TOC = BDR.PartTypeTableOfContent
RELEVANT_PART_TYPES = {PART_TYPE_TEXT, PART_TYPE_EDITORIAL, PART_TYPE_TOC}


# ── CSV download ───────────────────────────────────────────────────────────


def download_nlm_csv() -> list[dict[str, str]]:
    """Download and decompress the NLM CSV from eroux.fr.

    Returns list of dicts with keys: mw_id, w_id, i_id.
    """
    logger.info("Downloading NLM CSV from %s", NLM_CSV_URL)
    response = requests.get(NLM_CSV_URL, timeout=60)
    response.raise_for_status()

    decompressed = gzip.decompress(response.content).decode("utf-8")
    rows: list[dict[str, str]] = []
    for cols in csv.reader(decompressed.splitlines()):
        if len(cols) < 3 or not cols[0].strip():
            continue
        if cols[0].strip().lower() in ("mw", "mw_id"):
            continue
        rows.append({"mw_id": cols[0].strip(), "w_id": cols[1].strip(), "i_id": cols[2].strip()})

    logger.info("Loaded %d rows from NLM CSV", len(rows))
    return rows


# ── S3 version discovery ──────────────────────────────────────────────────


_s3_config = botocore.config.Config(
    max_pool_connections=20,
    retries={"max_attempts": 5, "mode": "adaptive"},
)
_s3_client = boto3.client("s3", config=_s3_config)


def discover_gv_version(w_id: str, i_id: str) -> str | None:
    """List the S3 prefix gv/{w_id}/{i_id}/ to find the vol_version hash."""
    prefix = f"gv/{w_id}/{i_id}/"
    try:
        response = _s3_client.list_objects_v2(
            Bucket=Config.S3_OCR_BUCKET,
            Prefix=prefix,
            Delimiter="/",
        )
        common_prefixes = response.get("CommonPrefixes", [])
        if not common_prefixes:
            return None
        # Take the first (and typically only) version directory
        version_prefix = common_prefixes[0]["Prefix"]
        # prefix is "gv/W.../I.../HASH/" — extract HASH
        parts = version_prefix.rstrip("/").split("/")
        return parts[-1]
    except (botocore.exceptions.ClientError, botocore.exceptions.ConnectionClosedError):
        logger.exception("S3 error listing prefix %s", prefix)
        return None


# ── Phase 1: OCR import ───────────────────────────────────────────────────


_GV_CACHE_FILE = Path(DEFAULT_DATA_DIR) / "gv_versions_cache.json"


def _load_gv_cache() -> dict[str, str]:
    """Load cached GV versions from disk. Keys are 'w_id|i_id'."""
    if _GV_CACHE_FILE.exists():
        with _GV_CACHE_FILE.open(encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_gv_cache(flat: dict[str, str]) -> None:
    """Save GV versions cache to disk."""
    _GV_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with _GV_CACHE_FILE.open("w", encoding="utf-8") as f:
        json.dump(flat, f)


def discover_all_gv_versions(
    rows: list[dict[str, str]],
    max_workers: int = 16,
) -> dict[tuple[str, str], str]:
    """Discover vol_version for all unique (w_id, i_id) pairs using parallel S3 listing.

    Results are cached to disk so subsequent runs are instant.
    Returns a dict mapping (w_id, i_id) -> vol_version.
    """
    unique_keys = list({(r["w_id"], r["i_id"]) for r in rows})

    cache = _load_gv_cache()
    versions: dict[tuple[str, str], str] = {}
    to_discover: list[tuple[str, str]] = []

    for key in unique_keys:
        cached = cache.get(f"{key[0]}|{key[1]}")
        if cached is not None:
            versions[key] = cached
        else:
            to_discover.append(key)

    if not to_discover:
        logger.info("GV version discovery: all %d versions loaded from cache", len(versions))
        return versions

    logger.info(
        "Discovering google_vision versions for %d volumes (%d cached, %d workers)...",
        len(to_discover),
        len(versions),
        max_workers,
    )

    no_gv = 0
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {executor.submit(discover_gv_version, k[0], k[1]): k for k in to_discover}
        for future in concurrent.futures.as_completed(future_to_key):
            key = future_to_key[future]
            vol_version = future.result()
            if vol_version is None:
                no_gv += 1
            else:
                versions[key] = vol_version
                cache[f"{key[0]}|{key[1]}"] = vol_version
            done += 1
            if done % 500 == 0:
                logger.info("  ... discovered %d/%d (found %d, no GV %d)", done, len(to_discover), len(versions), no_gv)

    logger.info("Discovery complete: %d found, %d no GV data — saving cache", len(versions), no_gv)
    _save_gv_cache(cache)
    return versions


def phase1_import_ocr(
    rows: list[dict[str, str]],
    versions: dict[tuple[str, str], str],
    *,
    dry_run: bool = False,
    force: bool = False,
    start_from: int = 0,
) -> None:
    """Import google_vision OCR volumes from S3 for each NLM row."""
    succeeded = 0
    skipped = 0
    failed = 0
    no_gv = 0
    failed_rows: list[tuple[int, dict[str, str], str]] = []

    total = len(rows)
    for i, row in enumerate(rows):
        if i < start_from:
            continue

        w_id = row["w_id"]
        i_id = row["i_id"]

        vol_version = versions.get((w_id, i_id))
        if vol_version is None:
            no_gv += 1
            continue

        if dry_run:
            logger.info(
                "[%d/%d] [dry-run] Would import %s / %s / %s / %s",
                i + 1,
                total,
                w_id,
                i_id,
                vol_version,
                ETEXT_SOURCE,
            )
            continue

        doc_id = _volume_doc_id(w_id, i_id, vol_version, ETEXT_SOURCE)
        if not force and opensearch_client.exists(index=index_name, id=doc_id):
            logger.info("[%d/%d] Skipping %s (already indexed)", i + 1, total, doc_id)
            skipped += 1
            continue

        logger.info("[%d/%d] Importing %s / %s / %s / %s", i + 1, total, w_id, i_id, vol_version, ETEXT_SOURCE)

        try:
            t0 = time.monotonic()
            result_doc_id = import_ocr_from_s3(w_id, i_id, vol_version, ETEXT_SOURCE)
            elapsed = time.monotonic() - t0
            logger.info("  ✓ Indexed as %s  (%.1fs)", result_doc_id, elapsed)
            succeeded += 1
        except Exception:
            logger.exception("  ✗ Failed to import row %d (%s / %s)", i, w_id, i_id)
            failed += 1
            failed_rows.append((i, row, str(sys.exc_info()[1])))

    logger.info("=" * 60)
    logger.info(
        "Phase 1 complete: %d succeeded, %d skipped, %d no GV data, %d failed out of %d",
        succeeded,
        skipped,
        no_gv,
        failed,
        total,
    )
    if failed_rows:
        logger.warning("Failed rows:")
        for idx, row, err in failed_rows:
            logger.warning("  [%d] %s / %s — %s", idx, row["w_id"], row["i_id"], err)


# ── Lookup maps from local git data ───────────────────────────────────────

_RE_OUTLINE_OF = re.compile(r":outlineOf\s+bdr:(\S+)")
_RE_CL_INSTANCE = re.compile(r":contentLocationInstance\s+bdr:(\S+)")
_RE_STATUS_RELEASED = re.compile(r"adm:status\s+bda:StatusReleased")


def _scan_outlines_repo(
    outlines_repo_path: Path,
    target_mw_ids: set[str],
) -> tuple[dict[str, str], dict[str, list[str]]]:
    """Scan the local outlines repo to build mw_id_to_o_id and o_id_to_rep_ids.

    Only includes outlines that are StatusReleased and whose MW ID is in target_mw_ids.
    Uses grep to pre-filter files for speed.
    """
    mw_id_to_o_id: dict[str, str] = {}
    o_id_to_rep_ids: dict[str, list[str]] = {}

    # Pre-filter: only read files that contain :outlineOf (much faster than reading all)
    logger.info("  Pre-filtering outline files with grep...")
    result = subprocess.run(
        ["grep", "-rl", "--include=*.trig", ":outlineOf", str(outlines_repo_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    trig_files = [Path(p) for p in result.stdout.strip().splitlines() if p]
    logger.info("  Found %d files containing :outlineOf — scanning...", len(trig_files))

    for i, trig_file in enumerate(trig_files):
        o_id = trig_file.stem
        text = trig_file.read_text(encoding="utf-8")

        # Check if released
        if not _RE_STATUS_RELEASED.search(text):
            continue

        # Extract outlineOf MW ID
        match = _RE_OUTLINE_OF.search(text)
        if not match:
            continue
        mw_id = match.group(1).rstrip(" ;.")
        if mw_id not in target_mw_ids:
            continue

        mw_id_to_o_id[mw_id] = o_id

        # Extract contentLocationInstance rep IDs
        rep_ids: set[str] = set()
        for cl_match in _RE_CL_INSTANCE.finditer(text):
            rep_ids.add(cl_match.group(1).rstrip(" ;."))
        if rep_ids:
            o_id_to_rep_ids[o_id] = list(rep_ids)

        if (i + 1) % 2000 == 0:
            logger.info("    ... scanned %d/%d files (%d outlines matched)", i + 1, len(trig_files), len(mw_id_to_o_id))

        # Early exit once all target MW IDs have been found
        if len(mw_id_to_o_id) == len(target_mw_ids):
            logger.info(
                "    All %d target MW IDs matched — stopping early at file %d/%d",
                len(target_mw_ids),
                i + 1,
                len(trig_files),
            )
            break

    logger.info("  Scan complete: %d outlines matched", len(mw_id_to_o_id))
    return mw_id_to_o_id, o_id_to_rep_ids


def build_lookup_maps(
    csv_rows: list[dict[str, str]],
    outlines_repo_path: Path,
) -> dict[str, Any]:
    """Build lookup maps needed for outline segment import.

    rep_id_to_mw_id is derived directly from the CSV.
    mw_id_to_o_id and o_id_to_rep_ids are scanned from the local outlines repo.
    """
    logger.info("Building lookup maps...")

    # 1. rep_id_to_mw_id: derived from CSV (w_id -> mw_id)
    rep_id_to_mw_id: dict[str, str] = {}
    for row in csv_rows:
        rep_id_to_mw_id[row["w_id"]] = row["mw_id"]
    logger.info("  rep_id_to_mw_id: %d entries (from CSV)", len(rep_id_to_mw_id))

    # 2 & 3. mw_id_to_o_id + o_id_to_rep_ids: from local outlines repo
    target_mw_ids = {row["mw_id"] for row in csv_rows}
    mw_id_to_o_id, o_id_to_rep_ids = _scan_outlines_repo(outlines_repo_path, target_mw_ids)
    logger.info("  mw_id_to_o_id: %d entries", len(mw_id_to_o_id))
    logger.info("  o_id_to_rep_ids: %d entries", len(o_id_to_rep_ids))

    logger.info("Lookup maps complete.")
    return {
        "rep_id_to_mw_id": rep_id_to_mw_id,
        "mw_id_to_o_id": mw_id_to_o_id,
        "o_id_to_rep_ids": o_id_to_rep_ids,
    }


# ── Outline repo management ───────────────────────────────────────────────


def clone_or_pull_outlines(data_dir: str = DEFAULT_DATA_DIR) -> Path:
    """Clone or pull the outlines-20220922 repo."""
    repo_url = f"{BDRC_GITLAB_BASE}/{OUTLINES_REPO_NAME}.git"
    repo_path = Path(data_dir) / OUTLINES_REPO_NAME

    if repo_path.exists():
        logger.info("Pulling latest for %s", OUTLINES_REPO_NAME)
        subprocess.run(
            ["git", "pull", "--ff-only"],
            cwd=str(repo_path),
            check=True,
            capture_output=True,
            text=True,
        )
    else:
        logger.info("Cloning %s from %s", OUTLINES_REPO_NAME, repo_url)
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["git", "clone", "--single-branch", repo_url, str(repo_path)],
            check=True,
            capture_output=True,
            text=True,
        )

    return repo_path


# ── Outline parsing ───────────────────────────────────────────────────────


def _rdf_first(graph: ConjunctiveGraph, subject: Node, predicate: URIRef) -> Node | None:
    """Return the first object for (subject, predicate, ?) or None."""
    return next(graph.objects(subject, predicate), None)


def _rdf_int(graph: ConjunctiveGraph, subject: Node, predicate: URIRef) -> int | None:
    """Return the first object as int, or None."""
    obj = next(graph.objects(subject, predicate), None)
    return int(str(obj)) if obj is not None else None


def _bdr_local_id(uri: Node) -> str | None:
    """Extract the local name from a BDR URI (e.g. bdr:WA123 -> 'WA123')."""
    if isinstance(uri, URIRef) and str(uri).startswith(str(BDR)):
        return str(uri)[len(str(BDR)) :]
    return None


def _ewts_to_unicode(ewts_text: str) -> str:
    """Convert EWTS transliteration to Tibetan Unicode."""
    return EWTS_CONVERTER.toUnicode(ewts_text)


def _extract_outline_label(graph: ConjunctiveGraph, subject: Node) -> str | None:
    """Extract a skos:prefLabel, preferring @bo over @bo-x-ewts (converted)."""
    labels: dict[str | None, str] = {
        obj.language: str(obj) for obj in graph.objects(subject, SKOS.prefLabel) if isinstance(obj, Literal)
    }
    if "bo" in labels:
        return labels["bo"]
    if "bo-x-ewts" in labels:
        return _ewts_to_unicode(labels["bo-x-ewts"])
    return None


def _derive_import_info(
    row: dict[str, str],
    lookups: dict[str, Any],
) -> dict[str, str | None] | None:
    """Derive outline import info for a volume row.

    Returns dict with outline_id, import_mode, cl_rep_id, or None if no outline.
    """
    w_id = row["w_id"]
    mw_id = row["mw_id"]

    mw_id_to_o_id = lookups["mw_id_to_o_id"]
    o_id_to_rep_ids = lookups["o_id_to_rep_ids"]

    o_id = mw_id_to_o_id.get(mw_id)
    if o_id is None:
        return None

    rep_ids_in_outline = o_id_to_rep_ids.get(o_id, [])
    if w_id in rep_ids_in_outline:
        return {
            "outline_id": o_id,
            "import_mode": "direct",
            "cl_rep_id": w_id,
        }
    return {
        "outline_id": o_id,
        "import_mode": "no_location",
        "cl_rep_id": rep_ids_in_outline[0] if rep_ids_in_outline else None,
    }


def _load_outline_graph(outline_id: str, outlines_repo_path: Path) -> ConjunctiveGraph | None:
    """Load an outline .trig file from the local outlines repo."""
    hash_prefix = hashlib.md5(outline_id.encode()).hexdigest()[:2]  # noqa: S324
    trig_path = outlines_repo_path / hash_prefix / f"{outline_id}.trig"

    if not trig_path.exists():
        logger.warning("Outline file not found: %s", trig_path)
        return None

    try:
        graph = ConjunctiveGraph()
        graph.parse(str(trig_path), format="trig")
    except Exception:
        logger.exception("Failed to parse outline trig: %s", trig_path)
        return None
    else:
        return graph


def _cl_matches_volume(graph: ConjunctiveGraph, cl: Node, volume_number: int) -> bool:
    """Return True if content location *cl* covers *volume_number*."""
    cl_vnum = _rdf_int(graph, cl, BDO.contentLocationVolume)
    cl_end_vnum = _rdf_int(graph, cl, BDO.contentLocationEndVolume)
    if cl_end_vnum is None:
        return cl_vnum == volume_number
    return cl_vnum is not None and cl_vnum <= volume_number <= cl_end_vnum


def _page_to_char(
    pnum: int | None,
    lookup: dict[int, int],
    fallback: int,
    label: str,
    pnum_range: tuple[int, int],
) -> int:
    """Map a page number to a character offset, logging if the page is missing."""
    if pnum is not None and pnum in lookup:
        return lookup[pnum]
    if pnum is not None:
        logger.warning("%s page %d not found in volume pages (range %d-%d)", label, pnum, *pnum_range)
    return fallback


def _extract_segments_from_outline(
    graph: ConjunctiveGraph,
    import_info: dict[str, str | None],
    volume_number: int,
    pages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Extract segments from an outline graph for a specific volume.

    Args:
        graph: The parsed outline ConjunctiveGraph
        import_info: Dict with outline_id, import_mode, cl_rep_id
        volume_number: The volume number to filter content locations for
        pages: The pages array from the volume doc (for mapping page nums to char offsets)

    Returns:
        List of segment dicts ready to store on the volume document.
    """
    cl_rep_id = import_info["cl_rep_id"]
    import_mode = import_info["import_mode"]

    if cl_rep_id is None:
        logger.warning("No cl_rep_id for outline %s — cannot extract segments", import_info["outline_id"])
        return []

    # Content locations for this reproduction, filtered to our volume
    all_cls = graph.subjects(BDO.contentLocationInstance, BDR[cl_rep_id])
    volume_cls = [cl for cl in all_cls if _cl_matches_volume(graph, cl, volume_number)]
    if not volume_cls:
        return []

    # Build page-number → char-offset maps
    pnum_to_cstart: dict[int, int] = {}
    pnum_to_cend: dict[int, int] = {}
    max_cend = 0
    for page in pages:
        pnum = page.get("pnum")
        if pnum is not None:
            pnum_to_cstart[pnum] = page["cstart"]
            pnum_to_cend[pnum] = page["cend"]
        max_cend = max(max_cend, page.get("cend", 0))

    pnum_range = (
        min(pnum_to_cstart, default=0),
        max(pnum_to_cstart, default=0),
    )

    # Extract raw segments from matching content locations
    raw_segments: list[dict[str, Any]] = []
    for cl in volume_cls:
        for mw_part in graph.subjects(BDO.contentLocation, cl):
            part_type_uri = next(
                (pt for pt in graph.objects(mw_part, BDO.partType) if pt in RELEVANT_PART_TYPES),
                None,
            )
            if part_type_uri is None:
                continue

            # Both editorial and TOC map to editorial
            segment_type = SegmentType.TEXT if part_type_uri == PART_TYPE_TEXT else SegmentType.EDITORIAL

            wa_obj = _rdf_first(graph, mw_part, BDO.instanceOf)

            pstart = None
            pend = None
            if import_mode == "direct":
                pstart = _rdf_int(graph, cl, BDO.contentLocationPage)
                pend = _rdf_int(graph, cl, BDO.contentLocationEndPage)
                # If content spans past this volume, treat as ending at end of volume
                cl_end_vnum = _rdf_int(graph, cl, BDO.contentLocationEndVolume)
                if cl_end_vnum is not None and cl_end_vnum > volume_number:
                    pend = None

            raw_segments.append(
                {
                    "mw_part_id": _bdr_local_id(mw_part),
                    "segment_type": segment_type,
                    "title_bo": _extract_outline_label(graph, mw_part),
                    "wa_id": _bdr_local_id(wa_obj) if wa_obj is not None else None,
                    "pstart": pstart,
                    "pend": pend,
                }
            )

    if not raw_segments:
        return []

    raw_segments.sort(key=lambda s: s["pstart"] if s["pstart"] is not None else 0)

    # Map page numbers to character offsets and build final segments
    segments: list[dict[str, Any]] = []
    for idx, seg in enumerate(raw_segments):
        if import_mode == "direct" and pnum_to_cstart:
            cstart = _page_to_char(seg["pstart"], pnum_to_cstart, 0, "Start", pnum_range)
            cend = _page_to_char(seg["pend"], pnum_to_cend, max_cend, "End", pnum_range)
        else:
            cstart = 0
            cend = max_cend

        # Log overlap warnings inline
        if idx < len(raw_segments) - 1:
            next_pstart = raw_segments[idx + 1]["pstart"]
            pend = seg["pend"]
            if pend is not None and next_pstart is not None:
                if pend > next_pstart:
                    logger.warning(
                        "  ⚠ Overlap: segment ending at page %d overlaps with next starting at page %d",
                        pend,
                        next_pstart,
                    )
                elif pend == next_pstart:
                    logger.warning("  ⚠ Shared page boundary at page %d — annotators may need to refine", pend)

        segment_dict: dict[str, Any] = {
            "cstart": cstart,
            "cend": cend,
            "segment_type": seg["segment_type"].value,
        }
        if seg["title_bo"]:
            segment_dict["title_bo"] = [seg["title_bo"]]
        if seg["mw_part_id"]:
            segment_dict["mw_id"] = seg["mw_part_id"]
        if seg["wa_id"]:
            segment_dict["wa_id"] = seg["wa_id"]

        segments.append(segment_dict)

    return segments


# ── Phase 2: Segment import from outlines ─────────────────────────────────


def phase2_import_segments(
    rows: list[dict[str, str]],
    versions: dict[tuple[str, str], str],
    *,
    dry_run: bool = False,
    start_from: int = 0,
    data_dir: str = DEFAULT_DATA_DIR,
) -> None:
    """For each imported volume, extract and store segments from BDRC outlines."""
    # Clone/pull outlines repo
    outlines_repo_path = clone_or_pull_outlines(data_dir)

    # Build lookup maps (scans outlines repo locally)
    lookups = build_lookup_maps(rows, outlines_repo_path)

    # Cache for loaded outline graphs (same outline used by multiple volumes)
    outline_cache: dict[str, ConjunctiveGraph | None] = {}

    total = len(rows)
    segments_imported = 0
    no_outline = 0
    no_volume = 0
    skipped_has_segments = 0
    failed = 0

    for i, row in enumerate(rows):
        if i < start_from:
            continue

        if (i + 1) % 500 == 0:
            logger.info(
                "[%d/%d] progress: %d segments imported, %d no outline, %d no volume, %d skipped, %d failed",
                i + 1,
                total,
                segments_imported,
                no_outline,
                no_volume,
                skipped_has_segments,
                failed,
            )

        w_id = row["w_id"]
        i_id = row["i_id"]

        # Skip rows without GV data early (no volume will exist in OS)
        vol_version = versions.get((w_id, i_id))
        if vol_version is None:
            no_volume += 1
            continue

        # Derive import info (outline, mode, cl_rep_id)
        import_info = _derive_import_info(row, lookups)
        if import_info is None:
            no_outline += 1
            continue

        outline_id = import_info["outline_id"]
        if outline_id is None:
            no_outline += 1
            continue

        doc_id = _volume_doc_id(w_id, i_id, vol_version, ETEXT_SOURCE)

        if dry_run:
            segments_imported += 1
            if (i + 1) % 1000 == 0 or i + 1 == total:
                logger.info(
                    "[%d/%d] [dry-run] %d volumes with outlines so far (outline %s, mode=%s)",
                    i + 1,
                    total,
                    segments_imported,
                    outline_id,
                    import_info["import_mode"],
                )
            continue

        try:
            # Find the volume document in OpenSearch
            existing = get_document(doc_id)
            if existing is None:
                logger.debug("[%d/%d] Volume %s not indexed — skipping segments", i + 1, total, doc_id)
                no_volume += 1
                continue

            # Skip if already has segments
            existing_segments = existing.get("segments", [])
            if existing_segments:
                skipped_has_segments += 1
                continue

            # Get volume number from the document (set during OCR import)
            volume_number = existing.get("volume_number")
            if volume_number is None:
                logger.warning("[%d/%d] No volume number for %s — skipping segments", i + 1, total, i_id)
                no_volume += 1
                continue

            # Load outline graph (with caching)
            if outline_id not in outline_cache:
                outline_cache[outline_id] = _load_outline_graph(outline_id, outlines_repo_path)
            outline_graph = outline_cache[outline_id]
            if outline_graph is None:
                no_outline += 1
                continue

            # Get pages array for coordinate mapping
            pages = existing.get("pages", [])

            segments = _extract_segments_from_outline(outline_graph, import_info, volume_number, pages)

            if not segments:
                no_outline += 1
                continue

            # Store segments on the volume document
            update_document(doc_id, {"segments": segments}, refresh=False)
            segments_imported += 1
            logger.info(
                "[%d/%d] ✓ Imported %d segments for %s (outline %s, mode=%s)",
                i + 1,
                total,
                len(segments),
                doc_id,
                outline_id,
                import_info["import_mode"],
            )
        except Exception:
            logger.exception("[%d/%d] Failed to process %s", i + 1, total, doc_id)
            failed += 1

    if segments_imported and not dry_run:
        logger.info("Refreshing index...")
        refresh_index()

    logger.info("=" * 60)
    logger.info(
        "Phase 2 complete: %d volumes got segments, %d no outline, %d no volume doc, "
        "%d already had segments, %d failed out of %d",
        segments_imported,
        no_outline,
        no_volume,
        skipped_has_segments,
        failed,
        total,
    )


# ── Main ──────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="Import NLM volumes and outline segments")
    parser.add_argument(
        "--phase",
        choices=["ocr", "segments", "both"],
        default="both",
        help="Which phase to run (default: both)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only list what would be imported")
    parser.add_argument("--force", action="store_true", help="Reimport volumes even if already indexed")
    parser.add_argument(
        "--start-from",
        type=int,
        default=1,
        help="1-based row number to resume from (matches log output)",
    )
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATA_DIR,
        help=f"Directory for git repos (default: {DEFAULT_DATA_DIR})",
    )
    args = parser.parse_args()

    rows = download_nlm_csv()
    versions = discover_all_gv_versions(rows)

    # Convert 1-based --start-from to 0-based index
    start_idx = max(args.start_from - 1, 0)

    if args.phase in ("ocr", "both"):
        logger.info("=== Phase 1: Import OCR volumes ===")
        phase1_import_ocr(rows, versions, dry_run=args.dry_run, force=args.force, start_from=start_idx)

    if args.phase in ("segments", "both"):
        logger.info("=== Phase 2: Import segments from outlines ===")
        phase2_import_segments(
            rows,
            versions,
            dry_run=args.dry_run,
            start_from=start_idx,
            data_dir=args.data_dir,
        )


if __name__ == "__main__":
    main()
