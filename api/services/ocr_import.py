"""Import OCR results from parquet files stored in S3 into OpenSearch volumes."""

import gzip
import hashlib
import json
import logging
import re
from datetime import UTC, datetime
from io import BytesIO

import boto3
import botocore.exceptions
import pyarrow.parquet as pq
import requests
from rdflib import Graph, Namespace

from api.config import Config
from api.models import Chunk, DocumentType, PageEntry, VolumeStatus
from api.services.opensearch import _get_document, _index_document, _volume_doc_id

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1000
BDRC_RESOURCE_URL = "https://purl.bdrc.io/resource/"
S3_ARCHIVE_BUCKET = "archive.tbrc.org"

# Define RDF namespaces
BDO = Namespace("http://purl.bdrc.io/ontology/core/")
BDR = Namespace("http://purl.bdrc.io/resource/")

_TIB_CHUNK_PATTERN = re.compile(r"([སའངགདནབམརལཏ]ོ[་༌]?[།-༔][^ཀ-ཬ]*|(།།|[༎-༒])[^ཀ-ཬ༠-༩]*[།-༔][^ཀ-ཬ༠-༩]*)")


def get_s3_folder_prefix(w_id: str, i_id: str) -> str:
    """
    Get the S3 prefix (~folder) in which the volume will be present.

    Inspired from https://github.com/buda-base/buda-iiif-presentation/blob/master/src/main/java/
    io/bdrc/iiif/presentation/ImageInfoListService.java#L73

    Example:
       - w_id=W22084, i_id=I0886
       - result = "Works/60/W22084/images/W22084-0886/"
    where:
       - 60 is the first two characters of the md5 of the string W22084
       - 0886 is:
          * the image group ID without the initial "I" if the image group ID is in the form I\\d\\d\\d\\d
          * or else the full image group ID (including the "I")
    """
    md5 = hashlib.md5(w_id.encode())  # noqa: S324
    two = md5.hexdigest()[:2]

    pre, rest = i_id[0], i_id[1:]
    suffix = rest if pre == "I" and rest.isdigit() and len(rest) == 4 else i_id

    return f"Works/{two}/{w_id}/images/{w_id}-{suffix}/"


def get_s3_blob(s3_key: str) -> BytesIO | None:
    """Download a blob from S3 archive bucket into memory."""
    s3 = boto3.client("s3")
    buffer = BytesIO()
    try:
        s3.download_fileobj(S3_ARCHIVE_BUCKET, s3_key, buffer)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return None
        raise
    else:
        return buffer


def get_image_list_s3(w_id: str, i_id: str) -> list[dict[str, str | int]] | None:
    """
    Get the image list from S3. The format is:
    [
      {
         "filename": "I0TTBBC0077_0020001.jpg",
         "width": 2650,
         "height": 1731
      }
    ]

    Excludes entries where filename ends with .json or where width/height is absent or null.
    Page number of filename X is the index of the entry in the list that has filename = X, starting at 1.
    """
    s3_key = get_s3_folder_prefix(w_id, i_id) + "dimensions.json"
    logger.info("Fetching dimensions from s3://%s/%s", S3_ARCHIVE_BUCKET, s3_key)

    blob = get_s3_blob(s3_key)
    if blob is None:
        logger.warning("dimensions.json not found at %s", s3_key)
        return None

    try:
        blob.seek(0)
        b = blob.read()
        ub = gzip.decompress(b)
        s = ub.decode("utf8")
        data = json.loads(s)

        # Filter out invalid entries
        filtered_data = [
            entry
            for entry in data
            if not entry.get("filename", "").endswith(".json")
            and entry.get("width") is not None
            and entry.get("height") is not None
        ]

        logger.info("Loaded %d valid image entries from dimensions.json", len(filtered_data))

    except Exception:
        logger.exception("Error parsing dimensions.json: %s")
        return None
    else:
        return filtered_data


def build_filename_to_pnum_map(w_id: str, i_id: str) -> dict[str, int]:
    """
    Build a mapping from filename to page number based on dimensions.json.

    Returns empty dict if dimensions.json cannot be fetched or parsed.
    """
    image_list = get_image_list_s3(w_id, i_id)
    if image_list is None:
        return {}

    filename_to_pnum = {}
    for idx, entry in enumerate(image_list, start=1):
        filename = entry.get("filename")
        if filename:
            filename_to_pnum[filename] = idx

    return filename_to_pnum


def fetch_volume_metadata(i_id: str) -> dict[str, int | str | None]:
    """
    Fetch volume metadata from BDRC TTL resource.

    Args:
        i_id: Image instance ID (e.g., "I1CZ35")

    Returns:
        Dict with volume metadata including volume_number
    """
    url = f"{BDRC_RESOURCE_URL}{i_id}.ttl"

    try:
        logger.info("Fetching volume metadata from %s", url)
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        # Parse TTL content
        graph = Graph()
        graph.parse(data=response.text, format="turtle")

        # Build the subject URI for this resource
        subject = BDR[i_id]

        # Extract metadata
        metadata: dict[str, int | str | None] = {
            "volume_number": None,
            "volume_pages_tbrc_intro": None,
            "volume_pages_total": None,
        }

        # Get volume number
        for _, _, vol_num in graph.triples((subject, BDO.volumeNumber, None)):
            metadata["volume_number"] = int(vol_num)
            break

        # Get bibliographic note (optional)
        for _, _, note in graph.triples((subject, BDO.volumePagesTbrcIntro, None)):
            metadata["volume_pages_tbrc_intro"] = int(note)
            break

        # Get total pages (optional)
        for _, _, pages in graph.triples((subject, BDO.volumePagesTotal, None)):
            metadata["volume_pages_total"] = int(pages)
            break

        logger.info("Fetched metadata for %s: %s", i_id, metadata)

    except requests.RequestException as e:
        logger.warning("Failed to fetch volume metadata from %s: %s", url, e)
        return {"volume_number": None, "volume_pages_tbrc_intro": None, "volume_pages_total": None}
    except Exception:
        logger.exception("Error parsing volume metadata for %s", i_id)
        return {"volume_number": None, "volume_pages_tbrc_intro": None, "volume_pages_total": None}

    else:
        return metadata


def _build_chunks(text: str, chunk_size: int = CHUNK_SIZE) -> list[Chunk]:
    """Split text into chunks of ~chunk_size chars, breaking at Tibetan sentence endings or newlines."""
    text_len = len(text)
    if text_len <= chunk_size:
        return [Chunk(cstart=0, cend=text_len, text_bo=text)] if text else []

    breaks = [m.end() for m in _TIB_CHUNK_PATTERN.finditer(text)]

    chunks: list[Chunk] = []
    start = 0
    break_index = 0

    while text_len - start > chunk_size:
        target = start + chunk_size
        max_end = min(text_len, start + 2 * chunk_size)

        while break_index < len(breaks) and breaks[break_index] < target:
            break_index += 1

        if break_index > 0 and breaks[break_index - 1] > start:
            end = breaks[break_index - 1]
        elif break_index < len(breaks) and breaks[break_index] <= max_end:
            end = breaks[break_index]
        else:
            # Fallback: look for space or newline as break point
            # Search up to max_end for better break points
            newline = text.rfind("\n", start + 1, max_end)
            space = text.rfind(" ", start + 1, max_end)

            # Use whichever is closer to target
            best_break = max(newline, space)

            end = best_break + 1 if best_break != -1 else max_end

        chunks.append(Chunk(cstart=start, cend=end, text_bo=text[start:end]))
        start = end

    if start < text_len:
        chunks.append(Chunk(cstart=start, cend=text_len, text_bo=text[start:text_len]))

    return chunks


def _s3_key(w_id: str, i_id: str, i_version: str, etext_source: str) -> str:
    """Build the S3 object key for an OCR parquet file."""
    source_in_fname = etext_source
    if etext_source == "ocrv1-ws-ldv1":
        source_in_fname = "ocrv1"
    filename = f"{w_id}-{i_id}-{i_version}_{source_in_fname}.parquet"
    return f"{etext_source}/{w_id}/{i_id}/{i_version}/{filename}"


def _download_from_s3(s3_key: str) -> BytesIO:
    """Download a parquet file from S3 directly into memory."""
    s3 = boto3.client("s3")
    buffer = BytesIO()
    logger.info("Downloading s3://%s/%s", Config.S3_OCR_BUCKET, s3_key)
    s3.download_fileobj(Config.S3_OCR_BUCKET, s3_key, buffer)
    buffer.seek(0)  # Reset buffer position to beginning
    return buffer


def import_ocr_from_s3(
    w_id: str,
    i_id: str,
    i_version: str,
    etext_source: str,
) -> str:
    """
    Download a parquet OCR file from S3 and index the resulting volume into OpenSearch.

    Returns the document ID of the created volume.
    """
    key = _s3_key(w_id, i_id, i_version, etext_source)
    parquet_buffer = _download_from_s3(key)
    return _import_parquet(w_id, i_id, i_version, etext_source, parquet_buffer)


def _import_parquet(
    w_id: str,
    i_id: str,
    i_version: str,
    etext_source: str,
    parquet_data: BytesIO,
) -> str:
    """
    Read a parquet OCR file from memory and index the resulting volume into OpenSearch.

    Returns the document ID of the created volume.
    """
    table = pq.read_table(parquet_data)
    logger.info("Read %d rows from parquet file", table.num_rows)

    # Collect successful pages: (filename, line_texts)
    pages_raw: list[tuple[str, list[str]]] = []
    skipped = 0
    for i in range(table.num_rows):
        ok = table.column("ok")[i].as_py()
        if not ok:
            skipped += 1
            continue
        fname = table.column("img_file_name")[i].as_py()
        lines = table.column("line_texts")[i].as_py() or []
        pages_raw.append((fname, lines))

    pages_raw.sort(key=lambda x: x[0])
    logger.info("Processing %d pages (%d skipped due to errors)", len(pages_raw), skipped)

    # Build filename to page number mapping from dimensions.json
    filename_to_pnum = build_filename_to_pnum_map(w_id, i_id)

    # Build continuous text and page entries
    full_text_parts: list[str] = []
    pages: list[PageEntry] = []
    offset = 0

    for fname, lines in pages_raw:
        page_text = "\n".join(lines)
        cstart = offset
        cend = offset + len(page_text)

        # Get correct pnum from dimensions.json, fallback to None if not found
        pnum = filename_to_pnum.get(fname)
        if pnum is None:
            logger.warning("Page number not found for filename: %s", fname)

        pages.append(
            PageEntry(
                cstart=cstart,
                cend=cend,
                pnum=pnum,
                pname=fname,
            )
        )

        full_text_parts.append(page_text)
        offset = cend + 1  # +1 for the page separator newline

    full_text = "\n".join(full_text_parts)

    # Build search chunks
    chunks = _build_chunks(full_text)

    # Fetch volume metadata from BDRC
    metadata = fetch_volume_metadata(i_id)

    # Check if document already exists to preserve certain fields
    doc_id = _volume_doc_id(w_id, i_id, i_version, etext_source)
    existing_doc = _get_document(doc_id)

    # Assemble and index the volume document
    now = datetime.now(UTC).isoformat()

    # Preserve fields from existing document if it exists
    if existing_doc:
        first_imported_at = existing_doc.get("first_imported_at", now)
        existing_segments = existing_doc.get("segments", [])
        existing_status = existing_doc.get("status", VolumeStatus.NEW.value)
        logger.info(
            "Reimporting existing volume %s - preserving %d segments and status=%s",
            doc_id,
            len(existing_segments),
            existing_status,
        )
    else:
        first_imported_at = now
        existing_segments = []
        existing_status = VolumeStatus.NEW.value
        logger.info("Creating new volume %s", doc_id)

    body = {
        "type": DocumentType.VOLUME_ETEXT.value,
        "w_id": w_id,
        "i_id": i_id,
        "i_version": i_version,
        "etext_source": etext_source,
        "status": existing_status,
        "volume_number": metadata["volume_number"],
        "nb_pages": len(pages),
        "pages": [p.model_dump() for p in pages],
        "segments": existing_segments,
        "chunks": [c.model_dump() for c in chunks],
        "cstart": 0,
        "cend": len(full_text),
        "first_imported_at": first_imported_at,
        "last_updated_at": now,
        "join_field": {"name": "instance"},
    }

    _index_document(doc_id, body)

    return doc_id
