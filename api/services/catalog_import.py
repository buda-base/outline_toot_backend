import logging
from datetime import UTC, datetime
from typing import Any

from api.models import ImportRecord
from api.services.audit import log_event
from api.services.os_client import bulk_operation

logger = logging.getLogger(__name__)

# Painless script for scripted_upsert:
# - Always updates source_meta.updated_at and import.last_run_at
# - Only overwrites content fields when curation.modified is false
# - Sets import.last_result accordingly
_UPSERT_SCRIPT = """
    if (ctx._source.source_meta == null) { ctx._source.source_meta = [:]; }
    ctx._source.source_meta.updated_at = params.doc.source_meta.updated_at;

    if (ctx._source.import == null) { ctx._source.import = [:]; }
    ctx._source.import.last_run_at = params.now;

    boolean modified = (ctx._source.curation != null && ctx._source.curation.modified == true);

    if (!modified) {
        if (params.doc.pref_label_bo != null) {
            ctx._source.pref_label_bo = params.doc.pref_label_bo;
        }
        if (params.doc.alt_label_bo != null) {
            ctx._source.alt_label_bo = params.doc.alt_label_bo;
        }
        if (params.doc.db_score != null) {
            ctx._source.db_score = params.doc.db_score;
        }
        if (params.doc.authors != null) {
            ctx._source.authors = params.doc.authors;
        }
        ctx._source.import.last_result = 'updated_or_created';
    } else {
        ctx._source.import.last_result = 'skipped_modified';
    }

    if (ctx._source.curation == null) {
        ctx._source.curation = params.doc.curation;
    }
"""

_DEFAULT_CURATION = {
    "modified": False,
    "modified_at": None,
    "modified_by": None,
    "edit_version": 0,
}


def _build_upsert_action(record: ImportRecord, now: str) -> list[dict[str, Any]]:
    """Build the two-line NDJSON action + body for a single scripted_upsert."""
    doc_body: dict[str, Any] = {
        "type": record.type,
        "origin": "imported",
        "source_meta": {
            "updated_at": now,
        },
        "curation": {**_DEFAULT_CURATION},
        "pref_label_bo": record.pref_label_bo,
        "alt_label_bo": record.alt_label_bo,
        "authors": record.authors,
    }
    if record.db_score is not None:
        doc_body["db_score"] = record.db_score

    upsert_body: dict[str, Any] = {
        **doc_body,
        "record_status": "active",
        "canonical_id": None,
    }

    action = {"update": {"_id": record.id}}
    body: dict[str, Any] = {
        "scripted_upsert": True,
        "upsert": upsert_body,
        "script": {
            "lang": "painless",
            "source": _UPSERT_SCRIPT,
            "params": {
                "now": now,
                "doc": doc_body,
            },
        },
    }
    return [action, body]


def bulk_upsert_from_import(records: list[ImportRecord], now: str | None = None) -> dict[str, int]:
    """
    Bulk upsert records from a BDRC import using scripted_upsert.

    For each record:
    - If curation.modified == false (or doc is new): overwrite source-owned fields.
    - If curation.modified == true: update only source_meta.* + import.*, skip content fields.
    - New docs get origin="imported", record_status="active" (catalog-ready; API-created locals use "new"), default curation block.

    Returns:
        Counts: {"updated": N, "created": N, "skipped": N}
    """
    if not records:
        return {"updated": 0, "created": 0, "skipped": 0}

    if now is None:
        now = datetime.now(UTC).isoformat()

    type_by_id: dict[str, str] = {r.id: r.type for r in records}

    bulk_body: list[dict[str, Any]] = []
    for record in records:
        bulk_body.extend(_build_upsert_action(record, now))

    response = bulk_operation(bulk_body)

    counts: dict[str, int] = {"updated": 0, "created": 0, "skipped": 0}

    for item in response.get("items", []):
        update_result = item.get("update", {})
        result_status = update_result.get("result", "")
        doc_id = update_result.get("_id", "unknown")
        doc_type = type_by_id.get(doc_id, "unknown")

        if result_status == "created":
            counts["created"] += 1
            log_event(doc_id, doc_type, "import_create", "importer", correlation_id=f"import-{now}")
        elif result_status == "updated":
            counts["updated"] += 1
            log_event(doc_id, doc_type, "import_update", "importer", correlation_id=f"import-{now}")
        elif result_status == "noop":
            counts["skipped"] += 1
        else:
            error = update_result.get("error")
            if error:
                logger.error("Bulk upsert error for %s: %s", doc_id, error)
            counts["skipped"] += 1

    logger.info(
        "Bulk upsert complete: %d created, %d updated, %d skipped",
        counts["created"],
        counts["updated"],
        counts["skipped"],
    )
    return counts
