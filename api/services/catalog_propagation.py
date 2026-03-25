"""Propagate catalog record_status when annotation volumes are reviewed."""

from __future__ import annotations

import logging
from typing import Any

from api.models import DocumentType, RecordStatus
from api.services.audit import log_event
from api.services.os_client import bulk_operation, mget_documents

logger = logging.getLogger(__name__)

_ACTOR = "volume_reviewed"


def propagate_active_for_reviewed_volume_segments(
    segments: list[dict[str, Any]],
    *,
    volume_id: str,
) -> None:
    """
    Set record_status=active on works referenced by segment wa_id and on their author persons.

    Skips works/persons with record_status duplicate or withdrawn.
    """
    wa_ids: set[str] = set()
    for seg in segments:
        wa_id = seg.get("wa_id")
        if wa_id:
            wa_ids.add(wa_id)

    if not wa_ids:
        return

    work_map = mget_documents(list(wa_ids))

    work_ids_eligible: list[str] = []
    work_ids_to_activate: list[str] = []
    for wid in wa_ids:
        doc = work_map.get(wid)
        if doc is None:
            continue
        if doc.get("type") != DocumentType.WORK.value:
            continue
        rs = (doc.get("record_status") or "").lower()
        if rs in (RecordStatus.DUPLICATE.value, RecordStatus.WITHDRAWN.value):
            continue
        work_ids_eligible.append(wid)
        if rs != RecordStatus.ACTIVE.value:
            work_ids_to_activate.append(wid)

    author_ids: set[str] = set()
    for wid in work_ids_eligible:
        for aid in work_map.get(wid, {}).get("authors", []) or []:
            author_ids.add(aid)

    person_map = mget_documents(list(author_ids)) if author_ids else {}

    person_ids_to_activate: list[str] = []
    for pid in author_ids:
        doc = person_map.get(pid)
        if doc is None:
            continue
        if doc.get("type") != DocumentType.PERSON.value:
            continue
        rs = (doc.get("record_status") or "").lower()
        if rs in (RecordStatus.DUPLICATE.value, RecordStatus.WITHDRAWN.value):
            continue
        if rs == RecordStatus.ACTIVE.value:
            continue
        person_ids_to_activate.append(pid)

    all_updates: list[tuple[str, str]] = [(wid, DocumentType.WORK.value) for wid in work_ids_to_activate] + [
        (pid, DocumentType.PERSON.value) for pid in person_ids_to_activate
    ]

    if not all_updates:
        return

    bulk_body: list[dict[str, Any]] = []
    for doc_id, _ in all_updates:
        bulk_body.append({"update": {"_id": doc_id}})
        bulk_body.append({"doc": {"record_status": RecordStatus.ACTIVE.value}})

    try:
        response = bulk_operation(bulk_body, refresh=True)
    except Exception:
        logger.exception("Bulk activate failed after volume %s reviewed", volume_id)
        raise

    for i, (doc_id, type_str) in enumerate(all_updates):
        item = response.get("items", [])[i] if i < len(response.get("items", [])) else {}
        result = next(iter(item.values()), {}) if item else {}
        if result.get("error"):
            logger.warning(
                "Activate failed for %s %s after volume %s: %s",
                type_str,
                doc_id,
                volume_id,
                result.get("error"),
            )
            continue
        log_event(
            doc_id,
            type_str,
            "edit",
            _ACTOR,
            diff={"record_status": RecordStatus.ACTIVE.value},
            correlation_id=volume_id,
        )
