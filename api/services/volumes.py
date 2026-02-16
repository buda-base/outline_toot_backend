from datetime import UTC, datetime
from typing import Any

from api.exceptions import NotFoundError
from api.models import DocumentType, VolumeInput, VolumeOutput
from api.services.os_client import extract_hits, get_document, search, update_document


def _volume_doc_id(w_id: str, i_id: str, i_version: str, etext_source: str) -> str:
    return f"{w_id}_{i_id}_{i_version}_{etext_source}"


def list_volumes(
    status: str | None = None,
    etext_source: str | None = None,
    w_id: str | None = None,
    offset: int = 0,
    limit: int = 50,
) -> tuple[list[VolumeOutput], int]:
    filters: list[dict[str, Any]] = [
        {"term": {"type": DocumentType.VOLUME_ETEXT.value}},
    ]
    if status is not None:
        filters.append({"term": {"status": status}})
    if etext_source is not None:
        filters.append({"term": {"etext_source": etext_source}})
    if w_id is not None:
        filters.append({"term": {"w_id": w_id}})

    body: dict[str, Any] = {"query": {"bool": {"filter": filters}}}
    response = search(
        body,
        size=limit,
        offset=offset,
        source_excludes=["chunks", "pages", "segments"],
    )
    total: int = response["hits"]["total"]["value"]
    items = [VolumeOutput.model_validate(h) for h in extract_hits(response)]
    return items, total


def get_volume(w_id: str, i_id: str) -> VolumeOutput | None:
    """
    Get the best matching volume for the given w_id and i_id.

    Selection logic:
    1. Prefer documents with segments over those without
    2. Within each group, prefer most recent (by last_updated_at)
    """
    # Search for all volumes matching w_id and i_id
    filters: list[dict[str, Any]] = [
        {"term": {"type": DocumentType.VOLUME_ETEXT.value}},
        {"term": {"w_id": w_id}},
        {"term": {"i_id": i_id}},
    ]

    body: dict[str, Any] = {
        "query": {"bool": {"filter": filters}},
        "sort": [{"last_updated_at": {"order": "desc"}}],
    }

    response = search(body, size=100)  # Get up to 100 versions
    hits = extract_hits(response)

    if not hits:
        return None

    # Separate documents with and without segments
    with_segments = [h for h in hits if h.get("segments") and len(h.get("segments", [])) > 0]
    without_segments = [h for h in hits if not h.get("segments") or len(h.get("segments", [])) == 0]

    # Choose the best: prefer with segments, then most recent
    chosen = with_segments[0] if with_segments else (without_segments[0] if without_segments else hits[0])

    return VolumeOutput.model_validate({**chosen, "id": chosen["id"], "w_id": w_id, "i_id": i_id})


def update_volume(w_id: str, i_id: str, data: VolumeInput) -> VolumeOutput:
    """
    Partial update of an existing volume (only client-sent fields).
    Requires i_version and etext_source to identify the specific document to update.
    """
    if not data.i_version or not data.etext_source:
        raise ValueError("i_version and etext_source are required to update a volume")

    doc_id = _volume_doc_id(w_id, i_id, data.i_version, data.etext_source)

    existing = get_document(doc_id)
    if existing is None:
        raise NotFoundError("Volume", doc_id)

    partial = data.model_dump(exclude_unset=True)
    partial["last_updated_at"] = datetime.now(UTC).isoformat()
    update_document(doc_id, partial)
    return VolumeOutput.model_validate({**existing, **partial, "id": doc_id})
