from datetime import UTC, datetime
from typing import Any

from api.exceptions import ConflictError, NotFoundError
from api.models import (
    CurationMeta,
    DocumentType,
    Origin,
    PersonInput,
    PersonOutput,
    RecordStatus,
    WorkInput,
    WorkOutput,
    WorkWithAuthors,
)
from api.services.audit import log_event
from api.services.os_client import extract_hits, get_document, index_document, mget_documents, search, update_document
from query_builder import build_search_query


def _catalog_list_filters(
    doc_type: DocumentType,
    *,
    modified_by: str | None = None,
    pref_label_bo: str | None = None,
    record_origin: Origin | None = None,
    record_status: RecordStatus | None = None,
    author_id: str | None = None,
) -> list[dict[str, Any]]:
    """OpenSearch filter clauses for work/person list and search endpoints."""
    filters: list[dict[str, Any]] = [
        {"term": {"type": doc_type.value}},
    ]
    is_work = doc_type == DocumentType.WORK
    if record_status is not None:
        filters.append({"term": {"record_status": record_status.value}})
    else:
        filters.append({"term": {"record_status": RecordStatus.ACTIVE.value}})
    if modified_by is not None:
        filters.append({"term": {"curation.modified_by": modified_by}})
    if pref_label_bo is not None:
        filters.append({"match_phrase": {"pref_label_bo": pref_label_bo}})
    if record_origin is not None:
        filters.append({"term": {"origin": record_origin.value}})
    if is_work and author_id is not None:
        filters.append({"term": {"authors": author_id}})
    return filters


def _next_sequential_id(prefix: str, start: int, doc_type: DocumentType) -> str:
    """Generate the next sequential ID by querying OpenSearch for the current max."""
    body: dict[str, Any] = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"type": doc_type.value}},
                    {"term": {"origin": Origin.LOCAL.value}},
                ],
            },
        },
    }
    response = search(body, size=10000, source_excludes=["*"])

    max_num = start - 1
    for hit in response["hits"]["hits"]:
        suffix = hit["_id"].removeprefix(prefix)
        if suffix.isdigit():
            num = int(suffix)
            max_num = max(max_num, num)

    return f"{prefix}{max_num + 1}"


def _build_curation(modified_by: str, edit_version: int = 1) -> dict[str, Any]:
    """Build a curation metadata block."""
    return CurationMeta(
        modified=True,
        modified_at=datetime.now(UTC),
        modified_by=modified_by,
        edit_version=edit_version,
    ).model_dump(mode="json")


def _create_record(
    data: WorkInput | PersonInput,
    doc_type: DocumentType,
    doc_id: str,
) -> dict[str, Any]:
    """Create a new local record. Returns the full body dict with 'id' key."""
    body = {
        **data.model_dump(exclude={"modified_by"}),
        "type": doc_type.value,
        "origin": Origin.LOCAL.value,
        "record_status": RecordStatus.NEW.value,
        "curation": _build_curation(data.modified_by, edit_version=1),
    }
    index_document(doc_id, body)
    log_event(doc_id, doc_type.value, "create", data.modified_by)
    return {**body, "id": doc_id}


def _update_record(
    doc_id: str,
    data: WorkInput | PersonInput,
    doc_type: DocumentType,
    label: str,
) -> dict[str, Any]:
    """Partial update of an existing record with curation bookkeeping."""
    existing = get_document(doc_id)
    if existing is None:
        raise NotFoundError(label, doc_id)

    current_version = (existing.get("curation") or {}).get("edit_version", 0)

    partial = data.model_dump(exclude_unset=True, exclude={"modified_by"})
    partial["curation"] = _build_curation(data.modified_by, edit_version=current_version + 1)
    update_document(doc_id, partial)
    log_event(doc_id, doc_type.value, "edit", data.modified_by, diff=partial)
    return {**existing, **partial, "id": doc_id}


def _merge_record(
    doc_id: str,
    canonical_id: str,
    modified_by: str,
    doc_type: DocumentType,
    label: str,
) -> dict[str, Any]:
    """Mark a record as duplicate, pointing to the canonical record."""
    if doc_id == canonical_id:
        raise ConflictError(f"Cannot merge {label} '{doc_id}' into itself")

    existing = get_document(doc_id)
    if existing is None:
        raise NotFoundError(label, doc_id)

    if (existing.get("record_status") or "") == RecordStatus.DUPLICATE.value:
        raise ConflictError(f"{label} '{doc_id}' is already marked as duplicate")

    canonical = get_document(canonical_id)
    if canonical is None:
        raise NotFoundError(f"{label} (canonical target)", canonical_id)

    if canonical.get("type") != doc_type.value:
        raise ConflictError(f"Canonical target '{canonical_id}' is not a {doc_type.value}")

    current_version = (existing.get("curation") or {}).get("edit_version", 0)
    partial: dict[str, Any] = {
        "record_status": RecordStatus.DUPLICATE.value,
        "canonical_id": canonical_id,
        "curation": _build_curation(modified_by, edit_version=current_version + 1),
    }
    update_document(doc_id, partial)
    log_event(doc_id, doc_type.value, "merge", modified_by, diff={"canonical_id": canonical_id})
    return {**existing, **partial, "id": doc_id}


def _get_record(doc_id: str) -> dict[str, Any] | None:
    """Fetch a record by ID, returning the source dict with 'id' included."""
    source = get_document(doc_id)
    if source is None:
        return None
    return {**source, "id": doc_id}


def create_work(data: WorkInput) -> WorkOutput:
    """Create a new local work record with a generated ID."""
    work_id = _next_sequential_id(prefix="WA1BC", start=10, doc_type=DocumentType.WORK)
    return WorkOutput.model_validate(_create_record(data, DocumentType.WORK, work_id))


def update_work(work_id: str, data: WorkInput) -> WorkOutput:
    """Partial update of an existing work with curation bookkeeping."""
    return WorkOutput.model_validate(_update_record(work_id, data, DocumentType.WORK, "Work"))


def get_work(work_id: str) -> WorkWithAuthors | None:
    record = _get_record(work_id)
    if record is None:
        return None
    author_ids = record.get("authors", [])
    person_map = mget_documents(author_ids)
    author_records = [
        PersonOutput.model_validate({**person_map[aid], "id": aid}) for aid in author_ids if aid in person_map
    ]
    return WorkWithAuthors.model_validate({**record, "author_records": author_records})


def _works_from_hits(hits: list[dict[str, Any]]) -> list[WorkWithAuthors]:
    all_author_ids = list({aid for h in hits for aid in h.get("authors", [])})
    person_map = mget_documents(all_author_ids)

    def _resolve_authors(author_ids: list[str]) -> list[PersonOutput]:
        return [PersonOutput.model_validate({**person_map[aid], "id": aid}) for aid in author_ids if aid in person_map]

    return [
        WorkWithAuthors.model_validate({**h, "author_records": _resolve_authors(h.get("authors", []))}) for h in hits
    ]


def list_works(
    *,
    modified_by: str | None = None,
    pref_label_bo: str | None = None,
    record_origin: Origin | None = None,
    record_status: RecordStatus | None = None,
    author_id: str | None = None,
    offset: int = 0,
    limit: int = 50,
) -> tuple[list[WorkWithAuthors], int]:
    """List works with optional catalog filters and stable id ordering."""
    filters = _catalog_list_filters(
        DocumentType.WORK,
        modified_by=modified_by,
        pref_label_bo=pref_label_bo,
        record_origin=record_origin,
        record_status=record_status,
        author_id=author_id,
    )
    body: dict[str, Any] = {
        "query": {"bool": {"filter": filters}},
        "sort": [{"id": {"order": "asc"}}],
    }
    response = search(body, size=limit, offset=offset)
    total: int = response["hits"]["total"]["value"]
    hits = extract_hits(response)
    return _works_from_hits(hits), total


def search_works(
    title: str | None = None,
    author_name: str | None = None,
    *,
    modified_by: str | None = None,
    pref_label_bo: str | None = None,
    record_origin: Origin | None = None,
    record_status: RecordStatus | None = None,
    author_id: str | None = None,
    size: int = 20,
) -> list[WorkWithAuthors]:
    type_filter = _catalog_list_filters(
        DocumentType.WORK,
        modified_by=modified_by,
        pref_label_bo=pref_label_bo,
        record_origin=record_origin,
        record_status=record_status,
        author_id=author_id,
    )
    search_text_parts: list[str] = []
    if title:
        search_text_parts.append(title)
    if author_name:
        search_text_parts.append(author_name)

    body = build_search_query(
        {
            "query": " ".join(search_text_parts) if search_text_parts else "",
            "filter": type_filter,
        }
    )

    response = search(body, size=size)
    hits = extract_hits(response)
    return _works_from_hits(hits)


def merge_work(work_id: str, canonical_id: str, modified_by: str) -> WorkOutput:
    """Mark a work as duplicate, pointing to the canonical work."""
    return WorkOutput.model_validate(_merge_record(work_id, canonical_id, modified_by, DocumentType.WORK, "Work"))


def create_person(data: PersonInput) -> PersonOutput:
    """Create a new local person record with a generated ID."""
    person_id = _next_sequential_id(prefix="P1BC", start=1, doc_type=DocumentType.PERSON)
    return PersonOutput.model_validate(_create_record(data, DocumentType.PERSON, person_id))


def update_person(person_id: str, data: PersonInput) -> PersonOutput:
    """Partial update of an existing person with curation bookkeeping."""
    return PersonOutput.model_validate(_update_record(person_id, data, DocumentType.PERSON, "Person"))


def get_person(person_id: str) -> PersonOutput | None:
    record = _get_record(person_id)
    if record is None:
        return None
    return PersonOutput.model_validate(record)


def list_persons(
    *,
    modified_by: str | None = None,
    pref_label_bo: str | None = None,
    record_origin: Origin | None = None,
    record_status: RecordStatus | None = None,
    offset: int = 0,
    limit: int = 50,
) -> tuple[list[PersonOutput], int]:
    """List persons with optional catalog filters and stable id ordering."""
    filters = _catalog_list_filters(
        DocumentType.PERSON,
        modified_by=modified_by,
        pref_label_bo=pref_label_bo,
        record_origin=record_origin,
        record_status=record_status,
    )
    body: dict[str, Any] = {
        "query": {"bool": {"filter": filters}},
        "sort": [{"id": {"order": "asc"}}],
    }
    response = search(body, size=limit, offset=offset)
    total: int = response["hits"]["total"]["value"]
    return [PersonOutput.model_validate(h) for h in extract_hits(response)], total


def search_persons(
    author_name: str | None = None,
    *,
    modified_by: str | None = None,
    pref_label_bo: str | None = None,
    record_origin: Origin | None = None,
    record_status: RecordStatus | None = None,
    size: int = 20,
) -> list[PersonOutput]:
    filters = _catalog_list_filters(
        DocumentType.PERSON,
        modified_by=modified_by,
        pref_label_bo=pref_label_bo,
        record_origin=record_origin,
        record_status=record_status,
    )
    body = build_search_query({"query": (author_name or "").strip(), "filter": filters})
    response = search(body, size=size)
    return [PersonOutput.model_validate(h) for h in extract_hits(response)]


def merge_person(person_id: str, canonical_id: str, modified_by: str) -> PersonOutput:
    """Mark a person as duplicate, pointing to the canonical person."""
    return PersonOutput.model_validate(
        _merge_record(person_id, canonical_id, modified_by, DocumentType.PERSON, "Person")
    )
