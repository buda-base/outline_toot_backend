from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status

from api.exceptions import NotFoundError
from api.models import (
    MergeRequest,
    Origin,
    RecordStatus,
    WorkInput,
    WorkOutput,
    WorksPaginatedResponse,
    WorkWithAuthors,
)
from api.services.records import create_work, get_work, list_works, merge_work, search_works, update_work

router = APIRouter(prefix="/works", tags=["works"])


@router.get("")
async def list_work_records(
    modified_by: Annotated[str | None, Query()] = None,
    pref_label_bo: Annotated[str | None, Query()] = None,
    record_origin: Annotated[Origin | None, Query()] = None,
    record_status: Annotated[RecordStatus | None, Query()] = None,
    author_id: Annotated[
        str | None,
        Query(description="Person id (P…) — work must list this id in ``authors``."),
    ] = None,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> WorksPaginatedResponse:
    """List works with optional catalog filters and pagination (same filters as ``/works/search``)."""
    items, total = list_works(
        modified_by=modified_by,
        pref_label_bo=pref_label_bo,
        record_origin=record_origin,
        record_status=record_status,
        author_id=author_id,
        offset=offset,
        limit=limit,
    )
    return WorksPaginatedResponse(total=total, offset=offset, limit=limit, items=items)


@router.get("/search")
async def find_work(
    title: Annotated[str | None, Query()] = None,
    author_name: Annotated[str | None, Query()] = None,
    modified_by: Annotated[str | None, Query()] = None,
    pref_label_bo: Annotated[str | None, Query()] = None,
    record_origin: Annotated[Origin | None, Query()] = None,
    record_status: Annotated[RecordStatus | None, Query()] = None,
    author_id: Annotated[str | None, Query()] = None,
) -> list[WorkWithAuthors]:
    """Search works by title and/or author name, with optional catalog filters."""
    has_text = bool(title or author_name)
    has_filters = any(
        v is not None for v in (modified_by, pref_label_bo, record_origin, record_status, author_id)
    )
    if not has_text and not has_filters:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide at least one of: title, author_name, or a filter "
            "(modified_by, pref_label_bo, record_origin, record_status, author_id)",
        )
    return search_works(
        title=title,
        author_name=author_name,
        modified_by=modified_by,
        pref_label_bo=pref_label_bo,
        record_origin=record_origin,
        record_status=record_status,
        author_id=author_id,
    )


@router.get("/{work_id}")
async def get_work_data(work_id: str) -> WorkWithAuthors:
    """Get work data by ID."""
    work = get_work(work_id)
    if work is None:
        raise NotFoundError("Work", work_id)
    return work


@router.post("", status_code=status.HTTP_201_CREATED)
async def post_work_data(body: WorkInput) -> dict[str, str]:
    """Create a new work with a server-generated ID."""
    work = create_work(body)
    return {"id": work.id}


@router.put("/{work_id}")
async def put_work_data(work_id: str, body: WorkInput) -> WorkOutput:
    """Update an existing work (only the provided fields)."""
    return update_work(work_id, body)


@router.post("/{work_id}/merge")
async def merge_work_data(work_id: str, body: MergeRequest) -> WorkOutput:
    """Mark a work as duplicate of the canonical work."""
    return merge_work(work_id, body.canonical_id, body.modified_by)
