from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class VolumeStatus(StrEnum):
    """Annotation workflow status - managed by the annotation code."""

    ACTIVE = "active"
    IN_PROGRESS = "in_progress"
    IN_REVIEW = "in_review"
    REVIEWED = "reviewed"


class SegmentType(StrEnum):
    TEXT = "text"
    EDITORIAL = "editorial"


class DocumentType(StrEnum):
    VOLUME_ETEXT = "volume_etext"
    WORK = "work"
    PERSON = "person"


class RecordStatus(StrEnum):
    """Catalog record lifecycle - from BDRC or for Works/Persons."""

    ACTIVE = "active"
    DUPLICATE = "duplicate"
    WITHDRAWN = "withdrawn"


class Origin(StrEnum):
    IMPORTED = "imported"
    LOCAL = "local"


class ParsedRecord(BaseModel):
    id: str
    type: str
    is_released: bool
    replacement_id: str | None = None
    pref_label_bo: str | None = None
    alt_label_bo: list[str] = Field(default_factory=list)
    authors: list[str] = Field(default_factory=list)


class ImportRecord(BaseModel):
    id: str
    type: str
    pref_label_bo: str | None = None
    alt_label_bo: list[str] = Field(default_factory=list)
    authors: list[str] = Field(default_factory=list)
    db_score: float | None = None


class SyncCounts(BaseModel):
    upserted: int = 0
    merged: int = 0
    withdrawn: int = 0
    skipped: int = 0


class Chunk(BaseModel):
    cstart: int
    cend: int
    text_bo: str | None = None


class PageEntry(BaseModel):
    cstart: int
    cend: int
    pnum: int | None = None
    pname: str | None = None
    img_link: str | None = None


class Segment(BaseModel):
    id: str | None = None
    cstart: int
    cend: int
    segment_type: SegmentType = SegmentType.TEXT
    parent_segment: str | None = None
    title_bo: str | list[str] | None = None
    author_name_bo: str | list[str] | None = None


class AnnotatedSegment(BaseModel):
    """Segment with full annotation data from frontend."""

    cstart: int
    cend: int
    title_bo: str | list[str]  # Mandatory, can be string or list
    author_name_bo: str | list[str] | None = None  # Optional, can be string or list
    mw_id: str  # Must start with '{parent_mw_id}_'
    wa_id: str | None = None  # Mandatory for part_type='text'
    part_type: SegmentType  # 'text' or 'editorial'

    @field_validator("mw_id")
    @classmethod
    def validate_mw_id_format(cls, v: str) -> str:
        """Validate that mw_id contains an underscore and has proper format."""
        if "_" not in v:
            raise ValueError("mw_id must contain an underscore (format: {parent_mw_id}_{segment_id})")
        prefix = v.split("_", maxsplit=1)[0]
        if not prefix or not prefix[0].isupper():
            raise ValueError("mw_id must start with a valid ID prefix (e.g., MW123_456)")
        return v

    @model_validator(mode="after")
    def validate_wa_id_for_text(self) -> AnnotatedSegment:
        """Validate that wa_id is present when part_type is 'text'."""
        if self.part_type == SegmentType.TEXT and not self.wa_id:
            raise ValueError("wa_id is mandatory when part_type is 'text'")
        return self


class CurationMeta(BaseModel):
    modified: bool = False
    modified_at: datetime | None = None
    modified_by: str | None = None
    edit_version: int = 0


class SourceMeta(BaseModel):
    updated_at: datetime | None = None


class ImportMeta(BaseModel):
    last_run_at: datetime | None = None
    last_result: str | None = None


class RecordOutput(BaseModel):
    id: str
    origin: Origin | None = None
    record_status: RecordStatus | None = None
    canonical_id: str | None = None
    curation: CurationMeta | None = None
    source_meta: SourceMeta | None = None
    import_meta: ImportMeta | None = None
    db_score: float | None = None


class PersonBase(BaseModel):
    pref_label_bo: str | None = None
    alt_label_bo: list[str] = Field(default_factory=list)
    dates: str | None = None


class PersonInput(PersonBase):
    modified_by: str


class PersonOutput(PersonBase, RecordOutput):
    pass


class WorkBase(BaseModel):
    pref_label_bo: str | None = None
    alt_label_bo: list[str] = Field(default_factory=list)
    authors: list[str] = Field(default_factory=list)
    versions: list[str] = Field(default_factory=list)


class WorkInput(WorkBase):
    modified_by: str


class WorkOutput(WorkBase, RecordOutput):
    pass


class MergeRequest(BaseModel):
    canonical_id: str
    modified_by: str


class VolumeBase(BaseModel):
    vol_version: str | None = None
    etext_source: str | None = None
    volume_number: int | None = None
    wa_id: str | None = None
    mw_id: str | None = None
    status: VolumeStatus = VolumeStatus.ACTIVE
    pages: list[PageEntry] = Field(default_factory=list)
    segments: list[Segment] = Field(default_factory=list)


class VolumeInput(VolumeBase):
    pass


class VolumeOutput(VolumeBase):
    id: str
    rep_id: str
    vol_id: str
    nb_pages: int | None = None
    first_imported_at: datetime | None = None
    last_updated_at: datetime | None = None
    replaced_by: str | None = None
    cstart: int | None = None
    cend: int | None = None
    chunks: list[Chunk] = Field(default_factory=list)


class PaginatedResponse(BaseModel):
    total: int
    offset: int
    limit: int
    items: list[VolumeOutput] = Field(default_factory=list)


class ImportOCRRequest(BaseModel):
    rep_id: str
    vol_id: str
    vol_version: str
    etext_source: str


class VolumeAnnotationInput(BaseModel):
    """Input model for annotated volume from frontend."""

    model_config = ConfigDict(extra="forbid")

    rep_id: str
    vol_id: str
    vol_version: str
    status: VolumeStatus
    base_text: str  # The base text (not chunked)
    segments: list[AnnotatedSegment]

    @field_validator("segments")
    @classmethod
    def validate_segments_non_empty(cls, v: list[AnnotatedSegment]) -> list[AnnotatedSegment]:
        """Validate that segments list is not empty."""
        if not v:
            raise ValueError("segments list cannot be empty")
        return v

    @field_validator("segments")
    @classmethod
    def validate_mw_ids_unique(cls, v: list[AnnotatedSegment]) -> list[AnnotatedSegment]:
        """Validate that all mw_ids are unique."""
        mw_ids = [seg.mw_id for seg in v]
        if len(mw_ids) != len(set(mw_ids)):
            duplicates = [mw_id for mw_id in mw_ids if mw_ids.count(mw_id) > 1]
            raise ValueError(f"mw_id values must be unique. Duplicates found: {set(duplicates)}")
        return v


class CatalogBreakdown(BaseModel):
    with_preexisting_catalog: int = 0
    no_preexisting_catalog: int = 0


class Stats(BaseModel):
    nb_volumes_imported: CatalogBreakdown = Field(default_factory=CatalogBreakdown)
    nb_volumes_finished: CatalogBreakdown = Field(default_factory=CatalogBreakdown)
    nb_segments_total: int = 0
    nb_works_total: int = 0
    nb_persons_total: int = 0
    nb_merges_identified: int = 0
