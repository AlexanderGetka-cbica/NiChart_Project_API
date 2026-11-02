"""
Request/response schemas for file management endpoints.
"""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class FileEntry(BaseModel):
    """A single node in the project file tree."""

    name: str = Field(description="Filename or directory name.")
    path: str = Field(description="Path relative to the project root.")
    type: Literal["file", "directory"] = Field(description="Node kind.")
    size: int | None = Field(default=None, description="File size in bytes (files only).")
    mtime: float | None = Field(default=None, description="Last-modified UNIX timestamp.")


class DirectoryTree(BaseModel):
    """Flat listing of all visible nodes in a project directory."""

    entries: list[FileEntry] = Field(default_factory=list)


class DownloadZipRequest(BaseModel):
    """Request body for bundling multiple files/directories into a single zip."""

    paths: list[str] = Field(
        min_length=1,
        description=(
            "Paths relative to the project root — a mix of files and directories is allowed. "
            "Each entry keeps its project-relative path inside the resulting archive, so the "
            "original directory structure is preserved."
        ),
    )


# ── NIfTI upload / staging ───────────────────────────────────────────────────

class NiftiUploadProposal(BaseModel):
    """Server-inferred metadata for a single uploaded NIfTI file."""

    filename: str = Field(description="Original uploaded filename.")
    inferred_mrid: str | None = Field(
        default=None, description="MRID inferred from the filename by stripping known suffixes."
    )
    inferred_modality: str | None = Field(
        default=None,
        description="Modality inferred from the filename (see GET /catalog/modalities), or null if not detectable.",
    )


class NiftiStagingResult(BaseModel):
    """Response after uploading NIfTI file(s); requires a follow-up commit call."""

    staging_id: str = Field(description="Opaque staging area identifier. Pass to the commit endpoint.")
    proposals: list[NiftiUploadProposal] = Field(
        description="Server's best-effort mapping of filenames to MRID and modality."
    )
    skipped_duplicates: list[str] = Field(
        default=[],
        description=(
            "Original relative paths that were skipped because they flattened to the same "
            "filename as an earlier entry in this upload (first occurrence wins)."
        ),
    )


class NiftiMapping(BaseModel):
    """Confirmed mapping for a single staged NIfTI file."""

    filename: str = Field(description="Filename as returned in the staging proposals.")
    mrid: str = Field(description="Subject identifier. Becomes the file stem in the target directory.")
    modality: str = Field(
        description="NiChart modality code (see GET /catalog/modalities). Determines which subdirectory the file lands in."
    )

    @field_validator("modality")
    @classmethod
    def _known_modality(cls, v: str) -> str:
        from app import modalities

        if not modalities.is_valid(v):
            raise ValueError(f"Unknown modality {v!r}. Valid: {list(modalities.MODALITY_CODES)}")
        return v


class NiftiCommitRequest(BaseModel):
    """Request body for the NIfTI staging commit endpoint."""

    mappings: list[NiftiMapping] = Field(
        description="One entry per staged file; every staged file must be accounted for."
    )


class CommittedFile(BaseModel):
    mrid: str
    modality: str
    path: str = Field(description="Path relative to the project root where the file was written.")


class NiftiCommitResult(BaseModel):
    """Response after a successful NIfTI commit."""

    committed: list[CommittedFile]
    skipped: list[CommittedFile] = Field(
        default=[],
        description=(
            "Entries whose target file already existed and were left untouched instead of "
            "being overwritten."
        ),
    )


# ── Participants CSV ──────────────────────────────────────────────────────────

class ParticipantRow(BaseModel):
    """One row from participants.csv.

    ``MRID`` is required. Any additional columns (Age, Sex, MMSE, etc.) are
    preserved as extra fields and round-trip through the API unchanged.
    """

    model_config = ConfigDict(extra="allow")

    MRID: str = Field(description="Subject identifier. Must match the stem of uploaded NIfTI files.")


class ParticipantsList(BaseModel):
    """List of participant rows."""

    rows: list[ParticipantRow] = Field(default_factory=list)


class ParticipantsUpdate(BaseModel):
    """Request body to replace the participants list."""

    rows: list[dict[str, Any]] = Field(
        description=(
            "Complete replacement list. Each row must contain an 'MRID' key. "
            "Any additional keys are written as extra columns in participants.csv. "
            "Existing participants.csv is overwritten."
        )
    )


# ── Manual intermediate uploads ───────────────────────────────────────────────
#
# Supports starting a pipeline from a later step when the user already holds the
# earlier steps' results. See GET /projects/{id}/files/intermediate-inputs/{pipeline_id}.


class SlotConsumer(BaseModel):
    """A step that reads from a slot, and the exact path it reads."""

    step_id: str = Field(description="Step that consumes this slot.")
    input: str = Field(description="That step's input label.")
    path: str = Field(
        description=(
            "Project-relative path the step actually reads. May be a file inside "
            "a directory slot, e.g. 'dlmuse_vol/DLMUSE_Volumes.csv' for the "
            "'dlmuse_vol' slot."
        )
    )


class SlotContentRequirement(BaseModel):
    """One file (or family of files) a slot must contain, and whether it does."""

    pattern: str = Field(
        description=(
            "Expected file name relative to the slot. '{MRID}' stands for a "
            "subject ID; anything else is matched as a glob."
        )
    )
    per_subject: bool = Field(
        description="True when one matching file is expected per project subject."
    )
    csv_columns: list[str] | None = Field(
        default=None,
        description="Columns the file must contain when parsed as CSV; null if not a CSV.",
    )
    source: Literal["pipeline_path", "tool_declared"] = Field(
        description=(
            "Where the requirement comes from. 'pipeline_path': a step at or after "
            "the start step reads this exact path, so it must exist whatever any "
            "tool declares. 'tool_declared': the producing tool's output contents "
            "or a consuming tool's input contents."
        )
    )
    status: Literal["met", "missing", "invalid"] = Field(
        description=(
            "'met': satisfied. 'missing': no matching file. 'invalid': a file "
            "matched but failed its check, e.g. missing a required CSV column."
        )
    )
    problem: str | None = Field(
        default=None,
        description="Plain-text explanation when status is not 'met'; shown verbatim.",
    )


class SlotCoverage(BaseModel):
    """Per-subject coverage of a slot that has at least one per-subject rule."""

    expected_subjects: int | None = Field(
        description=(
            "Number of subjects expected. Null when the project has neither "
            "imaging nor a participants.csv, so the expected set is unknowable — "
            "the normal case for a project whose raw data was never uploaded. "
            "Coverage is then reported but not used to decide status."
        )
    )
    matched: int = Field(description="Subjects for which every per-subject rule is satisfied.")
    missing_mrids: list[str] = Field(
        default_factory=list,
        description="Expected subjects with no matching file. Empty when expected_subjects is null.",
    )


class ProjectInputHint(BaseModel):
    """Which existing uploader a client should send the user to for a project input."""

    type: Literal["modality", "participants", "idat", "other"] = Field(
        description="Kind of project input this path holds."
    )
    code: str | None = Field(
        default=None,
        description="Modality code when type is 'modality' (e.g. 't1'); null otherwise.",
    )


class IntermediateSlot(BaseModel):
    """One pipeline path needed by the steps that will run, and its state on disk."""

    slot_id: str = Field(
        description=(
            "Opaque identifier, stable for a given (pipeline, start step). Pass it "
            "back verbatim when uploading. Never a path — the server derives the "
            "destination itself."
        )
    )
    path: str = Field(
        description="Project-relative path, '${STUDY}/' stripped. Display only."
    )
    type: Literal["directory", "file"] = Field(description="Whether the slot is a directory or a single file.")
    kind: Literal["upload", "project_input", "generated"] = Field(
        description=(
            "'upload': produced by a skipped earlier step, so it may be supplied by "
            "hand. 'project_input': raw project data that no step produces — use the "
            "normal images/participants uploaders. 'generated': a step that will run "
            "produces it, so it is shown but not checked."
        )
    )
    produced_by_step: str | None = Field(
        default=None,
        description=(
            "For 'upload', the skipped step that normally produces this slot. For "
            "'generated', the step that will produce it. Null for 'project_input'."
        ),
    )
    consumed_by: list[SlotConsumer] = Field(
        default_factory=list,
        description="Steps at or after the start step that read this slot.",
    )
    project_input_hint: ProjectInputHint | None = Field(
        default=None, description="Set only when kind is 'project_input'."
    )
    required_contents: list[SlotContentRequirement] = Field(
        default_factory=list,
        description="What the slot must hold. Empty for 'generated' slots and for undeclared project inputs.",
    )
    present: bool = Field(description="True when the slot exists on disk and is not empty.")
    coverage: SlotCoverage | None = Field(
        default=None, description="Null when the slot has no per-subject rule."
    )
    status: Literal["met", "partial", "missing"] = Field(
        description=(
            "'missing': absent or empty. 'met': every requirement satisfied and "
            "coverage full (or no per-subject rule). 'partial': anything else."
        )
    )
    problems: list[str] = Field(
        default_factory=list,
        description="Plain-text warnings about this slot, shown verbatim.",
    )


class IntermediateInputsResponse(BaseModel):
    """Everything the steps from ``start_from_step`` onwards need, and its state."""

    pipeline_id: str = Field(description="Pipeline these slots belong to.")
    start_from_step: str = Field(description="Step the run would start from.")
    slots: list[IntermediateSlot] = Field(
        description=(
            "One entry per distinct path the running steps need, in pipeline order. "
            "Slots that no step at or after the start step reads are omitted. Empty "
            "when the start step is the first step and nothing is skipped."
        )
    )


class RejectedUploadFile(BaseModel):
    """A file that was not written, and why."""

    name: str = Field(description="Name as sent by the client.")
    reason: str = Field(description="Plain-text reason, shown verbatim.")


class IntermediateUploadResult(BaseModel):
    """Outcome of one manual intermediate upload."""

    slot: IntermediateSlot = Field(description="The slot's state after the upload.")
    written: int = Field(description="Number of files written into the slot.")
    rejected: list[RejectedUploadFile] = Field(
        default_factory=list,
        description=(
            "Files not written because of a disallowed extension, an oversized "
            "file, or an unexpected name. These do not fail the request unless "
            "nothing valid remains."
        ),
    )
    skipped: list[str] = Field(
        default_factory=list,
        description=(
            "Files dropped as irrelevant rather than rejected: any path component "
            "starting with '.' and anything under '__MACOSX/'. Not errors — "
            "Finder-made archives almost always contain them."
        ),
    )
    replaced: bool = Field(
        default=False, description="True when the slot was cleared before writing."
    )
    cache_entries_cleared: int = Field(
        default=0,
        description=(
            "Cache entries dropped because their outputs were replaced, so the "
            "producing step will re-run on a normal full run."
        ),
    )
