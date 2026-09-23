"""Response schemas for the pipeline results endpoints."""

from pydantic import BaseModel, Field

from app.models.catalog import LabelInfo  # noqa: F401 — re-exported for results consumers


class PerSubjectFileStatus(BaseModel):
    """Availability of a single per-subject output file."""

    available: bool
    download_path: str | None = Field(
        default=None,
        description="Relative path for use with GET /projects/{id}/files/download.",
    )


class PerSubjectOutput(BaseModel):
    """A named per-subject output type (e.g. a segmentation NIfTI)."""

    id: str = Field(description="Output identifier as declared in the pipeline results spec.")
    type: str = Field(description="Output type, e.g. 'segmentation_nifti'.")
    display_name: str | None = Field(
        default=None,
        description=(
            "Human-readable label for this overlay, e.g. 'DLMUSE segmentation'. "
            "Use as a subtitle in the MRI panel (fall back to id when absent)."
        ),
    )
    subjects: dict[str, PerSubjectFileStatus] = Field(
        description="Map of MRID → file availability and download path."
    )


class BatchFeaturesResult(BaseModel):
    """Summary of the pipeline's batch-level feature CSV."""

    available: bool
    download_path: str | None = Field(
        default=None,
        description="Relative path for use with GET /projects/{id}/files/download.",
    )
    columns: list[str] = Field(
        default_factory=list,
        description="Feature columns in the CSV (MRID column excluded).",
    )
    row_count: int = Field(default=0)
    label_map: dict[str, LabelInfo] | None = Field(
        default=None,
        description=(
            "Maps each feature column to its segmentation label information. "
            "Columns not present in the label map have no segmentation correspondence. "
            "Present only when a label_map resource is declared in the pipeline YAML."
        ),
    )
    column_units: dict[str, str] | None = Field(
        default=None,
        description=(
            "Maps each feature column name to its unit string (e.g. 'mm³', 'years', 'a.u.'). "
            "Populated from ``default_unit`` / ``column_units`` in the pipeline YAML. "
            "For segmentation pipelines the unit is also present on each ``label_map`` entry. "
            "Null when no units are declared for this pipeline."
        ),
    )


class SubjectCompleteness(BaseModel):
    """Overall completeness of a subject's pipeline outputs."""

    complete: bool
    missing: list[str] = Field(
        description="IDs of per_subject outputs that are missing for this subject."
    )


class PipelineResultSummary(BaseModel):
    """Quick summary of a pipeline's result availability within a project."""

    pipeline_id: str
    pipeline_name: str
    has_batch_features: bool = Field(
        description="True if the batch feature CSV exists in the project."
    )
    expected_batch_features_file: str | None = Field(
        default=None,
        description=(
            "Project-relative path where this pipeline's batch feature CSV is expected "
            "(from the pipeline's results.batch_features.file). Present whenever the "
            "pipeline declares a batch_features spec, regardless of has_batch_features — "
            "lets the UI tell the user exactly what file is missing."
        ),
    )
    per_subject_ids: list[str] = Field(
        description="IDs of declared per-subject output types."
    )
    has_atlas: bool = Field(
        description="True if the atlas resource file is present on the server."
    )


class PipelineResultDetail(BaseModel):
    """Full result detail for one pipeline within a project."""

    pipeline_id: str
    pipeline_name: str
    batch_features: BatchFeaturesResult | None = None
    per_subject: list[PerSubjectOutput] = Field(default_factory=list)
    atlas_resource_path: str | None = Field(
        default=None,
        description=(
            "Resource path for the brain atlas NIfTI. "
            "Fetch with GET /catalog/resources/{path}."
        ),
    )
    atlas_segmentation_resource_path: str | None = Field(
        default=None,
        description=(
            "Resource path for the atlas segmentation NIfTI "
            "(reference-mode overlay when no subject data is selected). "
            "Fetch with GET /catalog/resources/{path}."
        ),
    )
    subjects: dict[str, SubjectCompleteness] = Field(
        default_factory=dict,
        description="Per-subject completeness across all declared per_subject outputs.",
    )
