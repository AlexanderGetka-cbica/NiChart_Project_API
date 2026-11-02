"""
Response schemas for the pipeline/tool catalog endpoints.
"""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class ModalityInfo(BaseModel):
    """An imaging modality the platform recognizes."""

    code: str = Field(description="Canonical modality code — also the study subdirectory name and the key used in uploads/requirements.")
    label: str = Field(description="Human-readable label, e.g. 'T1-weighted'.")
    dir: str = Field(description="Study subdirectory where images of this modality are stored (${STUDY}/{dir}/).")


class ParameterSpec(BaseModel):
    """Specification for a single configurable parameter."""

    type: str = Field(description="Python type name: 'int', 'float', 'bool', or 'str'.")
    default: Any | None = Field(default=None, description="Default value if not supplied by the caller.")
    description: str | None = Field(default=None, description="Human-readable description for UI rendering.")
    choices: list[Any] | None = Field(default=None, description="Exhaustive list of allowed values, if constrained.")
    min: float | None = Field(default=None, description="Minimum value (numeric types only).")
    max: float | None = Field(default=None, description="Maximum value (numeric types only).")


class SlotContentRule(BaseModel):
    """One expected-content rule for a tool's directory slot.

    Declares what a directory normally holds, so that a directory populated by
    something other than the producing tool — a manual intermediate upload —
    can be checked against the same expectation. Without such a declaration the
    naming convention exists only inside the tool's ``command`` string (e.g.
    ``--dlmuse_suff _DLMUSE.nii.gz``) and nothing can verify it.

    Uses the same ``{MRID}`` placeholder as a pipeline's
    ``results.per_subject[].pattern``; the two are cross-checked by
    scripts/check_pipeline_tool_filenames.py so they cannot drift apart.
    """

    pattern: str = Field(
        description=(
            "Expected file name relative to the slot directory. ``{MRID}`` is "
            "substituted per subject; any other character is matched as a glob, "
            "so ``*.nii.gz`` is also valid."
        )
    )
    per_subject: bool = Field(
        default=False,
        description=(
            "True when one matching file is expected for every subject in the "
            "project. False for a single fixed file such as a summary CSV."
        ),
    )
    csv_columns: list[str] | None = Field(
        default=None,
        description=(
            "Column names the file must contain when parsed as CSV. "
            "Null for non-CSV files."
        ),
    )


class IOField(BaseModel):
    """An input or output slot on a tool."""

    type: Literal["file", "directory"] = Field(description="Whether this slot is a single file or a directory.")
    description: str | None = Field(default=None, description="Human-readable description of this slot.")
    merge: Literal["directory_union", "directory_union_csv_concat", "csv_concat"] | None = Field(
        default=None,
        description=(
            "Output merge strategy used when the tool runs in parallel chunks. "
            "Omit (or null) for input slots. "
            "'directory_union': copy all files from each chunk output into the final directory — "
            "NIfTI filenames must be unique across chunks (MRID-keyed). "
            "'directory_union_csv_concat': same as directory_union, but CSV files with matching "
            "names across chunks are row-concatenated (header kept once). "
            "'csv_concat': concatenate a single CSV file output."
        ),
    )
    contents: list[SlotContentRule] | None = Field(
        default=None,
        description=(
            "Files this directory slot is expected to hold. Declared on "
            "directory slots only; omit for file slots, which use "
            "``csv_columns`` instead. Null when the tool declares nothing, "
            "in which case an upload to this slot is checked only against the "
            "paths the consuming pipeline steps actually read."
        ),
    )
    csv_columns: list[str] | None = Field(
        default=None,
        description=(
            "For a file slot: column names the file must contain when parsed "
            "as CSV. Null for non-CSV file slots and for directory slots."
        ),
    )


class ResourceSpec(BaseModel):
    """Compute resources required by a tool."""

    vcpus: int = Field(description="Number of virtual CPUs.")
    memory: int = Field(description="Memory in MiB.")
    gpus: int = Field(default=0, description="Number of GPUs required.")


class ToolSummary(BaseModel):
    """Abbreviated tool information for list responses."""

    id: str = Field(description="Tool identifier (YAML basename without extension).")
    name: str = Field(description="Human-readable tool name.")
    description: str | None = Field(default=None)


class ToolDetail(ToolSummary):
    """Full tool specification."""

    inputs: dict[str, IOField] = Field(description="Named input slots.")
    outputs: dict[str, IOField] = Field(description="Named output slots.")
    resources: ResourceSpec
    parameters: dict[str, ParameterSpec] = Field(default_factory=dict)
    time_per_subject_seconds: float | None = Field(
        default=None,
        description=(
            "Expected wall-clock seconds to process one subject. "
            "Used by GET /cloud/status to estimate queue-drain time."
        ),
    )
    fixed_seconds: float | None = Field(
        default=None,
        description=(
            "One-off wall-clock seconds per invocation regardless of cohort size — "
            "container start, model loading, interpreter warm-up. A run's estimate is "
            "fixed_seconds + time_per_subject_seconds x subjects. Tools whose cost is "
            "almost entirely startup declare only this."
        ),
    )
    parallelizable: bool = Field(
        default=False,
        description=(
            "When True, the pipeline orchestrator may split directory inputs into subject chunks "
            "and run them as parallel backend jobs, then merge the results."
        ),
    )
    subjects_per_chunk: int | None = Field(
        default=None,
        description=(
            "Default number of subjects per parallel chunk for this tool. "
            "Null means use the server global default (10). "
            "Only meaningful when parallelizable is True."
        ),
    )
    github_url: str | None = Field(
        default=None,
        description="Link to the tool's source code repository on GitHub.",
    )


class PipelineStep(BaseModel):
    """A single step within a pipeline definition."""

    id: str = Field(description="Step identifier, unique within the pipeline.")
    name: str | None = Field(
        default=None,
        description=(
            "Human-readable step name: the step's optional 'name' key in the pipeline "
            "YAML, falling back to the invoked tool's name. Null when neither exists — "
            "deliberately NOT the raw id, so a client can choose its own fallback."
        ),
    )
    description: str | None = Field(
        default=None,
        description=(
            "Step description: the step's optional 'description' key in the pipeline "
            "YAML, falling back to the invoked tool's description. Null when neither exists."
        ),
    )
    tool: str = Field(description="Tool ID this step invokes.")
    inputs: dict[str, str] = Field(description="Input slot → path template mapping.")
    outputs: dict[str, str] = Field(description="Output slot → path template mapping.")
    params: dict[str, Any] = Field(default_factory=dict, description="Parameter overrides for this step.")


class PipelineSummary(BaseModel):
    """Abbreviated pipeline information for list responses."""

    id: str = Field(description="Pipeline identifier (YAML basename without extension).")
    name: str = Field(description="Human-readable pipeline name.")
    description: str | None = Field(default=None)
    input_notes: list[str] = Field(
        default_factory=list,
        description=(
            "Short user-facing notes about input requirements that readiness checks "
            "can't verify (e.g. 'Upload raw, unprocessed T1 images'). Plain text, "
            "shown verbatim. Empty when the pipeline declares none."
        ),
    )
    categories: list[str] = Field(default_factory=list)
    requires: list[str] = Field(
        default_factory=list,
        description="Data prerequisites (e.g. 'needs_T1', 'needs_demographics').",
    )
    is_harmonized: bool = Field(
        default=False,
        description="True if this pipeline applies harmonization to its inputs.",
    )
    harmonized_variant: str | None = Field(
        default=None,
        description=(
            "Pipeline ID of the harmonized version of this pipeline. "
            "Present on base pipelines only; null on harmonized pipelines and those "
            "with no harmonized counterpart. Use to render a 'Switch to harmonized' action."
        ),
    )
    base_variant: str | None = Field(
        default=None,
        description=(
            "Pipeline ID of the standard (non-harmonized) version of this pipeline. "
            "Present on harmonized pipelines only; null on base pipelines and those "
            "with no base counterpart. Use to render a 'Switch to standard' action."
        ),
    )
    docs_id: str | None = Field(
        default=None,
        description=(
            "Documentation topic identifier for this pipeline. "
            "Use with GET /catalog/docs/{docs_id} to retrieve the manifest, and "
            "GET /catalog/docs/{docs_id}/{file} to fetch individual sections. "
            "Multiple pipelines may share the same docs_id (e.g. harmonized variants)."
        ),
    )
    root_pipeline: str | None = Field(
        default=None,
        description=(
            "Grouping key for this pipeline, used to cluster related variants "
            "(e.g. base/harmonized/CVM flavours) under a single entry in the UI. "
            "Pipelines sharing the same root_pipeline belong to the same group; "
            "null if the pipeline YAML does not declare one."
        ),
    )
    harmonized: bool = Field(
        default=False,
        description=(
            "True if this pipeline applies harmonization to its inputs. "
            "Explicit counterpart to the 'harmonized' category tag, for callers "
            "that want to filter/group variants without scanning categories."
        ),
    )
    modalities: list[str] = Field(
        default_factory=list,
        description=(
            "Imaging/data modalities this pipeline requires (e.g. ['T1'], ['T1', 'FLAIR'], "
            "['PET'], ['idat']), derived from its 'needs_*' prerequisites. "
            "Empty for pipelines with no modality requirement (e.g. the test pipeline)."
        ),
    )


class ColumnSpec(BaseModel):
    """Validation schema for a single required participants.csv column."""

    name: str = Field(description="Column name as it must appear in the CSV header.")
    type: Literal["string", "int", "float", "categorical"] = Field(
        default="string",
        description=(
            "'string' — any non-empty text. "
            "'int' — whole number, optionally bounded by min/max. "
            "'float' — decimal number, optionally bounded by min/max. "
            "'categorical' — must be one of the strings listed in values."
        ),
    )
    min: float | None = Field(
        default=None,
        description="Inclusive lower bound for numeric types. Null means no lower bound.",
    )
    max: float | None = Field(
        default=None,
        description="Inclusive upper bound for numeric types. Null means no upper bound.",
    )
    values: list[str] | None = Field(
        default=None,
        description="Exhaustive list of accepted values for categorical columns.",
    )
    description: str | None = Field(
        default=None,
        description="Human-readable description shown in the UI alongside the column input.",
    )


class SubjectPredicate(BaseModel):
    """One condition on a participants.csv column, used to define a subject cohort.

    Conditions combine with AND within a single constraint. String comparisons
    (``equals``, ``in``) are case-insensitive and whitespace-stripped; numeric
    bounds (``min``, ``max``) are inclusive, matching ColumnSpec.
    """

    model_config = ConfigDict(populate_by_name=True)

    column: str = Field(description="participants.csv column this condition applies to.")
    equals: str | None = Field(
        default=None,
        description="Cell must equal this value, compared case-insensitively.",
    )
    in_: list[str] | None = Field(
        default=None,
        alias="in",
        description="Cell must be one of these values, compared case-insensitively.",
    )
    min: float | None = Field(
        default=None, description="Inclusive numeric lower bound. Null means unbounded."
    )
    max: float | None = Field(
        default=None, description="Inclusive numeric upper bound. Null means unbounded."
    )


class SubjectConstraintSpec(BaseModel):
    """A declared minimum number of subjects matching a predicate.

    Only subjects that have imaging *and* a participants.csv row satisfying every
    condition count towards this minimum.
    """

    label: str | None = Field(
        default=None, description="Human-readable cohort name, for display."
    )
    where: list[SubjectPredicate] = Field(
        default_factory=list,
        description="Conditions defining this cohort, combined with AND.",
    )
    required: int = Field(description="Minimum matching subjects needed to run at all.")
    recommended: int = Field(
        description="Recommended number of matching subjects for reliable results."
    )


class MinSubjectsSpec(BaseModel):
    """Declared subject-count requirement, with optional per-cohort minimums.

    Thresholds are returned with pipeline-YAML defaults already resolved, so a
    client never has to replicate the defaulting rules.
    """

    required: int = Field(description="Minimum total subjects needed to run at all.")
    recommended: int = Field(
        description="Recommended total number of subjects for reliable results."
    )
    constraints: list[SubjectConstraintSpec] = Field(
        default_factory=list,
        description=(
            "Per-cohort minimums, e.g. 'at least 30 subjects with DX == CN'. "
            "Empty when the pipeline declares none."
        ),
    )


class LabelInfo(BaseModel):
    """Display metadata for a single feature column.

    ``label_ids`` is only present for pipelines that produce a segmentation NIfTI.
    When present, the values are the voxel intensities in the atlas segmentation that
    together form this region and should be used to build ROI overlays.
    For pipelines without segmentation output, ``label_map`` on ``PipelineDetail``
    will be null rather than containing ``LabelInfo`` entries with empty label_ids.
    """

    display_name: str = Field(description="Human-readable region name.")
    label_ids: list[int] | None = Field(
        default=None,
        description=(
            "Voxel values in the segmentation NIfTI that together form this region. "
            "Null for pipelines that do not produce a segmentation output."
        ),
    )
    unit: str | None = Field(
        default=None,
        description=(
            "Physical unit for this column's values, e.g. 'mm³', 'years'. "
            "Null when no unit is declared for this pipeline."
        ),
    )


class FeatureGroup(BaseModel):
    """A named group of feature columns for hierarchical display in the UI."""

    name: str = Field(description="Display name for this group (e.g. 'Lobar', 'Global').")
    columns: list[str] = Field(description="Feature column names belonging to this group.")


class VarGroup(BaseModel):
    """A curated, cross-pipeline group of related variables.

    Defined in ``resources/dicts/dict_var_groups.yaml``. Each group is either a flat
    list of explicitly named variables (``values``, e.g. SPARE scores) or a
    prefix-based group of ROI-indexed variables (``prefix``, e.g. DLMUSE volumes) —
    never both. For prefix-based groups, combine ``prefix`` with a ROI index from one
    of ``roi_lists`` on ``VarGroupCatalogResponse`` to build a candidate raw column
    name, then resolve its display name via the owning pipeline's ``label_map``.
    """

    key: str = Field(description="Group identifier (YAML top-level key), e.g. 'group_dlmuse-vol'.")
    label: str = Field(description="Human-readable group name shown in the category selector.")
    desc: str | None = Field(default=None, description="Longer description for tooltips/help text.")
    category: str = Field(description="Coarse bucket for UI sectioning, e.g. 'demog', 'biomarker', 'roi'.")
    pipeline: list[str] = Field(
        default_factory=list,
        description="Pipeline IDs these variables come from; empty for cross-pipeline groups like demographics.",
    )
    vtype: str | None = Field(
        default=None,
        description="'name' for flat named variables; unset/null for ROI-indexed groups.",
    )
    values: list[str] | None = Field(
        default=None,
        description="Explicit raw column names belonging to this group. Mutually exclusive with prefix.",
    )
    prefix: str | None = Field(
        default=None,
        description=(
            "Column-name prefix for ROI-indexed variables, e.g. 'DL_MUSE_Volume_'. May be "
            "empty ('') when the CSV columns are the ROI names themselves (e.g. SynthSeg). "
            "Mutually exclusive with values."
        ),
    )
    atlas: str | None = Field(
        default=None,
        description=(
            "For prefix-based groups: the atlas whose roi_lists apply to this group "
            "(matches RoiList.atlas), e.g. 'muse' or 'synthseg'."
        ),
    )


class RoiList(BaseModel):
    """A curated sub-list of ROIs, used to narrow a prefix-based VarGroup.

    Defined in ``resources/dicts/dict_roi_lists.yaml``.
    """

    key: str = Field(description="List identifier (YAML top-level key), e.g. 'list_muse-all'.")
    desc: str | None = Field(default=None, description="Human-readable description of this list.")
    atlas: str = Field(description="Atlas these indices belong to, e.g. 'muse'.")
    values: list[int | str] = Field(
        description=(
            "ROIs in this list: integer indices (e.g. MUSE) or ROI names when the atlas's "
            "CSV columns are named by region (e.g. SynthSeg)."
        ),
    )


class VarGroupCatalogResponse(BaseModel):
    """Response from GET /catalog/var-groups."""

    groups: dict[str, VarGroup] = Field(description="All curated variable groups, keyed by group id.")
    roi_lists: dict[str, RoiList] = Field(description="All curated ROI sub-lists, keyed by list id.")


class FeatureDisplayMeta(BaseModel):
    """Display metadata for a single centile variable."""

    hidden: bool = Field(
        default=False,
        description="When True, the variable is excluded from the selector entirely.",
    )
    disabled: bool = Field(
        default=False,
        description="When True, the variable is shown in the selector but cannot be selected.",
    )
    label: str | None = Field(
        default=None,
        description="Display name override. When null the variable name is used as-is.",
    )
    group: str | None = Field(
        default=None,
        description="Logical grouping name. The UI may use this to render nested/categorised selectors.",
    )


class CentileFeatureMetadataResponse(BaseModel):
    """Response from GET /catalog/centiles/feature-metadata."""

    features: dict[str, FeatureDisplayMeta] = Field(
        description=(
            "Per-variable display metadata keyed by variable name (matching VarName in the centile CSVs). "
            "Only variables with non-default behaviour are included; variables absent from this map "
            "should be treated as visible and enabled."
        )
    )


class PipelineDetail(PipelineSummary):
    """Full pipeline definition including ordered steps and user-configurable parameters."""

    steps: list[PipelineStep] = Field(default_factory=list)
    parameters: dict[str, ParameterSpec] = Field(
        default_factory=dict,
        description=(
            "User-overridable parameters for this pipeline. "
            "Each entry describes the type, default, and optional constraints. "
            "Pass values via ``params`` in the pipeline submit body. "
            "Step-level params in the YAML are fixed by the pipeline author and "
            "cannot be overridden."
        ),
    )
    min_subjects: MinSubjectsSpec | None = Field(
        default=None,
        description=(
            "Subject-count requirement declared by this pipeline, including any "
            "per-cohort constraints. Null when the pipeline declares no "
            "min_subjects requirement. Lets a client show what a pipeline needs "
            "before any data is uploaded; GET /projects/{id}/readiness/{pipeline_id} "
            "reports how a specific project measures up."
        ),
    )
    atlas_resource_path: str | None = Field(
        default=None,
        description=(
            "Resource path for the brain atlas NIfTI declared by this pipeline. "
            "Fetch with GET /catalog/resources/{path}. "
            "Null if no atlas is declared or the file is not present on the server."
        ),
    )
    atlas_segmentation_resource_path: str | None = Field(
        default=None,
        description=(
            "Resource path for the atlas segmentation NIfTI declared by this pipeline. "
            "Fetch with GET /catalog/resources/{path}. "
            "Null if no atlas segmentation is declared or the file is not present."
        ),
    )
    label_map: dict[str, LabelInfo] | None = Field(
        default=None,
        description=(
            "Maps each batch-feature column name to its display name and, when the "
            "pipeline produces a segmentation, the constituent voxel label IDs in the "
            "atlas segmentation NIfTI. Null if no label_map resource is configured or "
            "the resource file is absent. Pipelines without segmentation output will "
            "have this field null."
        ),
    )
    feature_groups: list[FeatureGroup] | None = Field(
        default=None,
        description=(
            "Ordered grouping of batch-feature columns for hierarchical UI display "
            "(e.g. nested dropdowns). Null if the pipeline does not declare "
            "feature_groups in its YAML."
        ),
    )
    column_schemas: dict[str, ColumnSpec] = Field(
        default_factory=dict,
        description=(
            "Validation schema for each column declared in csv_has_columns. "
            "Keyed by column name. Use this to drive client-side CSV validation: "
            "type checking, numeric range enforcement, and categorical value lists. "
            "Columns not listed here have no declared schema (accept any non-empty value)."
        ),
    )
