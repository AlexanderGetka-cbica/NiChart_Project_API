# Pipeline YAML Schema

A pipeline YAML declares an ordered sequence of tool steps that transform user data,
along with data prerequisites, user-overridable parameters, and result metadata used
by the results and visualization endpoints.

Pipeline files live in `resources/pipelines/` and are named `<pipeline_id>.yaml`.
The filename stem (`<pipeline_id>`) is the identifier used in API paths and in
`harmonized_variant` / `base_variant` cross-references.

---

## Full schema

```yaml
# ── Identity ──────────────────────────────────────────────────────────────────
pipeline_name: string     # Human-readable display name shown in the UI.
description: |            # Multi-line description. Rendered in the pipeline catalog.
  ...

# ── Variant cross-references ──────────────────────────────────────────────────
# Link harmonized and base versions of the same conceptual pipeline.
# Set exactly one of these (not both) if a counterpart exists; omit otherwise.
harmonized_variant: <pipeline_id>   # ID of the harmonized version. Set on base pipelines.
base_variant: <pipeline_id>         # ID of the base version. Set on harmonized pipelines.

# ── Categories ────────────────────────────────────────────────────────────────
# Free-form tags. Used by the UI for filtering and grouping.
# Include "harmonized" on every harmonized pipeline — the API reads this to set
# is_harmonized: true in the catalog response.
# Well-known values: image-processing, segmentation, feature-extraction,
#   inference, classification, spare, lesions, harmonized, highlighted, testing
categories:
  - tag1
  - tag2

# ── Prerequisites ─────────────────────────────────────────────────────────────
# Conditions that must be met before the pipeline can run.
# Each entry is either a plain string keyword or a structured dict.
requires:
  # Simple keywords — checked against the project's uploaded data:
  - needs_T1               # Project must have at least one file in t1/
  - needs_T1w              # Alias for needs_T1
  - needs_FLAIR            # Project must have at least one file in fl/
  - needs_FL               # Alias for needs_FLAIR
  - needs_T2               # Project must have at least one file in t2/
  - needs_T1CE             # Project must have at least one file in t1ce/
  - needs_ADC              # Project must have at least one file in adc/
  - needs_PET              # Project must have at least one file in pet/
  # Imaging modalities are defined centrally in app/modalities.py — needs_<code>
  # works for every registered modality (see GET /catalog/modalities).
  - needs_idat             # Project must have paired {MRID}_Red.idat + {MRID}_Grn.idat in idat/
  - needs_demographics     # Project must have a participants/participants.csv

  # When two or more imaging modality keywords are listed (e.g. needs_T1 + needs_FLAIR),
  # the readiness check also verifies that at least one subject has *both* files —
  # i.e. the same MRID stem appears in every required modality directory.
  # The response includes a `complete_sets` field listing complete vs. incomplete subjects.

  # Column validation — checks that participants.csv has the named columns,
  # optionally with type and range constraints. Used to drive client-side CSV
  # validation UI and server-side readiness checks.
  - csv_has_columns:
      - name: MRID
        type: string                       # string | int | float | categorical
      - name: Age
        type: float
        min: 0                             # Inclusive lower bound (numeric types).
        max: 120                           # Inclusive upper bound (numeric types).
      - name: Sex
        type: categorical
        values: [M, F]                     # Exhaustive list of accepted values.
        description: Biological sex.       # Optional. Shown in the UI.

  # Minimum subject count — shown as a readiness warning before submission:
  - min_subjects:
      required: 3          # Pipeline will refuse to run below this count.
      recommended: 30      # UI shows a warning below this count but still allows submission.

# ── User-overridable parameters ───────────────────────────────────────────────
# Parameters that the user can supply at submit time via the `params` request body.
# Omit the section (or use {}) if the pipeline has no user-facing parameters.
# Step-level `params` blocks in the steps section are separate and not overridable.
parameters:
  <param_name>:
    type: int | float | bool | str
    default: <value>
    description: string     # Shown in the UI.
    choices: [val1, val2]   # Optional. Restricts to an explicit set of values.
    min: <number>           # Optional. Inclusive lower bound (numeric types).
    max: <number>           # Optional. Inclusive upper bound (numeric types).

# ── Steps ─────────────────────────────────────────────────────────────────────
# Ordered list. Steps execute sequentially; each step must complete successfully
# before the next begins.
steps:
  - id: step_id             # Unique within this pipeline. Used in cross-step references.
    tool: <tool_id>         # Basename of a file in resources/tools/ (without .yaml).

    # Input slot assignments. Keys must match slot labels declared in the tool's
    # `inputs` section. Values are either:
    #   ${STUDY}/...           — absolute path within the project directory.
    #   ${<step_id>.outputs.<slot>} — output of a previous step (not yet implemented;
    #                               use ${STUDY}/... paths matched to the prior step's output).
    inputs:
      <slot_label>: ${STUDY}/path/to/input

    # Output slot assignments. Keys must match slot labels declared in the tool's
    # `outputs` section.
    outputs:
      <slot_label>: ${STUDY}/path/to/output

    # Fixed parameter values for this step. These are NOT user-overridable.
    # User-supplied params flow through automatically when the param name matches.
    params:
      <param_name>: <value>   # Optional. Omit or use {} if no overrides needed.

# ── Results metadata ──────────────────────────────────────────────────────────
# Describes the pipeline's outputs for the results and visualization endpoints.
# Omit the entire section if the pipeline has no structured results to display.
results:

  # Batch-level feature CSV (one row per subject across all subjects):
  batch_features:
    file: "relative/path/to/output.csv"   # Relative to the project root.
    mrid_column: "MRID"                    # Name of the subject-ID column (default: MRID).

    # Unit annotations for feature columns.
    # default_unit applies to all columns not listed in column_units.
    # column_units provides per-column overrides.
    # Both are optional; omit if units are not applicable or unknown.
    default_unit: "mm³"
    column_units:
      SomeColumn: "years"
      AnotherColumn: "a.u."

    # Feature definitions — semantic interpretation of output columns, surfaced to
    # the results API, the UI, and the MCP server (so an invoking LLM can explain
    # opaque columns like `y_score` or `prediction`). Sparse: list only columns
    # that need explanation; keyed by exact CSV column name. Columns absent from
    # this map are simply undocumented. Segmentation/ROI pipelines that carry a
    # `label_map` (below) document their columns that way instead — a `features`
    # block is optional there.
    #
    # Completeness is enforced by `scripts/check_feature_docs.py`. New pipelines
    # ship an empty `features: {}` placeholder; fill it in and remove any TODOs.
    #
    # Per-column fields:
    #   definition    REQUIRED — plain-language description (1–3 sentences).
    #   label         optional — human-readable display name.
    #   units         optional — e.g. 'mm³', 'years', 'logit', 'a.u.'.
    #   direction     optional — how to read magnitude ('higher = more X').
    #   interpretation optional — caveats/thresholds ('not a diagnosis', ...).
    #   keywords      recommended — literature search terms (list of strings).
    #   references    recommended — curated citations (see below). A starting
    #     point for the reader/LLM, not an exhaustive bibliography.
    #     Each reference: citation (REQUIRED), doi OR url (optional but strongly
    #     preferred).
    features:
      y_score:
        label: "Amyloid-positivity score"
        definition: "Model logit; higher values indicate greater likelihood of amyloid positivity."
        units: "logit"
        direction: "higher = more likely amyloid-positive"
        interpretation: "Continuous score, not a diagnosis; apply the study threshold to dichotomize."
        keywords:
          - "amyloid PET"
          - "Centiloid"
          - "amyloid positivity"
        references:
          - citation: "Author et al. YYYY, Journal"   # TODO: real citation
            doi: "10.xxxx/xxxxx"                       # TODO: real DOI (or use `url:`)

    # Segmentation label map — only for pipelines whose features correspond to
    # atlas parcellation labels (e.g. DLMUSE volume outputs).
    label_map: "atlases/<name>/mapping.csv"   # Path relative to resources/.
    # CSV format (no header): label_id, display_name[, constituent_id1, ...]
    # Trailing constituent IDs indicate this region aggregates multiple labels.

    # Template for constructing column names from label IDs in the label_map.
    # {id} is replaced with the label_id from the mapping CSV.
    column_template: "DL_MUSE_Volume_{id}"    # Default: "{id}"

    # Named groups for hierarchical display (e.g. nested dropdowns in the UI).
    # label_id_range: [lo, hi] — inclusive range of primary label IDs in this group.
    # Omit label_id_range on the last group to catch all remaining columns.
    feature_groups:
      - name: "Global"
        label_id_range: [600, 702]
      - name: "Individual Regions"    # no label_id_range → catches all remaining

  # Per-subject outputs (one file per subject, e.g. segmentation NIfTIs):
  per_subject:
    - id: "segmentation"                      # Unique identifier for this output type.
      pattern: "output_dir/{MRID}_seg.nii.gz" # {MRID} is replaced with each subject ID.
      type: "segmentation_nifti"              # Output type hint for the UI renderer.
                                              # Use "nifti" for non-segmentation NIfTIs.
      display_name: "My Segmentation"         # Optional. Label in the MRI panel.

  # Atlas files for the centile/overlay viewer.
  # Paths are relative to the resources/ directory.
  atlas: "atlases/<name>/atlas.nii.gz"
  atlas_segmentation: "atlases/<name>/atlas_seg.nii.gz"
```

---

## Path variables

| Variable | Expands to |
|---|---|
| `${STUDY}` | Absolute path to the project directory for the current user and project |

Paths are resolved at job submission time. The `${STUDY}` prefix is mandatory for all
input and output paths — the pipeline executor substitutes it before calling the backend.

---

## Step execution and caching

Steps run sequentially. After each successful step the executor writes a
`_provenance.json` file in each output directory recording the tool ID, input paths,
params, and finish time.

A step is skipped (cache hit) when:
- Its cache key `MD5(tool_id | json(inputs) | json(params))` matches a prior run, AND
- All input paths have modification times earlier than the prior `finished_time`.

Cache reuse can be disabled per-submission via `reuse_cached_steps: false` in the
submit request body.

---

## Harmonized pipeline conventions

- The base pipeline sets `harmonized_variant: <id>` pointing to the harmonized version.
- The harmonized pipeline sets `base_variant: <id>` and includes `harmonized` in `categories`.
- **Output paths must differ** between base and harmonized variants. Use distinct
  directory names (e.g. `ml_biomarkers/` vs `ml_biomarkers_harmonized/`) so that running
  one does not make the other appear complete in the results UI.

---

## Disabling a pipeline

Add its `<pipeline_id>` (one per line) to `resources/pipelines/disabled.txt`.
Lines beginning with `#` are treated as comments.

---

## Template

Copy this block into a new `<pipeline_id>.yaml` file and fill in the values.

```yaml
pipeline_name: My Pipeline Name
description: |
  One-paragraph description of what this pipeline does.

# harmonized_variant: my_pipeline_harmonized   # Set if a harmonized version exists.
# base_variant: my_pipeline_base               # Set if this IS the harmonized version.

categories:
  - my-category

requires:
  - needs_T1
  # - needs_demographics
  # - csv_has_columns:
  #     - name: MRID
  #       type: string
  #     - name: Age
  #       type: float
  #       min: 0
  #       max: 120

# parameters:
#   my_param:
#     type: int
#     default: 10
#     description: Description shown in the UI.

steps:
  - id: step_one
    tool: my_tool
    inputs:
      input_data: ${STUDY}/t1
    outputs:
      output_data: ${STUDY}/my_output
    params: {}

# results:
#   batch_features:
#     file: "my_output/results.csv"
#     mrid_column: "MRID"
#     default_unit: "mm³"
#   per_subject:
#     - id: "segmentation"
#       pattern: "my_output/{MRID}_seg.nii.gz"
#       type: "segmentation_nifti"
#       display_name: "My Segmentation"
```
