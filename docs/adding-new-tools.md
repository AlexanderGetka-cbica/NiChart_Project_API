# Adding a New Form of Processing to NiChart

This document explains how to expose a new containerized processing step through the
NiChart API and UI, from Docker image to results viewer.

---

## Overview

NiChart processing is built from two kinds of YAML definitions:

- **Tools** (`resources/tools/<tool_id>.yaml`) — describe a single containerized step:
  what container to run, how to mount data into it, what parameters it accepts.
- **Pipelines** (`resources/pipelines/<pipeline_id>.yaml`) — chain one or more tools
  into a named workflow that users can submit via the UI.

The server reads these files at request time; no code changes or restarts are needed
when adding new tools or pipelines.

---

## Step 1 — Build and publish the container

The tool must be packaged as a Docker image that:

- Reads inputs from and writes outputs to paths supplied as command-line arguments.
- Exits with code 0 on success and non-zero on failure.
- Does not require network access after the image is pulled.

Push the image to a registry accessible from both the local Docker daemon and AWS Batch
(e.g. Docker Hub, ECR, GHCR).

For Singularity/Apptainer deployments, a `.sif` file must also be built and placed in
`NICHART_SIF_DIR`. Use the naming convention `image_tag.sif` (replace `/` and `:` with
`_`). See `resources/tools/SCHEMA.md` for the `singularity_run_mode` option.

---

## Step 2 — Write the tool YAML

Create `resources/tools/<tool_id>.yaml`. The tool ID becomes the identifier referenced
by pipeline steps and the `/catalog/tools/{tool_id}` endpoint.

```yaml
name: My Tool Name
description: What this tool does in one paragraph.

inputs:
  input_data:
    type: directory    # or: file — for a single input file

outputs:
  output_data:
    type: directory    # or: file

mounts:
  input_data:
    path_in_container: /input
    mode: ro
  output_data:
    path_in_container: /output
    mode: rw

parameters: {}         # Add entries if the tool has configurable options.

resources:
  vcpus: 2
  memory: 8000         # MiB
  gpus: 0              # Set to 1 for GPU tools.

time_per_subject_seconds: 60    # Seconds per subject; null for batch tools.

container:
  image: your-registry/your-image:tag
  command: --input {input_data} --output {output_data}
```

See `resources/tools/SCHEMA.md` for the full field reference.

**Key rules:**
- Every slot name in `inputs` and `outputs` must have a corresponding entry in `mounts`.
- Slot labels in the `command` template (`{slot_name}`) are replaced at runtime with the
  container-side `path_in_container` value for that mount.
- For `file`-type outputs the backend mounts the *parent directory*, so the container
  must write to the exact filename declared as `path_in_container`.

---

## Step 3 — Write the pipeline YAML

Create `resources/pipelines/<pipeline_id>.yaml`. A single-step pipeline that wraps your
new tool is the minimal case:

```yaml
pipeline_name: My Pipeline
description: |
  What this pipeline does.

categories:
  - my-category

requires:
  - needs_T1         # Remove or replace with your actual data requirements.

steps:
  - id: run_my_tool
    tool: my_tool_id    # Must match the basename of your tool YAML.
    inputs:
      input_data: ${STUDY}/t1
    outputs:
      output_data: ${STUDY}/my_output

results:
  batch_features:
    file: "my_output/results.csv"
    mrid_column: "MRID"
```

See `resources/pipelines/SCHEMA.md` for the full field reference including multi-step
pipelines, harmonized variants, segmentation label maps, and per-subject outputs.

---

## Step 4 — Declare data requirements

The `requires` list drives the readiness check (`GET /projects/{id}/readiness/{pipeline_id}`)
and determines what the UI shows before submission.

| Requirement | Meaning |
|---|---|
| `needs_T1` | At least one `.nii.gz` in `t1/` |
| `needs_FLAIR` | At least one `.nii.gz` in `fl/` |
| `needs_T2` | At least one `.nii.gz` in `t2/` |
| `needs_demographics` | `participants/participants.csv` exists |
| `csv_has_columns: [...]` | Named columns present and typed correctly |
| `min_subjects: {required: N, recommended: M}` | Subject count thresholds |

---

## Step 5 — Expose results to the UI

The `results` section of the pipeline YAML tells the API what outputs to surface in the
results and visualization endpoints. Without it the pipeline runs successfully but
produces no structured results in the UI.

### Tabular outputs (feature CSVs)

```yaml
results:
  batch_features:
    file: "my_output/results.csv"
    mrid_column: "MRID"
    default_unit: "mm³"        # Optional: unit string for all feature columns.
    column_units:              # Optional: per-column overrides.
      SomeSpecialColumn: "years"
```

This populates `GET /projects/{id}/results/{pipeline_id}` with column names, row count,
download path, and unit annotations.

#### Feature definitions (interpretation layer)

Add a `features` block to give each opaque output column a machine-readable meaning.
This is surfaced to the results API, the UI, and the MCP server, so an invoking LLM
can explain columns like `y_score` or `prediction` and cite relevant literature
instead of guessing from the column name:

```yaml
results:
  batch_features:
    file: "my_output/results.csv"
    mrid_column: "MRID"
    features:                       # keyed by exact CSV column name; list only
      y_score:                      # columns that need explanation (sparse).
        label: "Amyloid-positivity score"
        definition: "..."           # REQUIRED — plain-language meaning.
        units: "logit"
        direction: "higher = more likely amyloid-positive"
        interpretation: "Not a diagnosis; apply the study threshold."
        keywords: ["amyloid PET", "Centiloid"]
        references:
          - { citation: "Author et al. 2024, Journal", doi: "10.xxxx/xxxxx" }
```

Segmentation/ROI pipelines that carry a `label_map` (below) document their columns
that way, so a `features` block is optional there. See `resources/pipelines/SCHEMA.md`
for the full field reference, and run **`python scripts/check_feature_docs.py`** to see
which pipelines are missing definitions, keywords, references, or DOIs.

### Segmentation outputs with atlas correspondence

If your pipeline produces per-region volumes that map to an atlas segmentation, use the
`label_map` field to link each CSV column to its display name and voxel label IDs:

```yaml
results:
  batch_features:
    file: "my_output/volumes.csv"
    label_map: "atlases/my_atlas/mapping.csv"
    column_template: "Volume_{id}"    # {id} is replaced with the label_id from the CSV.
    default_unit: "mm³"
    feature_groups:
      - name: "Global"
        label_id_range: [100, 199]
      - name: "Individual Regions"    # No label_id_range → catches all remaining.
  atlas: "atlases/my_atlas/atlas.nii.gz"
  atlas_segmentation: "atlases/my_atlas/atlas_seg.nii.gz"
```

The label map CSV has no header and uses the format:

```
label_id, display_name[, constituent_id1, constituent_id2, ...]
```

### Per-subject NIfTI outputs

```yaml
results:
  per_subject:
    - id: "segmentation"
      pattern: "my_output/{MRID}_seg.nii.gz"
      type: "segmentation_nifti"     # or "nifti" for non-segmentation volumes
      display_name: "My Segmentation"
```

---

## Step 6 — Add a harmonized variant (optional)

If your pipeline benefits from harmonization, create a second pipeline YAML that:

1. Adds a harmonization step before the main analysis step.
2. Writes outputs to **different paths** than the base pipeline (e.g. append `_harmonized`
   to directory names). This prevents the results UI from showing both pipelines as
   complete after only one has run.
3. Includes `harmonized` in `categories`.
4. Sets `base_variant: <base_pipeline_id>`.
5. Sets `harmonized_variant: <harmonized_pipeline_id>` on the base pipeline.

---

## Step 7 — Write pipeline documentation

Documentation lives in `resources/docs/<docs_id>/` and is served by the API at
`GET /catalog/docs/{docs_id}/...`. It is entirely data-driven: adding a folder and
manifest is sufficient — no code changes or server restarts are needed.

### Choosing a docs_id

The `docs_id` is a topic identifier, not a pipeline identifier. Multiple pipelines can
share one topic. Use a single topic for conceptually related pipelines:

- `run_dlmuse` and `run_dlmuse_harmonized` → `docs_id: dlmuse`
- All four SPARE variants → `docs_id: spare`

If your pipeline has a harmonized variant, both should point to the same `docs_id`.

### Create the topic folder

```
resources/docs/<docs_id>/
├── manifest.yaml       ← required
├── overview.md         ← recommended starting point
└── images/             ← place image files here; reference them as ![](images/fig.png)
```

### Write the manifest

`resources/docs/<docs_id>/manifest.yaml`:

```yaml
title: "My Pipeline"
description: "One-sentence summary shown in the docs index."
pipelines:
  - my_pipeline
  - my_pipeline_harmonized   # list every pipeline ID covered by this topic

thumbnail: "images/thumbnail.png"   # optional card image

sections:
  - id: overview
    title: "Overview"
    file: overview.md
    audience: user        # user | developer | all
    type: markdown        # markdown | data | image

  - id: methodology
    title: "Methodology"
    file: methodology.md
    audience: user

  - id: developer_notes
    title: "Developer Notes"
    file: developer.md
    audience: developer   # shown in developer docs, hidden in the end-user UI
```

**`audience` values:**

| Value | When to use |
|---|---|
| `user` | End-user prose: what the pipeline does, what it produces, how to interpret results |
| `developer` | Technical details: tool YAML conventions, container interface, known limitations |
| `all` | Content relevant to both audiences (e.g. references, citation requests) |

### Writing markdown

Standard CommonMark markdown. Images referenced with relative paths resolve automatically:

```markdown
![Pipeline diagram](images/pipeline_diagram.png)
```

The front-end fetches images from `GET /catalog/docs/<docs_id>/images/pipeline_diagram.png`,
which is the same path prefix as the markdown file. No URL rewriting is needed.

For data-driven plots (centile curves, reference histograms), declare a JSON section
with `type: data` in the manifest and structure the file to match whatever the front-end
plot component expects.

### Link the pipeline YAML

Add `docs_id` to every pipeline YAML that belongs to this topic:

```yaml
pipeline_name: My Pipeline
docs_id: my_docs_topic    # add this line

harmonized_variant: my_pipeline_harmonized
categories:
  - ...
```

### What to write

**User-facing (`audience: user`)** — write for a clinical researcher who knows neuroimaging
but not the software stack. Cover:
- What the pipeline measures and why it matters
- What files it produces and how to interpret them
- Which variant to choose (harmonized vs standard) and when
- Minimum data requirements beyond the machine-readable `requires` list

**Developer-facing (`audience: developer`)** — write for someone integrating or
maintaining the tool. Cover:
- Container interface (expected input formats, output naming conventions)
- Known failure modes or edge cases
- Training data provenance or model versioning notes
- Citation or licensing requirements for the underlying tool

---

## Step 8 — Test locally

1. Start the API server: `docker compose up`
2. Verify the tool appears: `GET /catalog/tools/<tool_id>`
3. Verify the pipeline appears: `GET /catalog/pipelines` and `GET /catalog/pipelines/<pipeline_id>`
4. Verify docs appear: `GET /catalog/docs` and `GET /catalog/docs/<docs_id>`
5. Create a test project, upload data, and submit: `POST /projects/<id>/jobs/pipelines`
6. Poll for completion: `GET /jobs/pipelines/<run_id>`
7. Inspect results: `GET /projects/<id>/results/<pipeline_id>`

The `dummy_pipeline` / `dummy_sleep` pair in `resources/pipelines/` and `resources/tools/`
can be used as a minimal smoke-test pattern that runs without any real imaging data.

---

## Checklist

- [ ] Docker image built and pushed to registry
- [ ] `resources/tools/<tool_id>.yaml` created and validated against schema
- [ ] `resources/pipelines/<pipeline_id>.yaml` created and validated against schema
- [ ] Data requirements in `requires` match what the pipeline actually needs
- [ ] Output paths are distinct from any related pipeline variant (harmonized/base)
- [ ] `results` section declared so the UI can display outputs
- [ ] Unit annotations added to `batch_features` where applicable
- [ ] Feature definitions authored under `batch_features.features` (definition, keywords, references + DOIs); verify with `python scripts/check_feature_docs.py`
- [ ] Harmonized variant created and cross-referenced (if applicable)
- [ ] `resources/docs/<docs_id>/manifest.yaml` created
- [ ] At least one user-facing markdown section written (`audience: user`)
- [ ] `docs_id` added to all pipeline YAMLs covered by this topic
- [ ] Pipeline tested end-to-end locally (tool, pipeline, docs all verified)

---

## Adding a new imaging modality

Modalities (t1, fl, t2, t1ce, adc, pet, …) are defined in **one place**:
[`app/modalities.py`](../app/modalities.py). To add one (say PET), add a single
`Modality(...)` entry to `MODALITIES` — its code (the `${STUDY}/{code}/` upload
subdirectory), a label, the filename regex used to detect it on upload, the
suffix tokens to strip for the MRID, and any `needs_*` aliases.

Everything else derives from that entry automatically — no other edits:

- **File uploads** infer and route the modality to `${STUDY}/{code}/`.
- **Readiness** honors `needs_{code}` in a pipeline's `requires:` (plus aliases).
- **The CLI** accepts `--image {code}=/path` (a named flag like `--pet` is an
  optional one-line convenience, not required).
- **The MCP tool** accepts it via `images={"{code}": "/path"}`.
- **`GET /catalog/modalities`** lists it — clients (frontend, etc.) should read
  that endpoint instead of hard-coding the modality list.

Ordering note: put a more specific token before one it contains (`t1ce` before
`t1`). See the module docstring and `tests/test_modalities.py`.
