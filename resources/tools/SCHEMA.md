# Tool YAML Schema

A tool YAML describes a single containerized processing step: the container image to run,
how to mount data into it, what parameters it accepts, and how many resources it needs.

Tool files live in `resources/tools/` and are named `<tool_id>.yaml`.
The filename stem (`<tool_id>`) is the identifier used in pipeline `steps[].tool` fields
and in the `GET /catalog/tools/{tool_id}` endpoint.

---

## Full schema

```yaml
# ── Identity ─────────────────────────────────────────────────────────────────
name: string              # Human-readable display name shown in the UI.
description: string       # One-paragraph description of what the tool does.

# ── I/O slots ────────────────────────────────────────────────────────────────
# Declare every data path the tool reads or writes.
# Each key is a slot label used in `mounts`, `inputs`, and `outputs` sections.
inputs:
  <slot_label>:
    type: directory | file   # "directory" for folders, "file" for a single file.
    description: string      # Optional. Shown in the tool catalog.

outputs:
  <slot_label>:
    type: directory | file
    description: string      # Optional.
    merge: directory_union | directory_union_csv_concat | csv_concat
    # Required only when parallelizable: true (see below). Tells the orchestrator how to
    # merge this output across parallel chunks:
    #   directory_union            — copy all files into the final output dir.
    #                                NIfTI filenames must be unique (MRID-keyed); use for
    #                                segmentation output dirs.
    #   directory_union_csv_concat — same, but CSV files with matching names across chunks
    #                                are row-concatenated (header kept once). Use when the
    #                                output dir contains both per-subject files AND a batch CSV.
    #   csv_concat                 — for a single file output that is a CSV; concatenates
    #                                rows across all chunks (header kept once).
    # Default when omitted: directory_union.

# ── Container mounts ─────────────────────────────────────────────────────────
# Map each slot label to a path inside the container and an access mode.
# Every slot declared in `inputs` and `outputs` must appear here.
mounts:
  <slot_label>:
    path_in_container: /path/inside/container   # Absolute path the tool will see.
    mode: ro | rw                               # ro = read-only, rw = read-write.

# ── Parameters ───────────────────────────────────────────────────────────────
# User- or pipeline-configurable scalar values injected into the command template.
# Use {} for no parameters.
parameters:
  <param_name>:
    type: int | float | bool | str   # Value type.
    default: <value>                 # Used when neither the pipeline nor user supplies a value.
    description: string              # Optional. Shown in the UI.
    choices: [val1, val2]            # Optional. Restricts allowed values to an explicit list.
    min: <number>                    # Optional. Inclusive lower bound (numeric types only).
    max: <number>                    # Optional. Inclusive upper bound (numeric types only).

# ── Resource requirements ─────────────────────────────────────────────────────
# Used by the cloud backend to size the AWS Batch job. Be accurate.
resources:
  vcpus: <int>       # Number of vCPUs.
  memory: <int>      # Memory in MiB. See https://docs.aws.amazon.com/batch/latest/APIReference/API_ResourceRequirement.html
  gpus: <int>        # 0 for CPU-only tools; 1 for tools that need a GPU.

# ── Timing hints ──────────────────────────────────────────────────────────────
# Expected wall-clock seconds to process one subject.
# Used by GET /cloud/status to estimate queue-drain time.
# Use null for batch tools that don't scale linearly with subject count.
time_per_subject_seconds: <float> | null

# One-off seconds per invocation, independent of cohort size: container start,
# model loading, interpreter warm-up. Optional.
#
# A run estimate is fixed_seconds + time_per_subject_seconds x subjects, with
# either treated as 0 when absent. Declare at least one of the two, or
# POST /projects/{id}/jobs/pipelines/plan reports the step as having no timing
# at all and leaves it out of the total.
#
# Measure it at two cohort sizes and solve for the slope and intercept rather
# than guessing: a startup-dominated tool given a per-subject-only figure will
# have its long-cohort estimate wildly overstated. For example harmonize_dlmuse
# measured 60.1 s at N=50 and 80.3 s at N=2485, i.e. ~60 s fixed plus
# ~0.008 s/subject — expressing that as a per-subject number alone would have
# predicted roughly 50 hours for 2485 subjects instead of 80 seconds.
fixed_seconds: <float> | null

# ── Parallelization ───────────────────────────────────────────────────────────
# When true, the orchestrator splits directory inputs by MRID across multiple
# backend jobs, waits for all to complete, then merges outputs using the
# per-output `merge` strategy declared above.
# Tools where every subject is processed independently are safe to mark true.
# Tools that require pairwise comparison or produce cross-subject statistics
# (e.g. harmonization models) must remain false.
parallelizable: true | false   # Default: false

# Default chunk size for this tool (subjects per parallel job).
# The orchestrator uses ceil(n_subjects / subjects_per_chunk) chunks, bounded
# to [1, n_subjects]. Omit to use the server global default (10).
subjects_per_chunk: <int>      # Optional.

# ── Source code ───────────────────────────────────────────────────────────────
github_url: <url>              # Optional. Link to the tool's GitHub repository.

# ── Container ─────────────────────────────────────────────────────────────────
container:
  image: <registry/image:tag>   # Docker image to pull and run.
  command: <template string>    # Command arguments passed to the container.
                                # Slot labels in {braces} are replaced at runtime with the
                                # container-side mount path (path_in_container).
                                # Parameter names in {braces} are replaced with their values.
                                # If the rendered command starts with "-", the Singularity
                                # backend uses "apptainer run"; otherwise "apptainer exec".
  singularity_run_mode: run | exec   # Optional. Override Singularity invocation mode explicitly.
```

---

## How slots, mounts, and the command template connect

```
pipeline YAML step                 tool YAML                     container
──────────────────────────────     ──────────────────────────    ──────────────────────
inputs:                            inputs:                       (bound read-only)
  t1_img: ${STUDY}/t1      ──►    t1_img: {type: directory}
                                   mounts:
                                     t1_img:
                                       path_in_container: /input/t1   ──►  /input/t1
                                       mode: ro

outputs:                           outputs:                      (bound read-write)
  dlmuse_vol: ${STUDY}/vol ──►    dlmuse_vol: {type: directory}
                                   mounts:
                                     dlmuse_vol:
                                       path_in_container: /output/vol  ──►  /output/vol
                                       mode: rw

                                   container:
                                     command: --in {t1_img} --out {dlmuse_vol}
                                                  ↑                   ↑
                                              becomes /input/t1   becomes /output/vol
```

**File-type outputs** — when `outputs.<slot>.type` is `file`, the backend mounts the
*parent directory* of the container path, not the file itself. The container must write
the file at the declared `path_in_container`. This prevents Docker from creating a
directory node at the file path when the destination doesn't exist yet.

---

## Parameter substitution

Parameters named in the `command` template are replaced with their resolved values before
the container is run. The resolution order is:

1. Value supplied by the pipeline step's `params` block.
2. Value supplied by the user at pipeline submit time.
3. `default` from this tool YAML.

Free-entry string parameters are validated against `^[a-zA-Z0-9._-]{1,256}` before
substitution to prevent command injection.

---

## Template

Copy this block into a new file, fill in the values, and delete unused optional fields.

```yaml
name: My Tool Name
description: One-paragraph description of what this tool does.

inputs:
  input_data:
    type: directory        # or: file

outputs:
  output_data:
    type: directory        # or: file

mounts:
  input_data:
    path_in_container: /input
    mode: ro
  output_data:
    path_in_container: /output
    mode: rw

parameters: {}             # Replace with parameter definitions or leave empty.

resources:
  vcpus: 2
  memory: 8000             # MiB
  gpus: 0

time_per_subject_seconds: null
# fixed_seconds: 10            # Uncomment for a tool whose cost is mostly startup.

parallelizable: false          # Set to true if each subject is processed independently.
# subjects_per_chunk: 10       # Uncomment and tune if parallelizable: true.
# github_url: https://github.com/org/repo

container:
  image: your-registry/your-image:tag
  command: --input {input_data} --output {output_data}
```
