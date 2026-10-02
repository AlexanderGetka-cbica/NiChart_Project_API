# Getting Started — NiChart Project API

A FastAPI server that drives the NiChart medical-imaging pipeline platform.
It manages user projects, handles file uploads, submits containerised
processing pipelines (locally via Docker or on AWS Batch), and serves results
to the React UI.

---

## Prerequisites

| Tool | Purpose |
|---|---|
| Docker + Docker Compose | Everything runs in containers — no host Python install needed |
| Python 3.x (host) | Only for `scripts/get-token.py` in cloud mode |
| AWS CLI + credentials | Cloud mode only |

---

## Local mode quick start

Local mode runs pipeline jobs via the host Docker daemon. No AWS account
needed for local development.

### 1. Start the server

```bash
docker compose up
```

The API starts on `http://localhost:8000`. Source is bind-mounted, so edits
are picked up immediately by `--reload` without rebuilding.

### 2. Verify it's up

```bash
curl http://localhost:8000/health
# {"status":"ok","execution_mode":"local","version":"0.1.0"}
```

### 3. Browse the interactive docs

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

In local mode all endpoints work without authentication — no token needed.

---

## Running the tests

```bash
docker compose run --rm -e NICHART_EXECUTION_MODE=local api pytest -v
```

117 tests covering auth, path security, catalog, projects, files, jobs, and
DICOM workflows. All should pass on a clean checkout.

---

## Project structure

```
app/
├── main.py               # App factory, CORS, router wiring
├── config.py             # All settings (NICHART_* env vars)
├── auth/
│   ├── cognito.py        # JWKS cache + RS256 JWT verification
│   └── dependencies.py   # require_auth, public, CurrentUser
├── routers/
│   ├── catalog.py        # Pipelines, tools, public resource files
│   ├── projects.py       # Project CRUD
│   ├── files.py          # Upload, download, staging, readiness check
│   ├── dicom.py          # DICOM upload → inspect → convert workflow
│   ├── jobs.py           # Submit pipeline, poll status, logs, cancel
│   ├── results.py        # Per-project pipeline output inspection
│   └── cloud.py          # GET /cloud/status — Batch queue depth
├── services/
│   ├── catalog_service.py  # Load pipeline/tool YAMLs
│   ├── file_service.py     # Project CRUD, file ops, NIfTI staging
│   ├── job_service.py      # In-memory run store, pipeline orchestration
│   ├── dicom_service.py    # DICOM staging and series inspection
│   ├── readiness_service.py # Pipeline data-readiness checks
│   ├── results_service.py  # Pipeline output inspection + label maps
│   └── path_security.py    # assert_safe_path, safe_unzip
├── backends/
│   ├── base.py             # JobBackend / JobHandle ABCs
│   ├── docker_backend.py   # Local Docker execution (DooD)
│   └── batch_backend.py    # AWS Batch via Lambda
└── models/                 # All Pydantic request/response schemas

resources/
├── pipelines/              # Pipeline YAML definitions
├── tools/                  # Tool YAML definitions
├── atlases/muse/           # MNI atlas NIfTIs + MUSE label map
└── reference_data/centiles/ # Normative centile CSVs (6 files, CN/AD × sex)
```

---

## Authentication

| Mode | Behaviour |
|---|---|
| `local` (default) | Auth is bypassed. All requests run as `LOCAL_USER`. |
| `cloud` | Every protected route requires the Cognito ID token in the `session` httpOnly cookie (set by the `/auth/login` → `/auth/callback` BFF flow). The server does not read an `Authorization: Bearer` header. |

Through the React UI, signing in sets the `session` cookie automatically. To call
the API **by hand** in cloud mode, obtain a Cognito ID token and pass it as that
cookie:

```bash
TOKEN=$(python3 scripts/get-token.py your@email.com)
# Prompts for password; prints the ID token.

curl -b "session=$TOKEN" http://localhost:8000/projects
```

Tokens expire after 1 hour by default. Re-run the script to refresh.

Public routes that require no token in either mode:

```
GET /health
GET /catalog/pipelines
GET /catalog/pipelines/{id}
GET /catalog/tools/{id}
GET /catalog/resources/{path}
GET /cloud/status
```

---

## Key workflows

### Create a project and upload a T1

```bash
# Create project
curl -s -X POST http://localhost:8000/projects \
  -H "Content-Type: application/json" \
  -d '{"name": "my-project"}' | python3 -m json.tool

# Upload a NIfTI (two-step: stage then commit)
STAGE=$(curl -s -X POST http://localhost:8000/projects/my-project/files/upload/nifti \
  -F "files=@/path/to/sub001_T1.nii.gz")
echo $STAGE | python3 -m json.tool

STAGING_ID=$(echo $STAGE | python3 -c "import sys,json; print(json.load(sys.stdin)['staging_id'])")

curl -s -X POST "http://localhost:8000/projects/my-project/files/stage/$STAGING_ID/commit" \
  -H "Content-Type: application/json" \
  -d '{"mappings": [{"filename": "sub001_T1.nii.gz", "mrid": "sub001", "modality": "t1"}]}' \
  | python3 -m json.tool
```

### Check pipeline readiness

```bash
curl -s http://localhost:8000/projects/my-project/readiness/run_dlmuse \
  | python3 -m json.tool
# Returns per-check status: imaging modalities present, required CSV columns, etc.
```

### Submit a pipeline and poll

```bash
# Submit
RUN_ID=$(curl -s -X POST http://localhost:8000/projects/my-project/jobs/pipelines \
  -H "Content-Type: application/json" \
  -d '{"pipeline_id": "run_dlmuse"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])")
echo "Run ID: $RUN_ID"

# Poll (run this repeatedly until status is succeeded/failed)
curl -s "http://localhost:8000/jobs/pipelines/$RUN_ID" | python3 -m json.tool

# Logs
curl -s "http://localhost:8000/jobs/pipelines/$RUN_ID/logs" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['logs'])"
```

The submit response returns the full run detail immediately, including all
pipeline steps in `pending` state — use this to render the step list in the UI
before polling begins.

In cloud mode, while a step is queued on AWS Batch, the poll response includes:
- `jobs_ahead` — number of jobs submitted before this one still waiting
- `estimated_wait_seconds` — sum of (num_subjects × time_per_subject_seconds)
  for each job ahead

### Inspect results

```bash
# Summary of all pipelines with output in this project
curl -s http://localhost:8000/projects/my-project/results | python3 -m json.tool

# Full detail for DLMUSE: feature columns, label map, per-subject file availability
curl -s http://localhost:8000/projects/my-project/results/run_dlmuse | python3 -m json.tool
```

### Download files

```bash
# Directory tree
curl -s http://localhost:8000/projects/my-project/files | python3 -m json.tool

# Single file
curl -s "http://localhost:8000/projects/my-project/files/download?path=dlmuse_vol/DLMUSE_Volumes.csv" \
  -o DLMUSE_Volumes.csv

# Directory as zip
curl -s "http://localhost:8000/projects/my-project/files/download?path=dlmuse_vol&zip=true" \
  -o dlmuse_vol.zip
```

### Fetch public reference data (no auth)

```bash
# MUSE label map — maps DL_MUSE_Volume_{id} columns to segmentation label IDs
curl -s http://localhost:8000/catalog/resources/atlases/muse/muse_mapping_derived.csv -o label_map.csv

# Normative centile data (choose CN or AD, All/Males/Females)
curl -s "http://localhost:8000/catalog/resources/reference_data/centiles/nichart_centiles_CN-All.csv" \
  -o centiles_CN_All.csv

# Atlas NIfTI for reference-mode viewer
curl -s "http://localhost:8000/catalog/resources/atlases/muse/MNI152_1mm_LPS_DLMUSE.nii.gz" \
  -o atlas_seg.nii.gz
```

Resource responses are cached for 24 hours (`Cache-Control: public, max-age=86400`).

---

## Adding a new pipeline or tool

### Tool YAML (`resources/tools/{tool_id}.yaml`)

```yaml
name: My Tool
description: What it does.

inputs:
  input_dir:
    type: directory

outputs:
  output_dir:
    type: directory

mounts:
  input_dir:
    path_in_container: /input
    mode: ro
  output_dir:
    path_in_container: /output
    mode: rw

parameters: {}

resources:
  vcpus: 4
  memory: 16000   # MiB
  gpus: 0

time_per_subject_seconds: 120   # Used for Batch queue-drain estimates

container:
  image: my-registry/my-tool:1.0.0
  command: --in {input_dir} --out {output_dir}
```

### Pipeline YAML (`resources/pipelines/{pipeline_id}.yaml`)

```yaml
pipeline_name: My Pipeline
description: End-to-end description.
categories:
  - image-processing

requires:
  - needs_T1                          # imaging modality check
  - csv_has_columns: [MRID, Age, Sex] # participants.csv column check

steps:
  - id: run_my_tool
    tool: my_tool                     # must match a tool YAML basename
    inputs:
      input_dir: ${STUDY}/t1
    outputs:
      output_dir: ${STUDY}/my_output

# Optional — drives GET /projects/{id}/results/{pipeline_id}
results:
  batch_features:
    file: "my_output/results.csv"
    mrid_column: "MRID"
    features:            # Optional: per-column definitions/keywords/references
      score: {}          #   for interpretation (see resources/pipelines/SCHEMA.md).
  per_subject:
    - id: "segmentation"
      pattern: "my_output/{MRID}_seg.nii.gz"
      type: "segmentation_nifti"
```

See `resources/pipelines/SCHEMA.md` for the full `results` schema (units, label
maps, and the `features` interpretation layer), and `scripts/check_feature_docs.py`
to audit feature-documentation completeness across pipelines.

`${STUDY}` resolves to `{NICHART_DATA_ROOT}/{user_sub}/{project_name}`.

No server restart needed in local mode — the source bind-mount means new YAMLs
are available immediately. Rebuild the Docker image for production.

---

## Storage layout

```
NICHART_DATA_ROOT/
└── {user_sub}/
    └── {project_name}/
        ├── t1/                 ← committed T1 NIfTIs ({MRID}.nii.gz)
        ├── fl/ t2/ t1ce/ adc/ ← other modalities
        ├── idat/
        ├── participants/
        │   └── participants.csv
        ├── _upload/            ← staging area (auto-cleaned after TTL)
        │   ├── nifti/
        │   ├── dicoms/
        │   └── bids/
        ├── _working/           ← metadata.json, step cache
        └── <tool_outputs>/     ← e.g. dlmuse_vol/, dlmuse_seg/
```

---

## Environment variables

All variables are prefixed `NICHART_`. Set them in `.env` or via the compose
`environment:` block. See `docker-compose.yml` for defaults.

| Variable | Default | Description |
|---|---|---|
| `NICHART_EXECUTION_MODE` | `local` | `local` (Docker) or `cloud` (AWS Batch) |
| `NICHART_DATA_ROOT` | `/data` | Root directory for all project data |
| `NICHART_HOST_DATA_ROOT` | — | Host path to data root, needed for DooD (local mode with sibling containers) |
| `NICHART_CORS_ORIGINS` | `["http://localhost:3000"]` | JSON array of allowed UI origins |
| `NICHART_STAGING_TTL_HOURS` | `24` | Hours before uncommitted uploads are cleaned up |
| `NICHART_INACTIVITY_TIMEOUT_SECONDS` | `0` (off) | Idle auto-shutdown: exit after N seconds with no API activity and no in-progress runs. `0`/`-1` disables. The CLI's spawned servers enable this by default. |
| `NICHART_S3_DATA_BUCKET` | — | S3 bucket for automatic data sync in cloud mode (e.g. `cbica-nichart-io`). When set, data is uploaded to S3 before each step and downloaded after. |
| `NICHART_S3_DATA_PREFIX` | `fsx` | Key prefix within the S3 data bucket |
| `NICHART_COGNITO_USER_POOL_ID` | `us-east-1_BSBhcKA66` | Cognito pool (cloud mode) |
| `NICHART_COGNITO_CLIENT_ID` | `1ugglpalgp9r2gvb24s2v7dunq` | App client ID (cloud mode) |
| `NICHART_LAMBDA_FUNCTION_NAME` | `cbica-nichart-submitjob` | Lambda for job submission (cloud mode) |
| `NICHART_BATCH_QUEUE_NAME` | `cbica-nichart-jobqueue-standard` | Batch queue (cloud mode) |

---

## Cloud mode

Cloud mode submits pipeline jobs to AWS Batch via a Lambda function and
stores data on FSx for Lustre (mirrored to S3). For a full walkthrough
including credential setup, FSx sync, and end-to-end testing from a local
machine, see [docs/cloud-local-testing.md](cloud-local-testing.md).
