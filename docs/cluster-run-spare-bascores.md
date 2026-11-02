# Running SPARE-ALL and BAScores on the cluster with Singularity/Apptainer

This walks through, on our SLURM cluster:

1. **Building** Singularity images (SIFs) from the same Docker Hub images/tags the
   NiChart pipeline definitions use (via a SLURM job), and
2. **Running** the SPARE-ALL and BAScores pipelines by invoking those SIFs
   step-by-step — reproducing what the NiChart server's SLURM/Singularity backend
   does internally.

Everything below assumes `apptainer` (a.k.a. `singularity`) is available
(`module load apptainer` on most clusters).

---

## 0. The containers each pipeline uses

Each pipeline is a chain of steps; each step is one container. Both pipelines
start with DLMUSE.

**SPARE-ALL** (`run_spare_all`) — 2 steps:
| Step | Docker image | GPU? |
|---|---|---|
| DLMUSE | `cbica/nichart_dlmuse:1.0.10-wrapped` | yes |
| SPARE scores | `cbica/nichart_spare_score_runall:0.0.4` | no |

**BAScores** (`run_bascores`, "DL-BA") — 4 steps:
| Step | Docker image | GPU? |
|---|---|---|
| DLMUSE | `cbica/nichart_dlmuse:1.0.10-wrapped` | yes |
| Binarize seg → ICV mask | `cbica/nifti-binarizer:0.0.1` | no |
| Align to MNI | `cbica/nifti-align-to-mni:0.0.1` | no |
| BA image scores | `cbica/nichart_bascores:0.0.1` | yes |

Union of images to build (DLMUSE is shared, build once): **5 SIFs**.

---

## Part 1 — Build the SIFs from Docker Hub (SLURM job)

`apptainer build <name>.sif docker://<image>:<tag>` pulls the Docker image and
assembles a SIF. Building needs **network egress to Docker Hub, scratch disk, and
no GPU** — so run it on a normal (non-GPU) partition.

Pick a directory on shared storage for the SIFs, e.g. `~/nichart_sif`.

```bash
#!/bin/bash
#SBATCH --job-name=build_nichart_sifs
#SBATCH --partition=<cpu-partition>
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=02:00:00
#SBATCH --output=build_sifs-%j.out
set -euo pipefail

module load apptainer   # or: module load singularity

SIF_DIR=~/nichart_sif
mkdir -p "$SIF_DIR"

# Put Apptainer's cache + temp on node-local scratch — the images are multi-GB
# and the default (~/.apptainer, /tmp) often overflows home/quota.
export APPTAINER_CACHEDIR="${SLURM_TMPDIR:-/tmp/$USER}/apptainer_cache"
export APPTAINER_TMPDIR="${SLURM_TMPDIR:-/tmp/$USER}/apptainer_tmp"
mkdir -p "$APPTAINER_CACHEDIR" "$APPTAINER_TMPDIR"

cd "$SIF_DIR"

build () {  # build <out.sif> <image:tag>
  if [ -f "$1" ]; then echo "exists, skip: $1"; return; fi
  echo "=== building $1 from docker://$2 ==="
  apptainer build "$1" "docker://$2"
}

build nichart_dlmuse_1.0.10-wrapped.sif        cbica/nichart_dlmuse:1.0.10-wrapped
build nichart_spare_score_runall_0.0.4.sif     cbica/nichart_spare_score_runall:0.0.4
build nifti-binarizer_0.0.1.sif                cbica/nifti-binarizer:0.0.1
build nifti-align-to-mni_0.0.1.sif             cbica/nifti-align-to-mni:0.0.1
build nichart_bascores_0.0.1.sif               cbica/nichart_bascores:0.0.1

echo "Done. SIFs in $SIF_DIR:"; ls -lh "$SIF_DIR"/*.sif
```

Submit: `sbatch build_nichart_sifs.sh`.

Notes:
- **Docker Hub rate limits:** anonymous pulls are rate-limited. If builds fail with
  "toomanyrequests", authenticate once before building:
  `apptainer registry login --username <dockerhub-user> docker://docker.io`
  (it'll prompt for a token/password), then re-run.
- **Compute-node internet:** if your cluster blocks outbound internet on compute
  nodes, run the build on a login node or a designated internet-enabled partition.
- Keep SIF filenames tied to the tag (as above) so it's obvious which image/tag a
  SIF corresponds to — these match the tags in the NiChart pipeline defs.

---

## Part 2 — Run the pipelines

### Data layout

Put your inputs in a study directory. One `{MRID}.nii.gz` per subject under `t1/`;
for SPARE you also need a participants CSV with `MRID,Age,Sex`:

```
$STUDY/
├── t1/                         {MRID}.nii.gz  (T1 scans)
└── participants/participants.csv   (SPARE only: MRID, Age, Sex)
```

### How the invocation works

Each step is `apptainer run` of the step's SIF, with:
- `--nv` for GPU steps (exposes the GPU; omit for CPU steps);
- `--writable-tmpfs` — the SIF root is read-only; tools that write to `/tmp`,
  caches, or in-image paths need this or they fail with "Read-only file system";
- `--cleanenv` — stop host Python/conda modules from leaking in;
- `--bind host:container:mode` — map your host dirs to the paths each tool expects
  **inside** the container (these are the tools' fixed mount points), then pass the
  command with those in-container paths.

(`run` is correct here because every tool's command is a set of **arguments** to
the image's built-in entrypoint — they all start with `-`.)

GPU steps must run on a GPU partition (`--gres=gpu:1`).

### SPARE-ALL

```bash
#!/bin/bash
#SBATCH --job-name=spare_all
#SBATCH --partition=<gpu-partition>
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=4
#SBATCH --mem=28G
#SBATCH --time=02:00:00
#SBATCH --output=spare_all-%j.out
set -euo pipefail
module load apptainer

SIF=~/nichart_sif
STUDY=/path/to/your/study
mkdir -p "$STUDY"/{dlmuse_vol,dlmuse_seg,ml_biomarkers}

# 1) DLMUSE (GPU) — T1 -> ROI volumes + segmentation
apptainer run --nv --writable-tmpfs --cleanenv \
  --bind "$STUDY/t1":/input/t1:ro,"$STUDY/dlmuse_vol":/output/dlmuse_vol:rw,"$STUDY/dlmuse_seg":/output/dlmuse_seg:rw \
  "$SIF/nichart_dlmuse_1.0.10-wrapped.sif" \
  -d cuda --in_dir /input/t1 --out_csvs /output/dlmuse_vol --out_segs /output/dlmuse_seg --cores 1 --refaced-data

# 2) SPARE scores (CPU) — DLMUSE volumes + demographics -> SPARE_ALL.csv
apptainer run --writable-tmpfs --cleanenv \
  --bind "$STUDY/dlmuse_vol/DLMUSE_Volumes.csv":/input/dlmuse_vol.csv:ro,"$STUDY/participants/participants.csv":/input/demographics.csv:ro,"$STUDY/ml_biomarkers":/output:rw \
  "$SIF/nichart_spare_score_runall_0.0.4.sif" \
  -i /input/dlmuse_vol.csv -demog /input/demographics.csv --category misc -o /output/SPARE_ALL.csv

echo "Result: $STUDY/ml_biomarkers/SPARE_ALL.csv"
```

### BAScores (DL-BA)

```bash
#!/bin/bash
#SBATCH --job-name=bascores
#SBATCH --partition=<gpu-partition>
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=4
#SBATCH --mem=28G
#SBATCH --time=02:00:00
#SBATCH --output=bascores-%j.out
set -euo pipefail
module load apptainer

SIF=~/nichart_sif
STUDY=/path/to/your/study
mkdir -p "$STUDY"/{dlmuse_vol,dlmuse_seg,dlicv_mask,t1_mni,bascores_out}

# 1) DLMUSE (GPU)
apptainer run --nv --writable-tmpfs --cleanenv \
  --bind "$STUDY/t1":/input/t1:ro,"$STUDY/dlmuse_vol":/output/dlmuse_vol:rw,"$STUDY/dlmuse_seg":/output/dlmuse_seg:rw \
  "$SIF/nichart_dlmuse_1.0.10-wrapped.sif" \
  -d cuda --in_dir /input/t1 --out_csvs /output/dlmuse_vol --out_segs /output/dlmuse_seg --cores 1 --refaced-data

# 2) Binarize DLMUSE seg -> ICV mask (CPU)
apptainer run --writable-tmpfs --cleanenv \
  --bind "$STUDY/dlmuse_seg":/input/segs:ro,"$STUDY/dlicv_mask":/output/masks:rw \
  "$SIF/nifti-binarizer_0.0.1.sif" \
  --input_dir /input/segs --output_dir /output/masks

# 3) Align T1 to MNI using the ICV mask (CPU)
apptainer run --writable-tmpfs --cleanenv \
  --bind "$STUDY/t1":/input/images:ro,"$STUDY/dlicv_mask":/input/masks:ro,"$STUDY/t1_mni":/output/mni:rw \
  "$SIF/nifti-align-to-mni_0.0.1.sif" \
  --img_dir /input/images --mask_dir /input/masks --output_dir /output/mni \
  --transform affine --mask_suffix _DLMUSE --template_mask /MNI_template_DLICV.nii.gz

# 4) BA image scores (GPU) — MNI-aligned T1 -> scores CSV + attention maps
apptainer run --nv --writable-tmpfs --cleanenv \
  --bind "$STUDY/t1_mni":/input/t1:ro,"$STUDY/bascores_out":/output:rw \
  "$SIF/nichart_bascores_0.0.1.sif" \
  -i /input/t1 -o /output/output_attention --csv /output/SPARE_BA_Image.csv --device cuda

echo "Result: $STUDY/bascores_out/SPARE_BA_Image.csv (+ output_attention/)"
```

---

## Gotchas

- **`--writable-tmpfs` is required**, not optional — without it the tools hit
  "Read-only file system" on their internal temp/cache writes.
- **GPU steps need a GPU partition** (`--gres=gpu:1`) and the host NVIDIA driver;
  `--nv` wires the driver in. The CPU steps must *not* get `--nv`.
- **Filename conventions matter between steps:** DLMUSE writes
  `dlmuse_vol/DLMUSE_Volumes.csv` and `{MRID}_DLMUSE.nii.gz` segmentations; the
  binarize/align steps rely on the `_DLMUSE` suffix (hence `--mask_suffix _DLMUSE`).
  Keep the directory names as shown.
- **Binding single files** (SPARE step 2) is supported; `--writable-tmpfs` lets
  Apptainer create the in-container mount points.
- These images/tags are exactly those in the NiChart pipeline defs
  (`resources/pipelines/run_spare_all.yaml`, `run_bascores.yaml` and the tools they
  reference), so results match the server-run pipelines. If a pipeline def's image
  tag changes, rebuild that SIF from the new tag.
