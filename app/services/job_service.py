"""
Pipeline run store and async orchestration background tasks.

Each submission creates a ``_RunRecord`` immediately and schedules an async
background task (via FastAPI's ``BackgroundTasks``) that drives step-by-step
execution. Clients poll the status/detail endpoints.

Persistence
-----------
Each run is stored in its own file at::

    {data_root}/_working/runs/{run_id}.json

Files are written atomically (write-to-tmp then os.replace) so a concurrent
reader never sees a partial write. All read endpoints (get_run_detail, list_runs,
etc.) load directly from disk — there is no shared in-memory cache. This makes
the store safe across multiple uvicorn workers and survives container restarts.

Cancellation
------------
``cancel_run`` writes ``cancelled: true`` to the run file on disk. The background
task (which owns the live JobHandle) re-reads this flag from disk at each polling
tick and calls handle.cancel() when it sees it. This works regardless of which
worker receives the cancel request.

Step caching
------------
Cache state is stored in ``{study_dir}/_working/metadata.json``, keyed by an
MD5 of ``tool_id|inputs|params``. A step is skipped when its cache entry shows
``status=success`` and no input path has been modified since ``finished_time``.
"""

import asyncio
import hashlib
import importlib.metadata
import json
import logging
import math
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    _API_VERSION = importlib.metadata.version("nichart-api")
except importlib.metadata.PackageNotFoundError:
    _API_VERSION = "0.1.0"

from fastapi import HTTPException

from app.backends.base import JobBackend, JobHandle
from app.models.jobs import (
    ChunkStatus,
    PipelineRunDetail,
    PipelineRunLogs,
    PipelineRunSummary,
    StepStatus,
)
from app.services import catalog_service, s3_sync_service

_log = logging.getLogger(__name__)


# ── Run store ──────────────────────────────────────────────────────────────────

@dataclass
class _ChunkRecord:
    chunk_idx: int
    status: str = "pending"     # pending | running | succeeded | failed
    subjects: list[str] = field(default_factory=list)
    job_id: str | None = None
    submitted_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    logs: str = ""


@dataclass
class _StepRecord:
    step_id: str
    tool_id: str
    status: str = "pending"
    submitted_at: datetime | None = None
    finished_at: datetime | None = None
    job_id: str | None = None
    log_path: str | None = None  # persistent log file path (SLURM only); used for reconnect
    container_image: str | None = None
    error: str | None = None
    logs: str = ""
    cached_from_run_id: str | None = None
    chunks: list[_ChunkRecord] = field(default_factory=list)


@dataclass
class _RunRecord:
    run_id: str
    user_id: str
    project_id: str
    pipeline_id: str
    status: str = "pending"
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    current_step: int = 0
    total_steps: int = 0
    steps: list[_StepRecord] = field(default_factory=list)
    error: str | None = None
    cancelled: bool = False
    backend_type: str = ""  # e.g. "docker", "singularity", "slurm", "batch"


# Set once at startup by load_runs_from_disk(); used by all persistence helpers.
_data_root: Path | None = None


@dataclass
class _StepFlight:
    """In-memory coordination handle for a step currently being executed.

    When a background task is about to submit a fresh job it registers one slot
    per unique cache key in ``_step_in_flight``, then calls ``resolve()`` when
    the step finishes. Concurrent runs that would submit the same step instead
    await the event, and use the sibling's output if it succeeded.
    """
    key: str
    event: asyncio.Event = field(default_factory=asyncio.Event)
    succeeded: bool = False

    def resolve(self, succeeded: bool) -> None:
        self.succeeded = succeeded
        self.event.set()
        _step_in_flight.pop(self.key, None)


_step_in_flight: dict[str, _StepFlight] = {}


# ── Path resolution (pipeline YAML templates) ─────────────────────────────────

_STUDY_RE = re.compile(r"\$\{STUDY\}")
_STEP_REF_RE = re.compile(r"\$\{(\w+)\.outputs\.(\w+)\}")


def _resolve_path(
    template: str,
    study_dir: Path,
    step_outputs: dict[str, dict[str, str]],
) -> str:
    result = _STUDY_RE.sub(str(study_dir), template)
    for m in _STEP_REF_RE.finditer(result):
        step_id, label = m.group(1), m.group(2)
        val = step_outputs.get(step_id, {}).get(label, "")
        result = result.replace(m.group(0), val)
    return result


# ── Step caching ───────────────────────────────────────────────────────────────

_METADATA_FILE = "_working/metadata.json"


def _load_metadata(study_dir: Path) -> dict:
    p = study_dir / _METADATA_FILE
    return json.loads(p.read_text()) if p.exists() else {}


def _save_metadata(study_dir: Path, metadata: dict) -> None:
    p = study_dir / _METADATA_FILE
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(metadata, indent=2, default=str))


def _cache_key(tool_id: str, inputs: dict, params: dict) -> str:
    raw = (
        f"{tool_id}|"
        f"{json.dumps(inputs, sort_keys=True)}|"
        f"{json.dumps(params, sort_keys=True)}"
    )
    return hashlib.md5(raw.encode()).hexdigest()


def _is_cached(metadata: dict, key: str, inputs: dict, outputs: dict) -> str | None:
    """Return the originating run_id if the step is validly cached, else None."""
    entry = metadata.get(key)
    if not entry or entry.get("status") != "success":
        return None
    finished = entry.get("finished_time", 0)
    for path_str in inputs.values():
        p = Path(path_str)
        if not p.exists() or p.stat().st_mtime > finished:
            return None
    # Outputs must still exist — deleted outputs invalidate the cache entry.
    for path_str in outputs.values():
        if not Path(path_str).exists():
            return None
    return entry.get("run_id")


def _verify_outputs_present(resolved_outputs: dict[str, str]) -> str | None:
    """Return an error string if any declared output failed to materialise, else None.

    Runs after the post-step S3 sync and before caching, so a run whose tool
    exited 0 but whose results never landed locally (failed publish or failed
    sync) is treated as a failure instead of being cached as an empty success.

    Path convention matches the rest of the orchestrator: a suffixed path is a
    file output (must exist and be non-empty); an unsuffixed path is a directory
    output (must exist and contain at least one entry). This check runs before
    _write_provenance, so a results-less output dir is genuinely empty here.
    """
    for label, path_str in resolved_outputs.items():
        p = Path(path_str)
        if p.suffix:  # file output
            if not p.is_file() or p.stat().st_size == 0:
                return f"expected output '{label}' missing or empty: {path_str}"
        else:  # directory output
            # Ignore _provenance.json: the API writes it into every output dir,
            # and a stale one pulled back from S3 must not mask an empty result.
            real = (
                [e for e in p.iterdir() if e.name != "_provenance.json"]
                if p.is_dir() else []
            )
            if not real:
                return f"expected output '{label}' missing or empty: {path_str}"
    return None


def _mark_cached(metadata: dict, key: str, run_id: str) -> None:
    metadata[key] = {"status": "success", "finished_time": time.time(), "run_id": run_id}


# ── Parallel chunk helpers ────────────────────────────────────────────────────

_DEFAULT_SUBJECTS_PER_CHUNK = 10
_NIFTI_SUFFIXES = {".nii", ".gz"}  # .nii.gz ends with .gz; .nii ends with .nii


def _collect_subjects_from_path(path_str: str) -> list[str]:
    """Return sorted MRID stems from NIfTI files in a directory, or [] for file paths."""
    p = Path(path_str)
    if not p.is_dir():
        return []
    mrids = []
    for f in p.iterdir():
        if not f.is_file():
            continue
        if f.name.endswith(".nii.gz"):
            mrids.append(f.name[:-7])
        elif f.suffix == ".nii":
            mrids.append(f.name[:-4])
    return sorted(mrids)


def _compute_chunk_count(tool_spec, n_subjects: int) -> int:
    """Return how many chunks to split into, bounded by [1, n_subjects]."""
    spc = tool_spec.subjects_per_chunk or _DEFAULT_SUBJECTS_PER_CHUNK
    return max(1, min(n_subjects, math.ceil(n_subjects / spc)))


def _create_fragment_dirs(
    study_dir: Path,
    run_id: str,
    step_id: str,
    resolved_inputs: dict[str, str],
    resolved_outputs: dict[str, str],
    n_chunks: int,
) -> tuple[list[list[str]], list[dict[str, str]], list[dict[str, str]]]:
    """
    Create per-chunk fragment directories for a parallelised step.

    For each input that is a NIfTI directory, splits subjects across n_chunks and
    populates each chunk's input dir with absolute symlinks to the source files.
    File inputs (CSV, etc.) are passed through unchanged to all chunks.
    Each chunk gets its own output directories.

    Returns
    -------
    chunk_subjects  : list of MRID lists (one per chunk)
    chunk_inputs    : list of {label: path} dicts for backend mount_paths (one per chunk)
    chunk_outputs   : list of {label: path} dicts for backend mount_paths (one per chunk)
    """
    # Determine which input labels are NIfTI directories and collect subjects.
    dir_inputs: dict[str, list[str]] = {}
    for label, path_str in resolved_inputs.items():
        subjects = _collect_subjects_from_path(path_str)
        if subjects:
            dir_inputs[label] = subjects

    # Use the intersection of MRIDs across all NIfTI inputs so every chunk has
    # complete data regardless of which modality is listed as the input.
    if dir_inputs:
        complete_mrids = sorted(
            set.intersection(*[set(v) for v in dir_inputs.values()])
        )
    else:
        complete_mrids = []

    n = len(complete_mrids)
    actual_chunks = max(1, min(n_chunks, n)) if n > 0 else 1

    # Distribute subjects evenly across chunks.
    chunk_size = math.ceil(n / actual_chunks) if n > 0 else 0
    chunk_subjects: list[list[str]] = []
    for i in range(actual_chunks):
        start = i * chunk_size
        chunk_subjects.append(complete_mrids[start: start + chunk_size])

    frag_base = study_dir / "_working" / "fragments" / run_id / step_id

    chunk_inputs: list[dict[str, str]] = []
    chunk_outputs: list[dict[str, str]] = []

    for i, subjects in enumerate(chunk_subjects):
        ci: dict[str, str] = {}
        for label, path_str in resolved_inputs.items():
            src = Path(path_str)
            if label in dir_inputs:
                # Create a fragment input dir with symlinks to this chunk's subjects.
                frag_in = frag_base / f"chunk_{i}" / f"in_{label}"
                frag_in.mkdir(parents=True, exist_ok=True)
                for mrid in subjects:
                    # Detect whether source file has .nii.gz or .nii extension.
                    for ext in (".nii.gz", ".nii"):
                        src_file = src / f"{mrid}{ext}"
                        if src_file.exists():
                            link = frag_in / f"{mrid}{ext}"
                            if not link.exists():
                                link.symlink_to(src_file)
                            break
                ci[label] = str(frag_in)
            else:
                # File or non-NIfTI dir: pass through unchanged.
                ci[label] = path_str
        chunk_inputs.append(ci)

        co: dict[str, str] = {}
        for label, path_str in resolved_outputs.items():
            src_p = Path(path_str)
            if src_p.suffix:
                # File output — use a chunk-specific temp file in a staging dir.
                frag_out = frag_base / f"chunk_{i}" / f"out_{label}"
                frag_out.mkdir(parents=True, exist_ok=True)
                co[label] = str(frag_out / src_p.name)
            else:
                frag_out = frag_base / f"chunk_{i}" / f"out_{label}"
                frag_out.mkdir(parents=True, exist_ok=True)
                co[label] = str(frag_out)
        chunk_outputs.append(co)

    return chunk_subjects, chunk_inputs, chunk_outputs


def _merge_chunk_outputs(
    chunk_output_dicts: list[dict[str, str]],
    final_outputs: dict[str, str],
    output_merge: dict[str, str],
) -> None:
    """
    Merge per-chunk output directories/files into the final output paths.

    Merge strategies per output label (from tool_spec.output_merge):
      "directory_union"            — copy all files into final dir (NIfTI names must be unique)
      "directory_union_csv_concat" — same, but CSVs with matching names are row-concatenated
      "csv_concat"                 — concatenate a single CSV file (header once, rows appended)
    Default for directory outputs: "directory_union".
    """
    import csv as csv_mod
    import shutil

    for label, final_path_str in final_outputs.items():
        final_path = Path(final_path_str)
        strategy = output_merge.get(label, "directory_union")
        chunk_paths = [Path(d[label]) for d in chunk_output_dicts if label in d]

        if strategy == "csv_concat":
            # Concatenate CSV files: header from first chunk, rows from all.
            final_path.parent.mkdir(parents=True, exist_ok=True)
            header_written = False
            with final_path.open("w", newline="") as fout:
                writer = csv_mod.writer(fout)
                for cp in chunk_paths:
                    if not cp.exists():
                        continue
                    with cp.open(newline="") as fin:
                        reader = csv_mod.reader(fin)
                        rows = list(reader)
                    if not rows:
                        continue
                    if not header_written:
                        writer.writerows(rows)
                        header_written = True
                    else:
                        writer.writerows(rows[1:])  # skip header

        elif strategy in ("directory_union", "directory_union_csv_concat"):
            final_path.mkdir(parents=True, exist_ok=True)
            # Collect all CSV files that appear in multiple chunks (same name → concat).
            csv_files: dict[str, list[Path]] = {}
            for cp in chunk_paths:
                if not cp.is_dir():
                    continue
                for f in cp.iterdir():
                    if not f.is_file():
                        continue
                    if strategy == "directory_union_csv_concat" and f.suffix == ".csv":
                        csv_files.setdefault(f.name, []).append(f)
                    else:
                        dest = final_path / f.name
                        shutil.copy2(str(f), str(dest))

            if strategy == "directory_union_csv_concat":
                for fname, sources in csv_files.items():
                    dest = final_path / fname
                    header_written = False
                    with dest.open("w", newline="") as fout:
                        writer = csv_mod.writer(fout)
                        for src in sources:
                            with src.open(newline="") as fin:
                                reader = csv_mod.reader(fin)
                                rows = list(reader)
                            if not rows:
                                continue
                            if not header_written:
                                writer.writerows(rows)
                                header_written = True
                            else:
                                writer.writerows(rows[1:])


def _cleanup_fragment_dirs(study_dir: Path, run_id: str, step_id: str) -> None:
    """Remove fragment directories after a successful merge."""
    import shutil
    frag_dir = study_dir / "_working" / "fragments" / run_id / step_id
    if frag_dir.exists():
        try:
            shutil.rmtree(frag_dir)
        except Exception as exc:
            _log.warning("Could not clean up fragment dir %s: %s", frag_dir, exc)


# ── Provenance ────────────────────────────────────────────────────────────────

def _write_provenance(
    resolved_outputs: dict[str, str],
    step_id: str,
    pipeline_id: str,
    container_image: str,
    params: dict,
    input_paths: dict,
    submitted_at: datetime | None,
    finished_at: datetime | None,
    execution_mode: str = "",
    user_id: str = "",
    backend: str = "",
) -> None:
    """Write _provenance.json into each output directory after a successful step.

    Never raises — a failed provenance write must not block the pipeline.
    """
    payload = {
        "generated_at": (finished_at or datetime.now(timezone.utc)).isoformat(),
        "api_version": _API_VERSION,
        "pipeline_id": pipeline_id,
        "step_id": step_id,
        "container_image": container_image,
        "submitted_at": submitted_at.isoformat() if submitted_at else None,
        "execution_mode": execution_mode,
        "user_id": user_id,
        "backend": backend,
        "params": params,
        "input_paths": input_paths,
    }
    # Collect the unique directories that this step wrote into.
    dirs: set[Path] = set()
    for path_str in resolved_outputs.values():
        p = Path(path_str)
        dirs.add(p if not p.suffix else p.parent)
    for d in dirs:
        try:
            d.mkdir(parents=True, exist_ok=True)
            (d / "_provenance.json").write_text(json.dumps(payload, indent=2, default=str))
        except Exception as exc:
            _log.warning("Could not write provenance to %s: %s", d, exc)


# ── Run store persistence ─────────────────────────────────────────────────────

def _run_to_dict(run: _RunRecord) -> dict:
    return {
        "run_id": run.run_id,
        "user_id": run.user_id,
        "project_id": run.project_id,
        "pipeline_id": run.pipeline_id,
        "status": run.status,
        "submitted_at": run.submitted_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "current_step": run.current_step,
        "total_steps": run.total_steps,
        "error": run.error,
        "cancelled": run.cancelled,
        "backend_type": run.backend_type,
        "steps": [
            {
                "step_id": s.step_id,
                "tool_id": s.tool_id,
                "status": s.status,
                "submitted_at": s.submitted_at.isoformat() if s.submitted_at else None,
                "finished_at": s.finished_at.isoformat() if s.finished_at else None,
                "job_id": s.job_id,
                "log_path": s.log_path,
                "container_image": s.container_image,
                "error": s.error,
                "logs": s.logs,
                "cached_from_run_id": s.cached_from_run_id,
                "chunks": [
                    {
                        "chunk_idx": c.chunk_idx,
                        "status": c.status,
                        "subjects": c.subjects,
                        "job_id": c.job_id,
                        "submitted_at": c.submitted_at.isoformat() if c.submitted_at else None,
                        "finished_at": c.finished_at.isoformat() if c.finished_at else None,
                        "error": c.error,
                        "logs": c.logs,
                    }
                    for c in s.chunks
                ],
            }
            for s in run.steps
        ],
    }


def _run_from_dict(d: dict) -> _RunRecord:
    def _dt(s: str | None) -> datetime | None:
        return datetime.fromisoformat(s) if s else None

    run = _RunRecord(
        run_id=d["run_id"],
        user_id=d["user_id"],
        project_id=d["project_id"],
        pipeline_id=d["pipeline_id"],
        status=d["status"],
        submitted_at=_dt(d.get("submitted_at")) or datetime.now(timezone.utc),
        finished_at=_dt(d.get("finished_at")),
        current_step=d.get("current_step", 0),
        total_steps=d.get("total_steps", 0),
        error=d.get("error"),
        cancelled=d.get("cancelled", False),
        backend_type=d.get("backend_type", ""),
    )
    run.steps = [
        _StepRecord(
            step_id=s["step_id"],
            tool_id=s["tool_id"],
            status=s["status"],
            submitted_at=_dt(s.get("submitted_at")),
            finished_at=_dt(s.get("finished_at")),
            job_id=s.get("job_id"),
            log_path=s.get("log_path"),
            container_image=s.get("container_image"),
            error=s.get("error"),
            logs=s.get("logs", ""),
            cached_from_run_id=s.get("cached_from_run_id"),
            chunks=[
                _ChunkRecord(
                    chunk_idx=c["chunk_idx"],
                    status=c["status"],
                    subjects=c.get("subjects", []),
                    job_id=c.get("job_id"),
                    submitted_at=_dt(c.get("submitted_at")),
                    finished_at=_dt(c.get("finished_at")),
                    error=c.get("error"),
                    logs=c.get("logs", ""),
                )
                for c in s.get("chunks", [])
            ],
        )
        for s in d.get("steps", [])
    ]
    return run


def _runs_dir() -> Path | None:
    return (_data_root / "_working" / "runs") if _data_root else None


def _save_run(run: _RunRecord) -> None:
    """Atomically write a run record to its per-run JSON file."""
    d = _runs_dir()
    if d is None:
        return
    d.mkdir(parents=True, exist_ok=True)
    tmp = d / f"{run.run_id}.json.tmp"
    target = d / f"{run.run_id}.json"
    tmp.write_text(json.dumps(_run_to_dict(run), indent=2, default=str))
    tmp.replace(target)


def _load_run(run_id: str) -> _RunRecord | None:
    """Read a run record from disk. Returns None if not found or corrupt."""
    d = _runs_dir()
    if d is None:
        return None
    p = d / f"{run_id}.json"
    if not p.exists():
        return None
    try:
        return _run_from_dict(json.loads(p.read_text()))
    except Exception:
        return None


def _is_cancel_requested(run_id: str) -> bool:
    """Check the on-disk cancel flag. Safe to call from any worker."""
    run = _load_run(run_id)
    return run.cancelled if run else False


async def _wait_for_in_flight_step(key: str, run_id: str) -> bool | None:
    """
    Wait for a concurrent in-flight step with the same cache key to complete.

    Returns:
      True  — sibling step succeeded; caller should reload metadata and skip.
      False — sibling step failed; caller should submit its own job.
      None  — no in-flight step, or cancel was requested, or 6-hour timeout.
    """
    flight = _step_in_flight.get(key)
    if flight is None:
        return None
    _log.info("Waiting for sibling in-flight step (key=%.8s) to complete", key)
    deadline = asyncio.get_event_loop().time() + 6 * 3600
    while not flight.event.is_set():
        if _is_cancel_requested(run_id):
            return None
        if asyncio.get_event_loop().time() > deadline:
            _log.warning("Timed out waiting for in-flight step %.8s; submitting own job", key)
            return None
        await asyncio.sleep(5)
    return flight.succeeded


def load_runs_from_disk(data_root: Path) -> None:
    """
    Called at startup: set the data root and fixup any runs left in a
    non-terminal state from the previous server process.

    Handling of in-progress runs:
    - pending: always marked failed (task never started, cannot resume)
    - running + backend_type == "slurm": left as "running" so resume_runs()
      can reconnect to the still-running SLURM job
    - running + any other backend: marked failed (child process is gone)
    """
    global _data_root
    _data_root = data_root

    d = _runs_dir()
    if d is None or not d.exists():
        return

    for p in d.glob("*.json"):
        try:
            run = _run_from_dict(json.loads(p.read_text()))
        except Exception:
            continue
        if run.status == "pending":
            run.status = "failed"
            run.error = "Server restarted before run started"
            run.finished_at = run.finished_at or datetime.now(timezone.utc)
            _save_run(run)
        elif run.status == "running" and run.backend_type != "slurm":
            run.status = "failed"
            run.error = "Server restarted while run was in progress"
            run.finished_at = run.finished_at or datetime.now(timezone.utc)
            _save_run(run)
        # SLURM "running" runs are left intact; resume_runs() handles them.


def has_slurm_runs_to_resume() -> bool:
    """Return True if any on-disk run is a SLURM run still marked 'running'."""
    d = _runs_dir()
    if d is None or not d.exists():
        return False
    for p in d.glob("*.json"):
        try:
            run = _run_from_dict(json.loads(p.read_text()))
            if run.status == "running" and run.backend_type == "slurm":
                return True
        except Exception:
            continue
    return False


async def resume_runs(settings: Any, backend: JobBackend) -> None:
    """
    Resume SLURM pipeline runs that were in-progress at the last shutdown.

    Called at startup after load_runs_from_disk(). For each SLURM run still
    marked "running", this reconnects to the SLURM job and re-attaches a
    polling background task. Runs whose pipeline YAML is missing or whose
    backend does not support reconnection are marked failed.
    """
    d = _runs_dir()
    if d is None or not d.exists():
        return

    for p in d.glob("*.json"):
        try:
            run = _run_from_dict(json.loads(p.read_text()))
        except Exception:
            continue
        if run.status != "running" or run.backend_type != "slurm":
            continue

        study_dir = settings.data_root / run.user_id / run.project_id

        try:
            from app.services import catalog_service
            pipeline = catalog_service.get_pipeline(settings.pipelines_path, run.pipeline_id)
        except Exception as exc:
            run.status = "failed"
            run.error = f"Cannot resume: pipeline '{run.pipeline_id}' not found: {exc}"
            run.finished_at = datetime.now(timezone.utc)
            _save_run(run)
            continue

        _log.info(
            "Resuming SLURM pipeline run %s (pipeline %s, step %d/%d)",
            run.run_id, run.pipeline_id, run.current_step, run.total_steps,
        )
        asyncio.create_task(
            run_pipeline_task(
                run=run,
                pipeline_steps=pipeline.steps,
                study_dir=study_dir,
                backend=backend,
                user_params={},      # params are already baked into the running SLURM job
                reuse_cached_steps=False,
                user_token=None,
                tools_path=settings.tools_path,
                s3_sync=None,
            )
        )


# ── Shared polling helper ─────────────────────────────────────────────────────

async def _poll_to_completion(
    handle: JobHandle,
    step: _StepRecord,
    run: _RunRecord,
) -> bool:
    """Poll handle until terminal. Updates step and run state. Returns True on success."""
    step.job_id = handle.job_id
    if handle.log_path:
        step.log_path = handle.log_path
    # Persist job_id and log_path immediately so a restart can reconnect to SLURM jobs.
    _save_run(run)

    while True:
        if _is_cancel_requested(run.run_id):
            run.cancelled = True
            await handle.cancel()
            step.status = "failed"
            step.error = "Cancelled by user"
            run.status = "failed"
            run.error = "Cancelled by user"
            run.finished_at = datetime.now(timezone.utc)
            return False

        job_status = await handle.status()
        if job_status in ("succeeded", "failed"):
            break
        await asyncio.sleep(5)

    step.logs = await handle.logs()
    step.finished_at = datetime.now(timezone.utc)

    if job_status == "succeeded":
        step.status = "succeeded"
        return True

    step.status = "failed"
    step.error = "Job exited with non-zero status"
    return False


# ── S3 sync config ────────────────────────────────────────────────────────────

@dataclass
class S3SyncConfig:
    """S3 sync parameters for cloud-mode pipeline runs.

    When set, ``run_pipeline_task`` uploads the project directory to S3 before
    each step (so the Batch job can pull inputs) and downloads from S3 after
    each step (to retrieve outputs the Batch job wrote back).
    """

    bucket: str
    prefix: str   # Top-level prefix, e.g. "fsx". Full project prefix =
                  # {prefix}/{user_id}/{project_name}.


# ── Pipeline orchestration task ───────────────────────────────────────────────

async def run_pipeline_task(
    run: _RunRecord,
    pipeline_steps: list,          # list of PipelineStep (from catalog model)
    study_dir: Path,
    backend: JobBackend,
    user_params: dict[str, Any],
    reuse_cached_steps: bool,
    user_token: str | None,
    tools_path: Path,
    s3_sync: S3SyncConfig | None = None,
    execution_mode: str = "",
) -> None:
    """Background task: drive each pipeline step in order, with caching and error handling."""
    run.backend_type = backend.backend_name

    # Wait until no other pipeline is running for this user (user-level serialisation).
    # The run stays in 'pending' while queued. Cancellation is honoured while waiting.
    while _has_running_pipeline(run.user_id, run.run_id):
        if _is_cancel_requested(run.run_id):
            run.cancelled = True
            run.status = "failed"
            run.error = "Cancelled while queued"
            run.finished_at = datetime.now(timezone.utc)
            _save_run(run)
            return
        await asyncio.sleep(5)

    # No await between the check above and the status write below, so no race.
    run.status = "running"
    _save_run(run)

    # Compute the project-scoped S3 prefix once, used for all steps.
    _s3_project_prefix: str | None = None
    if s3_sync:
        _s3_project_prefix = f"{s3_sync.prefix}/{run.user_id}/{study_dir.name}"

    if _s3_project_prefix:
        try:
            await s3_sync_service.sync_from_s3(s3_sync.bucket, _s3_project_prefix, study_dir)  # type: ignore[union-attr]
        except Exception as exc:
            _log.exception("Pipeline-start S3 sync failed for run %s", run.run_id)
            run.status = "failed"
            run.error = f"Pipeline-start S3 sync failed: {exc}"
            run.finished_at = datetime.now(timezone.utc)
            _save_run(run)
            return

    metadata = _load_metadata(study_dir)
    step_outputs: dict[str, dict[str, str]] = {}

    for i, step_def in enumerate(pipeline_steps):
        if _is_cancel_requested(run.run_id):
            run.cancelled = True
            run.status = "failed"
            run.error = "Cancelled by user"
            run.finished_at = datetime.now(timezone.utc)
            _save_run(run)
            return

        run.current_step = i
        step = run.steps[i]

        # Resolve ${STUDY} and ${prev.outputs.label} in path templates.
        # Done before the skip check so step_outputs can be populated correctly.
        resolved_inputs = {
            label: _resolve_path(val, study_dir, step_outputs)
            for label, val in (step_def.inputs or {}).items()
        }
        resolved_outputs = {
            label: _resolve_path(val, study_dir, step_outputs)
            for label, val in (step_def.outputs or {}).items()
        }
        mount_paths = {**resolved_inputs, **resolved_outputs}
        merged_params = {**user_params, **(step_def.params or {})}

        # Skip already-completed steps (SLURM restart recovery: resume mid-pipeline).
        if step.status in ("succeeded", "skipped"):
            step_outputs[step_def.id] = resolved_outputs
            continue
        if step.status == "failed":
            run.status = "failed"
            run.finished_at = run.finished_at or datetime.now(timezone.utc)
            _save_run(run)
            return

        key = _cache_key(step_def.tool, resolved_inputs, merged_params)

        # Check step cache (only for freshly-pending steps, not in-progress reconnects).
        if step.status == "pending" and reuse_cached_steps:
            cached_run_id = _is_cached(metadata, key, resolved_inputs, resolved_outputs)
            if cached_run_id is not None:
                step.status = "skipped"
                step.finished_at = datetime.now(timezone.utc)
                step.cached_from_run_id = cached_run_id
                step_outputs[step_def.id] = resolved_outputs
                _save_run(run)
                continue

        # If another run is currently executing this exact step (same cache key),
        # wait for it rather than submitting a duplicate job.
        if step.status == "pending":
            _wait = await _wait_for_in_flight_step(key, run.run_id)
            if _wait is True:
                # Sibling succeeded — its outputs are on disk. Reload metadata
                # so _is_cached can verify the entry, then treat as a cache hit.
                metadata = _load_metadata(study_dir)
                entry = metadata.get(key)
                step.cached_from_run_id = entry.get("run_id") if entry else None
                step.status = "skipped"
                step.finished_at = datetime.now(timezone.utc)
                step_outputs[step_def.id] = resolved_outputs
                _save_run(run)
                continue
            # _wait is False (sibling failed) or None (no sibling / cancelled): proceed.

        # Determine the job handle — either reconnect to an in-progress SLURM job
        # (restart recovery) or submit a fresh job.
        handle: JobHandle | None = None
        tool_spec = None  # set below; needed for provenance

        if step.status == "running" and step.job_id:
            # This step was already submitted before the server restarted.
            # Attempt to reconnect via the backend (only SLURM supports this).
            handle = backend.reconnect(step.job_id, step.log_path)
            if handle is None:
                step.status = "failed"
                step.error = "Job lost at server restart (backend does not support reconnection)"
                run.status = "failed"
                run.error = f"Step '{step_def.id}' lost at server restart"
                run.finished_at = datetime.now(timezone.utc)
                _save_run(run)
                return
            _log.info(
                "Reconnected to %s job %s for step '%s' (run %s)",
                run.backend_type, step.job_id, step_def.id, run.run_id,
            )
            try:
                tool_spec = catalog_service.load_tool_spec(tools_path, step_def.tool)
            except FileNotFoundError:
                pass  # provenance will be skipped for this step

        # Register an in-flight slot for fresh pending submissions so concurrent
        # runs can wait on this step rather than submitting duplicate jobs.
        flight: _StepFlight | None = None
        if handle is None:
            flight = _StepFlight(key=key)
            _step_in_flight[key] = flight

        if handle is None:
            # Fresh submission path.

            # Ensure output directories exist. For file outputs (path has a
            # suffix), mkdir the parent so Docker doesn't auto-create a
            # directory at the file path when it doesn't exist yet.
            for path_str in resolved_outputs.values():
                p = Path(path_str)
                target = p.parent if p.suffix else p
                target.mkdir(parents=True, exist_ok=True)

            # Load tool spec (also used for provenance after success)
            try:
                tool_spec = catalog_service.load_tool_spec(tools_path, step_def.tool)
            except FileNotFoundError as e:
                step.status = "failed"
                step.error = str(e)
                run.status = "failed"
                run.error = f"Step '{step_def.id}': {e}"
                run.finished_at = datetime.now(timezone.utc)
                if flight:
                    flight.resolve(False)
                _save_run(run)
                return

            # Estimate subject count for cloud queue-drain metric and SLURM time limits.
            def _count_subjects(p: Path) -> int:
                if p.is_dir():
                    return sum(1 for f in p.iterdir() if f.suffix in (".gz", ".nii"))
                return 1 if p.exists() else 0

            num_subjects = max(
                1,
                max((_count_subjects(Path(v)) for v in resolved_inputs.values()), default=0),
            ) if resolved_inputs else 1

            step.status = "running"
            step.submitted_at = datetime.now(timezone.utc)
            step.container_image = tool_spec.image

            # Upload project data to S3 before submitting so the Batch job can pull
            # the latest inputs. A sync failure aborts the step.
            if _s3_project_prefix:
                try:
                    await s3_sync_service.sync_to_s3(
                        study_dir, s3_sync.bucket, _s3_project_prefix  # type: ignore[union-attr]
                    )
                except Exception as exc:
                    step.status = "failed"
                    step.error = f"S3 pre-sync failed: {exc}"
                    run.status = "failed"
                    run.error = f"Step '{step_def.id}': S3 pre-sync failed: {exc}"
                    run.finished_at = datetime.now(timezone.utc)
                    if flight:
                        flight.resolve(False)
                    _save_run(run)
                    return

            # ── Parallel chunk path ───────────────────────────────────────────
            if tool_spec.parallelizable and num_subjects > 1:
                n_chunks = _compute_chunk_count(tool_spec, num_subjects)
                chunk_subjects, chunk_input_dicts, chunk_output_dicts = _create_fragment_dirs(
                    study_dir=study_dir,
                    run_id=run.run_id,
                    step_id=step_def.id,
                    resolved_inputs=resolved_inputs,
                    resolved_outputs=resolved_outputs,
                    n_chunks=n_chunks,
                )

                # Initialise chunk records.
                step.chunks = [
                    _ChunkRecord(chunk_idx=idx, subjects=subs)
                    for idx, subs in enumerate(chunk_subjects)
                ]
                _save_run(run)

                # Push the freshly-created fragment dirs to S3. The Batch job's
                # down-sync is now scoped to the exact input mount paths, so the
                # per-chunk fragment inputs must exist in S3 before submission —
                # the earlier pre-step sync ran before these dirs existed.
                if _s3_project_prefix:
                    try:
                        await s3_sync_service.sync_to_s3(
                            study_dir, s3_sync.bucket, _s3_project_prefix  # type: ignore[union-attr]
                        )
                    except Exception as exc:
                        step.status = "failed"
                        step.error = f"S3 fragment sync failed: {exc}"
                        run.status = "failed"
                        run.error = f"Step '{step_def.id}': S3 fragment sync failed: {exc}"
                        run.finished_at = datetime.now(timezone.utc)
                        if flight:
                            flight.resolve(False)
                        _save_run(run)
                        return

                # Submit all chunks concurrently.
                handles: list[JobHandle | None] = []
                for idx, (ci, co, subs) in enumerate(
                    zip(chunk_input_dicts, chunk_output_dicts, chunk_subjects)
                ):
                    chunk = step.chunks[idx]
                    chunk_mount = {**ci, **co}
                    try:
                        h = await backend.submit(
                            tool_spec=tool_spec,
                            mount_paths=chunk_mount,
                            params=merged_params,
                            num_subjects=len(subs),
                            user_token=user_token,
                            extra_readonly_mounts=[str(study_dir)],
                            study_id=study_dir.name,
                        )
                        chunk.job_id = h.job_id
                        chunk.submitted_at = datetime.now(timezone.utc)
                        chunk.status = "running"
                        handles.append(h)
                    except Exception as e:
                        chunk.status = "failed"
                        chunk.error = str(e)
                        chunk.finished_at = datetime.now(timezone.utc)
                        handles.append(None)
                _save_run(run)

                # Poll all chunks to completion concurrently.
                async def _poll_chunk(h: JobHandle, chunk: _ChunkRecord) -> bool:
                    if h is None:
                        return False
                    job_status = "running"
                    while job_status == "running":
                        if _is_cancel_requested(run.run_id):
                            await h.cancel()
                            chunk.status = "failed"
                            chunk.error = "Cancelled"
                            chunk.finished_at = datetime.now(timezone.utc)
                            return False
                        await asyncio.sleep(5)
                        try:
                            job_status = await h.status()
                        except Exception:
                            break
                    chunk.logs = await h.logs()
                    chunk.finished_at = datetime.now(timezone.utc)
                    if job_status == "succeeded":
                        chunk.status = "succeeded"
                        return True
                    chunk.status = "failed"
                    chunk.error = "Job exited with non-zero status"
                    return False

                results = await asyncio.gather(
                    *[
                        _poll_chunk(h, step.chunks[idx])
                        for idx, h in enumerate(handles)
                        if h is not None
                    ],
                    return_exceptions=True,
                )

                # For chunks that had None handles (submission failed), already marked failed.
                succeeded_chunks = [c for c in step.chunks if c.status == "succeeded"]
                failed_chunks = [c for c in step.chunks if c.status == "failed"]

                step.finished_at = datetime.now(timezone.utc)

                if failed_chunks:
                    # S3 sync after all chunks (best-effort; pull whatever succeeded).
                    if _s3_project_prefix:
                        try:
                            await s3_sync_service.sync_from_s3(
                                s3_sync.bucket, _s3_project_prefix, study_dir  # type: ignore[union-attr]
                            )
                        except Exception as exc:
                            _log.warning("S3 post-chunk sync failed: %s", exc)

                    if succeeded_chunks:
                        step.status = "partially_failed"
                        step.error = (
                            f"{len(failed_chunks)}/{len(step.chunks)} chunks failed"
                        )
                    else:
                        step.status = "failed"
                        step.error = "All chunks failed"
                    run.status = "failed"
                    run.error = f"Step '{step_def.id}': {step.error}"
                    run.finished_at = datetime.now(timezone.utc)
                    if flight:
                        flight.resolve(False)
                    _save_run(run)
                    return

                # All chunks succeeded — pull their outputs from S3 so the merge
                # can run locally. A sync failure FAILS the step: without the
                # chunk outputs the merge is empty and would poison the cache.
                if _s3_project_prefix:
                    for direction, fn, args in [
                        ("down", s3_sync_service.sync_from_s3,
                         (s3_sync.bucket, _s3_project_prefix, study_dir)),  # type: ignore[union-attr]
                        ("up",   s3_sync_service.sync_to_s3,
                         (study_dir, s3_sync.bucket, _s3_project_prefix)),  # type: ignore[union-attr]
                    ]:
                        try:
                            await fn(*args)
                        except Exception as exc:
                            step.status = "failed"
                            step.error = f"S3 post-chunk sync ({direction}) failed: {exc}"
                            run.status = "failed"
                            run.error = f"Step '{step_def.id}': S3 post-chunk sync ({direction}) failed: {exc}"
                            run.finished_at = datetime.now(timezone.utc)
                            if flight:
                                flight.resolve(False)
                            _save_run(run)
                            return

                try:
                    await asyncio.to_thread(
                        _merge_chunk_outputs,
                        chunk_output_dicts,
                        resolved_outputs,
                        tool_spec.output_merge,
                    )
                except Exception as exc:
                    step.status = "failed"
                    step.error = f"Output merge failed: {exc}"
                    run.status = "failed"
                    run.error = f"Step '{step_def.id}': output merge failed: {exc}"
                    run.finished_at = datetime.now(timezone.utc)
                    if flight:
                        flight.resolve(False)
                    _save_run(run)
                    return

                # Verify the merged outputs actually materialised before caching.
                if _s3_project_prefix:
                    missing = _verify_outputs_present(resolved_outputs)
                    if missing:
                        step.status = "failed"
                        step.error = missing
                        run.status = "failed"
                        run.error = f"Step '{step_def.id}': {missing}"
                        run.finished_at = datetime.now(timezone.utc)
                        if flight:
                            flight.resolve(False)
                        _save_run(run)
                        return

                _cleanup_fragment_dirs(study_dir, run.run_id, step_def.id)

                if _s3_project_prefix:
                    # Delete the fragment cruft we uploaded to S3 before submit
                    # (nothing uses --delete anymore, so it won't self-clean).
                    frag_prefix = (
                        f"{_s3_project_prefix}/_working/fragments/"
                        f"{run.run_id}/{step_def.id}/"
                    )
                    try:
                        await s3_sync_service.delete_s3_prefix(
                            s3_sync.bucket, frag_prefix  # type: ignore[union-attr]
                        )
                    except Exception as exc:
                        _log.warning(
                            "S3 fragment cleanup failed for step '%s': %s",
                            step_def.id, exc,
                        )

                    # Push the merged output to S3. The per-chunk syncs above only
                    # published fragment outputs; the merge produces the final
                    # output locally, so without this it would never reach S3
                    # (the Batch up-sync never sees it, auto-export is disabled).
                    try:
                        await s3_sync_service.sync_to_s3(
                            study_dir, s3_sync.bucket, _s3_project_prefix  # type: ignore[union-attr]
                        )
                    except Exception as exc:
                        _log.warning(
                            "S3 merged-output sync failed for step '%s' (data remains local): %s",
                            step_def.id, exc,
                        )

                step.status = "succeeded"
                step_outputs[step_def.id] = resolved_outputs

                subjects_complete = sum(len(c.subjects) for c in succeeded_chunks)
                _log.info(
                    "Step '%s' completed (%d/%d chunks, %d subjects) for run %s",
                    step_def.id, len(succeeded_chunks), len(step.chunks),
                    subjects_complete, run.run_id,
                )

                if tool_spec:
                    _write_provenance(
                        resolved_outputs=resolved_outputs,
                        step_id=step_def.id,
                        pipeline_id=run.pipeline_id,
                        container_image=tool_spec.image,
                        params=merged_params,
                        input_paths=resolved_inputs,
                        submitted_at=step.submitted_at,
                        finished_at=step.finished_at,
                        execution_mode=execution_mode,
                        user_id=run.user_id,
                        backend=run.backend_type,
                    )
                _mark_cached(metadata, key, run.run_id)
                _save_metadata(study_dir, metadata)
                if flight:
                    flight.resolve(True)
                _save_run(run)
                continue  # advance to next pipeline step

            # ── Single-job (non-parallel) path ────────────────────────────────
            try:
                handle = await backend.submit(
                    tool_spec=tool_spec,
                    mount_paths={k: str(v) for k, v in mount_paths.items()},
                    params=merged_params,
                    num_subjects=num_subjects,
                    user_token=user_token,
                    study_id=study_dir.name,
                )
            except Exception as e:
                step.status = "failed"
                step.error = str(e)
                run.status = "failed"
                run.error = f"Step '{step_def.id}' submission failed: {e}"
                run.finished_at = datetime.now(timezone.utc)
                if flight:
                    flight.resolve(False)
                _save_run(run)
                return

        success = await _poll_to_completion(handle, step, run)

        if not success:
            if not run.error:
                run.error = f"Step '{step_def.id}' failed"
            run.status = "failed"
            if not run.finished_at:
                run.finished_at = datetime.now(timezone.utc)
            if flight:
                flight.resolve(False)
            _save_run(run)
            return

        # Pull the Batch job's outputs from S3, then push local state back. A sync
        # failure now FAILS the step: without the pulled outputs we cannot confirm
        # or serve results, and caching an empty success here would make every
        # future cached run silently serve nothing.
        if _s3_project_prefix:
            for direction, fn, args in [
                ("down", s3_sync_service.sync_from_s3,
                 (s3_sync.bucket, _s3_project_prefix, study_dir)),  # type: ignore[union-attr]
                ("up",   s3_sync_service.sync_to_s3,
                 (study_dir, s3_sync.bucket, _s3_project_prefix)),  # type: ignore[union-attr]
            ]:
                try:
                    await fn(*args)
                except Exception as exc:
                    step.status = "failed"
                    step.error = f"S3 post-sync ({direction}) failed: {exc}"
                    run.status = "failed"
                    run.error = f"Step '{step_def.id}': S3 post-sync ({direction}) failed: {exc}"
                    run.finished_at = datetime.now(timezone.utc)
                    if flight:
                        flight.resolve(False)
                    _save_run(run)
                    return

        # Verify the tool's declared outputs actually materialised locally. A tool
        # can exit 0 without producing (or publishing) results; caching that would
        # serve empty output on every future cached run. Scoped to the S3-backed
        # path, where outputs return via sync and this failure mode exists.
        if _s3_project_prefix:
            missing = _verify_outputs_present(resolved_outputs)
            if missing:
                step.status = "failed"
                step.error = missing
                run.status = "failed"
                run.error = f"Step '{step_def.id}': {missing}"
                run.finished_at = datetime.now(timezone.utc)
                if flight:
                    flight.resolve(False)
                _save_run(run)
                return

        step_outputs[step_def.id] = resolved_outputs
        # Provenance must be written BEFORE _mark_cached so that finished_time
        # (set by time.time() inside _mark_cached) is >= the provenance file's
        # mtime. If provenance were written after, its mtime would exceed
        # finished_time and any downstream step whose input dir overlaps this
        # step's output dir would appear dirty on the next cache check.
        if tool_spec:
            _write_provenance(
                resolved_outputs=resolved_outputs,
                step_id=step_def.id,
                pipeline_id=run.pipeline_id,
                container_image=tool_spec.image,
                params=merged_params,
                input_paths=resolved_inputs,
                submitted_at=step.submitted_at,
                finished_at=step.finished_at,
                execution_mode=execution_mode,
                user_id=run.user_id,
                backend=run.backend_type,
            )
        _mark_cached(metadata, key, run.run_id)
        _save_metadata(study_dir, metadata)
        if flight:
            flight.resolve(True)
        _save_run(run)

    run.status = "succeeded"
    run.finished_at = datetime.now(timezone.utc)
    _save_run(run)


# ── Direct step execution (DICOM conversion, etc.) ────────────────────────────

@dataclass
class DirectStep:
    """A pre-resolved tool execution step — no pipeline YAML needed."""
    step_id: str
    tool_id: str
    mount_paths: dict[str, str]
    params: dict[str, Any] = field(default_factory=dict)


async def run_direct_steps_task(
    run: _RunRecord,
    direct_steps: list[DirectStep],
    backend: JobBackend,
    user_token: str | None,
    tools_path: Path,
    study_dir: Path | None = None,
) -> None:
    """Background task: execute a list of pre-resolved steps sequentially."""
    run.status = "running"
    _save_run(run)

    for i, step_def in enumerate(direct_steps):
        if _is_cancel_requested(run.run_id):
            run.cancelled = True
            run.status = "failed"
            run.error = "Cancelled by user"
            run.finished_at = datetime.now(timezone.utc)
            _save_run(run)
            return

        run.current_step = i
        step = run.steps[i]
        step.status = "running"
        step.submitted_at = datetime.now(timezone.utc)

        try:
            tool_spec = catalog_service.load_tool_spec(tools_path, step_def.tool_id)
            handle = await backend.submit(
                tool_spec=tool_spec,
                mount_paths=step_def.mount_paths,
                params=step_def.params,
                user_token=user_token,
                study_id=study_dir.name if study_dir else None,
            )
        except Exception as e:
            step.status = "failed"
            step.error = str(e)
            run.status = "failed"
            run.error = str(e)
            run.finished_at = datetime.now(timezone.utc)
            _save_run(run)
            return

        success = await _poll_to_completion(handle, step, run)
        if not success:
            if not run.error:
                run.error = f"Step '{step_def.step_id}' failed"
            run.status = "failed"
            if not run.finished_at:
                run.finished_at = datetime.now(timezone.utc)
            _save_run(run)
            return

        _save_run(run)

    run.status = "succeeded"
    run.finished_at = datetime.now(timezone.utc)
    _save_run(run)


# ── Finished-run polling ──────────────────────────────────────────────────────

_TERMINAL_STATUSES: frozenset[str] = frozenset({"succeeded", "failed"})


def _poll_cursor_path(user_id: str) -> Path | None:
    d = _runs_dir()
    if d is None:
        return None
    return d.parent / "poll_cursors" / f"{user_id}.txt"


def get_and_advance_poll_cursor(
    user_id: str,
) -> tuple[list[PipelineRunSummary], datetime]:
    """
    Return all terminal runs for *user_id* that finished after the last call,
    then advance the stored cursor to now.

    The cursor is persisted at ``{data_root}/_working/poll_cursors/{user_id}.txt``.
    On the first call (no cursor file) the epoch is used, so all existing
    terminal runs are returned — convenient for bootstrapping a UI on first load.
    """
    now = datetime.now(timezone.utc)

    cursor_p = _poll_cursor_path(user_id)
    if cursor_p is None or not cursor_p.exists():
        cursor = datetime.fromtimestamp(0, tz=timezone.utc)
    else:
        try:
            cursor = datetime.fromisoformat(cursor_p.read_text().strip())
        except Exception:
            cursor = datetime.fromtimestamp(0, tz=timezone.utc)

    d = _runs_dir()
    finished: list[_RunRecord] = []
    if d and d.exists():
        for p in d.glob("*.json"):
            run = _load_run(p.stem)
            if run is None or run.user_id != user_id:
                continue
            if run.status not in _TERMINAL_STATUSES:
                continue
            fa = run.finished_at
            if fa is None:
                continue
            if fa.tzinfo is None:
                fa = fa.replace(tzinfo=timezone.utc)
            if fa > cursor:
                finished.append(run)

    finished.sort(
        key=lambda r: r.finished_at or datetime.fromtimestamp(0, tz=timezone.utc)
    )

    if cursor_p is not None:
        cursor_p.parent.mkdir(parents=True, exist_ok=True)
        cursor_p.write_text(now.isoformat())

    return [_to_summary(r) for r in finished], now


# ── Public API ─────────────────────────────────────────────────────────────────

def create_pipeline_run(
    user_id: str,
    project_id: str,
    pipeline_id: str,
    pipeline_steps: list,
) -> _RunRecord:
    """Create and register a pipeline run record (does not start the task)."""
    run_id = str(uuid.uuid4())
    run = _RunRecord(
        run_id=run_id,
        user_id=user_id,
        project_id=project_id,
        pipeline_id=pipeline_id,
        total_steps=len(pipeline_steps),
        steps=[_StepRecord(step_id=s.id, tool_id=s.tool) for s in pipeline_steps],
    )
    _save_run(run)
    return run


def create_direct_run(
    user_id: str,
    project_id: str,
    pipeline_id: str,
    direct_steps: list[DirectStep],
) -> _RunRecord:
    """Create and register a direct-step run record (does not start the task)."""
    run_id = str(uuid.uuid4())
    run = _RunRecord(
        run_id=run_id,
        user_id=user_id,
        project_id=project_id,
        pipeline_id=pipeline_id,
        total_steps=len(direct_steps),
        steps=[_StepRecord(step_id=s.step_id, tool_id=s.tool_id) for s in direct_steps],
    )
    _save_run(run)
    return run


def _to_summary(run: _RunRecord) -> PipelineRunSummary:
    return PipelineRunSummary(
        run_id=run.run_id,
        project_id=run.project_id,
        pipeline_id=run.pipeline_id,
        status=run.status,
        submitted_at=run.submitted_at,
        finished_at=run.finished_at,
        current_step=run.current_step,
        total_steps=run.total_steps,
    )


def _to_detail(run: _RunRecord) -> PipelineRunDetail:
    return PipelineRunDetail(
        run_id=run.run_id,
        project_id=run.project_id,
        pipeline_id=run.pipeline_id,
        status=run.status,
        submitted_at=run.submitted_at,
        finished_at=run.finished_at,
        current_step=run.current_step,
        total_steps=run.total_steps,
        error=run.error,
        steps=[
            _step_record_to_status(s)
            for s in run.steps
        ],
    )


def has_active_runs() -> bool:
    """True if any pipeline run (any user) is still pending or running.

    Used by the server's inactivity auto-shutdown to avoid tearing itself down
    while it is still orchestrating/polling work — including long external
    (SLURM/Batch) jobs, whose run stays 'running' for the job's whole duration.
    """
    d = _runs_dir()
    if d is None or not d.exists():
        return False
    for p in d.glob("*.json"):
        run = _load_run(p.stem)
        if run and run.status in ("pending", "running"):
            return True
    return False


def _has_running_pipeline(user_id: str, exclude_run_id: str) -> bool:
    """Return True if the user has any pipeline currently in 'running' state."""
    d = _runs_dir()
    if d is None or not d.exists():
        return False
    for p in d.glob("*.json"):
        if p.stem == exclude_run_id:
            continue
        run = _load_run(p.stem)
        if run and run.user_id == user_id and run.status == "running":
            return True
    return False


def list_runs(
    user_id: str,
    project_id: str | None,
    limit: int,
) -> list[PipelineRunSummary]:
    d = _runs_dir()
    if d is None or not d.exists():
        return []
    runs: list[_RunRecord] = []
    for p in d.glob("*.json"):
        run = _load_run(p.stem)
        if run and run.user_id == user_id:
            if project_id is None or run.project_id == project_id:
                runs.append(run)
    runs.sort(key=lambda r: r.submitted_at, reverse=True)
    return [_to_summary(r) for r in runs[:limit]]


def _step_record_to_status(s: _StepRecord) -> StepStatus:
    """Convert an internal _StepRecord to the API-facing StepStatus model."""
    chunk_statuses = [
        ChunkStatus(
            chunk_idx=c.chunk_idx,
            status=c.status,
            subjects=c.subjects,
            job_id=c.job_id,
            submitted_at=c.submitted_at,
            finished_at=c.finished_at,
            error=c.error,
        )
        for c in s.chunks
    ]
    total_chunks = len(s.chunks) if s.chunks else None
    completed_chunks = sum(1 for c in s.chunks if c.status == "succeeded") if s.chunks else None
    failed_chunks = sum(1 for c in s.chunks if c.status == "failed") if s.chunks else None
    total_subjects = sum(len(c.subjects) for c in s.chunks) if s.chunks else None
    subjects_complete = (
        sum(len(c.subjects) for c in s.chunks if c.status == "succeeded")
        if s.chunks else None
    )
    return StepStatus(
        step_id=s.step_id,
        tool_id=s.tool_id,
        status=s.status,
        submitted_at=s.submitted_at,
        finished_at=s.finished_at,
        job_id=s.job_id,
        container_image=s.container_image,
        error=s.error,
        cached_from_run_id=s.cached_from_run_id,
        total_chunks=total_chunks,
        completed_chunks=completed_chunks,
        failed_chunks=failed_chunks,
        total_subjects=total_subjects,
        subjects_complete=subjects_complete,
        chunks=chunk_statuses,
    )


def get_run_detail(run_id: str, user_id: str) -> PipelineRunDetail:
    run = _load_run(run_id)
    if not run:
        raise HTTPException(404, f"Run '{run_id}' not found")
    if run.user_id != user_id:
        raise HTTPException(403, "Access denied")
    return _to_detail(run)


async def _live_logs_for_job(backend, job_id: str) -> str | None:
    """Best-effort live log fetch for a still-running job via a rebuilt handle.

    Returns None if the backend can't reconnect (e.g. Docker after restart) or on
    any error — callers fall back to the persisted step logs.
    """
    if backend is None or not job_id:
        return None
    try:
        handle = backend.reconnect(job_id)
        if handle is None:
            return None
        return await handle.logs()
    except Exception:
        return None


async def get_run_logs(run_id: str, user_id: str, backend=None) -> PipelineRunLogs:
    run = _load_run(run_id)
    if not run:
        raise HTTPException(404, f"Run '{run_id}' not found")
    if run.user_id != user_id:
        raise HTTPException(403, "Access denied")

    parts: list[str] = []
    for s in run.steps:
        if s.chunks:
            chunk_parts: list[str] = []
            for c in s.chunks:
                chunk_logs = c.logs
                # Fetch live logs for chunks still running (persisted logs are
                # only written when the chunk completes).
                if c.status == "running" and c.job_id:
                    live = await _live_logs_for_job(backend, c.job_id)
                    if live:
                        chunk_logs = live
                if chunk_logs:
                    chunk_parts.append(f"  --- chunk {c.chunk_idx}/{len(s.chunks)-1} ---\n{chunk_logs}")
            if chunk_parts:
                parts.append(f"=== Step {s.step_id} ===\n" + "\n".join(chunk_parts))
        else:
            live_logs = s.logs
            # Persisted step logs are only written at completion; for a running
            # step fetch live output so the client can stream it.
            if s.status == "running":
                if s.log_path:
                    # SLURM: log file on a shared filesystem — read it live.
                    p = Path(s.log_path)
                    if p.exists():
                        try:
                            live_logs = p.read_text()
                        except Exception:
                            pass
                elif s.job_id:
                    # Batch/cloud: rebuild the handle and pull live CloudWatch logs.
                    live = await _live_logs_for_job(backend, s.job_id)
                    if live:
                        live_logs = live
            if live_logs:
                parts.append(f"=== Step {s.step_id} ===\n{live_logs}")

    return PipelineRunLogs(run_id=run_id, logs="\n".join(parts))


def get_chunk_logs(run_id: str, user_id: str, step_id: str, chunk_idx: int) -> str:
    """Return logs for a specific chunk of a parallelized step."""
    run = _load_run(run_id)
    if not run:
        raise HTTPException(404, f"Run '{run_id}' not found")
    if run.user_id != user_id:
        raise HTTPException(403, "Access denied")
    step = next((s for s in run.steps if s.step_id == step_id), None)
    if step is None:
        raise HTTPException(404, f"Step '{step_id}' not found in run '{run_id}'")
    if not step.chunks:
        raise HTTPException(400, f"Step '{step_id}' was not run in parallel chunks")
    chunk = next((c for c in step.chunks if c.chunk_idx == chunk_idx), None)
    if chunk is None:
        raise HTTPException(404, f"Chunk {chunk_idx} not found in step '{step_id}'")
    return chunk.logs


def cancel_run(run_id: str, user_id: str) -> None:
    run = _load_run(run_id)
    if not run:
        raise HTTPException(404, f"Run '{run_id}' not found")
    if run.user_id != user_id:
        raise HTTPException(403, "Access denied")
    run.cancelled = True
    _save_run(run)
