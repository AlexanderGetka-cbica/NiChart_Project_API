"""
Pipeline job submission and status — authenticated.

Pipelines run asynchronously: submission returns a ``run_id`` immediately and
the actual orchestration (submit each tool step, poll, advance) happens in a
server-side background task. Clients poll ``GET /jobs/pipelines/{run_id}``.

URL structure:
  POST   /projects/{project_id}/jobs/pipelines  — submit (scoped to a project)
  GET    /jobs/pipelines                         — list (optional project filter)
  GET    /jobs/pipelines/{run_id}                — detail + per-step status
  GET    /jobs/pipelines/{run_id}/logs           — aggregated step logs
  DELETE /jobs/pipelines/{run_id}                — cancel
"""

import re

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query

from app.auth.dependencies import CurrentUser, require_auth
from app.backends import get_backend
from app.backends.base import JobBackend
from app.config import Settings, get_settings
from app.models.catalog import ParameterSpec, PipelineDetail
from app.models.errors import ErrorDetail
from app.models.jobs import (
    FinishedRunsResponse,
    PipelineRunDetail,
    PipelineRunLogs,
    PipelineRunSubmit,
    PipelineRunSummary,
    RunPlan,
)
from app.services import (
    catalog_service,
    file_service,
    job_service,
    readiness_service,
    retention_service,
    usage_service,
)
from app.services.job_service import S3SyncConfig


_TYPE_MAP: dict[str, type] = {"int": int, "float": float, "bool": bool, "str": str}

# Allowlist for free-entry string parameters. Rejects spaces and all shell
# metacharacters (; | & $ ` ( ) < > ! \ " ' # ~ { } [ ] * ? ^ % = + @).
# Choices-constrained string params bypass this check (they're whitelisted explicitly).
_SAFE_STRING_RE = re.compile(r"^[a-zA-Z0-9._-]{1,256}$")


def _resolve_params(
    submitted: dict,
    declared: dict[str, ParameterSpec],
) -> dict:
    """Validate submitted params against declared schema and apply defaults.

    Returns the effective params dict (defaults filled in, types coerced).
    Raises HTTPException(400) for:
      - unknown parameter names
      - wrong type
      - value not in ``choices``
      - numeric value outside ``min``/``max``
      - free-entry string containing spaces or shell metacharacters
    """
    unknown = set(submitted) - set(declared)
    if unknown:
        raise HTTPException(
            400,
            f"Unknown pipeline parameter(s): {sorted(unknown)}. "
            f"Accepted: {sorted(declared)}",
        )

    effective: dict = {}
    for name, spec in declared.items():
        if name in submitted:
            raw = submitted[name]
            # Coerce type — booleans must be checked before int since bool <: int.
            if spec.type == "bool":
                if not isinstance(raw, bool):
                    raise HTTPException(
                        400,
                        f"Parameter '{name}' must be a boolean (true/false), "
                        f"got {type(raw).__name__!r}",
                    )
            else:
                py_type = _TYPE_MAP.get(spec.type)
                if py_type is not None:
                    try:
                        raw = py_type(raw)
                    except (TypeError, ValueError):
                        raise HTTPException(
                            400,
                            f"Parameter '{name}' must be of type {spec.type!r}, "
                            f"got {type(raw).__name__!r}",
                        )

            # Choices whitelist (all types).
            if spec.choices is not None and raw not in spec.choices:
                raise HTTPException(
                    400,
                    f"Parameter '{name}': {raw!r} is not an allowed value. "
                    f"Must be one of: {spec.choices}",
                )

            # Numeric range (int and float only).
            if spec.type in ("int", "float"):
                if spec.min is not None and raw < spec.min:
                    raise HTTPException(400, f"Parameter '{name}' must be >= {spec.min}")
                if spec.max is not None and raw > spec.max:
                    raise HTTPException(400, f"Parameter '{name}' must be <= {spec.max}")

            # String injection guard.
            # Choices-constrained strings are safe (already whitelisted above).
            # Free-entry strings are restricted to [a-zA-Z0-9._-] to prevent
            # shell metacharacters from reaching bash -c in Batch jobs.
            if spec.type == "str" and spec.choices is None:
                if not _SAFE_STRING_RE.match(str(raw)):
                    raise HTTPException(
                        400,
                        f"Parameter '{name}': free-entry string values must contain "
                        f"only letters, digits, '.', '_', or '-' (max 256 chars). "
                        f"Spaces and shell metacharacters are not allowed.",
                    )

            effective[name] = raw
        else:
            if spec.default is not None:
                effective[name] = spec.default
    return effective

def _plan_subject_count(report) -> int:
    """Project-level subject count, matching the readiness check's rule.

    Subjects complete across every required modality when the pipeline needs two
    or more; otherwise the smallest per-modality imaging count. Per-step estimates
    use each step's own count instead (see RunPlanStep.subjects), which differs for
    steps whose input is a single CSV rather than an image directory.
    """
    if report.complete_sets is not None:
        return report.complete_sets.complete_count
    if report.imaging:
        return min(c.subject_count for c in report.imaging)
    return 0


# Two routers: one project-scoped (for submission), one top-level (for queries)
project_router = APIRouter(prefix="/projects/{project_id}/jobs", tags=["Jobs"])
jobs_router = APIRouter(prefix="/jobs", tags=["Jobs"])

_AUTH_ERRORS = {
    401: {"model": ErrorDetail, "description": "Missing or invalid token."},
    403: {"model": ErrorDetail, "description": "Access denied."},
}


# ── Submission (project-scoped) ───────────────────────────────────────────────

@project_router.post(
    "/pipelines",
    summary="Submit a pipeline run",
    description=(
        "Starts an asynchronous pipeline run for the given project. "
        "The server spawns a background task that executes each pipeline step in "
        "order, polling the job backend between steps. "
        "Use the returned ``run_id`` to track progress via ``GET /jobs/pipelines/{run_id}``."
    ),
    response_model=PipelineRunDetail,
    status_code=202,
    responses={
        **_AUTH_ERRORS,
        400: {"model": ErrorDetail, "description": "Invalid pipeline ID or parameters."},
        404: {"model": ErrorDetail, "description": "Project or pipeline not found."},
    },
)
async def submit_pipeline(
    project_id: str,
    body: PipelineRunSubmit,
    background_tasks: BackgroundTasks,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
    backend: JobBackend = Depends(get_backend),
) -> PipelineRunDetail:
    pdir = file_service.resolve_project(settings, user, project_id)
    pipeline = catalog_service.get_pipeline(settings.pipelines_path, body.pipeline_id)
    effective_params = _resolve_params(body.params, pipeline.parameters)
    job_service.validate_start_from_step(pipeline.steps, body.start_from_step)
    run = job_service.create_pipeline_run(
        user_id=user.sub,
        project_id=project_id,
        pipeline_id=body.pipeline_id,
        pipeline_steps=pipeline.steps,
        start_from_step=body.start_from_step,
        study_dir=pdir,
    )
    usage_service.record_pipeline_run(
        body.pipeline_id,
        settings.execution_mode,
        subjects=len(file_service.collect_project_mrids(pdir)),
    )
    # Submitting a run is active use, so it resets the retention countdown.
    # Done only once the submission has been accepted, so an invalid request
    # cannot keep a project alive.
    await retention_service.touch_project(
        settings.s3_data_bucket,
        settings.s3_data_prefix,
        user.sub,
        project_id,
        activity="pipeline submission",
    )
    s3_sync: S3SyncConfig | None = None
    if settings.s3_data_bucket and settings.execution_mode == "cloud":
        s3_sync = S3SyncConfig(
            bucket=settings.s3_data_bucket,
            prefix=settings.s3_data_prefix,
        )
    background_tasks.add_task(
        job_service.run_pipeline_task,
        run=run,
        pipeline_steps=pipeline.steps,
        study_dir=pdir,
        backend=backend,
        user_params=effective_params,
        reuse_cached_steps=body.reuse_cached_steps,
        user_token=user.token,
        tools_path=settings.tools_path,
        s3_sync=s3_sync,
        execution_mode=settings.execution_mode,
        start_from_step=body.start_from_step,
    )
    return job_service.get_run_detail(run.run_id, user.sub)


# ── Dry-run plan (project-scoped) ─────────────────────────────────────────────

@project_router.post(
    "/pipelines/plan",
    summary="Plan a pipeline run without submitting it",
    description=(
        "Predicts how a submission would execute right now: which steps would be "
        "reused from the step cache, which would run, and roughly how long the run "
        "would take.\n\n"
        "Read-only — creates no run record, submits no jobs and does not touch the "
        "step cache. It stats input/output paths and reads the cache metadata, so it "
        "is cheap enough to call whenever the pipeline, params or reuse toggle "
        "change.\n\n"
        "Takes the same body as the submit route, including ``start_from_step``. "
        "Estimates assume **serial** execution: they are not divided by chunks or "
        "workers, because chunking is not guaranteed and chunks need not run "
        "concurrently."
    ),
    response_model=RunPlan,
    responses={
        **_AUTH_ERRORS,
        400: {"model": ErrorDetail, "description": "Unknown start_from_step."},
        404: {"model": ErrorDetail, "description": "Project or pipeline not found."},
    },
)
async def plan_pipeline(
    project_id: str,
    body: PipelineRunSubmit,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> RunPlan:
    pdir = file_service.resolve_project(settings, user, project_id)
    pipeline = catalog_service.get_pipeline(
        settings.pipelines_path, body.pipeline_id, None, settings.tools_path
    )
    job_service.validate_start_from_step(pipeline.steps, body.start_from_step)
    effective_params = _resolve_params(body.params, pipeline.parameters)

    steps, total_seconds, full_seconds = job_service.plan_pipeline_run(
        study_dir=pdir,
        pipeline_steps=pipeline.steps,
        user_params=effective_params,
        reuse_cached_steps=body.reuse_cached_steps,
        tools_path=settings.tools_path,
        start_from_step=body.start_from_step,
    )

    # Readiness is informational here: an unready project still gets a plan,
    # because each step's prediction remains meaningful.
    raw_requires = catalog_service.get_pipeline_raw_requires(
        settings.pipelines_path, body.pipeline_id
    )
    report = readiness_service.check_readiness(pdir, body.pipeline_id, raw_requires)

    return RunPlan(
        pipeline_id=body.pipeline_id,
        reuse_cached_steps=body.reuse_cached_steps,
        start_from_step=body.start_from_step,
        ready=report.satisfied,
        subject_count=_plan_subject_count(report),
        steps=steps,
        estimated_total_seconds=total_seconds,
        estimated_full_seconds=full_seconds,
        steps_missing_timing=sum(
            1 for s in steps if s.action == "run" and s.estimated_seconds is None
        ),
        steps_reused=sum(1 for s in steps if s.action == "reuse"),
        steps_forced_skip=sum(1 for s in steps if s.action == "skip_forced"),
        steps_total=len(steps),
    )


# ── Queries (top-level, optional project filter) ──────────────────────────────

@jobs_router.get(
    "/pipelines",
    summary="List pipeline runs",
    description=(
        "Returns the authenticated user's pipeline runs in reverse-chronological order. "
        "Pass ``project_id`` to filter to a single project."
    ),
    response_model=list[PipelineRunSummary],
    responses=_AUTH_ERRORS,
)
async def list_pipeline_runs(
    project_id: str | None = Query(default=None, description="Filter runs to this project."),
    limit: int = Query(default=50, ge=1, le=200, description="Maximum number of runs to return."),
    user: CurrentUser = Depends(require_auth),
) -> list[PipelineRunSummary]:
    return job_service.list_runs(user_id=user.sub, project_id=project_id, limit=limit)


@jobs_router.get(
    "/pipelines/finished",
    summary="Poll for newly finished pipeline runs",
    description=(
        "Returns all pipeline runs that reached a terminal state (``succeeded`` or "
        "``failed``) since the last time this endpoint was called by the authenticated "
        "user. The server stores a per-user cursor so only genuinely new completions "
        "are returned on each call. "
        "\n\n"
        "**First call**: the cursor defaults to epoch, so all existing terminal runs "
        "are returned — useful for bootstrapping a UI on first load. "
        "\n\n"
        "Designed for lightweight notification polling (e.g. every 10–30 seconds). "
        "For full run history use ``GET /jobs/pipelines``."
    ),
    response_model=FinishedRunsResponse,
    responses=_AUTH_ERRORS,
)
async def get_finished_pipeline_runs(
    user: CurrentUser = Depends(require_auth),
) -> FinishedRunsResponse:
    runs, polled_at = job_service.get_and_advance_poll_cursor(user.sub)
    return FinishedRunsResponse(runs=runs, polled_at=polled_at)


@jobs_router.get(
    "/pipelines/{run_id}",
    summary="Get pipeline run detail",
    description=(
        "Returns the full run record including per-step status, timestamps, "
        "and any error messages. "
        "In cloud mode, while a step is actively waiting on the Batch queue, "
        "``jobs_ahead`` and ``estimated_wait_seconds`` are populated by querying "
        "all jobs submitted to the queue before this one. These fields are null "
        "in local mode or once a job has left the queue."
    ),
    response_model=PipelineRunDetail,
    responses={**_AUTH_ERRORS, 404: {"model": ErrorDetail}},
)
async def get_pipeline_run(
    run_id: str,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
    backend: JobBackend = Depends(get_backend),
) -> PipelineRunDetail:
    detail = job_service.get_run_detail(run_id=run_id, user_id=user.sub)

    if settings.execution_mode == "cloud" and detail.status == "running":
        from app.backends.batch_backend import BatchBackend

        if isinstance(backend, BatchBackend):
            pending_job_id = next(
                (s.job_id for s in detail.steps if s.status == "running" and s.job_id),
                None,
            )
            if pending_job_id:
                try:
                    jobs_ahead, wait_secs = await backend.get_queue_position(
                        job_id=pending_job_id,
                        queue_name=settings.batch_queue_name,
                        tools_path=settings.tools_path,
                    )
                    detail = detail.model_copy(update={
                        "jobs_ahead": jobs_ahead,
                        "estimated_wait_seconds": wait_secs,
                    })
                except Exception:
                    pass  # queue position is best-effort; don't fail the request

    return detail


@jobs_router.get(
    "/pipelines/{run_id}/logs",
    summary="Get pipeline run logs",
    description=(
        "Returns concatenated log output from all steps completed so far. "
        "Poll this endpoint alongside the status endpoint during a run."
    ),
    response_model=PipelineRunLogs,
    responses={**_AUTH_ERRORS, 404: {"model": ErrorDetail}},
)
async def get_pipeline_logs(
    run_id: str,
    user: CurrentUser = Depends(require_auth),
    backend: JobBackend = Depends(get_backend),
) -> PipelineRunLogs:
    return await job_service.get_run_logs(run_id=run_id, user_id=user.sub, backend=backend)


@jobs_router.get(
    "/pipelines/{run_id}/steps/{step_id}/chunks/{chunk_idx}/logs",
    summary="Get logs for a single parallel chunk",
    description=(
        "Returns the captured log output for one chunk of a parallelised pipeline step. "
        "Only available after the chunk has started executing. "
        "Use ``GET /jobs/pipelines/{run_id}/logs`` to retrieve all chunks concatenated."
    ),
    responses={
        **_AUTH_ERRORS,
        400: {"model": ErrorDetail, "description": "Step was not run in parallel chunks."},
        404: {"model": ErrorDetail, "description": "Run, step, or chunk not found."},
    },
)
async def get_chunk_logs(
    run_id: str,
    step_id: str,
    chunk_idx: int,
    user: CurrentUser = Depends(require_auth),
) -> str:
    return job_service.get_chunk_logs(run_id, user.sub, step_id, chunk_idx)


@jobs_router.delete(
    "/pipelines/{run_id}",
    summary="Cancel a pipeline run",
    description=(
        "Requests cancellation of a running pipeline. "
        "The currently executing tool step is cancelled on the backend; "
        "subsequent steps will not be submitted. "
        "No-op if the run is already in a terminal state."
    ),
    status_code=204,
    responses={**_AUTH_ERRORS, 404: {"model": ErrorDetail}},
)
async def cancel_pipeline_run(
    run_id: str,
    user: CurrentUser = Depends(require_auth),
) -> None:
    job_service.cancel_run(run_id=run_id, user_id=user.sub)
