"""
Request/response schemas for the pipeline jobs endpoints.
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class PipelineRunSubmit(BaseModel):
    """Request body to submit a pipeline run."""

    pipeline_id: str = Field(description="Pipeline identifier (matches a YAML basename in resources/pipelines/).")
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Parameter overrides applied to every step that accepts them.",
    )
    reuse_cached_steps: bool = Field(
        default=True,
        description=(
            "When True, steps whose inputs haven't changed since the last successful run "
            "are skipped. Set to False to force a full re-run."
        ),
    )
    start_from_step: str | None = Field(
        default=None,
        description=(
            "Advanced. Step id to start the run at. Every earlier step is marked skipped "
            "without any cache or output checks, and its declared outputs are assumed to "
            "be present at their resolved paths. The start step always runs (ignoring the "
            "cache); later steps follow reuse_cached_steps as normal. Null runs the whole "
            "pipeline. Returns 400 if the id is not one of the pipeline's steps."
        ),
    )


class ChunkStatus(BaseModel):
    """Status of one parallel subject chunk within a step."""

    chunk_idx: int = Field(description="Zero-based index of this chunk within the step.")
    status: Literal["pending", "running", "succeeded", "failed"] = Field(
        description="Execution state of this chunk."
    )
    subjects: list[str] = Field(
        default_factory=list,
        description="MRID stems assigned to this chunk.",
    )
    job_id: str | None = Field(
        default=None,
        description="Backend job ID for this chunk's container.",
    )
    submitted_at: datetime | None = Field(default=None)
    finished_at: datetime | None = Field(default=None)
    error: str | None = Field(default=None, description="Error message if this chunk failed.")


class StepStatus(BaseModel):
    """Status record for a single pipeline step."""

    step_id: str = Field(description="Step identifier within the pipeline definition.")
    tool_id: str = Field(description="Tool that this step invokes.")
    status: Literal["pending", "running", "succeeded", "failed", "skipped", "partially_failed"] = Field(
        description=(
            "Current execution state. 'partially_failed' means at least one chunk succeeded "
            "but at least one failed; the pipeline is halted and partial output is preserved."
        )
    )
    submitted_at: datetime | None = Field(default=None)
    finished_at: datetime | None = Field(default=None)
    job_id: str | None = Field(
        default=None,
        description=(
            "Backend job ID. For non-parallelized steps only; null when the step runs as chunks. "
            "See 'chunks' for per-chunk job IDs."
        ),
    )
    container_image: str | None = Field(
        default=None, description="Container image used for this step."
    )
    error: str | None = Field(default=None, description="Error message if the step failed.")
    cached_from_run_id: str | None = Field(
        default=None,
        description=(
            "Run ID that originally produced the cached output for this step. "
            "Only set when status is 'skipped' and the result was reused from a prior run."
        ),
    )
    skip_reason: Literal["cached", "forced"] | None = Field(
        default=None,
        description=(
            "Why a 'skipped' step was skipped. 'cached' — its result was reused from the "
            "step cache (or coalesced onto an identical step running in another run). "
            "'forced' — it precedes the run's start_from_step and was skipped without any "
            "checks. Null for steps that were not skipped."
        ),
    )
    # ── Parallel chunk progress ───────────────────────────────────────────────
    total_chunks: int | None = Field(
        default=None,
        description="Total number of parallel chunks for this step. Null for non-parallelized steps.",
    )
    completed_chunks: int | None = Field(
        default=None,
        description="Number of chunks that have succeeded so far.",
    )
    failed_chunks: int | None = Field(
        default=None,
        description="Number of chunks that have failed.",
    )
    total_subjects: int | None = Field(
        default=None,
        description="Total subjects assigned across all chunks.",
    )
    subjects_complete: int | None = Field(
        default=None,
        description="Subjects in all succeeded chunks (guaranteed to have output).",
    )
    chunks: list[ChunkStatus] = Field(
        default_factory=list,
        description="Per-chunk breakdown. Empty for non-parallelized steps.",
    )


class PipelineRunSummary(BaseModel):
    """Abbreviated pipeline run record for list responses."""

    run_id: str = Field(description="Unique run identifier (UUID).")
    project_id: str
    pipeline_id: str
    status: Literal["pending", "running", "succeeded", "failed"] = Field(
        description="Overall pipeline status."
    )
    submitted_at: datetime
    finished_at: datetime | None = Field(default=None)
    current_step: int = Field(default=0, description="Index of the step currently executing (0-based).")
    total_steps: int = Field(default=0)


class PipelineRunDetail(PipelineRunSummary):
    """Full pipeline run record including per-step breakdown."""

    steps: list[StepStatus] = Field(default_factory=list)
    error: str | None = Field(default=None, description="Top-level error message if the run failed.")
    start_from_step: str | None = Field(
        default=None,
        description=(
            "Step id the run was started at, if the caller requested one. Steps before it "
            "carry skip_reason 'forced'. Null for a normal full-pipeline run."
        ),
    )
    jobs_ahead: int | None = Field(
        default=None,
        description=(
            "Number of Batch jobs submitted before this one that are still waiting "
            "for compute capacity. Cloud mode only; null in local mode or when no "
            "step is currently pending on the Batch queue."
        ),
    )
    estimated_wait_seconds: float | None = Field(
        default=None,
        description=(
            "Estimated seconds until this job reaches the front of the queue, "
            "computed as the sum of (num_subjects × time_per_subject_seconds) for "
            "each job submitted ahead of this one. Null when timing data is "
            "unavailable for any ahead job."
        ),
    )


class PipelineRunLogs(BaseModel):
    """Aggregated logs for all completed steps in a pipeline run."""

    run_id: str
    logs: str = Field(description="Concatenated log output from all steps executed so far.")


class PipelineRunCreated(BaseModel):
    """Response after successfully submitting a pipeline run."""

    run_id: str = Field(description="Use this ID to poll /jobs/pipelines/{run_id} for status.")
    status: Literal["pending"] = Field(default="pending")


class FinishedRunsResponse(BaseModel):
    """Response from the finished-runs polling endpoint."""

    runs: list[PipelineRunSummary] = Field(
        description=(
            "Pipeline runs that reached a terminal state (succeeded or failed) "
            "since the last call to this endpoint for this user. "
            "Empty when no runs have finished since the last poll."
        )
    )
    polled_at: datetime = Field(
        description=(
            "Server timestamp when this response was generated. "
            "Stored server-side as the cursor for the next call — "
            "only runs finishing after this moment appear in the next response."
        )
    )


# ── Dry-run plan ──────────────────────────────────────────────────────────────

class RunPlanStep(BaseModel):
    """Predicted treatment of one pipeline step, had the run been submitted now."""

    step_id: str = Field(description="Step identifier within the pipeline.")
    tool_id: str = Field(description="Tool this step invokes.")
    name: str | None = Field(
        default=None, description="Step display name (step YAML, else the tool's name)."
    )
    action: Literal["run", "reuse", "skip_forced"] = Field(
        description=(
            "'run' — the step would execute. 'reuse' — its cached result would be "
            "reused and nothing would execute. 'skip_forced' — it precedes "
            "start_from_step and would be skipped unchecked."
        )
    )
    will_reuse: bool = Field(description="Convenience flag: action == 'reuse'.")
    cached_from_run_id: str | None = Field(
        default=None,
        description="Run that produced the cached result. Null unless will_reuse.",
    )
    reason: str | None = Field(
        default=None,
        description=(
            "Short human-readable explanation of why the step would NOT be reused. "
            "One of: 'cache reuse disabled', 'no previous successful run', "
            "'inputs changed since last run', 'inputs missing', 'outputs missing', "
            "'upstream step will re-run', 'start step always runs'. "
            "Null when will_reuse, and null for skip_forced steps."
        ),
    )
    subjects: int = Field(
        description=(
            "Subjects this step will process when the run reaches it — the number the "
            "real run will compute at that point, and therefore also what it will chunk "
            "on. The largest per-input count over the step's resolved inputs: a "
            "directory counts its .nii/.nii.gz files, a file always counts as 1, "
            "floored at 1. An input DIRECTORY that an earlier step is predicted to fill "
            "is credited with that step's count, since on disk it is still empty; file "
            "inputs are never inflated, because the real run counts a file as one "
            "subject whatever it contains. estimated_seconds is based on this, not on "
            "the project-level subject_count."
        )
    )
    estimated_seconds: float | None = Field(
        default=None,
        description=(
            "time_per_subject_seconds x subjects for a step that would run; 0 for "
            "'reuse' and 'skip_forced'. Null when the tool declares no timing."
        ),
    )
    time_per_subject_seconds: float | None = Field(
        default=None, description="Echoed from the tool spec. Null if the tool declares none."
    )
    fixed_seconds: float | None = Field(
        default=None,
        description=(
            "Echoed from the tool spec: one-off per-invocation cost independent of "
            "cohort size. estimated_seconds is fixed_seconds + "
            "time_per_subject_seconds x subjects, with either treated as 0 when absent."
        ),
    )
    parallelizable: bool = Field(
        default=False,
        description=(
            "Whether the tool may be split into parallel chunks. Informational only: "
            "estimates assume serial execution."
        ),
    )
    subjects_per_chunk: int | None = Field(
        default=None,
        description="Tool's chunk size when parallelizable. Null when not chunked.",
    )
    in_flight_elsewhere: bool = Field(
        default=False,
        description=(
            "An identical step (same cache key) is executing in another run right now. "
            "The real run would wait for it and then reuse its result, so this step may "
            "cost less than estimated_seconds. Not predicted as a reuse because the "
            "sibling could still fail."
        ),
    )
    missing_inputs: list[str] = Field(
        default_factory=list,
        description=(
            "Project-relative resolved input paths that do not exist on disk right now. "
            "Only populated for the start_from_step step, as a warning for the UI — it "
            "never blocks the plan or a submit."
        ),
    )


class RunPlan(BaseModel):
    """What a pipeline submission would do, without submitting it.

    Read-only: creates no run record, submits no jobs, and does not touch the
    step cache.
    """

    pipeline_id: str
    reuse_cached_steps: bool = Field(description="Echoed from the request.")
    start_from_step: str | None = Field(
        default=None, description="Echoed from the request."
    )
    ready: bool = Field(
        description=(
            "Whether the project satisfies the pipeline's readiness requirements. "
            "A plan is still returned when false — each step's prediction remains "
            "meaningful — so this is informational."
        )
    )
    subject_count: int = Field(
        description=(
            "Project-level subject count, using the same rule as the readiness check: "
            "the number of subjects complete across all required modalities when the "
            "pipeline needs two or more, otherwise the smallest per-modality imaging "
            "count. Per-step estimates use each step's own count (see "
            "RunPlanStep.subjects), which may differ."
        )
    )
    steps: list[RunPlanStep] = Field(default_factory=list)
    estimated_total_seconds: float = Field(
        description=(
            "Sum of estimated_seconds over steps that would run. Assumes serial "
            "execution and does not divide by chunks or workers. Steps whose tool "
            "declares no timing contribute 0, so treat this as a LOWER BOUND and see "
            "steps_missing_timing for how many were unaccounted for. Never null."
        )
    )
    estimated_full_seconds: float = Field(
        description=(
            "Same sum as if nothing were reused — the upper bound on reusable work. "
            "Excludes skip_forced steps. Untimed steps contribute 0 here too."
        )
    )
    steps_missing_timing: int = Field(
        default=0,
        description=(
            "Steps that would run but whose tool declares neither "
            "time_per_subject_seconds nor fixed_seconds, and so contributed 0 to "
            "estimated_total_seconds. When above zero the total is a minimum; the "
            "steps themselves carry estimated_seconds: null."
        ),
    )
    steps_reused: int = Field(description="Steps predicted to be reused from cache.")
    steps_forced_skip: int = Field(description="Steps skipped because of start_from_step.")
    steps_total: int = Field(description="Total steps in the pipeline.")
