"""
Abstract job-backend interface, plus the ToolSpec dataclass that both backends consume.

Concrete implementations live in ``docker_backend.py`` (local Docker) and
``batch_backend.py`` (AWS Batch via Lambda). All pipeline orchestration code
depends only on these abstractions.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


# ── Tool specification ────────────────────────────────────────────────────────

@dataclass
class MountSpec:
    """One mount entry from a tool YAML's ``mounts`` section."""
    path_in_container: str
    mode: str = "ro"
    # "directory" | "input_file" | "output_file" — drives how backends prepare the host path
    # and construct the bind-mount (parent-dir mount for output files to avoid Docker
    # auto-creating a directory at the file path when the host file doesn't exist yet).
    mount_type: str = "directory"


@dataclass
class ToolSpec:
    """
    Complete internal representation of a tool YAML, consumed by backends.

    Distinct from ``ToolDetail`` (the API response model) — it carries the
    container image, command template, and mount configuration that only the
    execution layer needs.
    """
    tool_id: str
    name: str
    image: str
    command_template: str
    mounts: dict[str, MountSpec] = field(default_factory=dict)
    parameters: dict[str, Any] = field(default_factory=dict)  # raw spec dicts from YAML
    resources: dict[str, Any] = field(default_factory=dict)   # vcpus, memory, gpus
    time_per_subject_seconds: float | None = None
    # Singularity invocation mode for this tool.
    # "run"  → apptainer run  sif <args>    (command template is args to the container entrypoint)
    # "exec" → apptainer exec sif <cmd>     (command template begins with the executable)
    # None   → auto-detect: use "run" if rendered command starts with '-', else "exec"
    singularity_run_mode: str | None = None

    # ── Parallelization ───────────────────────────────────────────────────────
    # When True, the orchestrator may split directory inputs into subject chunks
    # and submit them as parallel backend jobs, then merge results.
    parallelizable: bool = False
    # Default chunk size (subjects per backend job). None → use global default (10).
    subjects_per_chunk: int | None = None
    # Per-output merge strategy keyed by output label.
    # "directory_union"            — merge all files; NIfTI filenames must be unique (MRID-keyed)
    # "directory_union_csv_concat" — same, but CSVs with matching names are row-concatenated
    # "csv_concat"                 — concatenate a single CSV file output (header once, rows appended)
    output_merge: dict[str, str] = field(default_factory=dict)

    # ── Metadata ──────────────────────────────────────────────────────────────
    github_url: str | None = None

    def render_command(self, params: dict[str, Any]) -> str:
        """Substitute mount container paths and resolved params into the command template."""
        cmd = self.command_template
        for label, mount in self.mounts.items():
            cmd = cmd.replace(f"{{{label}}}", mount.path_in_container)
        for key, val in params.items():
            cmd = cmd.replace(f"{{{key}}}", str(val))
        return cmd

    def resolve_params(self, user_params: dict[str, Any]) -> dict[str, Any]:
        """Merge user-supplied params with YAML defaults. Raises ValueError for missing required params."""
        result: dict[str, Any] = {}
        for key, spec in self.parameters.items():
            val = user_params.get(key, spec.get("default"))
            if val is None and "default" not in spec:
                raise ValueError(f"Missing required parameter: '{key}'")
            result[key] = val
        return result


# ── Job handle ────────────────────────────────────────────────────────────────

class JobHandle(ABC):
    """Reference to a running or completed job on a specific backend."""

    @abstractmethod
    async def status(self) -> str:
        """
        Return the current job status as a normalised lowercase string.

        Common values: ``"running"``, ``"succeeded"``, ``"failed"``,
        ``"pending"``, ``"cancelled"``.
        """

    @abstractmethod
    async def logs(self, tail: int = 200) -> str:
        """Return the most recent *tail* lines of job output as a single string."""

    @abstractmethod
    async def cancel(self) -> None:
        """Request cancellation of the job. No-op if already terminal."""

    @property
    @abstractmethod
    def job_id(self) -> str:
        """Stable identifier for this job (container name or AWS Batch job ID)."""

    @property
    def log_path(self) -> str | None:
        """Persistent log file path, if available (e.g. SLURM output file). None otherwise."""
        return None


# ── Backend ───────────────────────────────────────────────────────────────────

class JobBackend(ABC):
    """
    Abstraction over a job execution environment.

    Exactly one backend is instantiated per server process, chosen by
    ``NICHART_EXECUTION_MODE``.
    """

    @property
    def backend_name(self) -> str:
        """Short lowercase identifier for this backend ('docker', 'singularity', 'slurm', 'batch')."""
        return "unknown"

    def reconnect(self, job_id: str, log_path: str | None = None) -> "JobHandle | None":
        """
        Reconstruct a ``JobHandle`` for a previously-submitted job after an API server restart.

        Returns ``None`` if this backend cannot reconnect (Docker, Singularity — child process
        is gone after restart).  The SLURM backend overrides this to poll the scheduler by
        job ID.
        """
        return None

    @abstractmethod
    async def submit(
        self,
        tool_spec: ToolSpec,
        mount_paths: dict[str, str],
        params: dict[str, Any],
        num_subjects: int = 1,
        user_token: str | None = None,
        extra_readonly_mounts: list[str] | None = None,
        study_id: str | None = None,
    ) -> JobHandle:
        """
        Submit a containerised tool job and return a handle immediately.

        Parameters
        ----------
        tool_spec:
            Loaded tool specification (image, command template, mounts, etc.).
        mount_paths:
            Mapping of mount label → absolute host-side path. Labels must match
            the keys in ``tool_spec.mounts``.
        params:
            Parameter values for this invocation (merged from pipeline and user overrides).
        num_subjects:
            Hint for cloud queue-drain estimation; passed to the Lambda as ``num_subjects``.
        user_token:
            Cognito ID token forwarded to the Lambda (cloud mode only).
        extra_readonly_mounts:
            Additional host paths to bind-mount read-only at the same container path.
            Used when chunk input directories contain absolute symlinks into the study
            directory — the study directory must also be mounted so the symlinks resolve
            inside the container.  Batch backend ignores this (FSx is shared across nodes).
        study_id:
            Project/study directory name (the ``{project}`` segment of
            ``/fsx/fsx/{user_sub}/{project}``). Forwarded to the Lambda so the cloud
            S3<->FSx sync can be scoped to this project instead of the whole user dir.
            Local backends (docker/singularity/slurm) ignore it — they read/write the
            study directory in place.
        """
