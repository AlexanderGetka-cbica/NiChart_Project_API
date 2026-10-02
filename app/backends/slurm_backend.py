"""
SLURM backend — submits Apptainer tool jobs to SLURM via ``sbatch``.

Extends SingularityBackend to reuse SIF lookup and apptainer argv construction,
then wraps the resulting command in an sbatch call with:

  - Resource flags derived from the tool YAML (--cpus-per-task, --mem, --gres=gpu:N)
  - A time limit computed from time_per_subject_seconds * num_subjects * safety_factor
  - A log file at ``<slurm_logs_dir>/{slurm_job_id}.log`` (SLURM's ``%j`` expansion)

Log path convention
-------------------
Logs are named by SLURM job ID: ``{slurm_logs_dir}/{slurm_job_id}.log``.
This makes the path reconstructible from the stored job ID alone, enabling
``reconnect()`` to poll and fetch logs after an API server restart without
any additional stored state.

Restart recovery
----------------
The SLURM scheduler manages job lifecycle independently of the API server.
``SlurmBackend.reconnect(job_id)`` reconstructs a polling handle from the
stored SLURM job ID so ``run_pipeline_task`` can resume tracking a job that
was submitted before the server restarted.
"""

import asyncio
import shlex
import uuid
from pathlib import Path
from typing import Any

from app.backends.base import JobHandle, ToolSpec
from app.backends.singularity_backend import SingularityBackend
from app.config import Settings


class SlurmJobHandle(JobHandle):
    """Handle for a SLURM job — polls via squeue/sacct, cancels via scancel."""

    _SQUEUE_MAP: dict[str, str] = {
        "PENDING": "pending",
        "CONFIGURING": "pending",
        "RUNNING": "running",
        "COMPLETING": "running",
        "COMPLETED": "succeeded",
        "FAILED": "failed",
        "CANCELLED": "failed",
        "TIMEOUT": "failed",
        "NODE_FAIL": "failed",
        "OUT_OF_MEMORY": "failed",
        "PREEMPTED": "failed",
    }

    def __init__(
        self,
        job_id: str,
        log_path: Path,
        squeue_cmd: str,
        sacct_cmd: str,
        scancel_cmd: str,
    ) -> None:
        self._job_id = job_id
        self._log_path = log_path
        self._squeue_cmd = squeue_cmd
        self._sacct_cmd = sacct_cmd
        self._scancel_cmd = scancel_cmd

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def log_path(self) -> str | None:
        return str(self._log_path)

    async def status(self) -> str:
        # squeue is fast and covers active jobs.
        proc = await asyncio.create_subprocess_exec(
            self._squeue_cmd, "-j", self._job_id, "-h", "-o", "%T",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        state = stdout.decode().strip().upper()
        if state:
            return self._SQUEUE_MAP.get(state, "running")

        # Job no longer in squeue → finished. sacct has the final state.
        proc2 = await asyncio.create_subprocess_exec(
            self._sacct_cmd, "-j", self._job_id, "-o", "State", "-n", "-X",
            "--parsable2",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout2, _ = await proc2.communicate()
        # sacct may suffix state with "+", e.g. "CANCELLED+"
        raw = stdout2.decode().strip().split("\n")[0].strip().rstrip("+").upper()
        return self._SQUEUE_MAP.get(raw, "failed")

    async def logs(self, tail: int = 200) -> str:
        if not self._log_path.exists():
            return f"[SLURM job {self._job_id} — log not yet written]"
        lines = self._log_path.read_text(errors="replace").splitlines()
        return "\n".join(lines[-tail:])

    async def cancel(self) -> None:
        try:
            proc = await asyncio.create_subprocess_exec(
                self._scancel_cmd, self._job_id,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()
        except Exception:
            pass


def _format_time_limit(
    time_per_subject_seconds: float | None,
    num_subjects: int,
    safety_factor: float,
    default_minutes: int,
) -> str:
    """Return an ``HH:MM:SS`` SLURM time string."""
    if time_per_subject_seconds:
        total = int(time_per_subject_seconds * num_subjects * safety_factor)
        total = max(total, 300)  # at least 5 minutes
    else:
        total = default_minutes * 60
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h}:{m:02d}:{s:02d}"


class SlurmBackend(SingularityBackend):
    """Submits Apptainer tool jobs to SLURM via ``sbatch``.

    Inherits SIF path resolution and apptainer argv construction from
    ``SingularityBackend``.  Each step becomes one ``sbatch --wrap`` call.

    Resource mapping (tool YAML → sbatch flags):
      resources.vcpus   → --cpus-per-task
      resources.memory  → --mem (MiB)
      resources.gpus    → --gres=gpu:N (omitted when 0)

    Time limit:
      time_per_subject_seconds * num_subjects * slurm_time_safety_factor,
      rounded up to the nearest second, minimum 5 minutes.
      Falls back to slurm_default_time_minutes when no estimate is available.
    """

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self._logs_dir: Path = settings.slurm_logs_dir or (settings.data_root / "_slurm_logs")
        self._partition = settings.slurm_partition
        self._account = settings.slurm_account
        self._extra_args = list(settings.slurm_extra_sbatch_args)
        self._safety_factor = settings.slurm_time_safety_factor
        self._default_time_minutes = settings.slurm_default_time_minutes
        self._sbatch = settings.slurm_sbatch_cmd
        self._squeue = settings.slurm_squeue_cmd
        self._sacct = settings.slurm_sacct_cmd
        self._scancel = settings.slurm_scancel_cmd

    @property
    def backend_name(self) -> str:
        return "slurm"

    def _make_handle(self, slurm_job_id: str) -> SlurmJobHandle:
        log_path = self._logs_dir / f"{slurm_job_id}.log"
        return SlurmJobHandle(slurm_job_id, log_path, self._squeue, self._sacct, self._scancel)

    def reconnect(self, job_id: str, log_path: str | None = None) -> SlurmJobHandle:
        """Reconstruct a polling handle from a stored SLURM job ID (restart recovery).

        The log path is derived from the job ID using the same naming convention as
        submit(), so no additional stored state is needed.
        """
        return self._make_handle(job_id)

    async def submit(
        self,
        tool_spec: ToolSpec,
        mount_paths: dict[str, str],
        params: dict[str, Any],
        num_subjects: int = 1,
        user_token: str | None = None,
        extra_readonly_mounts: list[str] | None = None,
        study_id: str | None = None,  # ignored: acts on the study dir in place
    ) -> SlurmJobHandle:
        apptainer_argv = self._build_apptainer_argv(tool_spec, mount_paths, params, extra_readonly_mounts)

        res = tool_spec.resources or {}
        vcpus = int(res.get("vcpus", 1))
        memory_mib = int(res.get("memory", 4096))
        gpus = int(res.get("gpus", 0))

        time_limit = _format_time_limit(
            tool_spec.time_per_subject_seconds,
            num_subjects,
            self._safety_factor,
            self._default_time_minutes,
        )

        job_name = f"nichart-{tool_spec.tool_id}-{uuid.uuid4().hex[:6]}"

        self._logs_dir.mkdir(parents=True, exist_ok=True)
        # SLURM replaces %j with the assigned job ID at submission time.
        log_template = str(self._logs_dir / "%j.log")

        sbatch_args = [
            self._sbatch,
            "--parsable",              # output: JOBID or JOBID;CLUSTER
            f"--job-name={job_name}",
            f"--cpus-per-task={vcpus}",
            f"--mem={memory_mib}M",
            f"--time={time_limit}",
            f"--output={log_template}",
            f"--error={log_template}",
        ]
        if gpus > 0:
            sbatch_args.append(f"--gres=gpu:{gpus}")
        if self._partition:
            sbatch_args.append(f"--partition={self._partition}")
        if self._account:
            sbatch_args.append(f"--account={self._account}")
        sbatch_args.extend(self._extra_args)
        sbatch_args += ["--wrap", shlex.join(apptainer_argv)]

        proc = await asyncio.create_subprocess_exec(
            *sbatch_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            raise RuntimeError(
                f"sbatch failed (exit {proc.returncode}): {stderr.decode().strip()}"
            )

        # --parsable prints "JOBID" or "JOBID;CLUSTER"
        slurm_job_id = stdout.decode().strip().split(";")[0].strip()
        if not slurm_job_id.isdigit():
            raise RuntimeError(f"Unexpected sbatch output: {stdout.decode().strip()!r}")

        return self._make_handle(slurm_job_id)
