"""
Singularity/Apptainer backend — runs tool jobs directly via ``apptainer``/``singularity``.

This backend is selected when ``NICHART_EXECUTION_MODE=local`` AND
``NICHART_SIF_DIR`` is set to a directory containing pre-built .sif files.
Build SIF files first with ``scripts/build-sif-registry.py``.

The API server must run natively on the host (not inside a Docker container) when
this backend is active, because Singularity runs containers as child processes rather
than via a daemon.

Invocation modes
----------------
Singularity/Apptainer has two relevant run modes:

    apptainer run  sif.sif [args...]
        Runs the container's runscript (entrypoint).  Use this when the tool
        command template is *args only* — i.e. it starts with ``-``.
        Equivalent to ``docker run image [args...]``.

    apptainer exec sif.sif cmd [args...]
        Runs ``cmd`` directly inside the container.  Use this when the command
        template includes the executable (e.g. ``sleep {duration_seconds}``).

Auto-detection: the backend uses "run" if the rendered command starts with ``-``,
otherwise "exec".  Override per-tool by setting ``container.singularity_run_mode``
in the tool YAML to ``"run"`` or ``"exec"``.

SIF naming convention
---------------------
Docker tag → SIF filename: replace ``/`` and ``:`` with ``_``.

    alpine:3.19                          → alpine_3.19.sif
    cbica/nichart_dlmuse:1.0.10-wrapped  → cbica_nichart_dlmuse_1.0.10-wrapped.sif
"""

import asyncio
import os
import shlex
import tempfile
import uuid
from pathlib import Path
from typing import Any

from app.backends.base import JobBackend, JobHandle, ToolSpec
from app.config import Settings


def _docker_tag_to_sif_name(image: str) -> str:
    """Convert a Docker image tag to the canonical SIF filename (mirrors build script)."""
    return image.replace("/", "_").replace(":", "_") + ".sif"


class SingularityJobHandle(JobHandle):
    """Wraps a running ``apptainer`` subprocess."""

    def __init__(
        self,
        proc: asyncio.subprocess.Process,
        log_path: Path,
        job_id: str,
    ) -> None:
        self._proc = proc
        self._log_path = log_path
        self._job_id = job_id

    @property
    def job_id(self) -> str:
        return self._job_id

    async def status(self) -> str:
        # asyncio updates returncode in the background via the subprocess watcher;
        # between polling sleep intervals the event loop has time to set it.
        if self._proc.returncode is None:
            return "running"
        return "succeeded" if self._proc.returncode == 0 else "failed"

    async def logs(self, tail: int = 200) -> str:
        if not self._log_path.exists():
            return "[No output yet]"
        lines = self._log_path.read_text(errors="replace").splitlines()
        return "\n".join(lines[-tail:])

    async def cancel(self) -> None:
        if self._proc.returncode is None:
            try:
                self._proc.terminate()
                await asyncio.wait_for(self._proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                self._proc.kill()
            except Exception:
                pass


class SingularityBackend(JobBackend):
    """Runs tool jobs as Singularity/Apptainer containers on the local host."""

    def __init__(self, settings: Settings) -> None:
        if not settings.sif_dir:
            raise ValueError("NICHART_SIF_DIR must be set to use the Singularity backend")
        self._sif_dir = settings.sif_dir
        self._runner = settings.container_runner
        self._data_root = settings.data_root

    @property
    def backend_name(self) -> str:
        return "singularity"

    def _sif_path(self, image: str) -> Path:
        sif_name = _docker_tag_to_sif_name(image)
        path = self._sif_dir / sif_name
        if not path.exists():
            raise FileNotFoundError(
                f"SIF not found: {path}\n"
                f"Run:  python scripts/build-sif-registry.py --sif-dir {self._sif_dir}"
            )
        return path

    def _effective_run_mode(self, tool_spec: ToolSpec, rendered_command: str) -> str:
        """Determine whether to use 'run' or 'exec' for this tool invocation."""
        if tool_spec.singularity_run_mode in ("run", "exec"):
            return tool_spec.singularity_run_mode
        # Auto-detect: if the command starts with '-', it's args to an entrypoint.
        return "run" if rendered_command.lstrip().startswith("-") else "exec"

    def _build_apptainer_argv(
        self,
        tool_spec: ToolSpec,
        mount_paths: dict[str, str],
        params: dict[str, Any],
        extra_readonly_mounts: list[str] | None = None,
    ) -> list[str]:
        """Build the full apptainer argv list for a tool invocation (shared with SlurmBackend)."""
        resolved_params = tool_spec.resolve_params(params)
        command = tool_spec.render_command(resolved_params)
        sif_path = self._sif_path(tool_spec.image)
        run_mode = self._effective_run_mode(tool_spec, command)

        bind_args: list[str] = []
        for label, container_path_str in mount_paths.items():
            if label not in tool_spec.mounts:
                continue
            mount = tool_spec.mounts[label]
            host_path = Path(container_path_str)
            mode = "ro" if mount.mode == "ro" else "rw"
            if mount.mount_type == "output_file":
                host_dir = host_path.parent
                container_dir = str(Path(mount.path_in_container).parent)
                host_dir.mkdir(parents=True, exist_ok=True)
                bind_args += ["--bind", f"{host_dir}:{container_dir}:{mode}"]
            elif mount.mount_type == "input_file":
                bind_args += ["--bind", f"{host_path}:{mount.path_in_container}:{mode}"]
            else:
                host_path.mkdir(parents=True, exist_ok=True)
                bind_args += ["--bind", f"{host_path}:{mount.path_in_container}:{mode}"]

        # Extra read-only mounts so absolute symlinks in chunk input dirs resolve.
        # API server runs natively here, so paths are already host paths.
        for path_str in (extra_readonly_mounts or []):
            bind_args += ["--bind", f"{path_str}:{path_str}:ro"]

        gpus = (tool_spec.resources or {}).get("gpus", 0)
        gpu_args = ["--nv"] if gpus else []

        # Overlay a throwaway writable tmpfs on the (read-only) container root so
        # tools that write inside the image — /tmp, $HOME, caches, or hardcoded
        # in-image paths that aren't bind-mounted — don't fail with "Read-only
        # file system". Docker's writable root layer masks this; Singularity/
        # Apptainer mount the container root read-only by default. These writes go
        # to RAM and are discarded at exit, so real outputs must still land in the
        # bound output mounts above.
        base_flags = ["--writable-tmpfs", *gpu_args]
        cmd_tokens = shlex.split(command)

        if run_mode == "run":
            return [self._runner, "run", *base_flags, *bind_args, str(sif_path), *cmd_tokens]
        else:
            return [self._runner, "exec", *base_flags, *bind_args, str(sif_path), *cmd_tokens]

    async def submit(
        self,
        tool_spec: ToolSpec,
        mount_paths: dict[str, str],
        params: dict[str, Any],
        num_subjects: int = 1,
        user_token: str | None = None,
        extra_readonly_mounts: list[str] | None = None,
        study_id: str | None = None,  # ignored: acts on the study dir in place
    ) -> SingularityJobHandle:
        argv = self._build_apptainer_argv(tool_spec, mount_paths, params, extra_readonly_mounts)

        # Capture stdout + stderr to a temp file so logs() can read them at any time.
        log_fd, log_path_str = tempfile.mkstemp(
            prefix="nichart-sg-", suffix=f"-{tool_spec.tool_id}.log"
        )
        log_path = Path(log_path_str)

        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=log_fd,
            stderr=log_fd,
        )
        # Close our copy of the fd — the subprocess holds its own inherited copy.
        os.close(log_fd)

        job_id = f"sg-{tool_spec.tool_id}-{uuid.uuid4().hex[:8]}"
        return SingularityJobHandle(proc, log_path, job_id)
