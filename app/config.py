import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Package dir (…/app) and its parent (the repo root in a source checkout, or
# site-packages when installed). Used to locate bundled resources and .env files
# independently of the current working directory, so the server runs from anywhere.
_PKG_DIR = Path(__file__).resolve().parent
_SRC_ROOT = _PKG_DIR.parent


def _default_resources_path() -> Path:
    """Locate the pipeline/tool ``resources/`` directory without relying on cwd.

    Prefers the copy bundled inside the installed package (``app/resources``,
    force-included in the wheel), then the top-level ``resources/`` of a dev
    checkout. ``NICHART_RESOURCES_PATH`` overrides this default.
    """
    for candidate in (_PKG_DIR / "resources", _SRC_ROOT / "resources"):
        if candidate.is_dir():
            return candidate
    return _SRC_ROOT / "resources"


def _env_files() -> tuple[str, ...]:
    """Candidate .env files, lowest → highest precedence (later wins in pydantic).

    Real ``NICHART_*`` environment variables still override all of these.
      1. ``<source root>/.env``      — dev-checkout fallback (last resort)
      2. ``~/.nichart/.env``         — the per-user configured location
      3. ``$NICHART_ENV_FILE``       — an explicit path (e.g. set by a launcher/GUI)
    """
    candidates: list[Path] = [_SRC_ROOT / ".env", Path.home() / ".nichart" / ".env"]
    explicit = os.environ.get("NICHART_ENV_FILE")
    if explicit:
        candidates.append(Path(explicit))
    return tuple(str(c) for c in candidates)


class Settings(BaseSettings):
    """
    All runtime configuration for the NiChart API.

    Every field is read from environment variables prefixed with ``NICHART_``.
    Config is also loaded from .env files (see ``_env_files``): a per-user
    ``~/.nichart/.env``, with a dev-checkout ``<repo>/.env`` as a last-resort
    fallback, and ``$NICHART_ENV_FILE`` as an explicit override. Environment
    variables take precedence over all .env files.
    """

    model_config = SettingsConfigDict(
        env_prefix="NICHART_",
        env_file=_env_files(),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    execution_mode: Literal["cloud", "local"] = Field(
        default="local",
        description="Job execution backend. 'local' uses Docker; 'cloud' uses AWS Batch via Lambda.",
    )
    data_root: Path = Field(
        default=Path("/data"),
        description="Root directory under which all user project data is stored.",
    )
    resources_path: Path = Field(
        default_factory=_default_resources_path,
        description=(
            "Directory containing pipeline and tool YAML definitions. Defaults to the "
            "copy bundled with the installed package (or resources/ in a dev checkout); "
            "resolved independently of the working directory."
        ),
    )

    # AWS Cognito — only used when execution_mode == "cloud"
    cognito_region: str = Field(default="us-east-1")
    cognito_user_pool_id: str = Field(default="us-east-1_BSBhcKA66")
    cognito_identity_pool_id: str = Field(
        default="us-east-1:12c87a16-8336-450c-bf25-b98990c7dcf8"
    )
    cognito_client_id: str = Field(
        default="1ugglpalgp9r2gvb24s2v7dunq",
        description=(
            "Cognito App Client ID. ID tokens must be issued for this client "
            "(audience claim check). Public SPA client — no secret."
        ),
    )

    # AWS Batch / Lambda — only used when execution_mode == "cloud"
    batch_queue_name: str = Field(
        default="cbica-nichart-jobqueue-standard",
        description="Name of the AWS Batch job queue used for pipeline jobs.",
    )
    lambda_function_name: str = Field(
        default="cbica-nichart-submitjob",
        description="Name of the Lambda function used to submit Batch jobs.",
    )

    # ── BFF OAuth / Cognito Hosted UI ────────────────────────────────────────────
    # Only required in cloud mode when the BFF auth flow is used.
    cognito_domain: str = Field(
        default="",
        description=(
            "Cognito Hosted UI domain (no trailing slash). "
            "e.g. 'https://myapp.auth.us-east-1.amazoncognito.com'. "
            "Required for GET /auth/login and GET /auth/callback."
        ),
    )
    cognito_client_secret: str = Field(
        default="",
        description=(
            "Cognito app client secret. Never commit this value — "
            "pass via NICHART_COGNITO_CLIENT_SECRET environment variable "
            "or inject from AWS Secrets Manager at startup."
        ),
    )
    frontend_url: str = Field(
        default="http://localhost:3000",
        description="Frontend origin to redirect to after successful login or logout.",
    )
    api_base_url: str = Field(
        default="http://localhost:8000",
        description=(
            "Public base URL of this API server. Used to construct the OAuth "
            "redirect_uri that Cognito calls back to. Must exactly match one of the "
            "registered callback URLs in the Cognito app client."
        ),
    )
    cookie_domain: str | None = Field(
        default=None,
        description=(
            "Domain attribute for auth cookies. Set to '.myapp.com' when the API "
            "and frontend share an eTLD+1 (e.g. api.myapp.com + myapp.com). "
            "Leave unset for localhost development."
        ),
    )
    cookie_secure: bool = Field(
        default=True,
        description=(
            "Set the Secure flag on auth cookies. "
            "Must be False for plain-HTTP local development (http://localhost). "
            "Always True in production."
        ),
    )

    cors_origins: list[str] = Field(
        default=["http://localhost:3000"],
        description=(
            "List of allowed CORS origins. In production set this to the UI's domain. "
            "Use [\"*\"] only for fully public, unauthenticated APIs."
        ),
    )

    # S3 data sync — cloud mode only.
    # When s3_data_bucket is set the job orchestrator syncs project data to S3
    # before submitting each step and pulls results back after completion.
    # This removes the FSx dependency for cloud-local testing and makes FSx
    # optional in production (FSx remains useful for concurrent jobs at scale).
    s3_data_bucket: str | None = Field(
        default=None,
        description=(
            "S3 bucket for project data sync. When set, data is uploaded before "
            "each pipeline step and downloaded after. e.g. 'cbica-nichart-io'."
        ),
    )
    s3_data_prefix: str = Field(
        default="fsx",
        description="Key prefix within s3_data_bucket mirroring the FSx layout.",
    )

    staging_ttl_hours: int = Field(
        default=24,
        description="Hours after which uncommitted staging uploads are eligible for cleanup.",
    )

    enable_test_endpoints: bool = Field(
        default=False,
        description=(
            "Mount /test/* convenience endpoints for debugging and manual QA. "
            "Never enable in production — these endpoints bypass normal constraints."
        ),
    )

    enable_intermediate_uploads: bool = Field(
        default=True,
        description=(
            "Allow manual upload of intermediate pipeline data, so a user who "
            "already holds partly-processed results can start a pipeline from a "
            "later step. An advanced, unsupported workflow: the uploaded data "
            "is taken on trust beyond the declared content checks. When false "
            "the routes are not mounted at all and return 404, which is how a "
            "client detects that a deployment has turned the feature off."
        ),
    )

    max_upload_file_mb: int = Field(
        default=1024,
        description=(
            "Largest single file accepted by an intermediate upload, in MiB. "
            "A file over the limit is rejected individually; the rest of the "
            "request still proceeds."
        ),
    )

    max_upload_request_mb: int = Field(
        default=10240,
        description=(
            "Largest total payload accepted by one intermediate upload request, "
            "in MiB. Exceeding it fails the whole request, since a partially "
            "written slot is worse than none."
        ),
    )

    inactivity_timeout_seconds: int = Field(
        default=0,
        description=(
            "Idle auto-shutdown: if > 0, the server shuts itself down after this many "
            "seconds with no API activity AND no in-progress pipeline runs. A long "
            "external job (SLURM/Batch) keeps its run 'running', which holds the timer "
            "off until the job finishes. Set to 0 or -1 to disable (default). The CLI's "
            "auto-spawned servers enable this by default as a courtesy on shared systems."
        ),
    )

    # Project retention — must be kept in sync with the Lambda sweep config.
    project_retention_days: int = Field(
        default=7,
        description=(
            "Number of days after the last heartbeat write before a project is eligible "
            "for automatic deletion by the Lambda sweep. Must match the Lambda's config."
        ),
    )

    # Custom CA bundle — needed when an SSL-inspection proxy (e.g. corporate VPN)
    # intercepts outbound HTTPS and presents a self-signed chain.
    # Point this at the PEM file containing the extra root CA(s).
    # Leave unset in production; the system trust store is used by default.
    ca_bundle: Path | None = Field(
        default=None,
        description=(
            "Path to a PEM CA bundle to trust for outbound HTTPS calls (e.g. Cognito token "
            "endpoint). Only needed behind SSL-inspection proxies. Leave unset in production."
        ),
    )

    @field_validator("ca_bundle", mode="before")
    @classmethod
    def _empty_str_to_none(cls, v: object) -> object:
        return None if v == "" else v

    # Explicit backend override.
    # When set, this takes precedence over the execution_mode auto-selection.
    # Valid values: "docker", "singularity", "batch".
    # When unset (default), the backend is chosen automatically:
    #   execution_mode=cloud                         → batch
    #   execution_mode=local + sif_dir is set        → singularity
    #   execution_mode=local (default)               → docker
    job_backend: str | None = Field(
        default=None,
        description=(
            "Explicit job backend override: 'docker', 'singularity', or 'batch'. "
            "When unset, the backend is inferred from execution_mode and sif_dir."
        ),
    )

    # Singularity/Apptainer local mode.
    # When sif_dir is set AND execution_mode is 'local', the Singularity backend is
    # used instead of Docker. Run scripts/build-sif-registry.py first to populate it.
    sif_dir: Path | None = Field(
        default=None,
        description=(
            "Directory containing pre-built .sif images. "
            "When set in local mode, the Singularity backend is used instead of Docker."
        ),
    )
    container_runner: str = Field(
        default="apptainer",
        description="Singularity runner command ('apptainer' or 'singularity').",
    )

    # Docker (local mode) — host-side path to the data directory so sibling containers
    # can mount the same directory via DooD (Docker outside Docker).
    # When None, data_root is used as-is (works when running outside Docker).
    host_data_root: Path | None = Field(
        default=None,
        description=(
            "Host-side path to the data root, used by the Docker backend to mount "
            "the data directory into sibling containers. Required when the API server "
            "itself runs inside Docker and spawns sibling containers (DooD). "
            "Defaults to data_root when not set."
        ),
    )

    # SLURM backend — only used when NICHART_JOB_BACKEND=slurm.
    # Requires apptainer on the compute nodes and NICHART_SIF_DIR set.
    slurm_partition: str | None = Field(
        default=None,
        description="SLURM partition for job submissions (e.g. 'gpu'). Uses cluster default if unset.",
    )
    slurm_account: str | None = Field(
        default=None,
        description="SLURM account/project to charge (--account flag). Uses user default if unset.",
    )
    slurm_extra_sbatch_args: list[str] = Field(
        default=[],
        description="Extra sbatch flags appended verbatim, e.g. ['--constraint=h100'].",
    )
    slurm_logs_dir: Path | None = Field(
        default=None,
        description=(
            "Directory for SLURM job log files. Named {job_id}.log. "
            "Defaults to <data_root>/_slurm_logs. "
            "Must be on a filesystem accessible to both the API server and compute nodes."
        ),
    )
    slurm_time_safety_factor: float = Field(
        default=2.0,
        description="Multiply estimated job time by this factor when setting --time.",
    )
    slurm_default_time_minutes: int = Field(
        default=1440,
        description="SLURM --time limit (minutes) when no time_per_subject_seconds estimate is available.",
    )
    slurm_sbatch_cmd: str = Field(default="sbatch", description="Path or name of the sbatch executable.")
    slurm_squeue_cmd: str = Field(default="squeue", description="Path or name of the squeue executable.")
    slurm_sacct_cmd: str = Field(default="sacct", description="Path or name of the sacct executable.")
    slurm_scancel_cmd: str = Field(default="scancel", description="Path or name of the scancel executable.")

    @property
    def jwks_url(self) -> str:
        return (
            f"https://cognito-idp.{self.cognito_region}.amazonaws.com"
            f"/{self.cognito_user_pool_id}/.well-known/jwks.json"
        )

    @property
    def cognito_issuer(self) -> str:
        return (
            f"https://cognito-idp.{self.cognito_region}.amazonaws.com"
            f"/{self.cognito_user_pool_id}"
        )

    @property
    def auth_callback_url(self) -> str:
        """Absolute redirect_uri for the OAuth callback. Must match the Cognito app client registration."""
        return f"{self.api_base_url.rstrip('/')}/auth/callback"

    @property
    def pipelines_path(self) -> Path:
        return self.resources_path / "pipelines"

    @property
    def tools_path(self) -> Path:
        return self.resources_path / "tools"

    @property
    def docs_path(self) -> Path:
        return self.resources_path / "docs"


@lru_cache
def get_settings() -> Settings:
    """Return the cached application settings singleton."""
    return Settings()
