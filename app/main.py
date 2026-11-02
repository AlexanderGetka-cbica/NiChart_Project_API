"""
NiChart API — application factory.

Import ``app`` for production use::

    uvicorn app.main:app --host 0.0.0.0 --port 8000

Call ``create_app()`` in tests to get a fresh, isolated instance.
"""

import importlib.metadata
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.auth.dependencies import public
from app.config import Settings, get_settings
from app.routers.auth import router as auth_router
from app.routers.catalog import router as catalog_router
from app.routers.cloud import router as cloud_router
from app.routers.dicom import router as dicom_router
from app.routers.docs import router as docs_router
from app.routers.files import router as files_router
from app.routers.intermediate import router as intermediate_router
from app.routers.jobs import jobs_router, project_router as jobs_project_router
from app.routers.projects import router as projects_router
from app.routers.results import router as results_router
from app.routers.test_endpoints import router as test_router

try:
    _VERSION = importlib.metadata.version("nichart-api")
except importlib.metadata.PackageNotFoundError:
    _VERSION = "0.1.0"


_REDACTED_FIELDS = {"cognito_client_secret"}
_REDACTED_ENV_VARS = {"AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}


def _log_settings(settings: Settings) -> None:
    """Log all configuration values at INFO level, redacting sensitive fields."""
    import logging
    import os

    log = logging.getLogger("uvicorn.error")
    lines = ["NiChart API — effective configuration:"]
    for field_name, value in settings.model_dump().items():
        if field_name in _REDACTED_FIELDS:
            display = "*** redacted ***"
        else:
            display = repr(value)
        lines.append(f"  NICHART_{field_name.upper():<36} = {display}")

    for var in sorted(_REDACTED_ENV_VARS):
        present = var in os.environ
        lines.append(f"  {var:<40} = {'*** redacted ***' if present else '(not set)'}")

    if settings.ca_bundle:
        p = settings.ca_bundle
        if not p.exists():
            lines.append(f"  [CA bundle] {p} — FILE NOT FOUND, falling back to system store")
        elif p.stat().st_size == 0:
            lines.append(f"  [CA bundle] {p} — empty (HOST_CA_BUNDLE unset?), falling back to system store")
        else:
            lines.append(f"  [CA bundle] {p} — OK ({p.stat().st_size} bytes)")

    log.info("\n".join(lines))


async def _inactivity_watchdog(app: FastAPI, timeout: int) -> None:
    """Shut the server down after ``timeout`` seconds of no activity and no work.

    "Activity" is any non-``/health`` request (recorded by the middleware); "work"
    is any pending/running pipeline run. While runs are in progress the idle clock
    is continuously reset, so a long external (SLURM/Batch) job — whose run stays
    'running' for its whole duration — can never trigger shutdown, and a full idle
    window elapses after the last run finishes before the server exits.
    """
    import asyncio
    import logging
    import os
    import signal
    import time

    from app.services import job_service

    _log = logging.getLogger("uvicorn.error")
    interval = max(5, min(timeout, 30))
    while True:
        await asyncio.sleep(interval)
        if job_service.has_active_runs():
            app.state.last_activity = time.monotonic()  # active work counts as activity
            continue
        idle = time.monotonic() - app.state.last_activity
        if idle >= timeout:
            _log.info(
                "Inactivity auto-shutdown: %ds idle, no active runs. Stopping server.",
                int(idle),
            )
            os.kill(os.getpid(), signal.SIGTERM)  # graceful uvicorn shutdown
            return


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Startup / shutdown hook. Extend here as services are added."""
    import asyncio
    import contextlib
    import logging
    import time

    from app.backends import get_backend_instance
    from app.services import job_service

    _log = logging.getLogger("uvicorn.error")
    settings = get_settings()
    _log_settings(settings)
    # Ensure data root exists (local mode); cloud FSx handles this transparently.
    if settings.execution_mode == "local":
        settings.data_root.mkdir(parents=True, exist_ok=True)
    # Restore run history from disk. SLURM runs still marked "running" are left
    # as-is so resume_runs() below can reconnect to them.
    job_service.load_runs_from_disk(settings.data_root)
    # Re-attach polling tasks for any SLURM jobs that survived the last restart.
    if job_service.has_slurm_runs_to_resume():
        try:
            backend = get_backend_instance(settings)
            await job_service.resume_runs(settings, backend)
        except Exception as exc:
            _log.warning("Could not resume SLURM runs at startup: %s", exc)

    # Inactivity auto-shutdown (opt-in via NICHART_INACTIVITY_TIMEOUT_SECONDS > 0).
    watchdog: asyncio.Task | None = None
    if settings.inactivity_timeout_seconds and settings.inactivity_timeout_seconds > 0:
        app.state.last_activity = time.monotonic()
        watchdog = asyncio.create_task(
            _inactivity_watchdog(app, settings.inactivity_timeout_seconds)
        )
        _log.info(
            "Inactivity auto-shutdown enabled: %ds idle with no active runs.",
            settings.inactivity_timeout_seconds,
        )

    yield

    if watchdog is not None:
        watchdog.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watchdog


def create_app() -> FastAPI:
    """
    Construct and return a configured FastAPI application instance.

    Keeping construction in a factory function (rather than module-level)
    means tests can call ``create_app()`` to get isolated instances with
    dependency overrides applied before the first request.
    """
    settings = get_settings()

    app = FastAPI(
        title="NiChart API",
        summary="Backend API for the NiChart medical-imaging pipeline platform.",
        description=(
            "Provides endpoints for managing projects, uploading imaging data, "
            "running containerised processing pipelines (locally via Docker or on "
            "AWS Batch), and retrieving results. "
            "\n\n"
            "**Authentication**: In cloud mode the server uses a BFF OAuth2 flow with "
            "Cognito. Tokens are stored in httpOnly cookies — never in JS-accessible "
            "storage. Navigate the browser to ``GET /auth/login`` to begin sign-in; "
            "all subsequent API requests carry the session cookie automatically. "
            "In local mode (``NICHART_EXECUTION_MODE=local``) authentication is bypassed."
        ),
        version=_VERSION,
        contact={"name": "CBICA", "url": "https://github.com/CBICA/NiChart_Project"},
        license_info={"name": "MIT"},
        lifespan=_lifespan,
        # Expose docs in all modes for now; restrict in prod if desired.
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # ── CORS ─────────────────────────────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Inactivity tracking ──────────────────────────────────────────────────
    # Record the time of the last real request so the inactivity watchdog (started
    # in the lifespan when enabled) knows when the server is idle. /health is
    # excluded so liveness probes don't keep an otherwise-idle server alive.
    import time as _time

    app.state.last_activity = _time.monotonic()

    @app.middleware("http")
    async def _track_activity(request, call_next):
        if request.url.path != "/health":
            app.state.last_activity = _time.monotonic()
        return await call_next(request)

    # ── Routers ───────────────────────────────────────────────────────────────
    app.include_router(auth_router)
    app.include_router(catalog_router)
    app.include_router(docs_router)
    app.include_router(cloud_router)
    app.include_router(projects_router)
    app.include_router(files_router)
    app.include_router(dicom_router)
    app.include_router(jobs_project_router)
    app.include_router(jobs_router)
    app.include_router(results_router)

    settings = get_settings()
    # Mounted conditionally: when the feature is off these paths 404, which is
    # how a client discovers that a deployment has disabled it.
    if settings.enable_intermediate_uploads:
        app.include_router(intermediate_router)
    if settings.enable_test_endpoints:
        app.include_router(test_router)

    # ── Health ────────────────────────────────────────────────────────────────
    @app.get(
        "/health",
        tags=["Health"],
        summary="Health check",
        description="Returns server status and the active execution mode. No authentication required.",
        dependencies=[],   # explicitly public — no require_auth
        response_class=JSONResponse,
    )
    async def health(settings: Settings = Depends(get_settings)):
        return {
            "status": "ok",
            "execution_mode": settings.execution_mode,
            "version": _VERSION,
        }

    return app


# Module-level singleton used by uvicorn / gunicorn
app = create_app()
