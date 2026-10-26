"""
DICOM staging, inspection, and conversion — authenticated.

DICOM → NIfTI conversion is a three-step interactive flow:

1. Upload a ``.zip`` of DICOM files → server stages it and returns a staging_id.
2. Client fetches the series list (lightweight pydicom header reads, no conversion).
3. Client submits the confirmed series → modality mapping to trigger a conversion
   job (dcm2niix running as a container). Returns a run_id for status polling.
"""

import asyncio

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, UploadFile
from fastapi import File as FastAPIFile

from app.auth.dependencies import CurrentUser, require_auth
from app.backends import get_backend
from app.backends.base import JobBackend
from app.config import Settings, get_settings
from app.models.dicom import (
    DicomConvertRequest,
    DicomConvertResult,
    DicomSeriesListing,
    DicomStagingResult,
)
from app.models.errors import ErrorDetail
from app.services import dicom_service, file_service, job_service, usage_service

router = APIRouter(prefix="/projects/{project_id}/files/dicom", tags=["DICOM"])

_AUTH_ERRORS = {
    401: {"model": ErrorDetail, "description": "Missing or invalid token."},
    403: {"model": ErrorDetail, "description": "Access denied to this project."},
    404: {"model": ErrorDetail, "description": "Project or staging area not found."},
}


@router.post(
    "/upload",
    summary="Upload DICOM zip to staging",
    description=(
        "Accepts a single ``.zip`` archive of DICOM files. The archive is extracted "
        "with symlink and path-traversal checks into a private staging area. "
        "Use the returned ``staging_id`` to inspect series and submit a conversion job."
    ),
    response_model=DicomStagingResult,
    status_code=202,
    responses={**_AUTH_ERRORS, 400: {"model": ErrorDetail}},
)
async def upload_dicom(
    project_id: str,
    file: UploadFile,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> DicomStagingResult:
    pdir = file_service.resolve_project(settings, user, project_id)
    contents = await file.read()
    usage_service.record_upload("dicom", settings.execution_mode, files=1, total_bytes=len(contents))
    staging_id = dicom_service.stage_dicom_upload(pdir, contents, file.filename or "")
    return DicomStagingResult(staging_id=staging_id)


@router.post(
    "/upload/files",
    summary="Upload individual DICOM files to staging",
    description=(
        "Accepts one or more raw DICOM files as multipart form data. "
        "Each filename is validated (no path traversal, no directory separators). "
        "Files are staged identically to the ZIP upload — use the returned "
        "``staging_id`` with the inspect and convert endpoints."
    ),
    response_model=DicomStagingResult,
    status_code=202,
    responses={**_AUTH_ERRORS, 400: {"model": ErrorDetail}},
)
async def upload_dicom_files(
    project_id: str,
    files: list[UploadFile] = FastAPIFile(...),
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> DicomStagingResult:
    pdir = file_service.resolve_project(settings, user, project_id)
    pairs = [(f.filename or "", await f.read()) for f in files]
    usage_service.record_upload(
        "dicom", settings.execution_mode,
        files=len(pairs), total_bytes=sum(len(d) for _n, d in pairs),
    )
    staging_id = dicom_service.stage_dicom_files(pdir, pairs)
    return DicomStagingResult(staging_id=staging_id)


@router.get(
    "/{staging_id}/series",
    summary="Inspect DICOM series",
    description=(
        "Reads DICOM headers (via pydicom) in the staging area and returns a list of "
        "detected series with their description, modality, study date, and file count. "
        "No conversion happens at this step — it is safe to call multiple times."
    ),
    response_model=DicomSeriesListing,
    responses=_AUTH_ERRORS,
)
async def list_dicom_series(
    project_id: str,
    staging_id: str,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> DicomSeriesListing:
    pdir = file_service.resolve_project(settings, user, project_id)
    series = await asyncio.to_thread(dicom_service.inspect_dicom_series, pdir, staging_id)
    return DicomSeriesListing(staging_id=staging_id, series=series)


@router.post(
    "/{staging_id}/convert",
    summary="Submit DICOM conversion job",
    description=(
        "Accepts a mapping of DICOM series UIDs to NiChart modality labels and submits "
        "a dcm2niix conversion job for each selected series. "
        "Series not listed are ignored. Returns a ``run_id`` for polling via "
        "``GET /jobs/pipelines/{run_id}``."
    ),
    response_model=DicomConvertResult,
    status_code=202,
    responses={**_AUTH_ERRORS, 400: {"model": ErrorDetail}},
)
async def convert_dicom(
    project_id: str,
    staging_id: str,
    body: DicomConvertRequest,
    background_tasks: BackgroundTasks,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
    backend: JobBackend = Depends(get_backend),
) -> DicomConvertResult:
    if not body.series_mappings:
        raise HTTPException(400, "series_mappings must not be empty")

    pdir = file_service.resolve_project(settings, user, project_id)

    # Organizing is a blocking filesystem + DICOM-header-parsing sweep over the whole
    # staging area; for a large folder/zip upload this can take long enough to stall
    # the event loop (and time out the request), so it runs off-thread.
    series_uids = {m.series_uid for m in body.series_mappings}
    organized = await asyncio.to_thread(
        dicom_service.organize_series_files_bulk, pdir, staging_id, series_uids
    )

    direct_steps: list[job_service.DirectStep] = []
    for mapping in body.series_mappings:
        series_dir, patient_id = organized[mapping.series_uid]
        mrid = mapping.mrid or patient_id or "subject"

        modality_dir = pdir / mapping.nichart_modality
        modality_dir.mkdir(parents=True, exist_ok=True)

        direct_steps.append(job_service.DirectStep(
            step_id=f"convert_{mapping.nichart_modality}_{mapping.series_uid[:8]}",
            tool_id="dcm2niix",
            mount_paths={
                "input": str(series_dir),
                "output": str(modality_dir),
            },
            params={"mrid": mrid},
        ))

    run = job_service.create_direct_run(
        user_id=user.sub,
        project_id=project_id,
        pipeline_id="dicom_convert",
        direct_steps=direct_steps,
    )
    usage_service.record_pipeline_run(
        "dicom_convert", settings.execution_mode, subjects=len(direct_steps)
    )
    background_tasks.add_task(
        job_service.run_direct_steps_task,
        run=run,
        direct_steps=direct_steps,
        backend=backend,
        user_token=user.token,
        tools_path=settings.tools_path,
        study_dir=pdir,
        execution_mode=settings.execution_mode,
    )
    return DicomConvertResult(run_id=run.run_id)


@router.delete(
    "/{staging_id}",
    summary="Discard DICOM staging area",
    description=(
        "Permanently deletes the staged DICOM files without converting them. "
        "Staging areas are also auto-cleaned after ``NICHART_STAGING_TTL_HOURS``."
    ),
    status_code=204,
    responses=_AUTH_ERRORS,
)
async def discard_dicom_staging(
    project_id: str,
    staging_id: str,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> None:
    pdir = file_service.resolve_project(settings, user, project_id)
    dicom_service.discard_dicom_staging(pdir, staging_id)
