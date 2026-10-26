"""
File management within a project — authenticated.

All paths from the client are validated against the project root before any I/O.
"""

from fastapi import APIRouter, Depends, File as FastAPIFile, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, StreamingResponse

from app.auth.dependencies import CurrentUser, require_auth
from app.config import Settings, get_settings
from app.models.errors import ErrorDetail
from app.models.files import (
    DirectoryTree,
    DownloadZipRequest,
    NiftiCommitRequest,
    NiftiCommitResult,
    NiftiStagingResult,
    ParticipantsList,
    ParticipantsUpdate,
)
from app.models.provenance import ProvenanceReport
from app.models.readiness import ReadinessReport
from app.services import (
    catalog_service,
    file_service,
    provenance_service,
    readiness_service,
    usage_service,
)

router = APIRouter(prefix="/projects/{project_id}", tags=["Files"])

_AUTH_ERRORS = {
    401: {"model": ErrorDetail, "description": "Missing or invalid token."},
    403: {"model": ErrorDetail, "description": "Access denied to this project."},
    404: {"model": ErrorDetail, "description": "Project or file not found."},
    400: {"model": ErrorDetail, "description": "Invalid or unsafe path."},
}


# ── Directory tree ────────────────────────────────────────────────────────────

@router.get(
    "/files",
    summary="List project files",
    description=(
        "Returns a flat listing of all visible files and directories in the project. "
        "Internal staging (``_upload/``) and working (``_working/``) directories are excluded."
    ),
    response_model=DirectoryTree,
    responses=_AUTH_ERRORS,
)
async def list_files(
    project_id: str,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> DirectoryTree:
    pdir = file_service.resolve_project(settings, user, project_id)
    return file_service.list_project_files(pdir)


# ── Download ──────────────────────────────────────────────────────────────────

@router.get(
    "/files/download",
    summary="Download a file or directory",
    description=(
        "Download a single file, or a directory as a streaming zip archive when "
        "``zip=true``. The ``path`` is relative to the project root and is validated "
        "to prevent traversal."
    ),
    response_model=None,
    responses={
        200: {"description": "File content or zip stream."},
        **_AUTH_ERRORS,
    },
)
async def download_file(
    project_id: str,
    path: str = Query(description="Path relative to the project root."),
    zip: bool = Query(default=False, description="Stream target directory as a zip archive."),
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> StreamingResponse | FileResponse:
    pdir = file_service.resolve_project(settings, user, project_id)
    target = file_service.resolve_file_path(pdir, path)

    if zip or target.is_dir():
        if not target.is_dir():
            raise HTTPException(400, "zip=true requires a directory path")
        return StreamingResponse(
            file_service.zip_directory_stream(target),
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{target.name}.zip"'},
        )

    return FileResponse(path=target, filename=target.name)


@router.post(
    "/files/archive",
    summary="Bundle a multi-file/directory selection into one zip",
    description=(
        "Bundles an arbitrary set of files and/or directories into a single zip archive, "
        "preserving each entry's path relative to the project root. Useful for downloading "
        "a multi-selection from the file browser as one archive instead of one request per item. "
        "Named to avoid 'download' in the URL, which some ad-blocker/privacy extensions filter."
    ),
    response_model=None,
    responses={
        200: {"description": "Zip stream."},
        **_AUTH_ERRORS,
    },
)
async def create_archive(
    project_id: str,
    body: DownloadZipRequest,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> StreamingResponse:
    pdir = file_service.resolve_project(settings, user, project_id)
    return StreamingResponse(
        file_service.zip_paths_stream(pdir, body.paths),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{project_id}.zip"'},
    )


# ── Delete ────────────────────────────────────────────────────────────────────

@router.delete(
    "/files",
    summary="Delete a file or directory",
    description="Permanently deletes the file or directory at ``path``.",
    status_code=204,
    responses=_AUTH_ERRORS,
)
async def delete_file(
    project_id: str,
    path: str = Query(description="Path relative to the project root."),
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> None:
    import shutil
    pdir = file_service.resolve_project(settings, user, project_id)
    target = file_service.resolve_file_path(pdir, path)
    if target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()


# ── NIfTI upload — stage ──────────────────────────────────────────────────────

@router.post(
    "/files/upload/nifti",
    summary="Upload NIfTI file(s) to staging",
    description=(
        "Accepts one or more ``.nii`` / ``.nii.gz`` files (including a folder upload, where "
        "filenames carry relative directory paths). Files land in the project staging area "
        "and the server returns its best-effort MRID and modality inference. If two entries "
        "flatten to the same filename, the first one wins and the rest are reported in "
        "``skipped_duplicates``. Follow up with the commit endpoint to move them into the "
        "project."
    ),
    response_model=NiftiStagingResult,
    status_code=202,
    responses=_AUTH_ERRORS,
)
async def upload_nifti(
    project_id: str,
    files: list[UploadFile],
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> NiftiStagingResult:
    pdir = file_service.resolve_project(settings, user, project_id)
    filenames = [f.filename or "" for f in files]
    file_data = [await f.read() for f in files]
    usage_service.record_upload(
        "nifti", settings.execution_mode,
        files=len(file_data), total_bytes=sum(len(d) for d in file_data),
    )
    return file_service.stage_nifti_files(pdir, filenames, file_data)


# ── NIfTI upload — zip ───────────────────────────────────────────────────────

@router.post(
    "/files/upload/nifti/zip",
    summary="Upload a zip of NIfTI files to staging",
    description=(
        "Accepts a ``.zip`` archive containing ``.nii`` / ``.nii.gz`` files "
        "(subdirectories are searched recursively; non-NIfTI entries are ignored). "
        "Files land in the project staging area and the server returns its best-effort "
        "MRID and modality inference. Directory components in the archive path contribute "
        "to modality detection (e.g. ``fl/subject001.nii.gz`` infers modality ``fl``). "
        "Nested directories are flattened into a single staging folder; if two entries "
        "flatten to the same filename, the first one wins and the rest are reported in "
        "``skipped_duplicates``. Follow up with the commit endpoint to move them into the "
        "project."
    ),
    response_model=NiftiStagingResult,
    status_code=202,
    responses=_AUTH_ERRORS,
)
async def upload_nifti_zip(
    project_id: str,
    file: UploadFile,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> NiftiStagingResult:
    pdir = file_service.resolve_project(settings, user, project_id)
    contents = await file.read()
    usage_service.record_upload("nifti", settings.execution_mode, files=1, total_bytes=len(contents))
    return file_service.stage_nifti_zip(pdir, contents, file.filename or "")


# ── NIfTI upload — commit ─────────────────────────────────────────────────────

@router.post(
    "/files/stage/{staging_id}/commit",
    summary="Commit staged NIfTI files",
    description=(
        "Confirms MRID and modality mappings for staged files and moves them into "
        "their final locations. Every staged file must appear in ``mappings``. If a "
        "subject already has a file at the target modality/MRID slot, the new upload "
        "is left in place there and the mapping is reported in ``skipped`` instead of "
        "``committed`` — existing scans are never overwritten by a re-upload."
    ),
    response_model=NiftiCommitResult,
    responses=_AUTH_ERRORS,
)
async def commit_nifti(
    project_id: str,
    staging_id: str,
    body: NiftiCommitRequest,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> NiftiCommitResult:
    pdir = file_service.resolve_project(settings, user, project_id)
    return file_service.commit_nifti_staging(pdir, staging_id, body.mappings)


# ── Staging discard ───────────────────────────────────────────────────────────

@router.delete(
    "/files/stage/{staging_id}",
    summary="Discard a staging area",
    description="Deletes all files in the staging area without committing.",
    status_code=204,
    responses=_AUTH_ERRORS,
)
async def discard_staging(
    project_id: str,
    staging_id: str,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> None:
    pdir = file_service.resolve_project(settings, user, project_id)
    file_service.discard_nifti_staging(pdir, staging_id)


# ── CSV upload ────────────────────────────────────────────────────────────────

@router.post(
    "/files/upload/csv",
    summary="Upload participants CSV",
    description=(
        "Replaces ``participants/participants.csv`` with the uploaded file. "
        "The file is stored as-is; must have at least an ``MRID`` column."
    ),
    status_code=204,
    responses=_AUTH_ERRORS,
)
async def upload_csv(
    project_id: str,
    file: UploadFile,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> None:
    pdir = file_service.resolve_project(settings, user, project_id)
    contents = await file.read()
    usage_service.record_upload("csv", settings.execution_mode, files=1, total_bytes=len(contents))
    file_service.store_csv_upload(pdir, contents, file.filename or "")


# ── BIDS upload ───────────────────────────────────────────────────────────────

@router.post(
    "/files/upload/bids",
    summary="Upload a BIDS dataset",
    description=(
        "Accepts a ``.zip`` archive containing a BIDS-layout dataset, extracts it, "
        "and reorganises it into the NiChart project layout. "
        "``sub-{id}_T1w.nii.gz`` → ``t1/{id}.nii.gz``, ``_FLAIR`` → ``fl/``, etc. "
        "A ``participants.tsv`` at the archive root is converted to ``participants/participants.csv``."
    ),
    status_code=202,
    responses=_AUTH_ERRORS,
)
async def upload_bids(
    project_id: str,
    file: UploadFile,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> None:
    pdir = file_service.resolve_project(settings, user, project_id)
    contents = await file.read()
    usage_service.record_upload("bids", settings.execution_mode, files=1, total_bytes=len(contents))
    file_service.store_bids_upload(pdir, contents, file.filename or "")


@router.post(
    "/files/upload/bids/files",
    summary="Upload individual BIDS files",
    description=(
        "Accepts one or more files as multipart form data representing a BIDS dataset. "
        "Filenames may include relative sub-paths (e.g. ``sub-01/anat/sub-01_T1w.nii.gz``) "
        "and are validated for path traversal before writing. "
        "Reorganisation into the NiChart project layout is identical to the ZIP upload."
    ),
    status_code=202,
    responses=_AUTH_ERRORS,
)
async def upload_bids_files(
    project_id: str,
    files: list[UploadFile] = FastAPIFile(...),
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> None:
    pdir = file_service.resolve_project(settings, user, project_id)
    pairs = [(f.filename or "", await f.read()) for f in files]
    usage_service.record_upload(
        "bids", settings.execution_mode,
        files=len(pairs), total_bytes=sum(len(d) for _n, d in pairs),
    )
    file_service.store_bids_files(pdir, pairs)


# ── IDAT upload ───────────────────────────────────────────────────────────────

@router.post(
    "/files/upload/idat",
    summary="Upload IDAT files",
    description="Accepts a ``.zip`` of ``.idat`` files. Extracted to ``idat/`` in the project.",
    status_code=204,
    responses=_AUTH_ERRORS,
)
async def upload_idat(
    project_id: str,
    file: UploadFile,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> None:
    pdir = file_service.resolve_project(settings, user, project_id)
    contents = await file.read()
    usage_service.record_upload("idat", settings.execution_mode, files=1, total_bytes=len(contents))
    file_service.store_idat_upload(pdir, contents, file.filename or "")


@router.post(
    "/files/upload/idat/files",
    summary="Upload individual IDAT files",
    description=(
        "Accepts one or more ``.idat`` files as multipart form data. "
        "Filenames must be flat (no directory separators) and end with ``.idat``. "
        "Each filename is validated before any file is written to ``idat/``."
    ),
    status_code=204,
    responses=_AUTH_ERRORS,
)
async def upload_idat_files(
    project_id: str,
    files: list[UploadFile] = FastAPIFile(...),
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> None:
    pdir = file_service.resolve_project(settings, user, project_id)
    pairs = [(f.filename or "", await f.read()) for f in files]
    usage_service.record_upload(
        "idat", settings.execution_mode,
        files=len(pairs), total_bytes=sum(len(d) for _n, d in pairs),
    )
    file_service.store_idat_files(pdir, pairs)


# ── Participants ──────────────────────────────────────────────────────────────

@router.get(
    "/participants",
    summary="Get participants list",
    description="Returns the contents of ``participants/participants.csv`` as JSON rows.",
    response_model=ParticipantsList,
    responses={**_AUTH_ERRORS, 404: {"model": ErrorDetail}},
)
async def get_participants(
    project_id: str,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> ParticipantsList:
    pdir = file_service.resolve_project(settings, user, project_id)
    return file_service.read_participants(pdir)


@router.get(
    "/participants/template",
    summary="Download participants CSV template",
    description=(
        "Returns a CSV template pre-populated with all MRIDs detected from committed "
        "NIfTI files in the project's modality directories. Rows already present in "
        "participants.csv retain their values; newly detected MRIDs appear with empty "
        "additional columns. Use this as a starting point for filling in demographic or "
        "clinical data before uploading."
    ),
    responses={**_AUTH_ERRORS},
)
async def get_participants_template(
    project_id: str,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> StreamingResponse:
    pdir = file_service.resolve_project(settings, user, project_id)
    csv_content = file_service.participants_template_csv(pdir)
    return StreamingResponse(
        content=iter([csv_content.encode()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{project_id}_participants_template.csv"'},
    )


@router.patch(
    "/participants",
    summary="Replace participants list",
    description=(
        "Overwrites ``participants/participants.csv`` with the provided rows. "
        "Full replacement — rows not included are removed."
    ),
    status_code=204,
    responses=_AUTH_ERRORS,
)
async def update_participants(
    project_id: str,
    body: ParticipantsUpdate,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> None:
    pdir = file_service.resolve_project(settings, user, project_id)
    file_service.write_participants(pdir, body.rows)


# ── Readiness check ───────────────────────────────────────────────────────────

@router.get(
    "/readiness/{pipeline_id}",
    summary="Check pipeline readiness",
    description=(
        "Evaluates whether the project has the data required to run a pipeline. "
        "Returns a per-requirement status so the client can guide users to upload "
        "missing imaging data or fill missing participants.csv columns. "
        "Imaging checks scan the modality directories (``t1/``, ``fl/``, etc.) for NIfTI files. "
        "CSV checks verify that required columns exist and are non-empty for every subject."
    ),
    response_model=ReadinessReport,
    responses=_AUTH_ERRORS,
)
async def check_readiness(
    project_id: str,
    pipeline_id: str,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> ReadinessReport:
    pdir = file_service.resolve_project(settings, user, project_id)
    raw_requires = catalog_service.get_pipeline_raw_requires(settings.pipelines_path, pipeline_id)
    return readiness_service.check_readiness(pdir, pipeline_id, raw_requires)


# ── Provenance verification ────────────────────────────────────────────────────

@router.get(
    "/provenance",
    summary="Verify step provenance",
    description=(
        "Scans all ``_provenance.json`` files written by the pipeline executor after each "
        "successful step, then checks whether any recorded input paths have been modified "
        "since the step finished. "
        "A 'dirty' entry means the step's cached result may be stale and the step should "
        "re-run. A 'missing_inputs' entry means a required input no longer exists. "
        "Returns 'no_provenance' summary when no steps have completed yet."
    ),
    response_model=ProvenanceReport,
    responses=_AUTH_ERRORS,
)
async def verify_provenance(
    project_id: str,
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> ProvenanceReport:
    pdir = file_service.resolve_project(settings, user, project_id)
    return provenance_service.verify_provenance(pdir, project_id)
