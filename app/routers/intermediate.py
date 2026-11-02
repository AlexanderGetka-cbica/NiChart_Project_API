"""
Manual upload of intermediate pipeline data (``/projects/{id}/files/...``).

An advanced, unsupported workflow for users who already hold part of a
pipeline's results but cannot upload the raw images they came from: they supply
a later step's inputs by hand and start the pipeline from that step.

This router is mounted only when ``NICHART_ENABLE_INTERMEDIATE_UPLOADS`` is
true. A deployment that turns the feature off therefore serves 404 for these
paths, which is how a client detects that the feature is unavailable.
"""

from fastapi import APIRouter, Depends, File as FastAPIFile, Form, Query, UploadFile

from app.auth.dependencies import CurrentUser, require_auth
from app.config import Settings, get_settings
from app.models.errors import ErrorDetail
from app.models.files import IntermediateInputsResponse, IntermediateUploadResult
from app.services import file_service, intermediate_service, usage_service

router = APIRouter(prefix="/projects/{project_id}", tags=["Files"])

_AUTH_ERRORS = {
    401: {"model": ErrorDetail, "description": "Missing or invalid token."},
    403: {"model": ErrorDetail, "description": "Access denied to this project."},
    404: {"model": ErrorDetail, "description": "Project or pipeline not found."},
    400: {"model": ErrorDetail, "description": "Unknown step or slot, or an unsafe upload."},
}

_UPLOAD_ERRORS = {
    **_AUTH_ERRORS,
    413: {"model": ErrorDetail, "description": "Upload exceeds the per-request size limit."},
    502: {
        "model": ErrorDetail,
        "description": "Written locally but could not be saved to cloud storage.",
    },
}


@router.get(
    "/files/intermediate-inputs/{pipeline_id}",
    summary="List the data needed to start a pipeline from a later step",
    description=(
        "Reports every path the steps from ``start_from_step`` onwards will read, "
        "and whether it is already in place.\n\n"
        "Each entry is one of three kinds:\n\n"
        "- ``upload`` — normally produced by a step that would be skipped, so it may "
        "be supplied by hand through the intermediate upload endpoint.\n"
        "- ``project_input`` — raw project data no step produces (``t1/``, "
        "``participants/participants.csv``). Reported with its status only; use the "
        "images or participants uploader for these.\n"
        "- ``generated`` — produced by a step that will run, so it is listed for "
        "context and not checked.\n\n"
        "Requirements come from the paths the running steps actually read plus any "
        "``contents`` the producing and consuming tools declare. Per-subject "
        "coverage is measured against the project's imaging subjects intersected "
        "with ``participants.csv``; when the project has neither, "
        "``coverage.expected_subjects`` is null and coverage does not affect "
        "``status``.\n\n"
        "Read-only. Starting at the first step is permitted and simply yields no "
        "``upload`` slots. Step names for the picker come from "
        "``GET /catalog/pipelines/{pipeline_id}``."
    ),
    response_model=IntermediateInputsResponse,
    responses=_AUTH_ERRORS,
)
async def get_intermediate_inputs(
    project_id: str,
    pipeline_id: str,
    start_from_step: str = Query(
        description="Step the run would start from. Must be one of the pipeline's steps.",
    ),
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> IntermediateInputsResponse:
    pdir = file_service.resolve_project(settings, user, project_id)
    return intermediate_service.get_requirements(
        settings, pdir, pipeline_id, start_from_step
    )


@router.post(
    "/files/upload/intermediate",
    summary="Upload intermediate data into a pipeline slot",
    description=(
        "Populates one ``upload`` slot by hand. The destination is derived from the "
        "pipeline YAML — no path is accepted from the client, and a slot that is not "
        "``kind: upload`` for this (pipeline, start step) is refused.\n\n"
        "For a directory slot each file's name is its path relative to the slot and "
        "may contain subdirectories. Files are merged into the slot, overwriting "
        "same-named entries; ``replace=true`` clears the slot first. For a file slot "
        "send exactly one file; it is saved under the slot's own name whatever the "
        "upload was called.\n\n"
        "Content is staged and fully checked before anything is moved into place, so "
        "a failed request leaves the slot untouched. Path traversal, absolute names "
        "and symlinks fail the whole request. Dotfiles and ``__MACOSX`` entries are "
        "dropped and listed in ``skipped``. A disallowed extension or an oversized "
        "file is listed in ``rejected`` and does not fail the request unless nothing "
        "valid remains.\n\n"
        "Uploading clears the cache entry of the step that normally produces the "
        "slot, so a later full run regenerates it rather than adopting these files. "
        "In cloud mode the response is returned only once the data has also reached "
        "S3, because a run begins by syncing the project down from there."
    ),
    response_model=IntermediateUploadResult,
    responses=_UPLOAD_ERRORS,
)
async def upload_intermediate(
    project_id: str,
    pipeline_id: str = Form(description="Pipeline the slot belongs to."),
    start_from_step: str = Form(description="Step the run will start from."),
    slot_id: str = Form(description="Slot ID from the intermediate-inputs response."),
    replace: bool = Form(
        default=False,
        description="Clear the slot before writing. Directory slots only.",
    ),
    files: list[UploadFile] = FastAPIFile(description="Files to write into the slot."),
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> IntermediateUploadResult:
    pdir = file_service.resolve_project(settings, user, project_id)
    result = await intermediate_service.upload_to_slot(
        settings=settings,
        user=user,
        project_id=project_id,
        study_dir=pdir,
        pipeline_id=pipeline_id,
        start_from_step=start_from_step,
        slot_id=slot_id,
        replace=replace,
        files=files,
    )
    usage_service.record_upload(
        "intermediate", settings.execution_mode,
        files=result.written,
        total_bytes=0,
    )
    return result


@router.post(
    "/files/upload/intermediate/zip",
    summary="Upload a zip of intermediate data into a pipeline slot",
    description=(
        "As the multipart upload, but takes a single ``.zip``. Preferred for large "
        "folders, where a request with hundreds of parts is fragile.\n\n"
        "The archive is validated before extraction: an absolute member path, a "
        "member escaping the destination, or a symlink member fails the whole "
        "request. Dotfiles and ``__MACOSX`` members are dropped and reported in "
        "``skipped``. A single top-level directory — what a Finder-zipped folder "
        "produces — is stripped so the archive's inner layout lines up with the slot."
    ),
    response_model=IntermediateUploadResult,
    responses=_UPLOAD_ERRORS,
)
async def upload_intermediate_zip(
    project_id: str,
    pipeline_id: str = Form(description="Pipeline the slot belongs to."),
    start_from_step: str = Form(description="Step the run will start from."),
    slot_id: str = Form(description="Slot ID from the intermediate-inputs response."),
    replace: bool = Form(
        default=False,
        description="Clear the slot before writing. Directory slots only.",
    ),
    file: UploadFile = FastAPIFile(description="A .zip archive of the slot's contents."),
    user: CurrentUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> IntermediateUploadResult:
    pdir = file_service.resolve_project(settings, user, project_id)
    result = await intermediate_service.upload_to_slot(
        settings=settings,
        user=user,
        project_id=project_id,
        study_dir=pdir,
        pipeline_id=pipeline_id,
        start_from_step=start_from_step,
        slot_id=slot_id,
        replace=replace,
        archive=file,
    )
    usage_service.record_upload(
        "intermediate", settings.execution_mode,
        files=result.written,
        total_bytes=0,
    )
    return result
