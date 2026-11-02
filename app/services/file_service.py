"""
File-system operations for projects and files within them.

All user-supplied paths are validated via path_security before any I/O.
"""

import csv
import io
import re
import shutil
import tempfile
import uuid
import zipfile
from collections.abc import Iterable, Iterator
from datetime import datetime
from pathlib import Path

from fastapi import HTTPException, UploadFile

from app import modalities
from app.auth.dependencies import CurrentUser
from app.config import Settings
from app.models.files import (
    CommittedFile,
    DirectoryTree,
    FileEntry,
    NiftiCommitResult,
    NiftiStagingResult,
    NiftiUploadProposal,
    ParticipantRow,
    ParticipantsList,
)
from app.models.projects import Project
from app.services import s3_sync_service
from app.services.path_security import (
    PathEscapeError,
    assert_safe_path,
    assert_safe_upload_filename,
    safe_unzip,
)


# ── Project helpers ───────────────────────────────────────────────────────────

def user_root(settings: Settings, user: CurrentUser) -> Path:
    return settings.data_root / user.sub


def resolve_project(settings: Settings, user: CurrentUser, project_id: str) -> Path:
    root = user_root(settings, user)
    pdir = root / project_id
    try:
        assert_safe_path(root, pdir)
    except PathEscapeError:
        raise HTTPException(400, "Invalid project ID")
    if not pdir.is_dir():
        raise HTTPException(404, f"Project '{project_id}' not found")
    return pdir


async def publish_uploads(
    settings: Settings, user: CurrentUser, project_id: str, project_path: Path
) -> None:
    """Push freshly uploaded project data to S3 before the request returns.

    What the user uploads is canonical. In cloud mode a pipeline run begins by
    syncing the project *down* from S3 (see ``job_service._run_pipeline``), and
    that download overwrites any local file whose size or ETag differs from the
    remote object. So an upload that only ever lands on local disk is silently
    reverted at submit time whenever S3 already holds an older copy of the same
    path — the new participants.csv, or a manually supplied intermediate, is
    replaced by the stale version and the run proceeds on it with no error.

    Pushing here closes that window: once local and S3 agree, the pre-run
    download has nothing to revert and the pre-step upload carries the data on
    to the compute node as usual.

    A failure is raised to the caller as 502 rather than swallowed. Reporting
    success for an upload that a later run would discard is the failure mode
    this exists to prevent.

    No-op outside cloud mode, or when no data bucket is configured.
    """
    if settings.execution_mode != "cloud" or not settings.s3_data_bucket:
        return
    prefix = s3_sync_service.project_prefix(
        settings.s3_data_prefix, user.sub, project_id
    )
    try:
        await s3_sync_service.sync_to_s3(project_path, settings.s3_data_bucket, prefix)
    except Exception as exc:
        raise HTTPException(
            502,
            "Upload was written locally but could not be saved to cloud storage, "
            f"so it is not safe to use in a run yet: {exc}",
        )


# ── Project CRUD ──────────────────────────────────────────────────────────────

def list_projects(settings: Settings, user: CurrentUser) -> list[Project]:
    root = user_root(settings, user)
    if not root.exists():
        return []
    return [
        Project(id=d.name, created_at=datetime.fromtimestamp(d.stat().st_mtime))
        for d in sorted(root.iterdir())
        if d.is_dir()
    ]


def create_project(settings: Settings, user: CurrentUser, name: str) -> Project:
    root = user_root(settings, user)
    root.mkdir(parents=True, exist_ok=True)
    pdir = root / name
    if pdir.exists():
        raise HTTPException(409, f"Project '{name}' already exists")
    pdir.mkdir()
    return Project(id=name, created_at=datetime.fromtimestamp(pdir.stat().st_mtime))


def delete_project(settings: Settings, user: CurrentUser, project_id: str) -> None:
    pdir = resolve_project(settings, user, project_id)
    shutil.rmtree(pdir)


# ── File listing ──────────────────────────────────────────────────────────────

_HIDDEN = {"_upload", "_working"}


def list_project_files(project_path: Path) -> DirectoryTree:
    entries: list[FileEntry] = []
    for item in sorted(project_path.rglob("*")):
        rel_parts = item.relative_to(project_path).parts
        if any(p in _HIDDEN for p in rel_parts):
            continue
        stat = item.stat()
        entries.append(FileEntry(
            name=item.name,
            path=str(item.relative_to(project_path)),
            type="directory" if item.is_dir() else "file",
            size=stat.st_size if item.is_file() else None,
            mtime=stat.st_mtime,
        ))
    return DirectoryTree(entries=entries)


# ── File path resolution ──────────────────────────────────────────────────────

def resolve_file_path(project_path: Path, user_path: str) -> Path:
    """Resolve a user-supplied relative path within a project, checking containment."""
    target = (project_path / user_path).resolve()
    try:
        assert_safe_path(project_path, target)
    except PathEscapeError:
        raise HTTPException(400, "Path escapes project root")
    if not target.exists():
        raise HTTPException(404, f"'{user_path}' not found in project")
    return target


# ── Directory zip streaming ───────────────────────────────────────────────────

class _StreamWriter:
    """File-like adapter that lets zipfile.ZipFile write into a small drainable buffer
    instead of an ever-growing in-memory archive — this is what lets us stream a zip
    as it's built rather than holding the whole (potentially huge) archive in RAM and
    only starting to respond once every entry has been compressed.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._pos = 0

    def write(self, data: bytes) -> int:
        self._buffer.extend(data)
        self._pos += len(data)
        return len(data)

    def tell(self) -> int:
        return self._pos

    def flush(self) -> None:  # zipfile calls this defensively; nothing to do
        pass

    def drain(self) -> bytes:
        data = bytes(self._buffer)
        self._buffer.clear()
        return data


def _stream_zip_entries(entries: Iterable[tuple[Path, str]]) -> Iterator[bytes]:
    """Stream a zip archive of (absolute_path, arcname) pairs, yielding bytes as each
    entry is compressed instead of buffering the whole archive before returning.
    """
    writer = _StreamWriter()
    with zipfile.ZipFile(writer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path, arcname in entries:
            zf.write(path, arcname)
            chunk = writer.drain()
            if chunk:
                yield chunk
    chunk = writer.drain()
    if chunk:
        yield chunk


def zip_directory_stream(directory: Path) -> Iterator[bytes]:
    # Resolve the file list eagerly (a plain function, not a generator) so any error
    # here surfaces immediately rather than after the StreamingResponse has already
    # started sending the 200 + headers to the client.
    entries = [
        (item, str(item.relative_to(directory)))
        for item in sorted(directory.rglob("*"))
        if item.is_file()
    ]
    return _stream_zip_entries(entries)


def _resolve_zip_path_entries(project_path: Path, paths: list[str]) -> list[tuple[Path, str]]:
    seen: set[str] = set()
    entries: list[tuple[Path, str]] = []
    for user_path in paths:
        target = resolve_file_path(project_path, user_path)
        items = (
            [item for item in sorted(target.rglob("*")) if item.is_file()]
            if target.is_dir()
            else [target]
        )
        for item in items:
            arcname = str(item.relative_to(project_path))
            if arcname in seen:
                continue
            seen.add(arcname)
            entries.append((item, arcname))
    return entries


def zip_paths_stream(project_path: Path, paths: list[str]) -> Iterator[bytes]:
    """Zip an arbitrary set of files/directories, keeping each entry's path relative to
    the project root — so a multi-selection spanning several folders keeps its structure.

    Path resolution/validation happens eagerly (this is a plain function, not a
    generator), so an invalid path (e.g. traversal) raises immediately instead of
    failing mid-stream after the response has already started.
    """
    entries = _resolve_zip_path_entries(project_path, paths)
    return _stream_zip_entries(entries)


# ── Participants CSV ──────────────────────────────────────────────────────────

_PARTICIPANTS_PATH = Path("participants") / "participants.csv"


def read_participants(project_path: Path) -> ParticipantsList:
    csv_path = project_path / _PARTICIPANTS_PATH
    if not csv_path.exists():
        return ParticipantsList(rows=[])
    rows = []
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return ParticipantsList(rows=[])
        col = {k.lower(): k for k in reader.fieldnames}
        mrid_col = col.get("mrid")
        if not mrid_col:
            return ParticipantsList(rows=[])
        for raw_row in reader:
            mrid = (raw_row.get(mrid_col) or "").strip()
            if not mrid:
                continue
            row_dict: dict = {"MRID": mrid}
            for col_name, value in raw_row.items():
                if col_name == mrid_col:
                    continue
                row_dict[col_name] = value
            rows.append(ParticipantRow(**row_dict))
    return ParticipantsList(rows=rows)


def write_participants(project_path: Path, rows: list) -> None:
    """Write rows to participants.csv.

    Accepts either ``ParticipantRow`` instances or plain dicts. Every row must
    have an ``MRID`` key (case-insensitive on dicts). Column order is preserved
    from the first row; any row may have a subset of columns.
    """
    csv_dir = project_path / "participants"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_dir / "participants.csv"

    if not rows:
        with csv_path.open("w", newline="") as f:
            csv.DictWriter(f, fieldnames=["MRID"]).writeheader()
        return

    # Normalise each row to a dict, ensuring MRID is the canonical key.
    normalised: list[dict] = []
    for row in rows:
        if isinstance(row, dict):
            d: dict = {}
            mrid_val = None
            for k, v in row.items():
                if k.upper() == "MRID":
                    mrid_val = v
                else:
                    d[k] = v
            if mrid_val is None:
                raise ValueError(f"Row is missing required 'MRID' key: {row!r}")
            normalised.append({"MRID": mrid_val, **d})
        else:
            normalised.append(row.model_dump())

    # Collect column names in encounter order, MRID always first.
    seen: dict[str, None] = {"MRID": None}
    for d in normalised:
        for k in d:
            seen[k] = None
    fieldnames = list(seen)

    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for d in normalised:
            writer.writerow({k: ("" if d.get(k) is None else d.get(k, "")) for k in fieldnames})


def collect_project_mrids(project_path: Path) -> list[str]:
    """Return sorted list of MRIDs detected from committed NIfTI files in modality directories."""
    mrids: set[str] = set()
    for mod in modalities.MODALITY_CODES:
        mod_path = project_path / mod
        if not mod_path.is_dir():
            continue
        for f in mod_path.iterdir():
            if f.name.endswith(".nii.gz"):
                mrids.add(f.name[:-7])
            elif f.name.endswith(".nii"):
                mrids.add(f.name[:-4])
    return sorted(mrids)


def participants_template_csv(project_path: Path) -> str:
    """Build a participants CSV template string.

    Pre-populates the MRID column with all MRIDs detected from modality directories.
    Rows already present in participants.csv keep their values; new MRIDs get empty
    additional columns. Column order mirrors the existing CSV if one exists.
    """
    detected = collect_project_mrids(project_path)
    existing = read_participants(project_path)

    existing_by_mrid: dict[str, dict] = {}
    extra_cols: list[str] = []
    for row in existing.rows:
        d = row.model_dump()
        mrid = d.pop("MRID")
        existing_by_mrid[mrid] = d
        for col in d:
            if col not in extra_cols:
                extra_cols.append(col)

    # Include MRIDs from existing CSV that have no NIfTI (keeps data they already entered)
    all_mrids = sorted(set(detected) | set(existing_by_mrid))
    fieldnames = ["MRID"] + extra_cols

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for mrid in all_mrids:
        row_data = {"MRID": mrid}
        row_data.update(existing_by_mrid.get(mrid, {}))
        writer.writerow(row_data)
    return buf.getvalue()


# ── NIfTI filename inference ──────────────────────────────────────────────────
# All modality logic lives in app.modalities (the single source of truth).


def infer_nifti_metadata(original_path: str) -> tuple[str | None, str | None]:
    """Infer MRID and modality from an upload path.

    MRID is derived from the basename (suffixes and modality tags stripped).
    Modality is detected from the basename first; if not found, the parent
    directory name is checked against known modality codes. This allows uploads
    like ``fl/subject001.nii.gz`` to resolve correctly even when the filename
    itself carries no modality indicator.
    """
    p = Path(original_path)
    basename = p.name
    modality = modalities.infer_modality(basename)
    if modality is None:
        parent = p.parent.name.lower()
        if modalities.is_valid(parent):
            modality = parent
    mrid = modalities.strip_to_mrid(basename)
    return mrid or None, modality


# ── NIfTI staging ─────────────────────────────────────────────────────────────

def stage_nifti_files(project_path: Path, filenames: list[str], file_data: list[bytes]) -> NiftiStagingResult:
    staging_id = str(uuid.uuid4())
    staging_dir = project_path / "_upload" / "nifti" / staging_id
    staging_dir.mkdir(parents=True, exist_ok=True)

    proposals = []
    skipped_duplicates = []
    for filename, data in zip(filenames, file_data):
        safe_name = Path(filename).name
        if not (safe_name.endswith(".nii") or safe_name.endswith(".nii.gz")):
            shutil.rmtree(staging_dir)
            raise HTTPException(400, f"'{filename}' is not a .nii or .nii.gz file")
        # Flattening (e.g. a folder upload with the same basename in two
        # subfolders) keeps the first occurrence and skips later duplicates.
        dest = staging_dir / safe_name
        if dest.exists():
            skipped_duplicates.append(filename)
            continue
        assert_safe_path(staging_dir, dest)
        dest.write_bytes(data)
        # Infer from the original upload path so that directory components
        # (e.g. "fl/subject001.nii.gz") contribute to modality detection.
        mrid, modality = infer_nifti_metadata(filename)
        proposals.append(NiftiUploadProposal(
            filename=safe_name,
            inferred_mrid=mrid,
            inferred_modality=modality,
        ))

    return NiftiStagingResult(staging_id=staging_id, proposals=proposals, skipped_duplicates=skipped_duplicates)


def stage_nifti_zip(project_path: Path, contents: bytes, filename: str) -> NiftiStagingResult:
    """Extract a zip of NIfTI files into a new staging area and return proposals."""
    if not filename.lower().endswith(".zip"):
        raise HTTPException(400, "NIfTI zip upload must be a .zip file")

    staging_id = str(uuid.uuid4())
    staging_dir = project_path / "_upload" / "nifti" / staging_id
    staging_dir.mkdir(parents=True, exist_ok=True)

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = Path(tmpdir) / "upload.zip"
            archive.write_bytes(contents)
            extract_dir = Path(tmpdir) / "extracted"
            extract_dir.mkdir()
            safe_unzip(archive, extract_dir)

            proposals = []
            skipped_duplicates = []
            for nifti in sorted(extract_dir.rglob("*")):
                if not (nifti.name.endswith(".nii") or nifti.name.endswith(".nii.gz")):
                    continue
                safe_name = nifti.name
                dest = staging_dir / safe_name
                rel = nifti.relative_to(extract_dir)
                # Flattening nested folders: the first occurrence of a basename
                # wins, later ones with the same flattened name are skipped.
                if dest.exists():
                    skipped_duplicates.append(str(rel))
                    continue
                assert_safe_path(staging_dir, dest)
                shutil.copy2(nifti, dest)
                # Pass the relative archive path so directory components contribute
                # to modality inference (e.g. "fl/subject001.nii.gz").
                mrid, modality = infer_nifti_metadata(str(rel))
                proposals.append(NiftiUploadProposal(
                    filename=safe_name,
                    inferred_mrid=mrid,
                    inferred_modality=modality,
                ))
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    return NiftiStagingResult(staging_id=staging_id, proposals=proposals, skipped_duplicates=skipped_duplicates)


def commit_nifti_staging(
    project_path: Path,
    staging_id: str,
    mappings: list,
) -> NiftiCommitResult:
    staging_dir = project_path / "_upload" / "nifti" / staging_id
    try:
        assert_safe_path(project_path, staging_dir)
    except PathEscapeError:
        raise HTTPException(400, "Invalid staging ID")
    if not staging_dir.is_dir():
        raise HTTPException(404, f"Staging area '{staging_id}' not found")

    committed = []
    skipped = []
    for mapping in mappings:
        src = staging_dir / mapping.filename
        assert_safe_path(staging_dir, src)
        if not src.exists():
            raise HTTPException(404, f"Staged file '{mapping.filename}' not found")
        target_dir = project_path / mapping.modality
        target = target_dir / f"{mapping.mrid}.nii.gz"
        # Re-uploading a subject that already has a scan for this modality is a
        # no-op: keep the existing file and drop the duplicate staged upload.
        if target.exists():
            src.unlink()
            skipped.append(CommittedFile(
                mrid=mapping.mrid,
                modality=mapping.modality,
                path=str(target.relative_to(project_path)),
            ))
            continue
        target_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), target)
        committed.append(CommittedFile(
            mrid=mapping.mrid,
            modality=mapping.modality,
            path=str(target.relative_to(project_path)),
        ))

    # Clean up empty staging dir
    if staging_dir.exists() and not any(staging_dir.iterdir()):
        staging_dir.rmdir()

    return NiftiCommitResult(committed=committed, skipped=skipped)


def discard_nifti_staging(project_path: Path, staging_id: str) -> None:
    staging_dir = project_path / "_upload" / "nifti" / staging_id
    try:
        assert_safe_path(project_path, staging_dir)
    except PathEscapeError:
        raise HTTPException(400, "Invalid staging ID")
    if not staging_dir.is_dir():
        raise HTTPException(404, f"Staging area '{staging_id}' not found")
    shutil.rmtree(staging_dir)


# ── CSV upload ────────────────────────────────────────────────────────────────

def store_csv_upload(project_path: Path, contents: bytes, filename: str) -> None:
    if not filename.lower().endswith(".csv"):
        raise HTTPException(400, "Uploaded file must be a .csv")
    csv_dir = project_path / "participants"
    csv_dir.mkdir(parents=True, exist_ok=True)
    (csv_dir / "participants.csv").write_bytes(contents)


# ── BIDS upload ───────────────────────────────────────────────────────────────

# BIDS suffix → NiChart modality: see app.modalities.infer_modality_bids.

_BIDS_SUB_RE = re.compile(r"^sub-(\w+)")


def store_bids_upload(project_path: Path, contents: bytes, filename: str) -> int:
    """
    Extract a BIDS zip and reorganise NIfTIs into the NiChart project layout.

    Returns the number of NIfTI files successfully placed.
    """
    if not filename.lower().endswith(".zip"):
        raise HTTPException(400, "BIDS upload must be a .zip file")

    committed = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_dir = Path(tmpdir)
        archive = tmp_dir / "upload.zip"
        archive.write_bytes(contents)
        extract_dir = tmp_dir / "extracted"
        extract_dir.mkdir()
        safe_unzip(archive, extract_dir)

        for nifti in extract_dir.rglob("*.nii.gz"):
            m = _BIDS_SUB_RE.match(nifti.name)
            if not m:
                continue
            mrid = m.group(1)
            modality = modalities.infer_modality_bids(nifti.name)
            if not modality:
                continue
            target_dir = project_path / modality
            target_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(nifti, target_dir / f"{mrid}.nii.gz")
            committed += 1

        # Convert participants.tsv if present
        for tsv in extract_dir.rglob("participants.tsv"):
            _convert_bids_participants_tsv(tsv, project_path)
            break  # use only the top-level one

    return committed


def _convert_bids_participants_tsv(tsv_path: Path, project_path: Path) -> None:
    """Convert a BIDS participants.tsv to participants/participants.csv.

    Maps ``participant_id`` → ``MRID`` (stripping the ``sub-`` prefix).
    All other BIDS columns are preserved with their original names.
    """
    csv_dir = project_path / "participants"
    csv_dir.mkdir(parents=True, exist_ok=True)
    with tsv_path.open(newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])
    if not rows:
        return
    col = {k.lower(): k for k in fieldnames}
    pid_key = col.get("participant_id") or col.get("subject_id") or ""
    if not pid_key:
        return
    extra_cols = [k for k in fieldnames if k != pid_key]
    out_fieldnames = ["MRID"] + extra_cols
    out_path = csv_dir / "participants.csv"
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames)
        writer.writeheader()
        for row in rows:
            pid = (row.get(pid_key, "") or "").strip().removeprefix("sub-")
            if not pid:
                continue
            out_row: dict = {"MRID": pid}
            for c in extra_cols:
                out_row[c] = (row.get(c, "") or "").strip()
            writer.writerow(out_row)


# ── IDAT upload ───────────────────────────────────────────────────────────────

def store_idat_upload(project_path: Path, contents: bytes, filename: str) -> None:
    if not filename.lower().endswith(".zip"):
        raise HTTPException(400, "IDAT upload must be a .zip file")
    idat_dir = project_path / "idat"
    idat_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp.write(contents)
        tmp_path = Path(tmp.name)
    try:
        safe_unzip(tmp_path, idat_dir)
    finally:
        tmp_path.unlink(missing_ok=True)


def store_bids_files(project_path: Path, files: list[tuple[str, bytes]]) -> int:
    """
    Stage a batch of BIDS files supplied as (relative_path, content) pairs,
    then reorganise NIfTIs into the NiChart project layout.

    Filenames may contain relative sub-paths (e.g. ``sub-01/anat/sub-01_T1w.nii.gz``).
    Each is validated for containment within the temporary extraction directory.
    Returns the number of NIfTI files committed.
    """
    committed = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        extract_dir = Path(tmpdir)

        for rel_path, content in files:
            assert_safe_upload_filename(rel_path, allow_subdirs=True)
            dest = extract_dir / rel_path
            assert_safe_path(extract_dir, dest)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(content)

        for nifti in extract_dir.rglob("*.nii.gz"):
            m = _BIDS_SUB_RE.match(nifti.name)
            if not m:
                continue
            mrid = m.group(1)
            modality = modalities.infer_modality_bids(nifti.name)
            if not modality:
                continue
            target_dir = project_path / modality
            target_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(nifti, target_dir / f"{mrid}.nii.gz")
            committed += 1

        for tsv in extract_dir.rglob("participants.tsv"):
            _convert_bids_participants_tsv(tsv, project_path)
            break

    return committed


def store_idat_files(project_path: Path, files: list[tuple[str, bytes]]) -> None:
    """
    Store a batch of IDAT files supplied as (filename, content) pairs.

    Filenames must be flat (no directory separators) and end with ``.idat``.
    Each filename is validated before any file is written.
    """
    for filename, _ in files:
        assert_safe_upload_filename(filename, allow_subdirs=False)
        if not filename.lower().endswith(".idat"):
            raise HTTPException(400, f"Only .idat files are accepted; got {filename!r}")

    idat_dir = project_path / "idat"
    idat_dir.mkdir(parents=True, exist_ok=True)

    for filename, content in files:
        dest = idat_dir / Path(filename).name
        assert_safe_path(idat_dir, dest)
        dest.write_bytes(content)
