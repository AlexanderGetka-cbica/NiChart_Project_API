"""
Record of manually uploaded intermediate data for a project.

A user who cannot upload raw images may instead supply a later step's inputs by
hand and start a pipeline from that step. Those uploads are recorded here, in
``{study_dir}/_working/manual_uploads.json``, for two purposes:

* **Provenance.** Results derived from hand-supplied intermediates are not
  reproducible from the project's raw data, and the record is the only thing
  that says so. It is append-only.
* **Cache correctness.** A step whose output was replaced by hand did not
  produce what is now on disk, so its cached result must not be reused. The
  check runs against recorded upload paths rather than against cache entries,
  which is what lets it cover entries written before cache records carried
  their output paths.

Lives apart from ``job_service`` so that the upload path can write records and
the orchestrator can read them without the two importing each other.
"""

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

_MANUAL_UPLOADS_FILE = "_working/manual_uploads.json"


def _atomic_write_text(path: Path, text: str) -> None:
    """Write via a uniquely-named temp file + rename.

    Same reasoning as ``job_service._atomic_write_text``: a plain write
    truncates the target first, and every file under a synced project directory
    may be read by an S3 upload mid-write. ``.tmp`` is excluded from S3 sync.
    """
    tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex[:8]}.tmp")
    try:
        tmp.write_text(text)
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _path(study_dir: Path) -> Path:
    return study_dir / _MANUAL_UPLOADS_FILE


def load_records(study_dir: Path) -> list[dict]:
    """All recorded manual uploads for a project, oldest first.

    Returns ``[]`` when nothing has been uploaded, and also when the file is
    unreadable: a corrupt record must not take a project's runs down with it.
    """
    p = _path(study_dir)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text())
    except Exception:
        return []
    records = data.get("uploads") if isinstance(data, dict) else None
    return [r for r in records if isinstance(r, dict)] if isinstance(records, list) else []


def append_record(study_dir: Path, record: dict) -> dict:
    """Append one upload record and return it with its timestamps filled in."""
    now = datetime.now(timezone.utc)
    entry = {
        **record,
        "uploaded_at": now.isoformat(),
        "uploaded_at_ts": now.timestamp(),
    }
    records = load_records(study_dir)
    records.append(entry)
    p = _path(study_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_text(p, json.dumps({"uploads": records}, indent=2, default=str))
    return entry


def override_paths(study_dir: Path) -> list[tuple[Path, float]]:
    """``(absolute_path, uploaded_at)`` for every recorded upload.

    Consumed by the cache verdict. A record whose path or timestamp is missing
    or malformed is skipped rather than guessed at — it would otherwise either
    invalidate everything or nothing, both worse than ignoring one bad row.
    """
    result: list[tuple[Path, float]] = []
    for record in load_records(study_dir):
        rel = record.get("path")
        ts = record.get("uploaded_at_ts")
        if not isinstance(rel, str) or not rel:
            continue
        try:
            result.append((study_dir / rel, float(ts)))
        except (TypeError, ValueError):
            continue
    return result


def records_for_response(study_dir: Path) -> list[dict]:
    """Recorded uploads trimmed to the fields a run detail should expose."""
    return [
        {
            "slot_id": r.get("slot_id"),
            "path": r.get("path"),
            "pipeline_id": r.get("pipeline_id"),
            "produced_by_step": r.get("produced_by_step"),
            "files": r.get("files"),
            "total_bytes": r.get("total_bytes"),
            "replace": r.get("replace"),
            "uploaded_at": r.get("uploaded_at"),
        }
        for r in load_records(study_dir)
    ]
