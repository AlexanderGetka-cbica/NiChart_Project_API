"""
Manual upload of intermediate pipeline data.

Covers slot classification, the content checks, every rejection path, merge vs
replace, and the two correctness properties the feature rests on: a cached step
whose output was supplied by hand is not reused, and in cloud mode the upload
reaches S3 before the request returns.
"""

import io
import json
import time
import zipfile
from pathlib import Path

import pytest

PIPELINE = "run_spare_all_cvm"
START = "merge_feature_csvs"
DLMUSE_VOL_SLOT = "run_nichart_dlmuse.dlmuse_vol"
DLWMLS_SLOT = "run_nichart_dlwmls.nichart_dlwmls_out"

PROJECT = "proj1"


def _make_project(client, name=PROJECT):
    resp = client.post("/projects", json={"name": name})
    assert resp.status_code in (200, 201), resp.text
    return resp


def _inputs(client, project=PROJECT, pipeline=PIPELINE, start=START):
    return client.get(
        f"/projects/{project}/files/intermediate-inputs/{pipeline}",
        params={"start_from_step": start},
    )


def _slot(body, slot_id):
    return next(s for s in body["slots"] if s["slot_id"] == slot_id)


def _upload(client, files, *, slot_id=DLMUSE_VOL_SLOT, replace=False, project=PROJECT):
    return client.post(
        f"/projects/{project}/files/upload/intermediate",
        data={
            "pipeline_id": PIPELINE,
            "start_from_step": START,
            "slot_id": slot_id,
            "replace": str(replace).lower(),
        },
        files=files,
    )


def _csv_bytes() -> bytes:
    """A DLMUSE volumes CSV with the columns nichart_dlmuse declares it writes."""
    return b"MRID,DL_MUSE_Volume_702\nS1,1400000\n"


# ── Slot classification ───────────────────────────────────────────────────────

def test_slots_classified_for_spare_all_cvm(data_client):
    """Starting run_spare_all_cvm at merge_feature_csvs yields exactly two
    upload slots, one generated output and participants.csv as project input."""
    _make_project(data_client)
    body = _inputs(data_client).json()

    assert body["pipeline_id"] == PIPELINE
    assert body["start_from_step"] == START

    by_kind: dict[str, list[str]] = {}
    for slot in body["slots"]:
        by_kind.setdefault(slot["kind"], []).append(slot["slot_id"])

    assert sorted(by_kind["upload"]) == sorted([DLMUSE_VOL_SLOT, DLWMLS_SLOT])
    assert by_kind["generated"] == ["merge_feature_csvs.out_csv"]
    assert by_kind["project_input"] == ["project.participants/participants.csv"]


def test_dlmuse_vol_slot_requires_the_csv_the_next_step_reads(data_client):
    """A step reading a path inside a slot makes that exact file required, and
    the requirement is reported before anything has been uploaded."""
    _make_project(data_client)
    slot = _slot(_inputs(data_client).json(), DLMUSE_VOL_SLOT)

    assert slot["path"] == "dlmuse_vol"
    assert slot["type"] == "directory"
    assert slot["produced_by_step"] == "run_nichart_dlmuse"
    assert slot["status"] == "missing"
    assert slot["present"] is False

    patterns = {r["pattern"]: r for r in slot["required_contents"]}
    assert "DLMUSE_Volumes.csv" in patterns
    required = patterns["DLMUSE_Volumes.csv"]
    assert required["status"] == "missing"
    # Declared by the tool AND read directly by merge_feature_csvs; the
    # pipeline path is the stronger source and wins the dedupe.
    assert required["source"] == "pipeline_path"

    assert [c["step_id"] for c in slot["consumed_by"]] == ["merge_feature_csvs"]
    assert slot["consumed_by"][0]["path"] == "dlmuse_vol/DLMUSE_Volumes.csv"


def test_participants_slot_points_at_its_own_uploader(data_client):
    _make_project(data_client)
    slot = _slot(_inputs(data_client).json(), "project.participants/participants.csv")
    assert slot["kind"] == "project_input"
    assert slot["project_input_hint"] == {"type": "participants", "code": None}
    assert slot["required_contents"] == []


def test_dlmuse_seg_is_absent_because_no_running_step_reads_it(data_client):
    """run_nichart_dlmuse also outputs dlmuse_seg, but only the skipped dlwmls
    step consumes it, so it is not something the user needs to supply."""
    _make_project(data_client)
    ids = {s["slot_id"] for s in _inputs(data_client).json()["slots"]}
    assert "run_nichart_dlmuse.dlmuse_seg" not in ids


def test_start_at_first_step_returns_no_upload_slots(data_client):
    """Permitted, and consistent with the plan and submit routes, which also
    accept it — rather than a 400."""
    _make_project(data_client)
    resp = _inputs(data_client, start="run_nichart_dlmuse")
    assert resp.status_code == 200
    kinds = {s["kind"] for s in resp.json()["slots"]}
    assert "upload" not in kinds
    assert kinds <= {"project_input", "generated"}


def test_unknown_start_step_is_rejected(data_client):
    _make_project(data_client)
    resp = _inputs(data_client, start="no_such_step")
    assert resp.status_code == 400
    assert "no_such_step" in resp.json()["detail"]


# ── Content checks and coverage ───────────────────────────────────────────────

def test_slot_becomes_met_once_the_required_csv_is_uploaded(data_client):
    _make_project(data_client)
    resp = _upload(data_client, [("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))])
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["written"] == 1
    assert body["rejected"] == []
    assert body["skipped"] == []
    assert body["slot"]["status"] == "met"
    assert body["slot"]["present"] is True
    assert all(r["status"] == "met" for r in body["slot"]["required_contents"])


def test_csv_missing_a_required_column_is_invalid_not_merely_present(data_client):
    """nichart_dlmuse declares DLMUSE_Volumes.csv carries DL_MUSE_Volume_702, and
    merge_feature_csvs_by_mrid declares it reads MRID. A file that parses but
    lacks a declared column is invalid, not merely present."""
    _make_project(data_client)
    resp = _upload(data_client, [
        ("files", ("DLMUSE_Volumes.csv", b"MRID\nS1\n", "text/csv")),
    ])
    assert resp.status_code == 200, resp.text
    slot = resp.json()["slot"]

    assert slot["present"] is True
    assert slot["status"] == "partial"
    invalid = [r for r in slot["required_contents"] if r["status"] == "invalid"]
    assert len(invalid) == 1
    assert "DL_MUSE_Volume_702" in invalid[0]["problem"]
    # Columns required by the producing and the consuming tool are unioned into
    # one requirement rather than reported as several.
    assert invalid[0]["csv_columns"] == ["MRID", "DL_MUSE_Volume_702"]


def test_coverage_unknowable_without_imaging_or_participants(data_client, tmp_path):
    """The feature's primary case: no raw data was ever uploaded, so the
    expected subject set cannot be derived and is reported as unknown rather
    than guessed at."""
    _make_project(data_client)
    resp = data_client.get(
        f"/projects/{PROJECT}/files/intermediate-inputs/run_bascores",
        params={"start_from_step": "align_dlicv_to_mni"},
    )
    assert resp.status_code == 200, resp.text
    per_subject_slots = [s for s in resp.json()["slots"] if s["coverage"] is not None]
    assert per_subject_slots, "expected a slot with a per-subject rule"
    coverage = per_subject_slots[0]["coverage"]
    assert coverage["expected_subjects"] is None
    assert coverage["missing_mrids"] == []
    assert any("cannot be checked" in p for p in per_subject_slots[0]["problems"])


def test_coverage_counts_against_imaging_subjects(data_client, tmp_path):
    """With imaging present, coverage is measured against those subjects and a
    partial upload is reported as partial."""
    _make_project(data_client)
    t1 = tmp_path / "LOCAL_USER" / PROJECT / "t1"
    t1.mkdir(parents=True, exist_ok=True)
    for mrid in ("S1", "S2", "S3"):
        (t1 / f"{mrid}.nii.gz").write_bytes(b"x")

    resp = data_client.post(
        f"/projects/{PROJECT}/files/upload/intermediate",
        data={
            "pipeline_id": "run_bascores",
            "start_from_step": "align_dlicv_to_mni",
            "slot_id": "binarize_dlmuse_to_dlicv.mask_dir",
            "replace": "false",
        },
        files=[("files", ("S1_DLMUSE.nii.gz", b"x", "application/gzip"))],
    )
    assert resp.status_code == 200, resp.text
    coverage = resp.json()["slot"]["coverage"]
    assert coverage["expected_subjects"] == 3
    assert coverage["matched"] == 1
    assert coverage["missing_mrids"] == ["S2", "S3"]
    assert resp.json()["slot"]["status"] == "partial"


# ── Rejections ────────────────────────────────────────────────────────────────

def test_path_traversal_fails_the_whole_request(data_client, tmp_path):
    _make_project(data_client)
    resp = _upload(data_client, [
        ("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv")),
        ("files", ("../escaped.csv", b"MRID\n", "text/csv")),
    ])
    assert resp.status_code == 400
    assert "nothing was saved" in resp.json()["detail"]
    # The valid file in the same request was not written either.
    assert not (tmp_path / "LOCAL_USER" / PROJECT / "dlmuse_vol").exists()


def test_absolute_path_fails_the_whole_request(data_client):
    _make_project(data_client)
    resp = _upload(data_client, [("files", ("/etc/passwd", b"x", "text/plain"))])
    assert resp.status_code == 400


def test_dotfiles_are_skipped_not_fatal(data_client):
    """Finder metadata must not break an otherwise valid upload."""
    _make_project(data_client)
    resp = _upload(data_client, [
        ("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv")),
        ("files", (".DS_Store", b"junk", "application/octet-stream")),
        ("files", ("__MACOSX/._DLMUSE_Volumes.csv", b"junk", "application/octet-stream")),
    ])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["written"] == 1
    assert sorted(body["skipped"]) == sorted([".DS_Store", "__MACOSX/._DLMUSE_Volumes.csv"])
    assert body["rejected"] == []
    assert body["slot"]["status"] == "met"


def test_disallowed_extension_is_a_per_file_rejection(data_client):
    _make_project(data_client)
    resp = _upload(data_client, [
        ("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv")),
        ("files", ("notes.exe", b"MZ", "application/octet-stream")),
    ])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["written"] == 1
    assert [r["name"] for r in body["rejected"]] == ["notes.exe"]
    assert "not an accepted file type" in body["rejected"][0]["reason"]
    assert body["slot"]["status"] == "met"


def test_request_with_only_rejected_files_fails(data_client):
    _make_project(data_client)
    resp = _upload(data_client, [("files", ("notes.exe", b"MZ", "application/octet-stream"))])
    assert resp.status_code == 400
    assert "No acceptable files" in resp.json()["detail"]


def test_oversized_file_is_rejected_individually(data_client, monkeypatch):
    _make_project(data_client)
    from app.config import Settings, get_settings

    overrides = data_client.app.dependency_overrides
    previous = overrides[get_settings]
    base = previous()
    overrides[get_settings] = lambda: Settings(
        execution_mode="local", data_root=base.data_root, max_upload_file_mb=0,
    )
    try:
        resp = _upload(data_client, [
            ("files", ("DLMUSE_Volumes.csv", b"MRID\nS1\n", "text/csv")),
        ])
        assert resp.status_code == 400
        assert "per-file limit" in resp.json()["detail"]
    finally:
        overrides[get_settings] = previous


def test_project_input_slot_cannot_be_uploaded_to(data_client):
    _make_project(data_client)
    resp = _upload(
        data_client,
        [("files", ("participants.csv", _csv_bytes(), "text/csv"))],
        slot_id="project.participants/participants.csv",
    )
    assert resp.status_code == 400
    assert "images or participants uploader" in resp.json()["detail"]


def test_generated_slot_cannot_be_uploaded_to(data_client):
    _make_project(data_client)
    resp = _upload(
        data_client,
        [("files", ("csv_out.csv", _csv_bytes(), "text/csv"))],
        slot_id="merge_feature_csvs.out_csv",
    )
    assert resp.status_code == 400
    assert "nothing to upload" in resp.json()["detail"]


def test_unknown_slot_is_rejected(data_client):
    _make_project(data_client)
    resp = _upload(
        data_client,
        [("files", ("x.csv", _csv_bytes(), "text/csv"))],
        slot_id="made.up",
    )
    assert resp.status_code == 400
    assert "Unknown slot" in resp.json()["detail"]


# ── Merge vs replace ──────────────────────────────────────────────────────────

def test_upload_merges_by_default(data_client, tmp_path):
    _make_project(data_client)
    slot_dir = tmp_path / "LOCAL_USER" / PROJECT / "dlmuse_vol"

    _upload(data_client, [("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))])
    _upload(data_client, [("files", ("extra.csv", b"MRID\nS9\n", "text/csv"))])

    names = sorted(f.name for f in slot_dir.iterdir() if f.name != "_provenance.json")
    assert names == ["DLMUSE_Volumes.csv", "extra.csv"]


def test_same_name_is_overwritten_on_merge(data_client, tmp_path):
    _make_project(data_client)
    slot_dir = tmp_path / "LOCAL_USER" / PROJECT / "dlmuse_vol"

    _upload(data_client, [("files", ("DLMUSE_Volumes.csv", b"MRID\nOLD\n", "text/csv"))])
    _upload(data_client, [("files", ("DLMUSE_Volumes.csv", b"MRID\nNEW\n", "text/csv"))])

    assert (slot_dir / "DLMUSE_Volumes.csv").read_text() == "MRID\nNEW\n"


def test_replace_clears_the_slot_first(data_client, tmp_path):
    _make_project(data_client)
    slot_dir = tmp_path / "LOCAL_USER" / PROJECT / "dlmuse_vol"

    _upload(data_client, [("files", ("stale.csv", b"MRID\nS0\n", "text/csv"))])
    resp = _upload(
        data_client,
        [("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))],
        replace=True,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["replaced"] is True

    names = sorted(f.name for f in slot_dir.iterdir() if f.name != "_provenance.json")
    assert names == ["DLMUSE_Volumes.csv"]


def test_subdirectories_are_preserved_for_folder_slots(data_client, tmp_path):
    _make_project(data_client)
    resp = _upload(data_client, [
        ("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv")),
        ("files", ("nested/inner.csv", b"MRID\nS2\n", "text/csv")),
    ])
    assert resp.status_code == 200, resp.text
    slot_dir = tmp_path / "LOCAL_USER" / PROJECT / "dlmuse_vol"
    assert (slot_dir / "nested" / "inner.csv").exists()


# ── Zip uploads ───────────────────────────────────────────────────────────────

def _zip_bytes(entries: dict[str, bytes], *, top: str | None = None) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in entries.items():
            zf.writestr(f"{top}/{name}" if top else name, data)
    return buf.getvalue()


def _upload_zip(client, payload, *, slot_id=DLMUSE_VOL_SLOT, replace=False):
    return client.post(
        f"/projects/{PROJECT}/files/upload/intermediate/zip",
        data={
            "pipeline_id": PIPELINE,
            "start_from_step": START,
            "slot_id": slot_id,
            "replace": str(replace).lower(),
        },
        files={"file": ("slot.zip", payload, "application/zip")},
    )


def test_zip_upload_populates_the_slot(data_client):
    _make_project(data_client)
    resp = _upload_zip(data_client, _zip_bytes({"DLMUSE_Volumes.csv": _csv_bytes()}))
    assert resp.status_code == 200, resp.text
    assert resp.json()["written"] == 1
    assert resp.json()["slot"]["status"] == "met"


def test_zip_single_top_level_directory_is_stripped(data_client, tmp_path):
    """A Finder-zipped folder nests everything one level down."""
    _make_project(data_client)
    resp = _upload_zip(
        data_client,
        _zip_bytes({"DLMUSE_Volumes.csv": _csv_bytes()}, top="dlmuse_vol"),
    )
    assert resp.status_code == 200, resp.text
    slot_dir = tmp_path / "LOCAL_USER" / PROJECT / "dlmuse_vol"
    assert (slot_dir / "DLMUSE_Volumes.csv").exists()
    assert not (slot_dir / "dlmuse_vol").exists()


def test_zip_macos_metadata_is_skipped(data_client):
    _make_project(data_client)
    payload = _zip_bytes({
        "DLMUSE_Volumes.csv": _csv_bytes(),
        "__MACOSX/._DLMUSE_Volumes.csv": b"junk",
        ".DS_Store": b"junk",
    })
    resp = _upload_zip(data_client, payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["written"] == 1
    assert sorted(body["skipped"]) == sorted(["__MACOSX/._DLMUSE_Volumes.csv", ".DS_Store"])


def test_zip_with_traversal_member_fails_the_whole_request(data_client, tmp_path):
    _make_project(data_client)
    payload = _zip_bytes({"../escaped.csv": b"MRID\n", "DLMUSE_Volumes.csv": _csv_bytes()})
    resp = _upload_zip(data_client, payload)
    assert resp.status_code == 400
    assert not (tmp_path / "LOCAL_USER" / PROJECT / "dlmuse_vol").exists()


def test_zip_with_symlink_member_fails_the_whole_request(data_client, tmp_path):
    _make_project(data_client)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        info = zipfile.ZipInfo("link.csv")
        info.external_attr = (0o120777 << 16)  # S_IFLNK
        zf.writestr(info, "/etc/passwd")
        zf.writestr("DLMUSE_Volumes.csv", _csv_bytes())
    resp = _upload_zip(data_client, buf.getvalue())
    assert resp.status_code == 400
    assert not (tmp_path / "LOCAL_USER" / PROJECT / "dlmuse_vol").exists()


# ── Cache interaction ─────────────────────────────────────────────────────────

def test_upload_clears_the_producing_steps_cache_entry(data_client, tmp_path):
    """A step whose output was supplied by hand did not produce what is on
    disk, so its cache entry must go and the step must re-run."""
    from app.services import job_service

    _make_project(data_client)
    study = tmp_path / "LOCAL_USER" / PROJECT
    inputs = {"t1_img": str(study / "t1")}
    outputs = {"dlmuse_vol": str(study / "dlmuse_vol"), "dlmuse_seg": str(study / "dlmuse_seg")}
    key = job_service._cache_key("nichart_dlmuse", inputs, {})

    metadata = {}
    job_service._mark_cached(
        metadata, key, "run-old", tool_id="nichart_dlmuse",
        inputs=inputs, outputs=outputs,
    )
    job_service._save_metadata(study, metadata)

    resp = _upload(data_client, [("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))])
    assert resp.status_code == 200, resp.text
    assert resp.json()["cache_entries_cleared"] == 1
    assert key not in job_service._load_metadata(study)


def test_legacy_cache_entry_without_outputs_is_still_not_reused(data_client, tmp_path):
    """Entries written before cache records carried output paths cannot be
    found by slot, so clearing cannot reach them. The manual-upload record
    invalidates them instead — otherwise a later full run would adopt the
    uploaded files as that step's own output."""
    from app.services import job_service

    _make_project(data_client)
    study = tmp_path / "LOCAL_USER" / PROJECT
    inputs = {"t1_img": str(study / "t1")}
    outputs = {"dlmuse_vol": str(study / "dlmuse_vol")}
    key = "legacy-key"

    # Old-shape entry: status/finished_time/run_id only. finished_time is set
    # after the inputs exist, so the mtime rule is satisfied and the only thing
    # that can invalidate this entry is the manual upload.
    (study / "_working").mkdir(parents=True, exist_ok=True)
    (study / "t1").mkdir(parents=True, exist_ok=True)
    job_service._save_metadata(study, {
        key: {
            "status": "success",
            "finished_time": time.time(),
            "run_id": "run-legacy",
        },
    })
    time.sleep(0.05)  # the upload must land strictly after finished_time

    resp = _upload(data_client, [("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))])
    assert resp.status_code == 200, resp.text
    # Nothing matchable to clear...
    assert resp.json()["cache_entries_cleared"] == 0
    assert key in job_service._load_metadata(study)

    # ...but the entry is no longer a valid cache hit.
    metadata = job_service._load_metadata(study)
    overrides = job_service.manual_upload_store.override_paths(study)
    assert job_service._is_cached(metadata, key, inputs, outputs, overrides) is None
    assert "manual upload" in job_service._explain_cache_miss(
        metadata, key, inputs, outputs, overrides
    )


def test_cache_entry_predating_the_upload_is_unaffected_for_other_steps(data_client, tmp_path):
    """Only steps whose outputs overlap the uploaded slot are invalidated."""
    from app.services import job_service

    _make_project(data_client)
    study = tmp_path / "LOCAL_USER" / PROJECT
    (study / "t1").mkdir(parents=True, exist_ok=True)
    unrelated_inputs = {"t1_img": str(study / "t1")}
    unrelated_outputs = {"synthseg_seg": str(study / "synthseg_seg")}
    (study / "synthseg_seg").mkdir(parents=True, exist_ok=True)
    key = job_service._cache_key("synthseg", unrelated_inputs, {})
    metadata = {}
    job_service._mark_cached(
        metadata, key, "run-x", tool_id="synthseg",
        inputs=unrelated_inputs, outputs=unrelated_outputs,
    )
    job_service._save_metadata(study, metadata)

    _upload(data_client, [("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))])

    metadata = job_service._load_metadata(study)
    assert key in metadata
    overrides = job_service.manual_upload_store.override_paths(study)
    assert job_service._is_cached(
        metadata, key, unrelated_inputs, unrelated_outputs, overrides
    ) == "run-x"


# ── Provenance ────────────────────────────────────────────────────────────────

def test_upload_is_recorded_for_provenance(data_client, tmp_path):
    _make_project(data_client)
    study = tmp_path / "LOCAL_USER" / PROJECT

    _upload(data_client, [("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))])

    log = json.loads((study / "_working" / "manual_uploads.json").read_text())
    assert len(log["uploads"]) == 1
    record = log["uploads"][0]
    assert record["slot_id"] == DLMUSE_VOL_SLOT
    assert record["path"] == "dlmuse_vol"
    assert record["produced_by_step"] == "run_nichart_dlmuse"
    assert record["files"] == 1
    assert record["user_id"] == "LOCAL_USER"
    assert record["uploaded_at_ts"] > 0

    marker = json.loads((study / "dlmuse_vol" / "_provenance.json").read_text())
    assert marker["source"] == "manual_intermediate_upload"
    assert marker["step_id"] == "run_nichart_dlmuse"


def test_manual_upload_log_is_hidden_from_the_file_browser(data_client):
    """_working/ is already excluded, so the record does not show up as a file
    the user can browse or delete by accident."""
    _make_project(data_client)
    _upload(data_client, [("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))])

    tree = data_client.get(f"/projects/{PROJECT}/files").json()
    assert "_working" not in json.dumps(tree)


def test_plan_marks_steps_that_consume_manual_uploads(data_client):
    _make_project(data_client)
    _upload(data_client, [("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))])

    resp = data_client.post(
        f"/projects/{PROJECT}/jobs/pipelines/plan",
        json={"pipeline_id": PIPELINE, "start_from_step": START},
    )
    assert resp.status_code == 200, resp.text
    steps = {s["step_id"]: s for s in resp.json()["steps"]}
    assert steps["merge_feature_csvs"]["uses_manual_uploads"] is True
    assert steps["run_spare_all_cvm"]["uses_manual_uploads"] is False


# ── Feature flag ──────────────────────────────────────────────────────────────

def test_routes_are_absent_when_the_feature_is_disabled(tmp_path, monkeypatch):
    """A disabled deployment serves 404, which is how the client detects it.

    The flag is read when the app is built, not per request, so it is set in
    the environment here rather than through a dependency override.
    """
    from fastapi.testclient import TestClient

    import app.config as config
    from app.auth.dependencies import require_auth
    from app.config import Settings, get_settings
    from app.main import create_app
    from tests.conftest import _override_require_auth

    monkeypatch.setenv("NICHART_ENABLE_INTERMEDIATE_UPLOADS", "false")
    config.get_settings.cache_clear()
    try:
        app = create_app()
        app.dependency_overrides[get_settings] = lambda: Settings(
            execution_mode="local", data_root=tmp_path,
            enable_intermediate_uploads=False,
        )
        app.dependency_overrides[require_auth] = _override_require_auth

        with TestClient(app) as client:
            client.post("/projects", json={"name": PROJECT})
            resp = client.get(
                f"/projects/{PROJECT}/files/intermediate-inputs/{PIPELINE}",
                params={"start_from_step": START},
            )
            assert resp.status_code == 404
            resp = client.post(
                f"/projects/{PROJECT}/files/upload/intermediate",
                data={"pipeline_id": PIPELINE, "start_from_step": START, "slot_id": "x"},
                files=[("files", ("a.csv", b"MRID\n", "text/csv"))],
            )
            assert resp.status_code == 404
        app.dependency_overrides.clear()
    finally:
        # create_app and the TestClient lifespan both populate the settings
        # cache while the env var is still set; leaving it populated would
        # un-mount the router for every later test in the session.
        config.get_settings.cache_clear()


# ── Cloud mode: the upload must be canonical ──────────────────────────────────
#
# A cloud run begins by syncing the project DOWN from S3
# (job_service._run_pipeline), and that download overwrites any local file whose
# size or ETag differs from the remote object. An upload that only ever lands on
# local disk is therefore silently reverted at submit time whenever S3 already
# holds an older copy of the same path. These tests pin the ordering that
# prevents it.


@pytest.fixture
def cloud_upload_client(tmp_path, monkeypatch):
    """Cloud-mode client with the S3 transfers recorded instead of performed."""
    from fastapi.testclient import TestClient

    from app.auth.dependencies import require_auth
    from app.config import Settings, get_settings
    from app.main import create_app
    from app.services import s3_sync_service
    from tests.conftest import _override_require_auth

    calls: list[tuple] = []

    async def _fake_to_s3(local_dir, bucket, prefix):
        calls.append(("up", str(local_dir), bucket, prefix,
                      sorted(str(f.relative_to(local_dir))
                             for f in Path(local_dir).rglob("*") if f.is_file())))

    async def _fake_delete(bucket, prefix):
        calls.append(("delete", bucket, prefix))

    monkeypatch.setattr(s3_sync_service, "sync_to_s3", _fake_to_s3)
    monkeypatch.setattr(s3_sync_service, "delete_s3_prefix", _fake_delete)

    app = create_app()
    app.dependency_overrides[get_settings] = lambda: Settings(
        execution_mode="cloud",
        data_root=tmp_path,
        s3_data_bucket="test-bucket",
        s3_data_prefix="fsx",
    )
    app.dependency_overrides[require_auth] = _override_require_auth
    with TestClient(app, raise_server_exceptions=True) as client:
        client.calls = calls
        yield client
    app.dependency_overrides.clear()


def test_cloud_upload_pushes_the_slot_to_s3(cloud_upload_client, tmp_path):
    _make_project(cloud_upload_client)
    resp = _upload(
        cloud_upload_client,
        [("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))],
    )
    assert resp.status_code == 200, resp.text

    ups = [c for c in cloud_upload_client.calls if c[0] == "up"]
    # The slot's own files go up first, keyed under the slot's prefix...
    assert ups[0][3] == "fsx/LOCAL_USER/proj1/dlmuse_vol"
    assert ups[0][4] == ["DLMUSE_Volumes.csv"]
    # ...then the project, carrying the cache and upload-record bookkeeping.
    assert ups[-1][3] == "fsx/LOCAL_USER/proj1"


def test_cloud_slot_push_happens_before_the_local_write(
    cloud_upload_client, tmp_path, monkeypatch
):
    """Ordering is the whole point: if S3 cannot be written, nothing is left on
    local disk for a run to pick up and then silently lose."""
    from app.services import s3_sync_service

    slot_dir = tmp_path / "LOCAL_USER" / PROJECT / "dlmuse_vol"
    _make_project(cloud_upload_client)

    seen: dict[str, bool] = {}

    async def _failing(local_dir, bucket, prefix):
        seen["slot_dir_existed_at_push"] = slot_dir.exists()
        raise RuntimeError("endpoint unreachable")

    monkeypatch.setattr(s3_sync_service, "sync_to_s3", _failing)
    resp = _upload(
        cloud_upload_client,
        [("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))],
    )

    assert resp.status_code == 502
    assert "Nothing was saved" in resp.json()["detail"]
    assert seen["slot_dir_existed_at_push"] is False
    assert not slot_dir.exists()


def test_cloud_replace_clears_the_remote_prefix_too(cloud_upload_client):
    """An additive sync would leave remote files the user meant to delete, and
    the next pre-run download would bring them back."""
    _make_project(cloud_upload_client)
    resp = _upload(
        cloud_upload_client,
        [("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))],
        replace=True,
    )
    assert resp.status_code == 200, resp.text
    deletes = [c for c in cloud_upload_client.calls if c[0] == "delete"]
    assert deletes == [("delete", "test-bucket", "fsx/LOCAL_USER/proj1/dlmuse_vol")]


def test_local_mode_does_not_touch_s3(data_client, monkeypatch):
    """No bucket, no sync — the local filesystem is already canonical."""
    from app.services import s3_sync_service

    calls = []

    async def _spy(*args):
        calls.append(args)

    monkeypatch.setattr(s3_sync_service, "sync_to_s3", _spy)
    _make_project(data_client)
    resp = _upload(
        data_client, [("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))]
    )
    assert resp.status_code == 200, resp.text
    assert calls == []


def test_participants_upload_is_pushed_to_s3_in_cloud_mode(cloud_upload_client):
    """The same hazard applied to the existing uploaders: a re-uploaded
    participants.csv could be reverted by the pre-run download."""
    _make_project(cloud_upload_client)
    resp = cloud_upload_client.post(
        f"/projects/{PROJECT}/files/upload/csv",
        files={"file": ("participants.csv", b"MRID,Age,Sex\nS1,70,F\n", "text/csv")},
    )
    assert resp.status_code in (200, 204), resp.text
    ups = [c for c in cloud_upload_client.calls if c[0] == "up"]
    assert ups and ups[-1][3] == "fsx/LOCAL_USER/proj1"


def test_run_detail_records_manual_uploads_in_place_at_submit(job_client, tmp_path):
    """A run derived from hand-supplied data must say so, and must keep saying
    so after later uploads change the project."""
    project = "jobproj"
    assert job_client.post("/projects", json={"name": project}).status_code in (200, 201)

    upload = job_client.post(
        f"/projects/{project}/files/upload/intermediate",
        data={
            "pipeline_id": PIPELINE,
            "start_from_step": START,
            "slot_id": DLMUSE_VOL_SLOT,
            "replace": "false",
        },
        files=[("files", ("DLMUSE_Volumes.csv", _csv_bytes(), "text/csv"))],
    )
    assert upload.status_code == 200, upload.text

    resp = job_client.post(
        f"/projects/{project}/jobs/pipelines",
        json={"pipeline_id": PIPELINE, "start_from_step": START},
    )
    assert resp.status_code in (200, 202), resp.text
    run_id = resp.json()["run_id"]

    detail = job_client.get(f"/jobs/pipelines/{run_id}").json()
    assert len(detail["manual_uploads"]) == 1
    record = detail["manual_uploads"][0]
    assert record["slot_id"] == DLMUSE_VOL_SLOT
    assert record["path"] == "dlmuse_vol"
    assert record["produced_by_step"] == "run_nichart_dlmuse"


def test_run_detail_has_no_manual_uploads_for_an_ordinary_run(job_client):
    project = "cleanproj"
    assert job_client.post("/projects", json={"name": project}).status_code in (200, 201)
    resp = job_client.post(
        f"/projects/{project}/jobs/pipelines", json={"pipeline_id": "test_pipeline"}
    )
    assert resp.status_code in (200, 202), resp.text
    detail = job_client.get(f"/jobs/pipelines/{resp.json()['run_id']}").json()
    assert detail["manual_uploads"] == []
