"""
Tests for GET /catalog/pipelines and GET /catalog/tools.

Uses the test_sleep tool and test_pipeline pipeline that live in resources/.
These endpoints are public — no auth token needed.
"""

import pytest


def test_list_tools_includes_dummy(local_client):
    resp = local_client.get("/catalog/tools")
    assert resp.status_code == 200
    ids = [t["id"] for t in resp.json()]
    assert "test_sleep" in ids


def test_get_tool_test_sleep(local_client):
    resp = local_client.get("/catalog/tools/test_sleep")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == "test_sleep"
    assert data["name"] == "test_sleep"
    assert "duration_seconds" in data["parameters"]
    assert data["resources"]["vcpus"] == 1
    assert data["time_per_subject_seconds"] is None


def test_get_tool_not_found(local_client):
    resp = local_client.get("/catalog/tools/nonexistent_tool")
    assert resp.status_code == 404


def test_list_pipelines_includes_dummy(local_client):
    resp = local_client.get("/catalog/pipelines")
    assert resp.status_code == 200
    ids = [p["id"] for p in resp.json()]
    assert "test_pipeline" in ids


def test_get_pipeline_dummy(local_client):
    resp = local_client.get("/catalog/pipelines/test_pipeline")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == "test_pipeline"
    assert data["name"] == "test_pipeline"
    assert data["categories"] == ["testing"]
    assert len(data["steps"]) == 1
    assert data["steps"][0]["tool"] == "test_sleep"


def test_get_pipeline_not_found(local_client):
    resp = local_client.get("/catalog/pipelines/no_such_pipeline")
    assert resp.status_code == 404


def test_catalog_requires_no_auth(local_client):
    """Catalog endpoints must work without any Authorization header."""
    for url in ["/catalog/pipelines", "/catalog/tools"]:
        resp = local_client.get(url, headers={})
        assert resp.status_code == 200


def test_get_var_group_catalog(local_client):
    resp = local_client.get("/catalog/var-groups")
    assert resp.status_code == 200
    data = resp.json()

    dlmuse_group = data["groups"]["group_dlmuse-vol"]
    assert dlmuse_group["label"] == "DLMUSE Volumes"
    assert dlmuse_group["prefix"] == "DL_MUSE_Volume_"
    assert dlmuse_group["values"] is None

    spare_group = data["groups"]["group_spare-scores"]
    assert spare_group["prefix"] is None
    assert "SPARE_AD" in spare_group["values"]

    roi_list = data["roi_lists"]["list_muse-single"]
    assert roi_list["atlas"] == "muse"
    assert 47 in roi_list["values"]

    # Name-based ROI groups: empty prefix, ROI lists hold region names, linked by atlas.
    synthseg_group = data["groups"]["group_synthseg-vol"]
    assert synthseg_group["prefix"] == ""
    assert synthseg_group["atlas"] == "synthseg"
    synthseg_list = data["roi_lists"]["list_synthseg-all"]
    assert synthseg_list["atlas"] == "synthseg"
    assert "left hippocampus" in synthseg_list["values"]


def test_var_group_catalog_requires_no_auth(local_client):
    resp = local_client.get("/catalog/var-groups", headers={})
    assert resp.status_code == 200


# ── requires names + min_subjects surfacing ───────────────────────────────────

def test_requires_lists_names_not_stringified_dicts(local_client):
    """Structured requirements contribute their key, not a repr of the dict."""
    resp = local_client.get("/catalog/pipelines/run_dlmuse_harmonized")
    assert resp.status_code == 200
    requires = resp.json()["requires"]

    assert "needs_T1" in requires
    assert "csv_has_columns" in requires
    assert "min_subjects" in requires
    # No entry should be a stringified Python dict.
    assert all(not r.startswith("{") for r in requires), requires


def test_min_subjects_surfaced_with_constraints(local_client):
    """The UI can show what a pipeline needs before any data is uploaded."""
    resp = local_client.get("/catalog/pipelines/run_dlmuse_harmonized")
    assert resp.status_code == 200
    spec = resp.json()["min_subjects"]

    assert spec["required"] == 30
    assert spec["recommended"] == 30

    constraint = spec["constraints"][0]
    assert constraint["label"] == "Cognitively normal controls"
    # Defaults resolved server-side: inherits the parent 30/30.
    assert constraint["required"] == 30
    assert constraint["recommended"] == 30
    assert constraint["where"] == [
        {"column": "DX", "equals": "CN", "in": None, "min": None, "max": None}
    ]


def test_min_subjects_null_when_not_declared(local_client):
    resp = local_client.get("/catalog/pipelines/test_pipeline")
    assert resp.status_code == 200
    assert resp.json()["min_subjects"] is None


# ── input_notes ───────────────────────────────────────────────────────────────

_RAW_T1_NOTE = (
    "Upload raw (unprocessed) T1 images — not skull-stripped, "
    "bias-corrected or registered T1s."
)


def test_input_notes_in_pipeline_list(local_client):
    """The list endpoint carries notes, so a pipeline card can show them."""
    resp = local_client.get("/catalog/pipelines")
    assert resp.status_code == 200
    by_id = {p["id"]: p for p in resp.json()}

    notes = by_id["run_brain2vec"]["input_notes"]
    # The raw-T1 note must be present; the pipeline also carries a second note
    # explaining that it preprocesses the scans itself.
    assert _RAW_T1_NOTE in notes
    assert len(notes) >= 1


def test_input_notes_in_pipeline_detail(local_client):
    resp = local_client.get("/catalog/pipelines/run_brain2vec")
    assert resp.status_code == 200
    assert _RAW_T1_NOTE in resp.json()["input_notes"]


def test_input_notes_default_empty(local_client):
    """A pipeline declaring none reports [], not null."""
    for url in ("/catalog/pipelines/test_pipeline", "/catalog/pipelines"):
        resp = local_client.get(url)
        assert resp.status_code == 200
    detail = local_client.get("/catalog/pipelines/test_pipeline").json()
    assert detail["input_notes"] == []

    summary = next(
        p for p in local_client.get("/catalog/pipelines").json()
        if p["id"] == "test_pipeline"
    )
    assert summary["input_notes"] == []


def test_malformed_input_notes_are_ignored():
    """A bad value must not make a pipeline unlistable."""
    from app.services.catalog_service import _parse_input_notes

    assert _parse_input_notes(None) == []
    assert _parse_input_notes("a bare string") == []        # not a list
    assert _parse_input_notes({"note": "x"}) == []          # not a list
    assert _parse_input_notes([]) == []
    assert _parse_input_notes(["  ", ""]) == []             # blanks dropped
    assert _parse_input_notes([" keep me ", 42]) == ["keep me", "42"]


# ── Singularity run-mode invariant ────────────────────────────────────────────

def test_ambiguous_commands_declare_singularity_run_mode():
    """A command not starting with '-' must state run vs exec explicitly.

    The Singularity backend auto-detects: "run" when the rendered command starts
    with '-' (clearly arguments to an entrypoint), otherwise "exec". That guess
    is wrong for every tool whose command is entrypoint arguments that happen to
    begin with a word — a dispatcher subcommand ('brain2vec', 'vit_ba'), a mount
    path (synthseg), or a script path handed to a 'python' entrypoint. Those ran
    fine on Docker and Batch and would have failed only on SLURM, so the rule is
    enforced here rather than left to be discovered per tool.
    """
    import yaml
    from pathlib import Path
    from app.config import Settings

    offenders = []
    for path in sorted(Settings().tools_path.glob("*.yaml")):
        container = (yaml.safe_load(path.read_text()) or {}).get("container") or {}
        command = (container.get("command") or "").strip()
        if not command or command.startswith("-"):
            continue  # auto-detection is reliable for these
        if container.get("singularity_run_mode") not in ("run", "exec"):
            offenders.append(path.stem)

    assert not offenders, (
        "these tools' commands do not start with '-', so Singularity would guess "
        f"'exec'; declare container.singularity_run_mode explicitly: {offenders}"
    )
