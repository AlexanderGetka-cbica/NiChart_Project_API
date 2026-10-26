"""
Tests for the dry-run plan endpoint and for start_from_step.

The planner's semantics are exercised at unit level against synthetic step lists
and a temp study dir, because the bundled test_pipeline has a single step with no
inputs and a tool that declares no timing — too thin to cover downstream
propagation or the time sums. The endpoint contract and the run-loop behaviour are
covered over HTTP.
"""

import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.config import Settings
from app.services import job_service


def _step(step_id, tool, inputs, outputs, params=None, name=None):
    """A stand-in for catalog PipelineStep; the planner only reads attributes."""
    return SimpleNamespace(
        id=step_id, tool=tool, inputs=inputs, outputs=outputs,
        params=params or {}, name=name,
    )


# nichart_dlmuse declares time_per_subject_seconds 120; ravens_toolkit_fireants 180.
TOOL_A, TPS_A = "nichart_dlmuse", 120.0
TOOL_B, TPS_B = "nichart_ravens_toolkit_fireants", 180.0


@pytest.fixture
def tools_path():
    return Settings().tools_path


@pytest.fixture
def study(tmp_path):
    """A study dir with two T1 images, so step 1 has a subject count of 2."""
    t1 = tmp_path / "t1"
    t1.mkdir()
    for mrid in ("sub1", "sub2"):
        (t1 / f"{mrid}.nii.gz").write_bytes(b"x")
    return tmp_path


def _chain(study):
    """step_a reads t1/ and writes out_a/; step_b reads out_a/ and writes out_b/."""
    return [
        _step("step_a", TOOL_A, {"i": "${STUDY}/t1"}, {"o": "${STUDY}/out_a"}),
        _step("step_b", TOOL_B, {"i": "${STUDY}/out_a"}, {"o": "${STUDY}/out_b"}),
    ]


def _invalidate(path: Path):
    """Push a path's mtime past the seeded finished_time, as a new upload would."""
    future = time.time() + 3600
    import os
    os.utime(path, (future, future))


def _plan(study, steps, tools_path, reuse=True, start=None, params=None):
    return job_service.plan_pipeline_run(
        study_dir=study,
        pipeline_steps=steps,
        user_params=params or {},
        reuse_cached_steps=reuse,
        tools_path=tools_path,
        start_from_step=start,
    )


def _seed_cache(study, steps, params=None, finished_time=None):
    """Make every step in `steps` look like a past success.

    Mirrors the planner's own resolution so the keys line up: outputs are created,
    and the recorded finished_time is set in the future relative to the inputs so
    nothing reads as modified-since.
    """
    params = params or {}
    metadata = {}
    step_outputs = {}
    for step_def in steps:
        resolved_inputs = {
            k: job_service._resolve_path(v, study, step_outputs)
            for k, v in step_def.inputs.items()
        }
        resolved_outputs = {
            k: job_service._resolve_path(v, study, step_outputs)
            for k, v in step_def.outputs.items()
        }
        step_outputs[step_def.id] = resolved_outputs
        for path in resolved_outputs.values():
            Path(path).mkdir(parents=True, exist_ok=True)
        key = job_service._cache_key(
            step_def.tool, resolved_inputs, {**params, **step_def.params}
        )
        metadata[key] = {
            "status": "success",
            "finished_time": finished_time or (time.time() + 60),
            "run_id": f"prior-{step_def.id}",
        }
    job_service._save_metadata(study, metadata)
    return metadata


# ── Planner semantics ─────────────────────────────────────────────────────────

def test_nothing_has_run_so_everything_runs(study, tools_path):
    steps, total, full = _plan(study, _chain(study), tools_path)

    assert [s.action for s in steps] == ["run", "run"]
    assert all(s.reason == "no previous successful run" for s in steps)
    assert all(not s.will_reuse and s.cached_from_run_id is None for s in steps)
    # step_a sees 2 T1s; step_b's input dir is empty on disk but step_a will fill
    # it, so step_b is credited with the same 2 (see the look-ahead tests below).
    assert steps[0].subjects == 2 and steps[1].subjects == 2
    assert steps[0].estimated_seconds == TPS_A * 2
    assert steps[1].estimated_seconds == TPS_B * 2
    assert total == TPS_A * 2 + TPS_B * 2
    assert full == total           # nothing reused, so the two agree


def test_everything_reused_after_a_clean_run(study, tools_path):
    chain = _chain(study)
    _seed_cache(study, chain)

    steps, total, full = _plan(study, chain, tools_path)

    assert [s.action for s in steps] == ["reuse", "reuse"]
    assert all(s.will_reuse and s.reason is None for s in steps)
    assert [s.cached_from_run_id for s in steps] == ["prior-step_a", "prior-step_b"]
    assert all(s.estimated_seconds == 0 for s in steps)
    assert total == 0
    assert full and full > 0       # the upper bound still counts both steps


def test_touching_an_input_carries_downstream(study, tools_path):
    chain = _chain(study)
    _seed_cache(study, chain)          # both steps start as valid cache hits
    # A new subject invalidates step_a only; step_b's input (out_a/) is untouched,
    # so step_b must be re-run purely because of the upstream change.
    (study / "t1" / "sub3.nii.gz").write_bytes(b"x")
    _invalidate(study / "t1")

    steps, total, _full = _plan(study, chain, tools_path)

    assert steps[0].action == "run"
    assert steps[0].reason == "inputs changed since last run"
    assert steps[1].action == "run"
    assert steps[1].reason == "upstream step will re-run"
    # step_b inherits the grown cohort, not out_a/'s stale size.
    assert steps[1].subjects == 3
    assert total == TPS_A * 3 + TPS_B * 3


def test_downstream_rule_matches_nested_paths(study, tools_path):
    """A step reading a FILE inside an upstream output DIRECTORY still re-runs."""
    chain = [
        _step("step_a", TOOL_A, {"i": "${STUDY}/t1"}, {"o": "${STUDY}/out_a"}),
        _step("step_b", TOOL_B, {"i": "${STUDY}/out_a/vol.csv"}, {"o": "${STUDY}/out_b"}),
    ]
    (study / "out_a").mkdir()
    (study / "out_a" / "vol.csv").write_text("MRID\n")
    _seed_cache(study, chain)          # step_b would otherwise be a cache hit
    _invalidate(study / "t1")          # only step_a is dirty

    steps, _t, _f = _plan(study, chain, tools_path)
    assert steps[0].reason == "inputs changed since last run"
    # step_b reads a FILE inside the directory step_a rewrites.
    assert steps[1].reason == "upstream step will re-run"


def test_reuse_disabled_means_nothing_is_reused(study, tools_path):
    chain = _chain(study)
    _seed_cache(study, chain)

    steps, total, full = _plan(study, chain, tools_path, reuse=False)

    assert [s.action for s in steps] == ["run", "run"]
    assert all(s.reason == "cache reuse disabled" for s in steps)
    assert total == full


def test_missing_outputs_are_reported(study, tools_path):
    chain = _chain(study)
    _seed_cache(study, chain)
    # Remove step_a's output directory: its cache entry is no longer serviceable.
    (study / "out_a").rmdir()

    steps, _t, _f = _plan(study, chain, tools_path)
    assert steps[0].action == "run"
    assert steps[0].reason == "outputs missing"


def test_untimed_steps_contribute_zero_to_the_total(study, tools_path):
    """An untimed tool must not void the whole estimate, only understate it."""
    chain = [_step("s", "merge_feature_csvs_by_mrid",
                   {"i": "${STUDY}/t1"}, {"o": "${STUDY}/o"})]
    steps, total, full = _plan(study, chain, tools_path)

    assert steps[0].time_per_subject_seconds is None
    # Still null per step, so a caller can tell "unknown" from "free".
    assert steps[0].estimated_seconds is None
    assert total == 0.0 and full == 0.0


def test_mixed_timing_totals_only_the_known_steps(study, tools_path):
    chain = [
        _step("timed", TOOL_A, {"i": "${STUDY}/t1"}, {"o": "${STUDY}/out_a"}),
        _step("untimed", "merge_feature_csvs_by_mrid",
              {"i": "${STUDY}/out_a"}, {"o": "${STUDY}/out_b"}),
    ]
    steps, total, _full = _plan(study, chain, tools_path)

    assert steps[0].estimated_seconds == TPS_A * 2
    assert steps[1].estimated_seconds is None
    assert total == TPS_A * 2, "the timed step still counts"


def test_in_flight_sibling_is_flagged_not_reused(study, tools_path, monkeypatch):
    chain = _chain(study)
    _seed_cache(study, chain)
    # Occupy the in-flight slot for step_a's key while leaving it a valid cache hit.
    resolved = {"i": str(study / "t1")}
    key = job_service._cache_key(TOOL_A, resolved, {})
    monkeypatch.setitem(job_service._step_in_flight, key, object())

    steps, _t, _f = _plan(study, chain, tools_path)
    # A valid cache hit still wins; the flag only applies to steps that would run.
    assert steps[0].action == "reuse"
    assert steps[0].in_flight_elsewhere is False

    steps, _t, _f = _plan(study, chain, tools_path, reuse=False)
    assert steps[0].action == "run"
    assert steps[0].in_flight_elsewhere is True


# ── start_from_step in the plan ───────────────────────────────────────────────

def test_start_from_step_forces_and_skips(study, tools_path):
    chain = [
        _step("step_a", TOOL_A, {"i": "${STUDY}/t1"}, {"o": "${STUDY}/out_a"}),
        _step("step_b", TOOL_B, {"i": "${STUDY}/out_a"}, {"o": "${STUDY}/out_b"}),
        _step("step_c", TOOL_B, {"i": "${STUDY}/out_b"}, {"o": "${STUDY}/out_c"}),
    ]
    _seed_cache(study, chain)   # every step would otherwise be a cache hit

    steps, total, full = _plan(study, chain, tools_path, start="step_b")

    assert [s.action for s in steps] == ["skip_forced", "run", "run"]
    # Skipped-by-user steps cost nothing and are excluded from both sums.
    assert steps[0].estimated_seconds == 0 and steps[0].reason is None
    assert steps[1].reason == "start step always runs"
    assert steps[2].reason == "upstream step will re-run"
    assert total == TPS_B * steps[1].subjects + TPS_B * steps[2].subjects
    assert full == total        # step_a excluded from the upper bound too


def test_start_step_reports_missing_inputs_as_a_warning(study, tools_path):
    chain = [
        _step("step_a", TOOL_A, {"i": "${STUDY}/t1"}, {"o": "${STUDY}/out_a"}),
        _step("step_b", TOOL_B,
              {"present": "${STUDY}/t1", "absent": "${STUDY}/nope/x.csv"},
              {"o": "${STUDY}/out_b"}),
    ]
    steps, _t, _f = _plan(study, chain, tools_path, start="step_b")

    assert steps[1].missing_inputs == [str(Path("nope") / "x.csv")]
    assert steps[0].missing_inputs == []     # only the start step reports these
    assert steps[1].action == "run"           # never blocks


def test_forced_skips_do_not_force_downstream_reruns(study, tools_path):
    """Skipped-before steps are assumed complete, so they don't dirty downstream."""
    chain = [
        _step("step_a", TOOL_A, {"i": "${STUDY}/t1"}, {"o": "${STUDY}/out_a"}),
        _step("step_b", TOOL_B, {"i": "${STUDY}/out_a"}, {"o": "${STUDY}/out_b"}),
        _step("step_c", TOOL_B, {"i": "${STUDY}/unrelated"}, {"o": "${STUDY}/out_c"}),
    ]
    (study / "unrelated").mkdir()      # step_c's input must exist to be cacheable
    _seed_cache(study, chain)
    steps, _t, _f = _plan(study, chain, tools_path, start="step_b")

    # step_c reads nothing step_b writes, so it keeps its cache hit.
    assert steps[2].action == "reuse"


# ── Endpoint contract ─────────────────────────────────────────────────────────

def _create_project(client, name="planproject"):
    assert client.post("/projects", json={"name": name}).status_code == 201
    return name


def test_plan_endpoint_returns_a_plan(job_client):
    pid = _create_project(job_client)
    resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines/plan",
        json={"pipeline_id": "test_pipeline"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["pipeline_id"] == "test_pipeline"
    assert body["steps_total"] == 1
    assert body["steps"][0]["step_id"] == "sleep"
    assert body["steps"][0]["action"] == "run"
    assert body["steps"][0]["name"] == "test_sleep"      # falls back to the tool
    assert body["reuse_cached_steps"] is True
    assert "ready" in body and "subject_count" in body


def test_plan_is_read_only(job_client, tmp_path):
    pid = _create_project(job_client, "planreadonly")
    before = job_client.get("/jobs/pipelines").json()
    job_client.post(
        f"/projects/{pid}/jobs/pipelines/plan",
        json={"pipeline_id": "test_pipeline"},
    )
    after = job_client.get("/jobs/pipelines").json()
    assert len(after) == len(before), "plan must not create a run record"


def test_plan_unknown_pipeline_404(job_client):
    pid = _create_project(job_client, "plan404")
    resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines/plan",
        json={"pipeline_id": "no_such_pipeline"},
    )
    assert resp.status_code == 404


def test_plan_unknown_start_step_400(job_client):
    pid = _create_project(job_client, "plan400")
    resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines/plan",
        json={"pipeline_id": "test_pipeline", "start_from_step": "nope"},
    )
    assert resp.status_code == 400
    assert "nope" in resp.json()["detail"]


def test_submit_unknown_start_step_400(job_client):
    pid = _create_project(job_client, "submit400")
    resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline", "start_from_step": "nope"},
    )
    assert resp.status_code == 400


# ── start_from_step on a real submission ──────────────────────────────────────

def test_cache_hit_carries_skip_reason_cached(job_client):
    pid = _create_project(job_client, "skipcached")
    body = {"pipeline_id": "test_pipeline", "params": {"duration_seconds": 1}}
    first = job_client.post(f"/projects/{pid}/jobs/pipelines", json=body)
    assert first.status_code == 202
    second = job_client.post(f"/projects/{pid}/jobs/pipelines", json=body)
    detail = job_client.get(f"/jobs/pipelines/{second.json()['run_id']}").json()

    step = detail["steps"][0]
    assert step["status"] == "skipped"
    assert step["skip_reason"] == "cached"
    assert step["cached_from_run_id"]


def test_submit_with_start_from_step_forces_earlier_steps(job_client):
    """Step 1 is skipped unchecked; step 2 runs and the detail records the start."""
    pid = _create_project(job_client, "startfrom")
    resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "run_spare_all", "start_from_step": "run_spare_all"},
    )
    assert resp.status_code == 202, resp.text
    detail = job_client.get(f"/jobs/pipelines/{resp.json()['run_id']}").json()

    assert detail["start_from_step"] == "run_spare_all"
    first, second = detail["steps"][0], detail["steps"][1]
    assert first["step_id"] == "run_nichart_dlmuse"
    assert first["status"] == "skipped"
    assert first["skip_reason"] == "forced"
    assert first["cached_from_run_id"] is None
    assert second["step_id"] == "run_spare_all"
    assert second["status"] in ("succeeded", "running")
    assert second["skip_reason"] is None


def test_start_step_runs_even_when_cached(job_client):
    """The start step ignores a valid cache entry."""
    pid = _create_project(job_client, "startforced")
    body = {"pipeline_id": "test_pipeline", "params": {"duration_seconds": 1}}
    assert job_client.post(f"/projects/{pid}/jobs/pipelines", json=body).status_code == 202

    forced = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={**body, "start_from_step": "sleep"},
    )
    detail = job_client.get(f"/jobs/pipelines/{forced.json()['run_id']}").json()
    step = detail["steps"][0]
    assert step["status"] != "skipped", "start step must not be served from cache"
    assert step["skip_reason"] is None


def test_pipeline_detail_exposes_step_names(local_client):
    resp = local_client.get("/catalog/pipelines/run_spare_all")
    assert resp.status_code == 200
    steps = resp.json()["steps"]
    assert steps[0]["name"] == "nichart_dlmuse"          # from the tool
    assert steps[0]["description"]
    assert steps[0]["id"] == "run_nichart_dlmuse"


# ── Look-ahead subject counts (follow-up: downstream underestimate) ───────────

def test_downstream_steps_inherit_upstream_subject_count(study, tools_path):
    """A fresh pipeline must not count unfilled output dirs as one subject.

    step_b reads the directory step_a will fill. On disk that directory is
    absent, which previously floored step_b at 1 subject and understated the
    whole run by the cohort size.
    """
    chain = _chain(study)                      # t1/ has 2 subjects
    steps, total, full = _plan(study, chain, tools_path)

    assert steps[0].subjects == 2
    assert steps[1].subjects == 2, "step_b should inherit step_a's predicted output size"
    assert steps[1].estimated_seconds == TPS_B * 2
    assert total == TPS_A * 2 + TPS_B * 2
    assert full == total


def test_look_ahead_chains_through_several_steps(study, tools_path):
    chain = [
        _step("a", TOOL_A, {"i": "${STUDY}/t1"}, {"o": "${STUDY}/out_a"}),
        _step("b", TOOL_B, {"i": "${STUDY}/out_a"}, {"o": "${STUDY}/out_b"}),
        _step("c", TOOL_B, {"i": "${STUDY}/out_b"}, {"o": "${STUDY}/out_c"}),
    ]
    steps, _t, _f = _plan(study, chain, tools_path)
    assert [s.subjects for s in steps] == [2, 2, 2]


def test_look_ahead_covers_growth_when_upstream_reruns(study, tools_path):
    """New subjects must reach downstream steps, not the stale output size."""
    chain = _chain(study)
    _seed_cache(study, chain)
    # out_a/ currently holds the old cohort; two more subjects arrive upstream.
    for mrid in ("sub3", "sub4"):
        (study / "t1" / f"{mrid}.nii.gz").write_bytes(b"x")
    _invalidate(study / "t1")

    steps, _t, _f = _plan(study, chain, tools_path)
    assert steps[0].subjects == 4
    assert steps[1].subjects == 4, "downstream must use the grown count, not out_a/'s"


def test_file_inputs_are_not_inflated(study, tools_path):
    """A CSV input stays at 1, matching what the real run computes and chunks on."""
    chain = [
        _step("a", TOOL_A, {"i": "${STUDY}/t1"}, {"o": "${STUDY}/out_a"}),
        _step("b", TOOL_B, {"i": "${STUDY}/out_a/vol.csv"}, {"o": "${STUDY}/out_b"}),
    ]
    steps, _t, _f = _plan(study, chain, tools_path)

    assert steps[0].subjects == 2
    assert steps[1].subjects == 1
    assert steps[1].reason == "no previous successful run"


def test_reused_and_forced_steps_do_not_inflate_downstream(study, tools_path):
    """Their outputs are already on disk, so the on-disk count is authoritative."""
    chain = _chain(study)
    _seed_cache(study, chain)
    # out_a/ exists but is empty, so the honest downstream count is 1.
    steps, _t, _f = _plan(study, chain, tools_path)
    assert steps[0].action == "reuse"
    assert steps[1].subjects == 1

    forced, _t, _f = _plan(study, chain, tools_path, start="step_b")
    assert forced[0].action == "skip_forced"
    assert forced[1].subjects == 1


def test_plan_reports_steps_missing_timing(job_client):
    """The endpoint tells the caller the total is a lower bound."""
    pid = _create_project(job_client, "planmissing")
    body = job_client.post(
        f"/projects/{pid}/jobs/pipelines/plan",
        json={"pipeline_id": "test_pipeline"},
    ).json()

    # test_sleep declares no time_per_subject_seconds.
    assert body["steps"][0]["estimated_seconds"] is None
    assert body["estimated_total_seconds"] == 0
    assert body["steps_missing_timing"] == 1


# ── fixed_seconds ─────────────────────────────────────────────────────────────

def test_fixed_seconds_counts_without_a_per_subject_time(study, tools_path):
    """A startup-dominated tool now contributes a real number, not null."""
    chain = [_step("s", "spare_all", {"i": "${STUDY}/t1"}, {"o": "${STUDY}/o"})]
    steps, total, _full = _plan(study, chain, tools_path)

    assert steps[0].time_per_subject_seconds is None
    assert steps[0].fixed_seconds == 10
    assert steps[0].estimated_seconds == 10        # independent of cohort size
    assert total == 10


def test_fixed_and_per_subject_add_up(study, tools_path):
    """harmonize_dlmuse: ~60 s fixed plus ~0.008 s/subject, as measured."""
    chain = [_step("s", "harmonize_dlmuse", {"i": "${STUDY}/t1"}, {"o": "${STUDY}/o"})]
    steps, total, _full = _plan(study, chain, tools_path)

    assert steps[0].subjects == 2
    assert steps[0].estimated_seconds == pytest.approx(60 + 0.008 * 2)
    assert total == pytest.approx(60.016)


def test_step_with_neither_timing_is_still_null(study, tools_path):
    chain = [_step("s", "merge_feature_csvs_by_mrid",
                   {"i": "${STUDY}/t1/a.csv"}, {"o": "${STUDY}/o.csv"})]
    steps, total, _full = _plan(study, chain, tools_path)

    assert steps[0].fixed_seconds is None
    assert steps[0].time_per_subject_seconds is None
    assert steps[0].estimated_seconds is None
    assert total == 0.0                            # contributes nothing


def test_harmonized_spare_pipeline_total_is_no_longer_a_stub(study, tools_path):
    """The real 3-step harmonized SPARE chain now produces a usable total."""
    import yaml
    from app.config import Settings
    raw = yaml.safe_load(
        (Settings().pipelines_path / "run_spare_all_harmonized.yaml").read_text()
    )
    chain = [
        _step(s["id"], s["tool"], s.get("inputs") or {}, s.get("outputs") or {})
        for s in raw["steps"]
    ]
    steps, total, _full = _plan(study, chain, tools_path)

    assert [s.tool_id for s in steps] == [
        "nichart_dlmuse", "harmonize_dlmuse", "spare_all_harmonized"
    ]
    # dlmuse 120 s/subject x 2 subjects, harmonize ~60 s, spare 10 s.
    assert steps[0].estimated_seconds == 240
    # harmonize reads dlmuse_vol/DLMUSE_Volumes.csv — a file, so 1 subject.
    assert steps[1].subjects == 1
    assert total == pytest.approx(240 + 60 + 0.008 * 1 + 10)
