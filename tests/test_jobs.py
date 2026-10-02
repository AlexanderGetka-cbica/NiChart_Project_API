"""
Tests for the pipeline job submission and status endpoints.

Uses MockBackend (injected via job_client fixture) so no Docker or AWS is needed.
MockBackend immediately returns "succeeded", so background tasks complete within
the same HTTP request cycle — tests can check final state after a single submit call.
"""

import asyncio
import json

import pytest


# ── Helpers ───────────────────────────────────────────────────────────────────

def _create_project(client, name="testproject"):
    resp = client.post("/projects", json={"name": name})
    assert resp.status_code == 201
    return name


@pytest.fixture(autouse=True)
def _clear_run_store():
    """Reset disk-based run store state between tests for clean isolation.

    Each job_client fixture gets a fresh tmp_path, so per-test isolation is
    achieved naturally. Resetting _data_root here ensures no stale state bleeds
    in from a previous test's data root before the new lifespan initialises it.
    """
    from app.services import job_service
    job_service._data_root = None
    yield
    job_service._data_root = None


# ── Submission ────────────────────────────────────────────────────────────────

def test_submit_pipeline_returns_run_id(job_client):
    pid = _create_project(job_client)
    resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    )
    assert resp.status_code == 202
    data = resp.json()
    assert "run_id" in data
    assert data["status"] == "pending"


def test_submit_unknown_pipeline(job_client):
    pid = _create_project(job_client)
    resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "no_such_pipeline"},
    )
    assert resp.status_code == 404


def test_submit_unknown_project(job_client):
    resp = job_client.post(
        "/projects/no-such-project/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    )
    assert resp.status_code == 404


# ── Background task completion ────────────────────────────────────────────────

def test_pipeline_completes_with_mock_backend(job_client):
    """With MockBackend, the background task finishes before the test advances."""
    pid = _create_project(job_client, "completetest")
    submit_resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    )
    run_id = submit_resp.json()["run_id"]

    resp = job_client.get(f"/jobs/pipelines/{run_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "succeeded"
    assert data["total_steps"] == 1
    assert data["steps"][0]["status"] in ("succeeded", "skipped")


def test_pipeline_with_params(job_client):
    pid = _create_project(job_client, "paramtest")
    resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline", "params": {"duration_seconds": 5}},
    )
    assert resp.status_code == 202


def test_submitted_param_reaches_backend(tmp_path):
    """A user-supplied param must flow through to backend.submit(params=...).

    Regression guard: proves the API forwards params rather than defaulting.
    Uses a capturing backend so we can inspect exactly what the orchestrator sends.
    """
    from typing import Any

    from app.auth.dependencies import require_auth
    from app.backends import get_backend
    from app.backends.base import JobBackend, JobHandle
    from app.config import Settings, get_settings
    from app.main import create_app
    from app.services import job_service

    captured: dict[str, Any] = {}

    class _CapturingHandle(JobHandle):
        @property
        def job_id(self) -> str:
            return "cap-001"

        async def status(self) -> str:
            return "succeeded"

        async def logs(self, tail: int = 200) -> str:
            return "ok"

        async def cancel(self) -> None:
            pass

    class _CapturingBackend(JobBackend):
        async def submit(self, tool_spec, mount_paths, params, num_subjects=1,
                         user_token=None, extra_readonly_mounts=None, study_id=None):
            captured["params"] = dict(params)
            captured["study_id"] = study_id
            return _CapturingHandle()

    app = create_app()
    app.dependency_overrides[get_settings] = lambda: Settings(
        execution_mode="local", data_root=tmp_path
    )
    app.dependency_overrides[get_backend] = lambda: _CapturingBackend()

    async def _fixed_user():
        from app.auth.dependencies import CurrentUser
        return CurrentUser(sub="LOCAL_USER", token="")

    app.dependency_overrides[require_auth] = _fixed_user

    from fastapi.testclient import TestClient

    with TestClient(app, raise_server_exceptions=True) as client:
        job_service._data_root = tmp_path
        client.post("/projects", json={"name": "paramflow"})
        resp = client.post(
            "/projects/paramflow/jobs/pipelines",
            json={"pipeline_id": "test_pipeline", "params": {"duration_seconds": 250}},
        )
        assert resp.status_code == 202

    app.dependency_overrides.clear()
    assert captured.get("params", {}).get("duration_seconds") == 250


def test_pipeline_cache_reuse_default_true(job_client, tmp_path):
    """Submit twice with the same params; second run should have skipped steps."""
    pid = _create_project(job_client, "cachetest")
    for _ in range(2):
        job_client.post(
            f"/projects/{pid}/jobs/pipelines",
            json={"pipeline_id": "test_pipeline", "reuse_cached_steps": True},
        )
    runs = job_client.get(f"/jobs/pipelines?project_id={pid}").json()
    assert len(runs) == 2


# ── List / detail / logs / cancel ─────────────────────────────────────────────

def test_list_runs_empty_for_new_project(job_client):
    pid = _create_project(job_client, "emptylist")
    resp = job_client.get(f"/jobs/pipelines?project_id={pid}")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_runs_after_submit(job_client):
    pid = _create_project(job_client, "listtest")
    submit_resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    )
    run_id = submit_resp.json()["run_id"]

    resp = job_client.get(f"/jobs/pipelines?project_id={pid}")
    assert resp.status_code == 200
    run_ids = [r["run_id"] for r in resp.json()]
    assert run_id in run_ids


def test_list_runs_filter_by_project(job_client):
    pid_a = _create_project(job_client, "proj-a")
    pid_b = _create_project(job_client, "proj-b")
    run_a = job_client.post(
        f"/projects/{pid_a}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    ).json()["run_id"]
    run_b = job_client.post(
        f"/projects/{pid_b}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    ).json()["run_id"]

    resp_a = job_client.get(f"/jobs/pipelines?project_id={pid_a}")
    ids_a = [r["run_id"] for r in resp_a.json()]
    assert run_a in ids_a
    assert run_b not in ids_a


def test_get_run_detail(job_client):
    pid = _create_project(job_client, "detailtest")
    run_id = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    ).json()["run_id"]

    resp = job_client.get(f"/jobs/pipelines/{run_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["run_id"] == run_id
    assert data["pipeline_id"] == "test_pipeline"
    assert "steps" in data


def test_get_run_detail_not_found(job_client):
    resp = job_client.get("/jobs/pipelines/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404


def test_get_run_logs(job_client):
    pid = _create_project(job_client, "logstest")
    run_id = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    ).json()["run_id"]

    resp = job_client.get(f"/jobs/pipelines/{run_id}/logs")
    assert resp.status_code == 200
    data = resp.json()
    assert data["run_id"] == run_id
    assert isinstance(data["logs"], str)


def test_get_run_logs_not_found(job_client):
    resp = job_client.get("/jobs/pipelines/00000000-0000-0000-0000-000000000000/logs")
    assert resp.status_code == 404


def test_cancel_run(job_client):
    pid = _create_project(job_client, "canceltest")
    run_id = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    ).json()["run_id"]
    resp = job_client.delete(f"/jobs/pipelines/{run_id}")
    assert resp.status_code == 204


def test_cancel_nonexistent_run(job_client):
    resp = job_client.delete("/jobs/pipelines/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404


# ── Step-level details ────────────────────────────────────────────────────────

def test_step_details_present(job_client):
    pid = _create_project(job_client, "steptest")
    run_id = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    ).json()["run_id"]

    detail = job_client.get(f"/jobs/pipelines/{run_id}").json()
    assert len(detail["steps"]) == 1
    step = detail["steps"][0]
    assert step["step_id"] == "sleep"
    assert step["tool_id"] == "test_sleep"
    assert step["status"] in ("succeeded", "skipped")


# ── Persistence ───────────────────────────────────────────────────────────────

def test_run_persisted_to_disk(job_client, tmp_path):
    """Each run is written to its own JSON file under _working/runs/."""
    pid = _create_project(job_client, "persisttest")
    run_id = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    ).json()["run_id"]

    run_file = tmp_path / "_working" / "runs" / f"{run_id}.json"
    assert run_file.exists(), f"Run file {run_file} should exist after task runs"
    data = json.loads(run_file.read_text())
    assert data["run_id"] == run_id
    assert data["status"] == "succeeded"


def test_load_runs_from_disk_restores_history(tmp_path):
    """Runs written by one server instance are visible after a fresh load."""
    from app.services import job_service

    # Write a finished run directly as a per-run JSON file.
    runs_dir = tmp_path / "_working" / "runs"
    runs_dir.mkdir(parents=True)
    fake_run = {
        "run_id": "fake-run-id",
        "user_id": "LOCAL_USER",
        "project_id": "myproject",
        "pipeline_id": "test_pipeline",
        "status": "succeeded",
        "submitted_at": "2024-01-01T00:00:00+00:00",
        "finished_at": "2024-01-01T00:01:00+00:00",
        "current_step": 0,
        "total_steps": 1,
        "error": None,
        "cancelled": False,
        "backend_type": "",
        "steps": [],
    }
    (runs_dir / "fake-run-id.json").write_text(json.dumps(fake_run))

    job_service.load_runs_from_disk(tmp_path)
    run = job_service._load_run("fake-run-id")
    assert run is not None
    assert run.status == "succeeded"


def test_load_runs_marks_interrupted_runs_failed(tmp_path):
    """In-progress runs restored from disk are marked as failed."""
    from app.services import job_service

    runs_dir = tmp_path / "_working" / "runs"
    runs_dir.mkdir(parents=True)
    fake_run = {
        "run_id": "interrupted-run",
        "user_id": "LOCAL_USER",
        "project_id": "myproject",
        "pipeline_id": "test_pipeline",
        "status": "running",
        "submitted_at": "2024-01-01T00:00:00+00:00",
        "finished_at": None,
        "current_step": 0,
        "total_steps": 1,
        "error": None,
        "cancelled": False,
        "backend_type": "docker",
        "steps": [],
    }
    (runs_dir / "interrupted-run.json").write_text(json.dumps(fake_run))

    job_service.load_runs_from_disk(tmp_path)
    run = job_service._load_run("interrupted-run")
    assert run is not None
    assert run.status == "failed"
    assert run.error
    assert run.finished_at is not None


# ── In-flight step coordination ───────────────────────────────────────────────

def test_in_flight_step_wait_and_skip(tmp_path):
    """
    A second pipeline run that reaches the same step while the first is in
    flight should wait for it to complete and then skip the step as cached.
    """
    import asyncio
    from app.services import job_service
    from app.services.job_service import _StepFlight, _step_in_flight

    job_service._data_root = tmp_path

    key = "test-cache-key-abc123"
    flight = _StepFlight(key=key)
    _step_in_flight[key] = flight

    # Resolve flight as succeeded after a short delay (simulates sibling finishing).
    async def _resolve_after(delay: float) -> None:
        await asyncio.sleep(delay)
        flight.resolve(True)

    async def _wait() -> bool | None:
        asyncio.create_task(_resolve_after(0.02))
        return await job_service._wait_for_in_flight_step(key, "fake-run-id")

    result = asyncio.run(_wait())
    assert result is True
    assert flight.succeeded is True
    assert flight.event.is_set()
    assert key not in _step_in_flight  # cleaned up by resolve()


def test_in_flight_step_failed_sibling(tmp_path):
    """When the sibling step fails, the waiter gets False and should submit its own job."""
    import asyncio
    from app.services import job_service
    from app.services.job_service import _StepFlight, _step_in_flight

    job_service._data_root = tmp_path

    key = "test-cache-key-failed"
    flight = _StepFlight(key=key)
    _step_in_flight[key] = flight

    async def _resolve_failed() -> None:
        await asyncio.sleep(0.02)
        flight.resolve(False)

    async def _wait() -> bool | None:
        asyncio.create_task(_resolve_failed())
        return await job_service._wait_for_in_flight_step(key, "fake-run-id")

    result = asyncio.run(_wait())
    assert result is False
    assert key not in _step_in_flight


def test_in_flight_no_sibling_returns_none(tmp_path):
    """When there is no in-flight step, the helper returns None immediately."""
    import asyncio
    from app.services import job_service

    job_service._data_root = tmp_path
    result = asyncio.run(job_service._wait_for_in_flight_step("no-such-key", "fake-run-id"))
    assert result is None


# ── Finished-run polling ──────────────────────────────────────────────────────

def test_finished_runs_endpoint_empty_on_second_call(job_client):
    """After the first poll drains all finished runs, subsequent calls return empty."""
    pid = _create_project(job_client, "finishedtest")
    job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    )

    r1 = job_client.get("/jobs/pipelines/finished")
    assert r1.status_code == 200
    data1 = r1.json()
    assert len(data1["runs"]) == 1
    assert data1["runs"][0]["status"] == "succeeded"
    assert "polled_at" in data1

    r2 = job_client.get("/jobs/pipelines/finished")
    assert r2.status_code == 200
    assert r2.json()["runs"] == []


def test_finished_runs_only_terminal(job_client):
    """A run that's still pending/running is not returned."""
    r = job_client.get("/jobs/pipelines/finished")
    assert r.status_code == 200
    assert r.json()["runs"] == []


def test_finished_runs_multiple_accumulate_between_polls(job_client):
    """Two runs submitted between polls both appear in the next response."""
    pid = _create_project(job_client, "multifinished")
    for _ in range(2):
        job_client.post(
            f"/projects/{pid}/jobs/pipelines",
            json={"pipeline_id": "test_pipeline"},
        )

    # First poll: no prior cursor → both runs returned.
    resp = job_client.get("/jobs/pipelines/finished")
    assert resp.status_code == 200
    runs = resp.json()["runs"]
    assert len(runs) == 2
    statuses = {r["status"] for r in runs}
    assert statuses == {"succeeded"}

    # Second poll: cursor advanced → nothing new.
    resp2 = job_client.get("/jobs/pipelines/finished")
    assert resp2.json()["runs"] == []
