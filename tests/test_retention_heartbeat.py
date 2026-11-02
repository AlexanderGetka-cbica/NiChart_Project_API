"""
Tests for the retention heartbeat refresh on pipeline submission.

The sweep Lambda measures staleness from the heartbeat object alone, so a
project can be deleted while in active use unless something explicitly refreshes
it. Submitting a run now does.
"""

import asyncio

import pytest

from app.services import retention_service


def _create_project(client, name="retentionproj"):
    assert client.post("/projects", json={"name": name}).status_code == 201
    return name


# ── touch_project ────────────────────────────────────────────────────────────

def test_touch_project_is_a_noop_without_a_bucket(monkeypatch):
    """Local mode has no S3 at all; nothing must be attempted."""
    calls = []

    async def _spy(**kw):
        calls.append(kw)

    monkeypatch.setattr(retention_service, "write_heartbeat", _spy)
    asyncio.run(retention_service.touch_project(None, "fsx", "u", "p", "test"))
    assert calls == []


def test_touch_project_writes_the_heartbeat(monkeypatch):
    calls = []

    async def _spy(**kw):
        calls.append(kw)

    monkeypatch.setattr(retention_service, "write_heartbeat", _spy)
    asyncio.run(retention_service.touch_project("buck", "fsx", "user-1", "proj", "test"))

    assert calls == [
        {"bucket": "buck", "prefix": "fsx", "user_id": "user-1", "project_id": "proj"}
    ]


def test_touch_project_swallows_failures(monkeypatch):
    """A broken heartbeat must never fail the action that triggered it."""
    async def _boom(**_kw):
        raise RuntimeError("S3 unavailable")

    monkeypatch.setattr(retention_service, "write_heartbeat", _boom)
    asyncio.run(retention_service.touch_project("buck", "fsx", "u", "p", "test"))


# ── wired into submission ────────────────────────────────────────────────────

def test_pipeline_submission_refreshes_retention(job_client, monkeypatch):
    calls = []

    async def _spy(bucket, prefix, user_id, project_id, activity):
        calls.append((project_id, activity))

    monkeypatch.setattr(retention_service, "touch_project", _spy)
    pid = _create_project(job_client, "retentionsubmit")
    resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline", "params": {"duration_seconds": 1}},
    )

    assert resp.status_code == 202
    assert calls == [(pid, "pipeline submission")]


def test_rejected_submission_does_not_refresh_retention(job_client, monkeypatch):
    """An invalid request must not keep a project alive."""
    calls = []

    async def _spy(*_a, **_kw):
        calls.append(1)

    monkeypatch.setattr(retention_service, "touch_project", _spy)
    pid = _create_project(job_client, "retentionreject")
    resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "no_such_pipeline"},
    )

    assert resp.status_code == 404
    assert calls == []


def test_submission_survives_a_heartbeat_failure(job_client, monkeypatch):
    async def _boom(**_kw):
        raise RuntimeError("S3 unavailable")

    monkeypatch.setattr(retention_service, "write_heartbeat", _boom)
    pid = _create_project(job_client, "retentionbroken")
    resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline", "params": {"duration_seconds": 1}},
    )
    assert resp.status_code == 202
