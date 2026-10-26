"""
Tests for usage counting.

Two concerns:

* The dispatcher itself must be inert when unconfigured, must never raise
  whatever the endpoint does, and must stop trying after repeated failures.
  Nothing here may ever be able to fail a pipeline run or an upload.
* The hooks must actually fire, with the right event shapes, for pipeline
  submissions, tool executions, cache hits and uploads.

Hook tests stub ``usage_service._dispatch`` and assert on the payloads, which
keeps them free of async-timing flakiness — the dispatcher is covered directly
by the unit tests above.
"""

import asyncio

import pytest

from app.services import usage_service


def _create_project(client, name="usageproject"):
    resp = client.post("/projects", json={"name": name})
    assert resp.status_code == 201
    return name


@pytest.fixture
def reset_usage(monkeypatch):
    """Give each test a clean breaker and a disabled endpoint by default."""
    monkeypatch.setattr(usage_service, "_failures", 0)
    monkeypatch.setattr(usage_service, "_disabled", False)
    monkeypatch.setattr(usage_service, "USAGE_ENDPOINT_URL", "")
    yield


@pytest.fixture
def captured(monkeypatch):
    """Capture dispatched payloads instead of sending them."""
    events: list[dict] = []
    monkeypatch.setattr(usage_service, "_dispatch", events.append)
    return events


class _FakeResponse:
    def __init__(self, status_code=204):
        self.status_code = status_code


class _FakeClient:
    """Stand-in for httpx.AsyncClient, recording posts or raising."""

    posts: list[tuple[str, dict]] = []
    status = 204
    boom: Exception | None = None

    def __init__(self, *_a, **_kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def post(self, url, json=None):
        if _FakeClient.boom is not None:
            raise _FakeClient.boom
        _FakeClient.posts.append((url, json))
        return _FakeResponse(_FakeClient.status)


@pytest.fixture
def fake_http(monkeypatch):
    _FakeClient.posts = []
    _FakeClient.status = 204
    _FakeClient.boom = None
    monkeypatch.setattr(usage_service.httpx, "AsyncClient", _FakeClient)
    return _FakeClient


# ── The dispatcher ────────────────────────────────────────────────────────────

def test_disabled_when_no_endpoint_configured(reset_usage, fake_http):
    """The shipped default must reach out to nothing at all."""
    async def go():
        usage_service.record_pipeline_run("run_dlmuse", "cloud", subjects=5)
        await asyncio.sleep(0)

    asyncio.run(go())
    assert fake_http.posts == []


def test_posts_expected_payload(reset_usage, fake_http, monkeypatch):
    monkeypatch.setattr(usage_service, "USAGE_ENDPOINT_URL", "https://example.test/u")

    async def go():
        usage_service.record_pipeline_run("run_dlmuse", "cloud", subjects=42)
        await asyncio.sleep(0.05)

    asyncio.run(go())

    assert len(fake_http.posts) == 1
    url, body = fake_http.posts[0]
    assert url == "https://example.test/u"
    assert body == {
        "kind": "pipeline", "name": "run_dlmuse", "mode": "cloud",
        "event": "submitted", "subjects": 42,
    }


def test_transport_failure_is_swallowed(reset_usage, fake_http, monkeypatch):
    """A broken endpoint must not propagate anything to the caller."""
    monkeypatch.setattr(usage_service, "USAGE_ENDPOINT_URL", "https://example.test/u")
    fake_http.boom = RuntimeError("connection refused")

    async def go():
        usage_service.record_tool_run("nichart_dlmuse", "local")
        await asyncio.sleep(0.05)

    asyncio.run(go())   # must not raise
    assert usage_service._failures == 1


def test_breaker_opens_after_repeated_failures(reset_usage, fake_http, monkeypatch):
    monkeypatch.setattr(usage_service, "USAGE_ENDPOINT_URL", "https://example.test/u")
    fake_http.boom = RuntimeError("down")

    async def go():
        for _ in range(usage_service._MAX_CONSECUTIVE_FAILURES):
            usage_service.record_cache_hit("nichart_dlmuse", "local")
            await asyncio.sleep(0.02)

    asyncio.run(go())

    assert usage_service._disabled is True
    assert not usage_service._enabled()

    # Once open, nothing further is even attempted.
    fake_http.boom = None
    async def go_again():
        usage_service.record_cache_hit("nichart_dlmuse", "local")
        await asyncio.sleep(0.02)

    asyncio.run(go_again())
    assert fake_http.posts == []


def test_non_2xx_counts_as_failure(reset_usage, fake_http, monkeypatch):
    """A deleted function returns 403/404 rather than raising — still a failure."""
    monkeypatch.setattr(usage_service, "USAGE_ENDPOINT_URL", "https://example.test/u")
    fake_http.status = 403

    async def go():
        usage_service.record_upload("nifti", "cloud", files=1, total_bytes=10)
        await asyncio.sleep(0.05)

    asyncio.run(go())
    assert usage_service._failures == 1


def test_no_event_loop_is_a_noop(reset_usage, monkeypatch):
    """Called from sync context there is no loop to attach a task to."""
    monkeypatch.setattr(usage_service, "USAGE_ENDPOINT_URL", "https://example.test/u")
    usage_service.record_pipeline_run("run_dlmuse", "cloud")   # must not raise


def test_unknown_upload_kind_is_dropped(reset_usage, captured):
    usage_service.record_upload("exe", "cloud", files=1, total_bytes=1)
    assert captured == []
    usage_service.record_upload("nifti", "cloud", files=1, total_bytes=1)
    assert len(captured) == 1


def test_mode_is_normalised(reset_usage, captured):
    usage_service.record_tool_run("t", "cloud")
    usage_service.record_tool_run("t", "local")
    usage_service.record_tool_run("t", "something-else")
    assert [e["mode"] for e in captured] == ["cloud", "local", "local"]


def test_payloads_carry_no_identifying_data(reset_usage, captured):
    """The privacy guarantee, enforced rather than assumed."""
    usage_service.record_pipeline_run("run_dlmuse", "cloud", subjects=3)
    usage_service.record_tool_outcome("nichart_dlmuse", "cloud", True, chunks=4)
    usage_service.record_upload("dicom", "local", files=2, total_bytes=99)

    allowed = {"kind", "name", "mode", "event", "subjects", "chunks", "files", "bytes"}
    for event in captured:
        assert set(event) <= allowed, event


# ── The hooks ─────────────────────────────────────────────────────────────────

def test_pipeline_submission_is_counted(job_client, reset_usage, captured):
    pid = _create_project(job_client)
    resp = job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    )
    assert resp.status_code == 202

    kinds = [(e["kind"], e.get("event")) for e in captured]
    assert ("pipeline", "submitted") in kinds

    submitted = next(
        e for e in captured if e["kind"] == "pipeline" and e["event"] == "submitted"
    )
    assert submitted["name"] == "test_pipeline"


def test_tool_execution_and_pipeline_outcome_are_counted(job_client, reset_usage, captured):
    """MockBackend completes within the request, so the whole run is covered."""
    pid = _create_project(job_client)
    job_client.post(
        f"/projects/{pid}/jobs/pipelines",
        json={"pipeline_id": "test_pipeline"},
    )

    tool_runs = [e for e in captured if e["kind"] == "tool" and e["event"] == "submitted"]
    outcomes = [e for e in captured if e["kind"] == "pipeline" and e["event"] in ("succeeded", "failed")]

    assert tool_runs, f"no tool run counted: {captured}"
    assert tool_runs[0]["name"] == "test_sleep"
    assert outcomes, f"no pipeline outcome counted: {captured}"
    assert "chunks" in outcomes[0]


def test_upload_is_counted_with_files_and_bytes(job_client, reset_usage, captured):
    pid = _create_project(job_client)
    payload = b"MRID,Age,Sex\nsub1,70,M\n"
    resp = job_client.post(
        f"/projects/{pid}/files/upload/csv",
        files={"file": ("participants.csv", payload, "text/csv")},
    )
    assert resp.status_code in (200, 204)

    uploads = [e for e in captured if e["kind"] == "upload"]
    assert len(uploads) == 1
    assert uploads[0]["name"] == "csv"
    assert uploads[0]["files"] == 1
    assert uploads[0]["bytes"] == len(payload)
