"""
Tests for the S3 sync helpers.

Three behaviours are covered:

* The download path must be resilient to FSx DRA "directory marker" objects —
  zero-byte S3 objects at a directory's key (no trailing slash) that collide with
  the real directory of the same name on the local filesystem. Downloading one
  onto a local directory raises IsADirectoryError, which previously crashed the
  pipeline background task.
* Transport-level failures (a closed connection mid-transfer) must be retried
  rather than aborting the whole pipeline run.
* Change detection must notice a small file whose content changed but whose size
  did not — _provenance.json is a fixed-schema JSON that frequently keeps its
  length across runs.
"""

import hashlib
from pathlib import Path

import pytest
from botocore.exceptions import ConnectionClosedError

from app.services import s3_sync_service


def _etag(data: bytes) -> str:
    """The ETag S3 reports for a single-part upload of this content."""
    return '"' + hashlib.md5(data).hexdigest() + '"'


class _Paginator:
    def __init__(self, contents):
        self._contents = contents

    def paginate(self, Bucket, Prefix):
        yield {"Contents": self._contents}


class _FakeS3:
    def __init__(self, contents, fail_times: int = 0):
        self._contents = contents
        self._fail_times = fail_times
        self.downloaded: list[str] = []
        self.uploaded: list[str] = []
        self.upload_attempts = 0
        self.download_attempts = 0

    def get_paginator(self, name):
        return _Paginator(self._contents)

    def _maybe_fail(self, attempts: int, bucket: str, key: str) -> None:
        if attempts <= self._fail_times:
            raise ConnectionClosedError(
                endpoint_url=f"https://{bucket}.s3.amazonaws.com/{key}"
            )

    def upload_file(self, path, bucket, key):
        self.upload_attempts += 1
        self._maybe_fail(self.upload_attempts, bucket, key)
        self.uploaded.append(key)

    def download_file(self, bucket, key, path):
        self.download_attempts += 1
        self._maybe_fail(self.download_attempts, bucket, key)
        self.downloaded.append(key)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text("x")


@pytest.fixture
def no_sleep(monkeypatch):
    """Collapse retry backoff so tests don't actually wait."""
    monkeypatch.setattr(s3_sync_service.time, "sleep", lambda _s: None)


def _patch_client(monkeypatch, fake):
    # _client() passes config=..., so the stub must tolerate keyword arguments.
    monkeypatch.setattr(s3_sync_service.boto3, "client", lambda svc, **kw: fake)


# ── Directory-marker handling ─────────────────────────────────────────────────

def test_download_sync_skips_directory_markers(tmp_path, monkeypatch):
    prefix = "fsx/U/proj"
    markers = {f"{prefix}/dlmuse_seg", f"{prefix}/dlmuse_vol"}
    keys = [
        f"{prefix}/t1/sub1.nii.gz",
        f"{prefix}/dlmuse_seg",                    # directory marker
        f"{prefix}/dlmuse_seg/sub1_seg.nii.gz",
        f"{prefix}/dlmuse_vol",                    # directory marker
        f"{prefix}/dlmuse_vol/vol.csv",
    ]
    contents = [{"Key": k, "Size": 0 if k in markers else 10} for k in keys]
    fake = _FakeS3(contents)
    _patch_client(monkeypatch, fake)

    n = s3_sync_service._download_sync("bucket", prefix, tmp_path)

    # Markers never downloaded (no IsADirectoryError), real files all pulled.
    assert markers.isdisjoint(fake.downloaded)
    assert f"{prefix}/dlmuse_seg/sub1_seg.nii.gz" in fake.downloaded
    assert f"{prefix}/dlmuse_vol/vol.csv" in fake.downloaded
    assert f"{prefix}/t1/sub1.nii.gz" in fake.downloaded
    assert n == 3


def test_download_sync_skips_object_when_path_is_existing_dir(tmp_path, monkeypatch):
    """Belt-and-suspenders: a stray marker whose local path is already a dir is skipped."""
    prefix = "fsx/U/proj"
    # Only the marker is listed (no sibling file to trigger the dir_keys heuristic),
    # but the local path already exists as a directory.
    (tmp_path / "dlmuse_seg").mkdir()
    contents = [{"Key": f"{prefix}/dlmuse_seg", "Size": 0}]
    fake = _FakeS3(contents)
    _patch_client(monkeypatch, fake)

    n = s3_sync_service._download_sync("bucket", prefix, tmp_path)

    assert fake.downloaded == []
    assert n == 0


# ── Content-based change detection ────────────────────────────────────────────

def test_upload_sync_reuploads_same_size_changed_content(tmp_path, monkeypatch):
    """A small file whose content changed but size did not must still upload."""
    prefix = "fsx/U/proj"
    local = b'{"step": "a"}'
    remote = b'{"step": "b"}'          # same length, different bytes
    assert len(local) == len(remote)

    (tmp_path / "dlmuse_vol").mkdir()
    (tmp_path / "dlmuse_vol" / "_provenance.json").write_bytes(local)
    contents = [{
        "Key": f"{prefix}/dlmuse_vol/_provenance.json",
        "Size": len(remote),
        "ETag": _etag(remote),
    }]
    fake = _FakeS3(contents)
    _patch_client(monkeypatch, fake)

    n = s3_sync_service._upload_sync(tmp_path, "bucket", prefix)

    assert fake.uploaded == [f"{prefix}/dlmuse_vol/_provenance.json"]
    assert n == 1


def test_upload_sync_skips_identical_content(tmp_path, monkeypatch):
    """Matching size and ETag means the object is already current."""
    prefix = "fsx/U/proj"
    data = b'{"step": "a"}'

    (tmp_path / "dlmuse_vol").mkdir()
    (tmp_path / "dlmuse_vol" / "_provenance.json").write_bytes(data)
    contents = [{
        "Key": f"{prefix}/dlmuse_vol/_provenance.json",
        "Size": len(data),
        "ETag": _etag(data),
    }]
    fake = _FakeS3(contents)
    _patch_client(monkeypatch, fake)

    n = s3_sync_service._upload_sync(tmp_path, "bucket", prefix)

    assert fake.uploaded == []
    assert n == 0


def test_upload_sync_trusts_size_for_large_files(tmp_path, monkeypatch):
    """Files above the content-compare threshold are not hashed; size alone decides."""
    prefix = "fsx/U/proj"
    big = b"\0" * (s3_sync_service._CONTENT_COMPARE_MAX_BYTES + 1)

    (tmp_path / "t1").mkdir()
    (tmp_path / "t1" / "sub1.nii.gz").write_bytes(big)
    contents = [{
        "Key": f"{prefix}/t1/sub1.nii.gz",
        "Size": len(big),
        "ETag": _etag(b"completely different"),   # would mismatch if hashed
    }]
    fake = _FakeS3(contents)
    _patch_client(monkeypatch, fake)

    n = s3_sync_service._upload_sync(tmp_path, "bucket", prefix)

    assert fake.uploaded == []
    assert n == 0


def test_upload_sync_falls_back_to_size_for_multipart_etag(tmp_path, monkeypatch):
    """A multipart/SSE-KMS ETag isn't an MD5, so a size match is accepted."""
    prefix = "fsx/U/proj"
    data = b'{"step": "a"}'

    (tmp_path / "dlmuse_vol").mkdir()
    (tmp_path / "dlmuse_vol" / "_provenance.json").write_bytes(data)
    contents = [{
        "Key": f"{prefix}/dlmuse_vol/_provenance.json",
        "Size": len(data),
        "ETag": '"d41d8cd98f00b204e9800998ecf8427e-3"',   # multipart form
    }]
    fake = _FakeS3(contents)
    _patch_client(monkeypatch, fake)

    n = s3_sync_service._upload_sync(tmp_path, "bucket", prefix)

    assert fake.uploaded == []
    assert n == 0


# ── Transport retry ───────────────────────────────────────────────────────────

def test_upload_retries_closed_connection(tmp_path, monkeypatch, no_sleep):
    """A transient ConnectionClosedError must not fail the sync."""
    prefix = "fsx/U/proj"
    (tmp_path / "dlmuse_vol").mkdir()
    (tmp_path / "dlmuse_vol" / "_provenance.json").write_bytes(b"{}")

    fake = _FakeS3([], fail_times=1)   # first PUT dies, second succeeds
    _patch_client(monkeypatch, fake)

    n = s3_sync_service._upload_sync(tmp_path, "bucket", prefix)

    assert fake.upload_attempts == 2
    assert fake.uploaded == [f"{prefix}/dlmuse_vol/_provenance.json"]
    assert n == 1


def test_download_retries_closed_connection(tmp_path, monkeypatch, no_sleep):
    prefix = "fsx/U/proj"
    contents = [{"Key": f"{prefix}/t1/sub1.nii.gz", "Size": 10}]
    fake = _FakeS3(contents, fail_times=1)
    _patch_client(monkeypatch, fake)

    n = s3_sync_service._download_sync("bucket", prefix, tmp_path)

    assert fake.download_attempts == 2
    assert n == 1


def test_upload_reraises_after_exhausting_retries(tmp_path, monkeypatch, no_sleep):
    """A persistently unreachable endpoint still surfaces as a hard failure."""
    prefix = "fsx/U/proj"
    (tmp_path / "dlmuse_vol").mkdir()
    (tmp_path / "dlmuse_vol" / "_provenance.json").write_bytes(b"{}")

    fake = _FakeS3([], fail_times=99)
    _patch_client(monkeypatch, fake)

    with pytest.raises(ConnectionClosedError):
        s3_sync_service._upload_sync(tmp_path, "bucket", prefix)

    assert fake.upload_attempts == s3_sync_service._RETRY_ATTEMPTS


# ── Staging-file exclusion ────────────────────────────────────────────────────

def test_should_exclude_covers_upload_dir_and_temp_files():
    assert s3_sync_service._should_exclude("_upload/nifti/sub1.nii.gz")
    assert s3_sync_service._should_exclude("_working/metadata.json.a1b2c3d4.tmp")
    assert s3_sync_service._should_exclude("dlmuse_vol/_provenance.json.deadbeef.tmp")
    # Real files are not excluded.
    assert not s3_sync_service._should_exclude("dlmuse_vol/_provenance.json")
    assert not s3_sync_service._should_exclude("_working/metadata.json")
    assert not s3_sync_service._should_exclude("t1/sub1.nii.gz")


def test_upload_sync_never_publishes_staging_files(tmp_path, monkeypatch):
    """An atomic-write temp file caught mid-flight must not reach S3.

    If one did, FSx DRA would import it onto the compute nodes and nothing would
    ever remove it.
    """
    prefix = "fsx/U/proj"
    (tmp_path / "dlmuse_vol").mkdir()
    (tmp_path / "dlmuse_vol" / "_provenance.json").write_bytes(b"{}")
    (tmp_path / "dlmuse_vol" / "_provenance.json.a1b2c3d4.tmp").write_bytes(b"partial")

    fake = _FakeS3([])
    _patch_client(monkeypatch, fake)

    n = s3_sync_service._upload_sync(tmp_path, "bucket", prefix)

    assert fake.uploaded == [f"{prefix}/dlmuse_vol/_provenance.json"]
    assert n == 1
