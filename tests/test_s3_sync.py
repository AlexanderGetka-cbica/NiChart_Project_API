"""
Tests for the S3 sync helpers.

Covered behaviours:

* The download path must be resilient to FSx DRA "directory marker" objects —
  zero-byte S3 objects at a directory's key (no trailing slash) that collide with
  the real directory of the same name on the local filesystem. Downloading one
  onto a local directory raises IsADirectoryError, which previously crashed the
  pipeline background task.
* Change detection must notice a small file whose content changed but whose size
  did not — _provenance.json is a fixed-schema JSON that frequently keeps its
  length across runs.
* Transport failures must be survivable at three levels: one file's transfer,
  the subset of files still failing after a pass, and the whole sync (which is
  the only level that covers a failure during the S3 listing).
* Transfers run on a worker pool, and staging files never reach S3.
"""

import asyncio
import hashlib
import threading
from pathlib import Path

import pytest
from botocore.exceptions import ConnectionClosedError

from app.services import s3_sync_service


def _etag(data: bytes) -> str:
    """The ETag S3 reports for a single-part upload of this content."""
    return '"' + hashlib.md5(data).hexdigest() + '"'


class _Paginator:
    def __init__(self, owner):
        self._owner = owner

    def paginate(self, Bucket, Prefix):
        self._owner.list_calls += 1
        if self._owner.list_calls <= self._owner.list_fail_times:
            raise ConnectionClosedError(
                endpoint_url=f"https://{Bucket}.s3.amazonaws.com/{Prefix}"
            )
        yield {"Contents": self._owner.contents}


class _FakeS3:
    def __init__(self, contents, fail_times: int = 0, list_fail_times: int = 0):
        self.contents = contents
        self.list_fail_times = list_fail_times
        self._fail_times = fail_times
        self.list_calls = 0
        self.downloaded: list[str] = []
        self.uploaded: list[str] = []
        self.upload_attempts = 0
        self.download_attempts = 0
        self.threads: set[str] = set()
        self._lock = threading.Lock()

    def get_paginator(self, name):
        return _Paginator(self)

    def _record(self, which: str, bucket: str, key: str) -> None:
        with self._lock:
            self.threads.add(threading.current_thread().name)
            if which == "up":
                self.upload_attempts += 1
                attempts = self.upload_attempts
            else:
                self.download_attempts += 1
                attempts = self.download_attempts
        if attempts <= self._fail_times:
            raise ConnectionClosedError(
                endpoint_url=f"https://{bucket}.s3.amazonaws.com/{key}"
            )

    def upload_file(self, path, bucket, key):
        self._record("up", bucket, key)
        with self._lock:
            self.uploaded.append(key)

    def download_file(self, bucket, key, path):
        self._record("down", bucket, key)
        with self._lock:
            self.downloaded.append(key)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text("x")


class _AlwaysFailsOneKey(_FakeS3):
    """Fails forever for one specific key, succeeds for everything else."""

    def __init__(self, contents, doomed_key: str):
        super().__init__(contents)
        self._doomed = doomed_key

    def upload_file(self, path, bucket, key):
        with self._lock:
            self.upload_attempts += 1
        if key.endswith(self._doomed):
            raise ConnectionClosedError(
                endpoint_url=f"https://{bucket}.s3.amazonaws.com/{key}"
            )
        with self._lock:
            self.uploaded.append(key)


@pytest.fixture
def no_sleep(monkeypatch):
    """Collapse retry backoff (sync and async) so tests don't actually wait."""
    monkeypatch.setattr(s3_sync_service.time, "sleep", lambda _s: None)

    async def _instant(_s):
        return None

    monkeypatch.setattr(s3_sync_service.asyncio, "sleep", _instant)


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


# ── Local walk ────────────────────────────────────────────────────────────────

def test_walk_local_files_excludes_staging_without_descending(tmp_path):
    (tmp_path / "_upload" / "nifti").mkdir(parents=True)
    (tmp_path / "_upload" / "nifti" / "staged.nii.gz").write_bytes(b"x")
    (tmp_path / "t1").mkdir()
    (tmp_path / "t1" / "sub1.nii.gz").write_bytes(b"abc")
    (tmp_path / "t1" / "sub1.nii.gz.a1b2c3d4.tmp").write_bytes(b"partial")

    found = {rel for rel, _path, _size in s3_sync_service._walk_local_files(tmp_path)}

    assert found == {str(Path("t1") / "sub1.nii.gz")}


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


# ── Transport retry: single file ──────────────────────────────────────────────

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
    """A persistently unreachable endpoint still surfaces as a hard failure.

    Each file is retried _RETRY_ATTEMPTS times, and the still-failing subset is
    retried over _BATCH_PASSES passes, after which the batch reports a
    summarising error chained from the original transport exception.
    """
    prefix = "fsx/U/proj"
    (tmp_path / "dlmuse_vol").mkdir()
    (tmp_path / "dlmuse_vol" / "_provenance.json").write_bytes(b"{}")

    fake = _FakeS3([], fail_times=99)
    _patch_client(monkeypatch, fake)

    with pytest.raises(RuntimeError) as err:
        s3_sync_service._upload_sync(tmp_path, "bucket", prefix)

    assert "_provenance.json" in str(err.value)
    assert isinstance(err.value.__cause__, ConnectionClosedError)
    assert fake.upload_attempts == (
        s3_sync_service._RETRY_ATTEMPTS * s3_sync_service._BATCH_PASSES
    )


# ── Transport retry: one bad file must not discard the rest ───────────────────

def test_upload_transfers_every_other_file_despite_one_failure(
    tmp_path, monkeypatch, no_sleep
):
    """The pre-fix behaviour aborted the whole sync on the first exception."""
    prefix = "fsx/U/proj"
    (tmp_path / "t1").mkdir()
    for i in range(6):
        (tmp_path / "t1" / f"sub{i}.nii.gz").write_bytes(b"data")
    (tmp_path / "t1" / "doomed.nii.gz").write_bytes(b"data")

    fake = _AlwaysFailsOneKey([], "doomed.nii.gz")
    _patch_client(monkeypatch, fake)

    with pytest.raises(RuntimeError) as err:
        s3_sync_service._upload_sync(tmp_path, "bucket", prefix)

    # Every healthy file still made it, and the error names the bad one.
    assert len(fake.uploaded) == 6
    assert all("doomed" not in key for key in fake.uploaded)
    assert "doomed.nii.gz" in str(err.value)


# ── Transport retry: whole sync (covers the listing phase) ────────────────────

def test_sync_to_s3_retries_whole_sync_when_listing_fails(
    tmp_path, monkeypatch, no_sleep
):
    """A failure during pagination is invisible to per-file retry.

    Only the sync-level retry can recover it, so sync_to_s3 must re-run the
    whole pass rather than propagate.
    """
    prefix = "fsx/U/proj"
    (tmp_path / "dlmuse_vol").mkdir()
    (tmp_path / "dlmuse_vol" / "_provenance.json").write_bytes(b"{}")

    fake = _FakeS3([], list_fail_times=1)   # first listing dies
    _patch_client(monkeypatch, fake)

    asyncio.run(s3_sync_service.sync_to_s3(tmp_path, "bucket", prefix))

    assert fake.list_calls == 2   # failed once, retried, succeeded
    assert fake.uploaded == [f"{prefix}/dlmuse_vol/_provenance.json"]


def test_sync_to_s3_raises_when_listing_never_recovers(tmp_path, monkeypatch, no_sleep):
    prefix = "fsx/U/proj"
    (tmp_path / "t1").mkdir()
    (tmp_path / "t1" / "sub1.nii.gz").write_bytes(b"x")

    fake = _FakeS3([], list_fail_times=99)
    _patch_client(monkeypatch, fake)

    with pytest.raises(ConnectionClosedError):
        asyncio.run(s3_sync_service.sync_to_s3(tmp_path, "bucket", prefix))

    assert fake.list_calls == s3_sync_service._SYNC_ATTEMPTS


# ── Concurrency ───────────────────────────────────────────────────────────────

def test_transfers_run_on_the_worker_pool(tmp_path, monkeypatch):
    """Transfers must be dispatched to the pool, not run on the calling thread."""
    prefix = "fsx/U/proj"
    (tmp_path / "t1").mkdir()
    for i in range(8):
        (tmp_path / "t1" / f"sub{i}.nii.gz").write_bytes(b"data")

    fake = _FakeS3([])
    _patch_client(monkeypatch, fake)

    s3_sync_service._upload_sync(tmp_path, "bucket", prefix)

    assert fake.threads, "no transfers recorded"
    assert all(name.startswith("s3-up") for name in fake.threads)
    assert threading.current_thread().name not in fake.threads
