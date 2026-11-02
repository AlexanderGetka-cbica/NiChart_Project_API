"""
S3 sync helpers for cloud-mode pipeline orchestration.

Implements an incremental sync (analogous to ``aws s3 sync``) using boto3
directly — no CLI dependency required inside the container.

Usage pattern in a pipeline run:
1. Before submitting each step: ``sync_to_s3()`` — upload local project files
   so the Batch job can pull them via its own S3 sync at container start.
2. After each step completes (success or failure): ``sync_from_s3()`` — download
   outputs that the Batch job wrote to S3 so the API server can serve them.

The _upload/ staging directory is excluded from sync; it contains uncommitted
upload staging files that are not inputs or outputs of any pipeline step.

Resilience
----------
A whole pipeline run used to die if any single HTTP request in any of these
syncs failed, which large projects hit regularly. Four layers now stand between
a dropped connection and a failed run, innermost first:

1. botocore retries each HTTP request (standard mode, 10 attempts).
2. ``_retry`` re-attempts one file's transfer.
3. ``_run_transfers`` re-attempts only the subset of files that still failed, so
   one bad file can never abort the thousands beside it.
4. ``_sync_with_retry`` re-runs the entire sync, which is the only layer that
   covers a failure during the listing phase. Re-running is cheap because the
   sync is incremental: a second pass re-lists and re-walks but transfers
   nothing it already transferred.

Ordering matters as much as retrying. The local filesystem walk happens *before*
the S3 listing, and per-file change detection happens inside the transfer
workers, so the first request goes out milliseconds after the listing finishes.
Previously a full recursive walk with a ``stat()`` per file sat between the two,
and on a large project that exceeded S3's ~20s idle timeout — the connection was
reaped and the first PUT died on a closed socket.
"""

import asyncio
import hashlib
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ConnectionError as BotoConnectionError
from botocore.exceptions import HTTPClientError

_log = logging.getLogger(__name__)

# Directories under the project root excluded from all sync operations.
_EXCLUDE_TOP_DIRS = frozenset({"_upload"})

# Suffix of the staging files that job_service._atomic_write_text creates beside
# the real file. They exist only for microseconds, but a sync that caught one
# would publish a transient staging file to S3, where FSx DRA would import it
# onto the compute nodes — and nothing would ever delete it again.
_EXCLUDE_SUFFIXES = frozenset({".tmp"})

# Concurrent transfers. Also keeps connections busy: a serial sync leaves pooled
# connections idle long enough for S3 to close them.
_MAX_WORKERS = 12

# A default boto3 client uses botocore's "legacy" retry mode, which gives up on a
# transfer after very few attempts and without adaptive backoff. The pool must be
# at least as large as the worker count or threads serialise on connections.
_S3_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "standard"},
    tcp_keepalive=True,
    max_pool_connections=_MAX_WORKERS + 4,
)

# Transport-level failures worth retrying outside botocore's own retry loop: the
# connection was closed or timed out before a response arrived, so the request
# never took effect and is safe to repeat. Covers ConnectionClosedError,
# ReadTimeoutError, ConnectTimeoutError and EndpointConnectionError.
_TRANSIENT_ERRORS = (HTTPClientError, BotoConnectionError)
_RETRY_ATTEMPTS = 3              # per individual file
_BATCH_PASSES = 2                # passes over the still-failing subset
_SYNC_ATTEMPTS = 2               # re-runs of the whole sync
_RETRY_BASE_DELAY_SECONDS = 1.0

# Files at or below this size are compared by content (local MD5 vs the object's
# ETag) instead of by size alone. Small metadata files — _provenance.json,
# DLMUSE_Volumes.csv, participants.csv — routinely change without changing
# length, and a size-only check would never re-transfer them. Larger files here
# are imaging volumes that are written once and never edited in place, so size
# remains a sufficient signal and hashing them would be pure overhead.
_CONTENT_COMPARE_MAX_BYTES = 1024 * 1024


def _client():
    """An S3 client configured for retry resilience.

    botocore clients are thread-safe, so one instance is shared by all transfer
    workers; ``max_pool_connections`` is sized for that concurrency.
    """
    return boto3.client("s3", config=_S3_CONFIG)


def _should_exclude(rel: str) -> bool:
    path = Path(rel)
    if path.parts and path.parts[0] in _EXCLUDE_TOP_DIRS:
        return True
    return path.suffix in _EXCLUDE_SUFFIXES


# ── Retry layers ──────────────────────────────────────────────────────────────

def _retry(description: str, fn, *args) -> None:
    """Call ``fn(*args)``, retrying transport-level failures with linear backoff.

    Re-raises the final exception once attempts are exhausted; the caller
    (``_run_transfers``) then decides whether to retry this file again.
    """
    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        try:
            fn(*args)
            return
        except _TRANSIENT_ERRORS as exc:
            if attempt == _RETRY_ATTEMPTS:
                raise
            delay = _RETRY_BASE_DELAY_SECONDS * attempt
            _log.warning(
                "s3 transfer failed (%s), attempt %d/%d, retrying in %.0fs: %s",
                description, attempt, _RETRY_ATTEMPTS, delay, exc,
            )
            time.sleep(delay)


def _run_transfers(kind: str, items: list[tuple], worker) -> int:
    """Run per-file transfers concurrently, retrying only what still fails.

    ``items`` are tuples whose first element is a display name. ``worker``
    receives one item and returns True if it actually transferred something,
    False if change detection decided the object was already current.

    A file that fails every pass is fatal — but only after every other file has
    been given its chance, so a single bad object no longer discards an entire
    project's worth of successful transfers.
    """
    remaining = list(items)
    transferred = 0
    failed: list[tuple] = []
    first_error: BaseException | None = None

    for pass_no in range(1, _BATCH_PASSES + 1):
        failed = []
        with ThreadPoolExecutor(
            max_workers=_MAX_WORKERS, thread_name_prefix=f"s3-{kind}"
        ) as pool:
            futures = {pool.submit(worker, item): item for item in remaining}
            for future in as_completed(futures):
                exc = future.exception()
                if exc is None:
                    if future.result():
                        transferred += 1
                    continue
                failed.append(futures[future])
                if first_error is None:
                    first_error = exc

        if not failed:
            return transferred

        if pass_no < _BATCH_PASSES:
            _log.warning(
                "s3-sync %s: %d file(s) failed on pass %d/%d, retrying those only",
                kind, len(failed), pass_no, _BATCH_PASSES,
            )
            time.sleep(_RETRY_BASE_DELAY_SECONDS * pass_no)
            remaining = failed

    examples = ", ".join(str(item[0]) for item in failed[:3])
    raise RuntimeError(
        f"s3-sync {kind}: {len(failed)} file(s) failed after {_BATCH_PASSES} "
        f"passes (e.g. {examples})"
    ) from first_error


async def _sync_with_retry(kind: str, fn, *args) -> int:
    """Run a full sync pass in a worker thread, retrying the whole operation.

    This is the only layer that covers a failure in the listing phase, which
    happens before any per-file retry can apply. Repeating a sync is cheap: it
    is incremental, so the second pass transfers only what the first missed.
    """
    last_error: BaseException | None = None
    for attempt in range(1, _SYNC_ATTEMPTS + 1):
        try:
            return await asyncio.to_thread(fn, *args)
        except Exception as exc:
            last_error = exc
            if attempt == _SYNC_ATTEMPTS:
                break
            delay = _RETRY_BASE_DELAY_SECONDS * attempt
            _log.warning(
                "s3-sync %s failed (attempt %d/%d), retrying whole sync in %.0fs: %s",
                kind, attempt, _SYNC_ATTEMPTS, delay, exc,
            )
            await asyncio.sleep(delay)
    assert last_error is not None
    raise last_error


# ── Change detection ──────────────────────────────────────────────────────────

def _md5_hex(path: Path) -> str | None:
    """MD5 of a local file, or None if it cannot be read."""
    try:
        digest = hashlib.md5()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return None


def _etag_md5(etag: str | None) -> str | None:
    """Return the MD5 hex an ETag encodes, or None when it isn't a plain MD5.

    Multipart uploads produce '<md5>-<partcount>' and SSE-KMS objects produce an
    opaque value; neither is comparable to a local file hash.
    """
    if not etag:
        return None
    value = etag.strip('"')
    if "-" in value or len(value) != 32:
        return None
    return value


def _differs(
    local_path: Path,
    local_size: int,
    remote_size: int | None,
    remote_etag: str | None,
) -> bool:
    """True when the local file and the S3 object are not known to be identical.

    Size is checked first because it is free. For small files a matching size is
    not conclusive, so the content is compared via the object's ETag. When the
    ETag is not a plain MD5 (multipart or SSE-KMS) there is nothing to compare
    against, so a size match is accepted rather than re-transferring blindly —
    the sync also carries _working/ state that must not be clobbered on a guess.
    """
    if remote_size is None or remote_size != local_size:
        return True
    if local_size > _CONTENT_COMPARE_MAX_BYTES:
        return False
    remote_md5 = _etag_md5(remote_etag)
    if remote_md5 is None:
        return False
    local_md5 = _md5_hex(local_path)
    if local_md5 is None:
        return True
    return local_md5 != remote_md5


# ── Listing / walking ─────────────────────────────────────────────────────────

def _walk_local_files(local_dir: Path) -> list[tuple[str, Path, int]]:
    """Collect (rel, path, size) for every syncable file under local_dir.

    Uses os.scandir and sorts per directory rather than materialising and
    sorting every path in the tree, and skips excluded directories without
    descending into them. Runs before any S3 client exists, so this pass — the
    slow one on Lustre, where metadata operations dominate — cannot leave a
    pooled connection idle long enough for S3 to reap it.
    """
    found: list[tuple[str, Path, int]] = []
    stack = [local_dir]
    while stack:
        current = stack.pop()
        try:
            entries = sorted(os.scandir(current), key=lambda e: e.name)
        except OSError:
            continue
        for entry in entries:
            path = Path(entry.path)
            rel = str(path.relative_to(local_dir))
            if _should_exclude(rel):
                continue
            try:
                # Recurse into real directories only — a symlinked directory
                # could loop. Symlinks to files are followed, matching the
                # previous rglob()/is_file() behaviour.
                if entry.is_dir(follow_symlinks=False):
                    stack.append(path)
                elif entry.is_file():
                    found.append((rel, path, entry.stat().st_size))
            except OSError:
                continue
    return found


def _list_remote(s3, bucket: str, prefix: str) -> list[dict]:
    """Every object under prefix, as returned by list_objects_v2."""
    objects: list[dict] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        objects.extend(page.get("Contents", []))
    return objects


# ── Sync passes ───────────────────────────────────────────────────────────────

def _upload_sync(local_dir: Path, bucket: str, prefix: str) -> int:
    """Upload new/changed local files to S3. Returns number of files uploaded."""
    started = time.monotonic()

    # Walk first (see module docstring): no connection is open yet.
    local_files = _walk_local_files(local_dir)
    if not local_files:
        return 0

    s3 = _client()
    existing: dict[str, tuple[int, str | None]] = {
        obj["Key"][len(prefix) + 1:]: (obj["Size"], obj.get("ETag"))
        for obj in _list_remote(s3, bucket, prefix)
    }

    def _upload_one(item: tuple[str, Path, int]) -> bool:
        rel, file_path, local_size = item
        remote_size, remote_etag = existing.get(rel, (None, None))
        # Change detection runs here, inside the worker, so the first PUT
        # follows the listing immediately instead of after a serial stat/hash
        # pass over the whole project.
        if not _differs(file_path, local_size, remote_size, remote_etag):
            return False
        key = f"{prefix}/{rel}"
        _log.info("s3-sync up: %s → s3://%s/%s", rel, bucket, key)
        _retry(f"upload {rel}", s3.upload_file, str(file_path), bucket, key)
        return True

    uploaded = _run_transfers("up", local_files, _upload_one)
    _log.debug(
        "s3-sync up: %d/%d file(s) transferred in %.1fs",
        uploaded, len(local_files), time.monotonic() - started,
    )
    return uploaded


def _download_sync(bucket: str, prefix: str, local_dir: Path) -> int:
    """Download new/changed S3 objects to local_dir. Returns number downloaded."""
    started = time.monotonic()

    s3 = _client()
    objects = _list_remote(s3, bucket, prefix)

    # FSx DRA auto-export represents directories as zero-byte objects at the
    # directory's key (no trailing slash), which collide with the real directory
    # on the local filesystem (download onto a dir → IsADirectoryError). Detect
    # these "directory marker" keys: any key that is an ancestor path of another
    # key is a directory, not a file, and must be skipped. Order-independent —
    # computed from the full key set, not local filesystem state.
    dir_keys: set[str] = set()
    for obj in objects:
        parts = obj["Key"].split("/")
        for i in range(1, len(parts)):
            dir_keys.add("/".join(parts[:i]))

    candidates: list[tuple[str, str, int, str | None]] = []
    for obj in objects:
        key = obj["Key"]
        rel = key[len(prefix) + 1:]
        if not rel:
            continue
        if _should_exclude(rel):
            continue
        if key.endswith("/") or key in dir_keys:
            continue  # directory marker, not a real file
        candidates.append((rel, key, obj["Size"], obj.get("ETag")))

    def _download_one(item: tuple[str, str, int, str | None]) -> bool:
        rel, key, remote_size, remote_etag = item
        local_file = local_dir / rel
        if local_file.is_dir():
            return False  # stray marker whose path is already a real directory
        if local_file.exists() and not _differs(
            local_file, local_file.stat().st_size, remote_size, remote_etag
        ):
            return False  # already up to date
        local_file.parent.mkdir(parents=True, exist_ok=True)
        _log.info("s3-sync down: s3://%s/%s → %s", bucket, key, rel)
        _retry(f"download {rel}", s3.download_file, bucket, key, str(local_file))
        return True

    downloaded = _run_transfers("down", candidates, _download_one)
    _log.debug(
        "s3-sync down: %d/%d object(s) transferred in %.1fs",
        downloaded, len(candidates), time.monotonic() - started,
    )
    return downloaded


def project_prefix(top_prefix: str, user_id: str, project_name: str) -> str:
    """Full S3 prefix for one user's project.

    The single definition of the remote layout, shared by the pipeline
    orchestrator and the upload routes so the two cannot disagree about where a
    project lives.
    """
    return f"{top_prefix}/{user_id}/{project_name}"


async def sync_to_s3(local_dir: Path, bucket: str, prefix: str) -> None:
    """Upload project files from local_dir to s3://bucket/prefix/. Non-blocking."""
    n = await _sync_with_retry("up", _upload_sync, local_dir, bucket, prefix)
    _log.info("s3-sync up complete: %d file(s) → s3://%s/%s/", n, bucket, prefix)


async def sync_from_s3(bucket: str, prefix: str, local_dir: Path) -> None:
    """Download S3 objects under s3://bucket/prefix/ to local_dir. Non-blocking."""
    n = await _sync_with_retry("down", _download_sync, bucket, prefix, local_dir)
    _log.info("s3-sync down complete: %d file(s) ← s3://%s/%s/", n, bucket, prefix)


def _delete_prefix_sync(bucket: str, prefix: str) -> int:
    """Delete every object under s3://bucket/prefix. Returns count deleted."""
    s3 = _client()
    paginator = s3.get_paginator("list_objects_v2")
    batch: list[dict] = []
    deleted = 0

    def _flush(keys: list[dict]) -> None:
        _retry(
            f"delete {len(keys)} keys",
            lambda: s3.delete_objects(Bucket=bucket, Delete={"Objects": keys}),
        )

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            batch.append({"Key": obj["Key"]})
            if len(batch) == 1000:  # delete_objects caps at 1000 keys per call
                _flush(batch)
                deleted += len(batch)
                batch = []
    if batch:
        _flush(batch)
        deleted += len(batch)
    return deleted


async def delete_s3_prefix(bucket: str, prefix: str) -> None:
    """Delete all objects under s3://bucket/prefix. Non-blocking.

    Pass a prefix ending in '/' to avoid matching sibling keys that merely share
    the same leading string.
    """
    n = await _sync_with_retry("delete", _delete_prefix_sync, bucket, prefix)
    _log.info("s3 delete complete: %d object(s) under s3://%s/%s", n, bucket, prefix)
