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

Transfers are retried at the transport layer (see ``_S3_CONFIG`` and ``_retry``)
because a single closed connection during pre-sync would otherwise abort a whole
pipeline run before its compute job is even submitted. Change detection compares
content for small files rather than size alone — see ``_differs``.
"""

import asyncio
import hashlib
import logging
import time
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

# A default boto3 client uses botocore's "legacy" retry mode, which gives up on a
# transfer after very few attempts and without adaptive backoff. tcp_keepalive
# stops pooled connections from going stale while a long list_objects_v2
# pagination runs — the first PUT after listing is otherwise the likeliest
# casualty of a proxy or NAT idle timeout.
_S3_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "standard"},
    tcp_keepalive=True,
    max_pool_connections=10,
)

# Transport-level failures worth retrying outside botocore's own retry loop: the
# connection was closed or timed out before a response arrived, so the request
# never took effect and is safe to repeat. Covers ConnectionClosedError,
# ReadTimeoutError, ConnectTimeoutError and EndpointConnectionError.
_TRANSIENT_ERRORS = (HTTPClientError, BotoConnectionError)
_RETRY_ATTEMPTS = 3
_RETRY_BASE_DELAY_SECONDS = 1.0

# Files at or below this size are compared by content (local MD5 vs the object's
# ETag) instead of by size alone. Small metadata files — _provenance.json,
# DLMUSE_Volumes.csv, participants.csv — routinely change without changing
# length, and a size-only check would never re-transfer them. Larger files here
# are imaging volumes that are written once and never edited in place, so size
# remains a sufficient signal and hashing them would be pure overhead.
_CONTENT_COMPARE_MAX_BYTES = 1024 * 1024


def _client():
    """An S3 client configured for retry resilience."""
    return boto3.client("s3", config=_S3_CONFIG)


def _should_exclude(rel: str) -> bool:
    path = Path(rel)
    if path.parts and path.parts[0] in _EXCLUDE_TOP_DIRS:
        return True
    return path.suffix in _EXCLUDE_SUFFIXES


def _retry(description: str, fn, *args) -> None:
    """Call ``fn(*args)``, retrying transport-level failures with linear backoff.

    Re-raises the final exception once attempts are exhausted, so callers still
    see a hard failure when the endpoint is genuinely unreachable.
    """
    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        try:
            fn(*args)
            return
        except _TRANSIENT_ERRORS as exc:
            if attempt == _RETRY_ATTEMPTS:
                _log.error(
                    "s3 transfer failed permanently (%s) after %d attempts: %s",
                    description, _RETRY_ATTEMPTS, exc,
                )
                raise
            delay = _RETRY_BASE_DELAY_SECONDS * attempt
            _log.warning(
                "s3 transfer failed (%s), attempt %d/%d, retrying in %.0fs: %s",
                description, attempt, _RETRY_ATTEMPTS, delay, exc,
            )
            time.sleep(delay)


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


def _upload_sync(local_dir: Path, bucket: str, prefix: str) -> int:
    """Upload new/changed local files to S3. Returns number of files uploaded."""
    s3 = _client()

    # Index existing S3 objects by relative key → (size, etag) for change detection.
    existing: dict[str, tuple[int, str | None]] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            rel_key = obj["Key"][len(prefix) + 1:]
            existing[rel_key] = (obj["Size"], obj.get("ETag"))

    uploaded = 0
    for file_path in sorted(local_dir.rglob("*")):
        if not file_path.is_file():
            continue
        rel = str(file_path.relative_to(local_dir))
        if _should_exclude(rel):
            continue

        local_size = file_path.stat().st_size
        remote_size, remote_etag = existing.get(rel, (None, None))
        if not _differs(file_path, local_size, remote_size, remote_etag):
            continue  # already up to date

        key = f"{prefix}/{rel}"
        _log.info("s3-sync up: %s → s3://%s/%s", rel, bucket, key)
        _retry(f"upload {rel}", s3.upload_file, str(file_path), bucket, key)
        uploaded += 1

    return uploaded


def _download_sync(bucket: str, prefix: str, local_dir: Path) -> int:
    """Download new/changed S3 objects to local_dir. Returns number downloaded."""
    s3 = _client()

    paginator = s3.get_paginator("list_objects_v2")
    objects: list[dict] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        objects.extend(page.get("Contents", []))

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

    downloaded = 0
    for obj in objects:
        key = obj["Key"]
        rel = key[len(prefix) + 1:]
        if not rel:
            continue
        if _should_exclude(rel):
            continue
        if key.endswith("/") or key in dir_keys:
            continue  # directory marker, not a real file

        local_file = local_dir / rel
        if local_file.is_dir():
            continue  # stray marker whose path is already a real directory
        if local_file.exists() and not _differs(
            local_file, local_file.stat().st_size, obj["Size"], obj.get("ETag")
        ):
            continue  # already up to date

        local_file.parent.mkdir(parents=True, exist_ok=True)
        _log.info("s3-sync down: s3://%s/%s → %s", bucket, key, rel)
        _retry(f"download {rel}", s3.download_file, bucket, key, str(local_file))
        downloaded += 1

    return downloaded


async def sync_to_s3(local_dir: Path, bucket: str, prefix: str) -> None:
    """Upload project files from local_dir to s3://bucket/prefix/. Non-blocking."""
    n = await asyncio.to_thread(_upload_sync, local_dir, bucket, prefix)
    _log.info("s3-sync up complete: %d file(s) → s3://%s/%s/", n, bucket, prefix)


async def sync_from_s3(bucket: str, prefix: str, local_dir: Path) -> None:
    """Download S3 objects under s3://bucket/prefix/ to local_dir. Non-blocking."""
    n = await asyncio.to_thread(_download_sync, bucket, prefix, local_dir)
    _log.info("s3-sync down complete: %d file(s) ← s3://%s/%s/", n, bucket, prefix)


def _delete_prefix_sync(bucket: str, prefix: str) -> int:
    """Delete every object under s3://bucket/prefix. Returns count deleted."""
    s3 = _client()
    paginator = s3.get_paginator("list_objects_v2")
    batch: list[dict] = []
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            batch.append({"Key": obj["Key"]})
            if len(batch) == 1000:  # delete_objects caps at 1000 keys per call
                _retry(
                    f"delete {len(batch)} keys",
                    lambda b=batch: s3.delete_objects(Bucket=bucket, Delete={"Objects": b}),
                )
                deleted += len(batch)
                batch = []
    if batch:
        _retry(
            f"delete {len(batch)} keys",
            lambda b=batch: s3.delete_objects(Bucket=bucket, Delete={"Objects": b}),
        )
        deleted += len(batch)
    return deleted


async def delete_s3_prefix(bucket: str, prefix: str) -> None:
    """Delete all objects under s3://bucket/prefix. Non-blocking.

    Pass a prefix ending in '/' to avoid matching sibling keys that merely share
    the same leading string.
    """
    n = await asyncio.to_thread(_delete_prefix_sync, bucket, prefix)
    _log.info("s3 delete complete: %d object(s) under s3://%s/%s", n, bucket, prefix)
