"""
Usage counters — best-effort, fire-and-forget telemetry.

Posts one small JSON event per interesting thing (a pipeline submitted, a tool
executed, data uploaded) to the public ``cbica-nichart-usage-counter`` Lambda,
which increments atomic counters in DynamoDB. See
``scripts/reference/cbica-nichart-usage-counter.py`` for the receiving end and
the table layout.

Design constraints, in order of importance:

1. **Nothing here may ever surface an error.** Every public function returns
   None, swallows everything, and is safe to call from any code path. A failure
   to count must never fail a pipeline run or an upload.
2. **Nothing here may ever block a request.** Events are dispatched as
   fire-and-forget tasks; the caller does not await the HTTP round trip.
3. **At-most-once.** No retries. A dropped count is cheaper than a double count
   or a retry storm, and the data is aggregate anyway.
4. **No credentials.** The endpoint is unauthenticated precisely so that desktop
   and on-prem installs — which have no AWS credentials — still report usage.
5. **No identifying data, ever.** Only pipeline/tool IDs from our own catalog,
   the five upload type names, and aggregate counts. No user IDs, project names,
   filenames or MRIDs. This must not become a PHI store, and the receiving
   Lambda independently enforces the same allowlist.
"""

import asyncio
import logging

import httpx

_log = logging.getLogger(__name__)

# Function URL of the deployed cbica-nichart-usage-counter Lambda.
#
# Hardcoded rather than read from the environment on purpose: the desktop build
# ships this server, and an environment variable would mean desktop and on-prem
# installs silently never report anything. An empty string disables all counting
# — which is also what keeps tests and local development from reaching out.
USAGE_ENDPOINT_URL = "https://2dhwnzrg57h7d5nxes4vvtpsmm0ttjqo.lambda-url.us-east-1.on.aws/"

# The caller has already moved on, so a slow endpoint must not keep a task alive.
_TIMEOUT = httpx.Timeout(2.0, connect=1.0)

# After this many consecutive failures, stop trying for the lifetime of the
# process. Without it, a deleted function or revoked permission would mean one
# doomed task and one log line for every event, forever.
_MAX_CONSECUTIVE_FAILURES = 5

_failures = 0
_disabled = False

# Strong references to in-flight tasks. asyncio only holds weak references to
# tasks, so without this a dispatched event can be garbage-collected mid-flight.
_pending: set[asyncio.Task] = set()

_UPLOAD_KINDS = frozenset({"nifti", "csv", "bids", "idat", "dicom"})


def _enabled() -> bool:
    return bool(USAGE_ENDPOINT_URL) and not _disabled


def _dispatch(payload: dict) -> None:
    """Schedule one event. Returns immediately; never raises."""
    if not _enabled():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return  # called outside an event loop — nothing to attach a task to
    try:
        task = loop.create_task(_post(payload))
        _pending.add(task)
        task.add_done_callback(_pending.discard)
    except Exception:  # pragma: no cover - defensive
        return


async def _post(payload: dict) -> None:
    """Send one event. Absorbs every failure; counts them toward the breaker."""
    global _failures, _disabled
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            response = await client.post(USAGE_ENDPOINT_URL, json=payload)
        # A non-2xx is not an httpx exception, but it still means the endpoint
        # isn't accepting events — so it counts toward the breaker. Otherwise a
        # deleted function would never trip it.
        if response.status_code >= 300:
            raise RuntimeError(f"usage endpoint returned {response.status_code}")
        _failures = 0
    except Exception as exc:
        _failures += 1
        # Only log on the transition. Several events are usually in flight at
        # once, so without the `not _disabled` guard every one of them that
        # lands after the threshold logs its own warning.
        if _failures >= _MAX_CONSECUTIVE_FAILURES and not _disabled:
            _disabled = True
            _log.warning(
                "Usage counting disabled for this process after %d consecutive "
                "failures; last error: %s",
                _failures, exc,
            )
        else:
            _log.debug("Usage event dropped: %s", exc)


def _mode(execution_mode: str) -> str:
    """Normalise to the two values the endpoint accepts."""
    return "cloud" if execution_mode == "cloud" else "local"


# ── Pipelines ─────────────────────────────────────────────────────────────────

def record_pipeline_run(
    pipeline_id: str, execution_mode: str, subjects: int = 0
) -> None:
    """A pipeline was submitted. ``subjects`` is the project's subject count."""
    _dispatch({
        "kind": "pipeline",
        "name": pipeline_id,
        "mode": _mode(execution_mode),
        "event": "submitted",
        "subjects": max(0, int(subjects or 0)),
    })


def record_pipeline_outcome(
    pipeline_id: str, execution_mode: str, succeeded: bool, chunks: int = 0
) -> None:
    """A pipeline reached a terminal state.

    ``chunks`` is the total parallel chunks the run submitted — only knowable
    now, which is why the receiving end accepts it on this event rather than at
    submission.
    """
    _dispatch({
        "kind": "pipeline",
        "name": pipeline_id,
        "mode": _mode(execution_mode),
        "event": "succeeded" if succeeded else "failed",
        "chunks": max(0, int(chunks or 0)),
    })


# ── Tools ─────────────────────────────────────────────────────────────────────

def record_tool_run(
    tool_id: str, execution_mode: str, subjects: int = 0, chunks: int = 1
) -> None:
    """A tool was actually executed for one pipeline step.

    Counted once per step rather than once per chunk: a ten-chunk DLMUSE step is
    one use of the tool, with ten chunks of compute behind it.
    """
    _dispatch({
        "kind": "tool",
        "name": tool_id,
        "mode": _mode(execution_mode),
        "event": "submitted",
        "subjects": max(0, int(subjects or 0)),
        "chunks": max(0, int(chunks or 0)),
    })


def record_tool_outcome(
    tool_id: str, execution_mode: str, succeeded: bool, chunks: int = 0
) -> None:
    """A tool's step finished.

    ``chunks`` rides on this event rather than on the submission, for the same
    reason it does for pipelines: the number isn't known until the step has been
    planned and run.
    """
    _dispatch({
        "kind": "tool",
        "name": tool_id,
        "mode": _mode(execution_mode),
        "event": "succeeded" if succeeded else "failed",
        "chunks": max(0, int(chunks or 0)),
    })


def record_cache_hit(tool_id: str, execution_mode: str) -> None:
    """A step was served from the step cache, so the tool never ran.

    Deliberately not counted as a tool run — this measures what caching saves.
    """
    _dispatch({
        "kind": "tool",
        "name": tool_id,
        "mode": _mode(execution_mode),
        "event": "cache_hit",
    })


# ── Uploads ───────────────────────────────────────────────────────────────────

def record_upload(
    kind: str, execution_mode: str, files: int = 0, total_bytes: int = 0
) -> None:
    """Data of one of the five upload types was accepted into a project."""
    if kind not in _UPLOAD_KINDS:
        _log.debug("Ignoring usage upload event for unknown kind %r", kind)
        return
    _dispatch({
        "kind": "upload",
        "name": kind,
        "mode": _mode(execution_mode),
        "files": max(0, int(files or 0)),
        "bytes": max(0, int(total_bytes or 0)),
    })
