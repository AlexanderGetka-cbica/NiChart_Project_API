"""
cbica-nichart-usage-counter — reference copy of the deployed Lambda.

Sits behind a public (auth-type NONE) Function URL and increments usage counters
in DynamoDB. The API server POSTs one small JSON event per thing that happens;
nothing on the caller's side needs AWS credentials, which is what lets desktop
and on-prem installs report usage at all.

Because the endpoint is unauthenticated, anyone who finds it can inflate our
counters. That is an accepted risk. What is NOT accepted, and what this handler
defends against, is anything that costs real money or corrupts the dataset:

  * unbounded key cardinality — a million distinct names would mean a million
    DynamoDB items, so names must match a strict pattern and, for uploads, an
    exact allowlist;
  * absurd numbers — every numeric field is clamped;
  * unexpected fields — only allowlisted keys are ever read, so a client bug
    cannot leak a project name or MRID into the table;
  * oversized or batched-to-death payloads — body size and event count capped.

Invocation cost is capped by *reserved concurrency* on the function itself (see
Deployment below), which is the real control: a Function URL has no built-in
rate limiting.

Wire format (one event; or {"events": [...]} for up to 25 at once)
------------------------------------------------------------------
    {"kind": "pipeline", "name": "run_dlmuse", "mode": "cloud",
     "event": "submitted", "subjects": 42, "chunks": 5}

    {"kind": "tool", "name": "nichart_dlmuse", "mode": "local",
     "event": "cache_hit"}

    {"kind": "upload", "name": "nifti", "mode": "cloud",
     "files": 12, "bytes": 348000000}

kind     pipeline | tool | upload
name     ^[a-z0-9][a-z0-9_]{0,63}$ ; for uploads one of the five upload types
mode     local | cloud
event    submitted | succeeded | failed | cache_hit   (pipeline/tool; default submitted)

Table layout: one item per (metric, month).

    metric (PK, S)  "pipeline#run_dlmuse" | "tool#nichart_dlmuse" | "upload#nifti"
    period (SK, S)  "2026-10"   (UTC)

    count / count_local / count_cloud   submissions (or uploads)
    succeeded / failed                  terminal outcomes
    cache_hits                          steps served from the step cache
    subjects / chunks                   volume behind those submissions
    files / bytes                       upload volume
    updated_at                          ISO-8601, last touch

Per-metric totals are a Query on the partition key; monthly breakdown is free.

Deployment
----------
    TABLE=cbica-nichart-usagecounts
    REGION=us-east-1

    # 1. Table (on-demand; writes are ~$1.25/million)
    aws dynamodb create-table --table-name $TABLE --region $REGION \\
      --billing-mode PAY_PER_REQUEST \\
      --attribute-definitions AttributeName=metric,AttributeType=S \\
                              AttributeName=period,AttributeType=S \\
      --key-schema AttributeName=metric,KeyType=HASH \\
                   AttributeName=period,KeyType=RANGE

    # 2. Role: basic logging + UpdateItem on this one table ARN, nothing else.
    #    (trust policy: lambda.amazonaws.com)

    # 3. Function: python3.12, handler lambda_function.lambda_handler,
    #    timeout 5s, memory 128MB, env USAGE_TABLE=$TABLE

    # 4. Cost ceiling — do not skip this one.
    aws lambda put-function-concurrency --function-name cbica-nichart-usage-counter \\
      --reserved-concurrent-executions 5 --region $REGION

    # 5. Public URL (no CORS: the caller is always a server, never a browser)
    aws lambda create-function-url-config --function-name cbica-nichart-usage-counter \\
      --auth-type NONE --region $REGION
    aws lambda add-permission --function-name cbica-nichart-usage-counter \\
      --statement-id FunctionURLAllowPublicAccess --action lambda:InvokeFunctionUrl \\
      --principal '*' --function-url-auth-type NONE --region $REGION

The resulting URL is a hardcoded constant in the API server, not an
environment variable: see USAGE_ENDPOINT_URL in app/services/usage_service.py.
Hardcoding it is what lets desktop and on-prem installs report with no config.
"""

import base64
import json
import os
import re
from datetime import datetime, timezone

import boto3
from botocore.config import Config

TABLE_NAME = os.environ.get("USAGE_TABLE", "cbica-nichart-usagecounts")

MAX_BODY_BYTES = 8192
MAX_EVENTS_PER_REQUEST = 25

# A public endpoint must not be able to create arbitrary partition keys.
_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_]{0,63}$")

_KINDS = {"pipeline", "tool", "upload"}
_MODES = {"local", "cloud"}
_EVENTS = {"submitted", "succeeded", "failed", "cache_hit"}
_UPLOAD_NAMES = {"nifti", "csv", "bids", "idat", "dicom"}

# Clamps: one request must never be able to write 10**18 into an aggregate.
_MAX_SUBJECTS = 100_000
_MAX_CHUNKS = 10_000
_MAX_FILES = 1_000_000
_MAX_BYTES = 10 * (1024 ** 4)   # 10 TiB

# Built on first use rather than at import: a client constructed at module scope
# raises if no region is resolvable, which would turn a misconfiguration into a
# hard cold-start failure (and makes the handler awkward to test). Lambda always
# sets AWS_REGION; the fallback is only for local invocation.
_ddb = None


def _client():
    global _ddb
    if _ddb is None:
        _ddb = boto3.client(
            "dynamodb",
            region_name=os.environ.get("AWS_REGION") or "us-east-1",
            # Short timeouts, few retries: fire-and-forget telemetry, and the
            # caller has already stopped waiting for us.
            config=Config(retries={"max_attempts": 2}, connect_timeout=2, read_timeout=3),
        )
    return _ddb


def _clamp(value, maximum: int) -> int:
    """Coerce to a non-negative int no larger than maximum; junk becomes 0."""
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, min(number, maximum))


def _parse_body(event):
    """Pull the JSON payload out of a Function URL event, or None if unusable.

    Also accepts a bare event dict so the function can be tested with a direct
    `aws lambda invoke`.
    """
    if not isinstance(event, dict):
        return None
    if "body" not in event:
        return event

    body = event.get("body")
    if not isinstance(body, str):
        return None
    if event.get("isBase64Encoded"):
        try:
            body = base64.b64decode(body).decode("utf-8")
        except Exception:
            return None
    if len(body) > MAX_BODY_BYTES:
        return None
    try:
        return json.loads(body)
    except Exception:
        return None


def _plan(payload: dict):
    """Validate one event and return (metric, {attribute: delta}), or None.

    Only allowlisted keys are consulted. Anything unrecognised in the payload is
    silently ignored rather than stored.
    """
    kind = payload.get("kind")
    mode = payload.get("mode")
    name = payload.get("name")
    if kind not in _KINDS or mode not in _MODES or not isinstance(name, str):
        return None

    name = name.strip().lower()
    if not _NAME_RE.match(name):
        return None

    if kind == "upload":
        if name not in _UPLOAD_NAMES:
            return None
        deltas = {
            "count": 1,
            f"count_{mode}": 1,
            "files": _clamp(payload.get("files"), _MAX_FILES),
            "bytes": _clamp(payload.get("bytes"), _MAX_BYTES),
        }
    else:
        event_type = payload.get("event", "submitted")
        if event_type not in _EVENTS:
            return None
        if event_type == "submitted":
            deltas = {
                "count": 1,
                f"count_{mode}": 1,
                "subjects": _clamp(payload.get("subjects"), _MAX_SUBJECTS),
                "chunks": _clamp(payload.get("chunks"), _MAX_CHUNKS),
            }
        elif event_type == "cache_hit":
            deltas = {"cache_hits": 1}
        else:
            # succeeded | failed. 'chunks' is accepted here because a pipeline
            # does not know how many chunks it will submit until it finishes,
            # so that figure can only be reported at the terminal event.
            # 'subjects' is deliberately NOT accepted — it is counted at submit,
            # and taking it again here would double it.
            deltas = {
                event_type: 1,
                "chunks": _clamp(payload.get("chunks"), _MAX_CHUNKS),
            }

    # Drop zero deltas so an item never gains an attribute that says nothing.
    return f"{kind}#{name}", {k: v for k, v in deltas.items() if v}


def _write(metric: str, deltas: dict, period: str) -> None:
    """Atomically ADD the deltas onto this metric's item for the month.

    Every attribute goes through ExpressionAttributeNames because several of
    them ('count', 'bytes') are DynamoDB reserved words.
    """
    names = {}
    values = {}
    clauses = []
    for index, (attribute, delta) in enumerate(deltas.items()):
        name_ref, value_ref = f"#a{index}", f":v{index}"
        names[name_ref] = attribute
        values[value_ref] = {"N": str(delta)}
        clauses.append(f"{name_ref} {value_ref}")

    names["#updated"] = "updated_at"
    values[":now"] = {"S": datetime.now(timezone.utc).isoformat()}

    _client().update_item(
        TableName=TABLE_NAME,
        Key={"metric": {"S": metric}, "period": {"S": period}},
        UpdateExpression=f"ADD {', '.join(clauses)} SET #updated = :now",
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def _response(status: int) -> dict:
    """Minimal response. Probers learn nothing; the caller ignores this anyway."""
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": "" if status == 204 else json.dumps({"ok": status < 400}),
    }


def lambda_handler(event, context):
    payload = _parse_body(event)
    if not isinstance(payload, dict):
        return _response(400)

    batch = payload.get("events")
    items = batch if isinstance(batch, list) else [payload]
    if not items or len(items) > MAX_EVENTS_PER_REQUEST:
        return _response(400)

    period = datetime.now(timezone.utc).strftime("%Y-%m")
    accepted = 0
    rejected = 0
    write_failed = False

    for item in items:
        plan = _plan(item) if isinstance(item, dict) else None
        if plan is None:
            rejected += 1
            continue
        metric, deltas = plan
        try:
            _write(metric, deltas, period)
            accepted += 1
        except Exception as exc:
            # Logged, never raised: a broken table must not turn into retries.
            # Deliberately broader than ClientError — botocore can also raise
            # ParamValidationError or a connection error, and those escaping
            # would make the Function URL return a 502 instead of our own
            # response. The exception class is logged because it is the fastest
            # way to tell a permissions problem from a schema one.
            print(
                f"usage write failed metric={metric} period={period} "
                f"{type(exc).__name__}: {exc}"
            )
            write_failed = True

    if write_failed:
        return _response(500)
    if accepted == 0:
        print(f"usage request rejected: {rejected} invalid event(s)")
        return _response(400)
    return _response(204)
