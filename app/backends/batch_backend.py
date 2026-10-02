"""
AWS Batch backend — submits tool jobs via the ``cbica-nichart-submitjob`` Lambda.

Security boundary
-----------------
The Lambda loads the tool spec from S3, validates all mount paths within
``/fsx/fsx/{user_id}/``, and generates the Docker command internally.  The API
server never sends ``image`` or ``command`` to the Lambda — only:

    id_token, tool_name, user_mounts, user_params, num_subjects

Cloud setup requirements
------------------------
IAM permissions required on the API server's execution role:

- ``lambda:InvokeFunction`` on ``cbica-nichart-submitjob``
- ``batch:DescribeJobs`` (no resource restriction)
- ``batch:ListJobs`` on the job queue ARN
- ``batch:TerminateJob`` (no resource restriction)
- ``logs:GetLogEvents`` on ``arn:aws:logs:us-east-1:*:log-group:/aws/batch/job:*``

``NICHART_DATA_ROOT`` must be set to the FSx mount root (e.g. ``/fsx/fsx``)
so resolved data paths match what the Lambda expects.
"""

import asyncio
import json
from pathlib import Path
from typing import Any

import boto3

from app.backends.base import JobBackend, JobHandle, ToolSpec
from app.config import Settings

_BATCH_LOG_GROUP = "/aws/batch/job"


class BatchJobHandle(JobHandle):
    """Wraps an AWS Batch job ID."""

    _STATUS_MAP = {
        "SUBMITTED": "pending",
        "PENDING": "pending",
        "RUNNABLE": "pending",
        "STARTING": "running",
        "RUNNING": "running",
        "SUCCEEDED": "succeeded",
        "FAILED": "failed",
    }

    def __init__(self, job_id: str, batch_client, logs_client) -> None:
        self._job_id = job_id
        self._batch = batch_client
        self._logs = logs_client

    @property
    def job_id(self) -> str:
        return self._job_id

    async def status(self) -> str:
        resp = await asyncio.to_thread(self._batch.describe_jobs, jobs=[self._job_id])
        if not resp["jobs"]:
            return "failed"
        return self._STATUS_MAP.get(resp["jobs"][0]["status"], "running")

    async def logs(self, tail: int = 200) -> str:
        resp = await asyncio.to_thread(self._batch.describe_jobs, jobs=[self._job_id])
        if not resp["jobs"]:
            return f"Job {self._job_id} not found"
        job = resp["jobs"][0]

        log_stream: str | None = None
        try:
            containers = (
                job.get("ecsProperties", {})
                .get("taskProperties", [{}])[0]
                .get("containers", [{}])
            )
            if containers:
                log_stream = containers[0].get("logStreamName")
        except (KeyError, IndexError):
            pass

        if not log_stream:
            return f"[No log stream yet — job {self._job_id} status: {job['status']}]"

        try:
            cw_resp = await asyncio.to_thread(
                self._logs.get_log_events,
                logGroupName=_BATCH_LOG_GROUP,
                logStreamName=log_stream,
                startFromHead=False,
                limit=tail,
            )
            events = cw_resp.get("events", [])
            if not events:
                return f"[No log events yet — job {self._job_id} status: {job['status']}]"
            return "\n".join(e["message"] for e in events)
        except Exception as exc:
            return f"[CloudWatch error for job {self._job_id}: {exc}]"

    async def cancel(self) -> None:
        await asyncio.to_thread(
            self._batch.terminate_job,
            jobId=self._job_id,
            reason="Cancelled by user via NiChart API",
        )


class BatchBackend(JobBackend):
    """Submits tool jobs via the cbica-nichart-submitjob Lambda function."""

    def __init__(self, settings: Settings) -> None:
        region = settings.cognito_region
        self._lambda = boto3.client("lambda", region_name=region)
        self._batch = boto3.client("batch", region_name=region)
        self._logs = boto3.client("logs", region_name=region)
        self._lambda_name = settings.lambda_function_name

    @property
    def backend_name(self) -> str:
        return "batch"

    def reconnect(self, job_id: str, log_path: str | None = None) -> BatchJobHandle:
        """Rebuild a handle for an existing Batch job.

        Batch jobs are addressable purely by ID via the AWS API, so this works
        from any process/instance — used by the logs endpoint to fetch live
        CloudWatch output for a still-running step (the original handle lives only
        in the background task).
        """
        return BatchJobHandle(job_id, self._batch, self._logs)

    async def submit(
        self,
        tool_spec: ToolSpec,
        mount_paths: dict[str, str],
        params: dict[str, Any],
        num_subjects: int = 1,
        user_token: str | None = None,
        extra_readonly_mounts: list[str] | None = None,  # ignored: FSx shared across Batch nodes
        study_id: str | None = None,
    ) -> BatchJobHandle:
        payload = {
            "id_token": user_token or "",
            "tool_name": tool_spec.tool_id,
            "user_mounts": mount_paths,
            "user_params": params,
            "num_subjects": num_subjects,
            "study_id": study_id or "",
        }
        resp = await asyncio.to_thread(
            self._lambda.invoke,
            FunctionName=self._lambda_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode(),
        )
        result = json.loads(resp["Payload"].read())
        if result.get("statusCode") != 200:
            body_str = result.get("body") or "{}"
            try:
                body = json.loads(body_str)
                msg = body.get("error", body_str)
            except Exception:
                msg = body_str
            raise RuntimeError(f"Lambda error ({result.get('statusCode')}): {msg}")
        body = json.loads(result["body"])
        return BatchJobHandle(body["job_id"], self._batch, self._logs)

    async def get_queue_position(
        self,
        job_id: str,
        queue_name: str,
        tools_path: Path,
    ) -> tuple[int, float | None]:
        """Return (jobs_ahead, estimated_wait_seconds) for a queued Batch job.

        Counts all SUBMITTED/PENDING/RUNNABLE jobs on the queue that were
        submitted *before* the given job, then sums their
        num_subjects × time_per_subject_seconds to estimate the wait.
        """
        from app.services.catalog_service import get_tool

        our_resp = await asyncio.to_thread(self._batch.describe_jobs, jobs=[job_id])
        if not our_resp["jobs"]:
            return 0, None
        our_created_at: int = our_resp["jobs"][0].get("createdAt", 0)

        ahead_ids: list[str] = []
        for status in ("SUBMITTED", "PENDING", "RUNNABLE"):
            next_token = None
            while True:
                kwargs: dict[str, Any] = {"jobQueue": queue_name, "jobStatus": status}
                if next_token:
                    kwargs["nextToken"] = next_token
                resp = await asyncio.to_thread(self._batch.list_jobs, **kwargs)
                for j in resp.get("jobSummaryList", []):
                    if j["jobId"] != job_id and j.get("createdAt", float("inf")) < our_created_at:
                        ahead_ids.append(j["jobId"])
                next_token = resp.get("nextToken")
                if not next_token:
                    break

        if not ahead_ids:
            return 0, None

        total_secs = 0.0
        has_estimate = False
        for i in range(0, len(ahead_ids), 100):
            details = (
                await asyncio.to_thread(self._batch.describe_jobs, jobs=ahead_ids[i : i + 100])
            ).get("jobs", [])
            for job in details:
                params = job.get("parameters", {})
                tool_id = params.get("tool_id")
                try:
                    ns = int(params.get("num_subjects", 1))
                except (ValueError, TypeError):
                    ns = 1
                if tool_id:
                    try:
                        tspec = get_tool(tools_path, tool_id)
                        if tspec.time_per_subject_seconds is not None:
                            total_secs += tspec.time_per_subject_seconds * ns
                            has_estimate = True
                    except Exception:
                        pass

        return len(ahead_ids), (total_secs if has_estimate else None)
