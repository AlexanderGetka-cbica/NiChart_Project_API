"""
NiChart MCP server — expose NiChart as tools an LLM agent can call.

This is a thin **client** of the NiChart REST API (like the `nichart` CLI): it
holds a base URL and makes HTTP calls. An MCP host (Claude Desktop, Claude Code,
etc.) launches this over stdio, calls ``tools/list`` to discover the tools below,
and then issues ``tools/call`` as the model decides.

Setup: see ``docs/mcp.md``. Install with ``pip install -e ".[mcp]"``.

IMPORTANT: an MCP stdio server must keep **stdout** clean — it carries the
JSON-RPC protocol. This module therefore never prints to stdout; tools return
values or raise, and any diagnostics go to stderr.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Optional

import httpx

from app import modalities

# Resolved in main(); tools read this module global.
_BASE_URL = os.environ.get("NICHART_API_URL", "http://localhost:8000").rstrip("/")


# ── API helpers (never print to stdout) ───────────────────────────────────────

def _api(method: str, path: str, **kwargs) -> Any:
    """Call the NiChart REST API; return parsed JSON (``{}`` for empty) or raise.

    Errors are raised as ``RuntimeError`` with a human-readable message so the
    model can relay them to the user.
    """
    try:
        r = httpx.request(method, f"{_BASE_URL}{path}", timeout=120, **kwargs)
    except httpx.HTTPError as exc:
        raise RuntimeError(
            f"Cannot reach the NiChart API at {_BASE_URL}: {exc}. "
            "Is the server running? (Start it, or point --url at it.)"
        )
    if r.status_code >= 400:
        try:
            detail = r.json().get("detail", r.text)
        except Exception:
            detail = r.text
        raise RuntimeError(f"NiChart API error {r.status_code}: {detail}")
    if r.status_code == 204 or not r.content:
        return {}
    return r.json()


def _gather_niftis(path_str: str) -> list[Path]:
    p = Path(path_str).expanduser()
    if not p.exists():
        raise RuntimeError(f"Path not found: {p}")
    if p.is_dir():
        files = sorted(
            f for f in p.iterdir()
            if f.is_file() and (f.name.endswith(".nii") or f.name.endswith(".nii.gz"))
        )
        if not files:
            raise RuntimeError(f"No NIfTI files (.nii / .nii.gz) found in {p}")
        return files
    if not (p.name.endswith(".nii") or p.name.endswith(".nii.gz")):
        raise RuntimeError(f"Not a NIfTI file: {p}")
    return [p]


def _upload_modality(project: str, modality: str, path_str: str) -> list[str]:
    """Upload NIfTIs (a directory or single file) as a fixed modality; return MRIDs.

    MRIDs are inferred from filenames server-side; a file whose MRID can't be
    inferred aborts the batch with a clear error.
    """
    files = _gather_niftis(path_str)
    upload = [("files", (f.name, f.read_bytes(), "application/octet-stream")) for f in files]
    resp = _api("POST", f"/projects/{project}/files/upload/nifti", files=upload)
    staging_id = resp["staging_id"]
    proposals = resp["proposals"]
    missing = [p["filename"] for p in proposals if not p.get("inferred_mrid")]
    if missing:
        _api("DELETE", f"/projects/{project}/files/stage/{staging_id}")
        raise RuntimeError(
            f"Could not infer a subject ID (MRID) for {modality.upper()} file(s): {missing}. "
            "Rename them so the subject ID is derivable from the filename."
        )
    mappings = [
        {"filename": p["filename"], "mrid": p["inferred_mrid"], "modality": modality}
        for p in proposals
    ]
    result = _api("POST", f"/projects/{project}/files/stage/{staging_id}/commit",
                  json={"mappings": mappings})
    return [c["mrid"] for c in result.get("committed", [])]


# ── Tools ──────────────────────────────────────────────────────────────────────

def list_pipelines() -> list[dict]:
    """List the NiChart processing pipelines that can be run.

    Returns a list of pipelines, each with its ``id`` (use this to run it),
    human-readable ``name``, ``description``, ``categories``, and data
    ``requires`` (e.g. needs_T1). Call this first to discover what's available.
    """
    items = _api("GET", "/catalog/pipelines")
    return [
        {
            "id": p["id"],
            "name": p.get("name"),
            "description": p.get("description"),
            "categories": p.get("categories") or [],
            "requires": p.get("requires") or [],
        }
        for p in items
    ]


def check_readiness(project: str, pipeline_id: str) -> dict:
    """Check whether a project has the data a pipeline needs, before running it.

    Reports per-modality imaging counts, participants-CSV column checks, and
    subject-count requirements. Use it to tell the user what's missing.

    Args:
        project: Project name.
        pipeline_id: Pipeline ID (from list_pipelines).
    """
    return _api("GET", f"/projects/{project}/readiness/{pipeline_id}")


def run_pipeline(
    pipeline_id: str,
    project: str,
    t1: Optional[str] = None,
    fl: Optional[str] = None,
    t2: Optional[str] = None,
    t1ce: Optional[str] = None,
    adc: Optional[str] = None,
    pet: Optional[str] = None,
    images: Optional[dict] = None,
    participants: Optional[str] = None,
    existing: bool = False,
    params: Optional[dict] = None,
    force: bool = False,
) -> dict:
    """Run a pipeline end-to-end: create/select a project, upload data, and submit.

    Imaging arguments are **local paths** (a flat directory of NIfTIs, or a single
    .nii/.nii.gz) on the machine this server runs on — one per modality. Subject
    IDs are inferred from filenames. Returns immediately with a ``run_id``; poll
    ``get_run_status(run_id)`` until it finishes, then ``get_results``.

    If the project isn't ready and ``force`` is false, this does NOT submit — it
    returns ``status="not_ready"`` with the readiness report so you can tell the
    user what to fix.

    Args:
        pipeline_id: Pipeline to run (from list_pipelines).
        project: Project name. Created new unless ``existing`` is true.
        t1: Path to T1-weighted NIfTIs (directory or file).
        fl: Path to FLAIR NIfTIs.
        t2: Path to T2 NIfTIs.
        t1ce: Path to T1CE NIfTIs.
        adc: Path to ADC NIfTIs.
        pet: Path to PET NIfTIs.
        images: Any other modality as {modality_code: path}; use for modalities
            without a dedicated argument above.
        participants: Path to the participants/demographics CSV.
        existing: Add to an existing project instead of creating a new one.
        params: Pipeline parameter overrides, e.g. {"duration_seconds": 30}.
        force: Submit even if the readiness check fails.
    """
    _api("GET", f"/catalog/pipelines/{pipeline_id}")  # 404 → clear error

    names = {p["id"] for p in _api("GET", "/projects")}
    if existing:
        if project not in names:
            raise RuntimeError(f"Project '{project}' does not exist. Set existing=false to create it.")
    else:
        if project in names:
            raise RuntimeError(f"Project '{project}' already exists. Set existing=true to add to it, or choose another name.")
        _api("POST", "/projects", json={"name": project})

    per_modality: dict[str, list[str]] = {}
    for mod, path in (("t1", t1), ("fl", fl), ("t2", t2), ("t1ce", t1ce), ("adc", adc), ("pet", pet)):
        if path:
            per_modality[mod] = _upload_modality(project, mod, path)
    for code, path in (images or {}).items():
        code = str(code).strip().lower()
        if not modalities.is_valid(code):
            raise RuntimeError(f"Unknown modality {code!r}. Valid: {list(modalities.MODALITY_CODES)}")
        if code in per_modality:
            raise RuntimeError(f"Modality {code!r} given twice (named argument and images).")
        if path:
            per_modality[code] = _upload_modality(project, code, str(path))

    if participants:
        pcsv = Path(participants).expanduser()
        if not pcsv.is_file():
            raise RuntimeError(f"Participants CSV not found: {pcsv}")
        _api("POST", f"/projects/{project}/files/upload/csv",
             files={"file": (pcsv.name, pcsv.read_bytes(), "text/csv")})

    mismatch = None
    if len(per_modality) > 1 and len({len(v) for v in per_modality.values()}) > 1:
        all_mrids = set().union(*per_modality.values())
        mismatch = {m: sorted(all_mrids - set(v)) for m, v in per_modality.items()}
        mismatch = {m: miss for m, miss in mismatch.items() if miss}

    readiness = _api("GET", f"/projects/{project}/readiness/{pipeline_id}")
    if not readiness.get("satisfied", False) and not force:
        return {
            "status": "not_ready",
            "project": project,
            "uploaded": {m: len(v) for m, v in per_modality.items()},
            "subject_count_mismatch": mismatch,
            "readiness": readiness,
            "note": "Project is not ready. Fix the issues in 'readiness', or call run_pipeline again with force=true.",
        }

    run = _api("POST", f"/projects/{project}/jobs/pipelines",
               json={"pipeline_id": pipeline_id, "params": params or {}, "reuse_cached_steps": True})
    return {
        "status": "submitted",
        "run_id": run["run_id"],
        "project": project,
        "uploaded": {m: len(v) for m, v in per_modality.items()},
        "subject_count_mismatch": mismatch,
        "readiness_satisfied": readiness.get("satisfied"),
        "note": "Submitted. Poll get_run_status(run_id) until status is 'succeeded' or 'failed', then call get_results(project, pipeline_id).",
    }


def get_run_status(run_id: str) -> dict:
    """Get the current status of a pipeline run, including per-step progress.

    ``status`` is one of pending / running / succeeded / failed. On failure,
    ``error`` explains why. Poll this after run_pipeline until it's terminal.

    Args:
        run_id: The run ID returned by run_pipeline.
    """
    run = _api("GET", f"/jobs/pipelines/{run_id}")
    return {
        "run_id": run["run_id"],
        "status": run["status"],
        "current_step": run.get("current_step"),
        "total_steps": run.get("total_steps"),
        "steps": [
            {"step_id": s["step_id"], "tool_id": s.get("tool_id"), "status": s["status"], "error": s.get("error")}
            for s in run.get("steps", [])
        ],
        "error": run.get("error"),
    }


def get_results(project: str, pipeline_id: str) -> dict:
    """Summarize a pipeline's results for a project (feature table + per-subject outputs).

    Use after a run succeeds. Returns whether the batch-feature CSV is available
    (with row/column counts and a download path) and per-subject output coverage.

    Each documented feature column carries a ``definitions`` entry: a plain-language
    ``definition``, ``units``, a ``direction`` (how to read magnitude),
    ``interpretation`` caveats, literature ``keywords``, and curated ``references``
    (each with an optional ``doi``).

    HOW TO USE THESE VALUES when answering the user:
    - Refer to each column by its ``definition``/``label``, never the raw column
      name. Many names are opaque (``y_score``, ``prediction``); the name alone
      does not tell the user what was measured.
    - When you state a value, say what it means using ``units`` and ``direction``
      (e.g. "SPARE-AD = 1.8 — higher values indicate a more AD-like atrophy
      pattern"), not just the number.
    - Always convey the ``interpretation`` caveats. These are research-grade
      imaging biomarkers, not clinical diagnoses; respect stated thresholds and
      flag near-threshold values as uncertain. Do not tell a user they "have" a
      disease.
    - Ground claims in the ``references`` and cite their ``doi`` when you do; treat
      them as a starting point, and use the ``keywords`` to search for additional,
      independent literature rather than relying only on what is listed. Do not
      invent citations or DOIs beyond what is provided or what you can verify.
    - If a column has no ``definitions`` entry, say it is undocumented rather than
      guessing its meaning. To download the raw CSV, use ``download_path`` with the
      files-download endpoint.

    Args:
        project: Project name.
        pipeline_id: Pipeline ID.
    """
    r = _api("GET", f"/projects/{project}/results/{pipeline_id}")
    bf = r.get("batch_features") or None
    defs = bf.get("feature_definitions") if bf else None
    usage = (
        "Refer to columns by their definition/label (not the raw name); state each "
        "value with its units and direction; always convey interpretation caveats "
        "(research biomarker, not a diagnosis); cite reference DOIs and use keywords "
        "to find further literature; call a column with no definition undocumented."
    )
    return {
        "pipeline": r.get("pipeline_name", pipeline_id),
        "usage": usage if defs else None,
        "batch_features": None if not bf else {
            "available": bf.get("available"),
            "row_count": bf.get("row_count"),
            "column_count": len(bf.get("columns") or []),
            "columns": bf.get("columns"),
            "column_units": bf.get("column_units"),
            "definitions": defs,
            "download_path": bf.get("download_path"),
        },
        "per_subject": [
            {
                "id": o.get("id"),
                "type": o.get("type"),
                "available": sum(1 for v in (o.get("subjects") or {}).values() if v.get("available")),
                "total": len(o.get("subjects") or {}),
            }
            for o in r.get("per_subject", [])
        ],
    }


_TOOLS = (list_pipelines, check_readiness, run_pipeline, get_run_status, get_results)


def _build_server(host: str = "127.0.0.1", port: int = 8765):
    """Construct the FastMCP server with the tools registered. Requires the MCP SDK.

    ``host``/``port`` only matter for the Streamable-HTTP transport; they are
    ignored under stdio.
    """
    from mcp.server.fastmcp import FastMCP  # imported lazily so tests/CLI don't need it

    server = FastMCP("NiChart", host=host, port=port)
    for fn in _TOOLS:
        server.tool()(fn)
    return server


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        prog="nichart-mcp",
        description=(
            "NiChart MCP server. Exposes NiChart tools to an MCP host over stdio "
            "(default; for hosts that launch a subprocess — Claude Desktop, Claude "
            "Code, Codex) or Streamable HTTP (for remote/connector hosts — ChatGPT)."
        ),
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("NICHART_API_URL", "http://localhost:8000"),
        help="Base URL of a running NiChart API (default: %(default)s or $NICHART_API_URL).",
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default=os.environ.get("NICHART_MCP_TRANSPORT", "stdio"),
        help=(
            "MCP transport. 'stdio' (default) for local hosts that spawn the server; "
            "'http' (Streamable HTTP, endpoint at /mcp) for connector-based hosts. "
            "Env: NICHART_MCP_TRANSPORT."
        ),
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("NICHART_MCP_HOST", "127.0.0.1"),
        help="Bind host for --transport http (default: %(default)s; keep on loopback unless you add auth). Env: NICHART_MCP_HOST.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("NICHART_MCP_PORT", "8765")),
        help="Bind port for --transport http (default: %(default)s). Env: NICHART_MCP_PORT.",
    )
    args = parser.parse_args()

    global _BASE_URL
    _BASE_URL = args.url.rstrip("/")

    try:
        server = _build_server(host=args.host, port=args.port)
    except ImportError:
        sys.exit("The MCP SDK is not installed. Install it with:  pip install -e '.[mcp]'")

    if args.transport == "http":
        # Streamable HTTP — endpoint at http://{host}:{port}/mcp. Server logs may go
        # to stdout here, which is fine (this transport does not use stdio for JSON-RPC).
        server.run(transport="streamable-http")
    else:
        server.run()  # stdio transport (JSON-RPC on stdin/stdout; keep stdout clean)


if __name__ == "__main__":
    main()
