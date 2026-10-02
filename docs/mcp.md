# NiChart MCP server

Drive NiChart from an LLM agent. The **Model Context Protocol (MCP)** lets a host
app (Claude Desktop, Claude Code, Cline, Continue, …) discover and call a set of
NiChart tools — list pipelines, check readiness, run a pipeline on local data,
poll status, and read results — all in natural language.

The server (`app/mcp_server.py`) is a thin **client** of the NiChart REST API,
like the `nichart` CLI. The host launches it over **stdio**, calls `tools/list`
to discover the tools, and then issues `tools/call` as the model decides. It can
also serve over **Streamable HTTP** (`nichart-mcp --transport http`, endpoint at
`/mcp`) for connector-based hosts like ChatGPT.

> **Distributing the all-in-one desktop app?** This page covers a native install
> (absolute path to `nichart-mcp`). For wiring MCP into the user's LLM app from
> the Docker-based desktop distribution — the `docker exec` launch pattern,
> per-host setup, success-detection, and the setup-button UX — see
> [`mcp-desktop-integration.md`](mcp-desktop-integration.md).

---

## Prerequisites

1. **Install with the MCP extra** (into the same env as the API):
   ```bash
   pip install -e ".[mcp]"
   which nichart-mcp        # note the FULL path — you'll likely need it below
   ```
2. **A running NiChart API** for the server to talk to. Start one (from the repo
   root, so it loads `.env` + `resources/`):
   ```bash
   uvicorn app.main:app --host 127.0.0.1 --port 8000
   ```
   In `local` mode there's no auth, so the MCP server can use it directly. (A
   future version can auto-start one; for now, attach to a running API.)

The server reads `NICHART_API_URL` (or `--url`) to find the API; default
`http://localhost:8000`.

---

## The universal pattern (works in every MCP host)

Every MCP host registers a stdio server the same way — as a **command + args +
env**. So the NiChart invocation is always:

| Field | Value |
|-------|-------|
| command | `nichart-mcp` (or the absolute path to it — see the ⚠️ below) |
| args | `["--url", "http://localhost:8000"]` |
| env | optionally `{ "NICHART_API_URL": "http://localhost:8000" }` |

> ⚠️ **Use the absolute path to `nichart-mcp`.** Hosts launch the command with
> their *own* environment, which usually does **not** have your venv/conda on
> `PATH`. If NiChart is in a venv at `/opt/nichart/.venv`, the command is
> `/opt/nichart/.venv/bin/nichart-mcp` (Windows: `...\.venv\Scripts\nichart-mcp.exe`).
> This is the #1 cause of "server failed to start." `which nichart-mcp` gives it.

Only the *place* you enter that command differs per host. Below are the three
common ones; any other MCP client uses the same three fields.

### Claude Desktop

Edit `claude_desktop_config.json`:
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "nichart": {
      "command": "/opt/nichart/.venv/bin/nichart-mcp",
      "args": ["--url", "http://localhost:8000"]
    }
  }
}
```

Restart Claude Desktop. The NiChart tools appear (the tools/plug icon).

### Claude Code

```bash
claude mcp add nichart -- /opt/nichart/.venv/bin/nichart-mcp --url http://localhost:8000
# verify:
claude mcp list
```

### Any other MCP client (Cline, Continue, custom)

Register a **stdio** server with `command = /abs/path/to/nichart-mcp`,
`args = ["--url", "http://localhost:8000"]`. The exact config file/UI varies, but
those three fields are all that's needed.

---

## What it can do — a demo session

With the server connected, a researcher can say:

> **You:** "I have T1 scans in `~/data/study1/t1` and demographics in
> `~/data/study1/participants.csv`. What can I run on this?"
> → the model calls `list_pipelines`, explains the options and their requirements.
>
> **You:** "Run DLMUSE on it, in a project called study1."
> → `run_pipeline(pipeline_id="run_dlmuse", project="study1", t1="~/data/study1/t1",
> participants="~/data/study1/participants.csv")` — creates the project, uploads,
> checks readiness, submits, returns a run ID.
>
> **You:** "How's it going?" → `get_run_status(run_id)` — reports per-step progress.
>
> *(job finishes)* **You:** "Summarize the results."
> → `get_results(project="study1", pipeline_id="run_dlmuse")` — reads the volume
> table back, highlights notable values, offers the download path.

The model handles the whole discover → check → upload → run → poll → interpret
loop, and the researcher never touches JSON, the CLI, or the API.

> Even pointed at `dummy_pipeline` (no data needed) this shows the full loop —
> handy for rehearsing before a real pipeline is wired up on the demo box.

---

## Tools

| Tool | What it does |
|------|--------------|
| `list_pipelines()` | Discover runnable pipelines (id, name, requirements). |
| `check_readiness(project, pipeline_id)` | Whether a project's data meets a pipeline's needs. |
| `run_pipeline(pipeline_id, project, t1=…, fl=…, …, participants=…, existing=false, params={}, force=false)` | Create/select a project, upload data **by local path**, verify, and submit. Returns a `run_id`. If not ready and `force` is false, returns `not_ready` with details instead of submitting. |
| `get_run_status(run_id)` | Current status + per-step progress; `error` on failure. |
| `get_results(project, pipeline_id)` | Feature-table availability (rows/columns/download path) + per-subject output coverage. Documented columns include inline `definitions` — plain-language meaning, units, direction, interpretation caveats, literature `keywords`, and curated `references` (with DOIs, as a starting point) — so the model can explain opaque columns like `y_score` and, using the keywords, find and cite relevant research. The response also carries a `usage` note and the tool docstring spells out how to present the values (by definition not raw name; with units/direction; conveying "research biomarker, not a diagnosis" caveats). |

**Data ingestion is path-based:** imaging arguments are directories/files **on the
machine running the MCP server**. For a local researcher that's their laptop; on a
cluster it's a login node with shared storage — same model as the CLI's `--t1`.

**Long jobs are non-blocking:** `run_pipeline` returns immediately with a
`run_id`; the model polls `get_run_status`. Nothing blocks for the duration of a
job.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| "server failed to start" / not listed | Use the **absolute path** to `nichart-mcp` (host PATH ≠ your shell's). `which nichart-mcp`. |
| Tools error with "Cannot reach the NiChart API…" | No API running at `--url`. Start `uvicorn app.main:app …` (from the repo root). |
| "MCP SDK is not installed" | `pip install -e ".[mcp]"` into the env that provides `nichart-mcp`. |
| Cloud-mode API rejects calls (401) | This server targets **local mode** (no auth). Cloud/Cognito auth isn't wired up yet. |
| Very large NIfTI uploads are slow | Files are read into memory for upload (fine for typical studies); streaming is a future enhancement. |

Internal note for contributors: an MCP **stdio** server must keep **stdout**
clean — it carries the JSON-RPC protocol. `app/mcp_server.py` never prints to
stdout; keep it that way (diagnostics go to stderr).
