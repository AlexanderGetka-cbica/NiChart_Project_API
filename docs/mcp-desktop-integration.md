# Brief: integrating the NiChart MCP server into desktop LLM apps

**Audience:** the Electron/all-in-one desktop app developer.
**Goal:** give the user a one-click (or copy-paste) way to expose NiChart to their
LLM app of choice, plus programmatic ways to detect that it worked.

The all-in-one app already installs and runs the NiChart API in Docker. This doc
covers wiring the **MCP server** (`nichart-mcp`) into the user's LLM host so the
model can call NiChart tools (list/run pipelines, check readiness, poll status,
read results). See [`mcp.md`](mcp.md) for what the tools do.

---

## What we support

`nichart-mcp` is a thin MCP client of the local API. It speaks two transports:

| LLM host | Transport | Supported | Mechanism |
|---|---|---|---|
| **Claude Desktop** | stdio | ✅ | `claude_desktop_config.json` |
| **Claude Code** | stdio | ✅ | `claude mcp add` (or `.mcp.json`) |
| **OpenAI Codex CLI** | stdio | ✅ | `~/.codex/config.toml` |
| Other stdio hosts (Cursor, Windsurf, …) | stdio | ✅ | same `mcpServers` JSON shape |
| **ChatGPT** (desktop/web) | Streamable **HTTP** connector | ⚠️ advanced | `--transport http` + connector URL; requires ChatGPT *developer mode*, plan-gated |

**stdio is the primary, fully-supported path.** ChatGPT is supported only via the
HTTP transport as a *connector*, which the user must add in ChatGPT's UI (it does
not spawn local processes) — treat it as advanced/optional.

### Guaranteed by the app/image (already done in this repo)
- The Docker image ships the MCP SDK (`pip install -e ".[mcp]"`, pinned `mcp<2`),
  so `nichart-mcp` runs inside the container.
- The API container has a **stable name: `nichart-api`** (`container_name` in
  `docker-compose.yml`), so configs can reference it.
- The app ensures the container is **running** before the LLM host tries to use it.

---

## The launch command (stdio)

Every stdio host runs the same thing — `docker exec` into the running API
container (no host Python needed):

```
docker exec -i nichart-api nichart-mcp --url http://localhost:8000
```

- **`-i` is required; never `-t`.** A TTY corrupts the JSON-RPC stream.
- `http://localhost:8000` is resolved *inside* the container (uvicorn binds
  `0.0.0.0:8000`) — portable across macOS/Windows/Linux.
- Requires only that `docker` is on PATH and the `nichart-api` container is up.

---

## Per-host setup

### Claude Desktop
Deep-merge into `claude_desktop_config.json`, then the user restarts Claude Desktop.

Config path:
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`
- Linux: `~/.config/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "nichart": {
      "command": "docker",
      "args": ["exec", "-i", "nichart-api", "nichart-mcp", "--url", "http://localhost:8000"]
    }
  }
}
```

### Claude Code
One CLI call (user scope → available in every project):

```
claude mcp add nichart --scope user -- docker exec -i nichart-api nichart-mcp --url http://localhost:8000
```

Project scope alternative: write `.mcp.json` in the project root with the same
`mcpServers` shape as above.

### OpenAI Codex CLI
Append to `~/.codex/config.toml`:

```toml
[mcp_servers.nichart]
command = "docker"
args = ["exec", "-i", "nichart-api", "nichart-mcp", "--url", "http://localhost:8000"]
```

### ChatGPT (advanced, HTTP connector)
ChatGPT integrates MCP via **connectors** pointing at a **remote Streamable-HTTP**
endpoint; it does not launch local processes. Steps:

1. Start the server in HTTP mode, publishing the port. On Linux:
   ```
   docker run -d --rm --name nichart-mcp-http --network host \
     cbica/nichart-api:latest \
     nichart-mcp --transport http --host 0.0.0.0 --port 8765 --url http://localhost:8000
   ```
   (macOS/Windows Docker Desktop: replace `--network host` with `-p 127.0.0.1:8765:8765`
   and use `--url http://host.docker.internal:8000`; the API's port 8000 is already
   published by compose.)
2. In ChatGPT: Settings → Connectors → enable **Developer mode** → add a custom
   connector with URL `http://localhost:8765/mcp`.
3. Availability depends on the user's ChatGPT plan. Keep the bind on loopback;
   there is **no auth** on the MCP server, so do not expose 8765 publicly.

---

## Detecting success

Layer these from cheapest to strongest.

### 1. Prerequisites (before offering the button)
```bash
docker version                                             # Docker present
docker ps --filter 'name=^/nichart-api$' --filter status=running -q   # container up (non-empty)
docker exec nichart-api nichart-mcp --help                # exit 0 => [mcp] present in image
curl -fsS http://localhost:8000/health                    # API healthy
```

### 2. Config written (did we install the entry)
- Claude Desktop / Codex: parse the config file; assert `mcpServers.nichart`
  (resp. `mcp_servers.nichart`) exists and its `command`/`args` match.
- Claude Code: `claude mcp get nichart` exits 0 when present.

### 3. Host actually connected (strongest)
- **Claude Code** reports live connection status:
  ```
  claude mcp list          # each server prints ✓ connected / ✗ failed
  ```
  Parse for `nichart` + connected — the best automated signal for Claude Code.
- **Claude Desktop / Codex / ChatGPT** have no query CLI. Use the host-agnostic
  self-test below, then tell the user to confirm the tools appear in-app.

### 4. Host-agnostic self-test (recommended)
Run a real MCP handshake against the *same* launch command and assert the five
tools come back. The app is Node, so use `@modelcontextprotocol/sdk`:

```js
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const transport = new StdioClientTransport({
  command: "docker",
  args: ["exec", "-i", "nichart-api", "nichart-mcp", "--url", "http://localhost:8000"],
});
const client = new Client({ name: "nichart-selftest", version: "1.0.0" });
await client.connect(transport);
const { tools } = await client.listTools();
const names = tools.map(t => t.name).sort();
const ok = ["check_readiness","get_results","get_run_status","list_pipelines","run_pipeline"]
  .every(n => names.includes(n));
await client.close();
// ok === true  => integration verified end-to-end
```

CLI equivalent for manual checks:
```
npx @modelcontextprotocol/inspector --cli \
  docker exec -i nichart-api nichart-mcp --url http://localhost:8000 --method tools/list
```

---

## The button (recommended UX)

1. **Detect installed hosts** — presence of each config file, or the `claude` /
   `codex` binary on PATH. Only show a host if detected.
2. **"Connect to <host>"** button per detected host:
   - Claude Desktop / Codex → idempotent **deep-merge** of the `nichart` entry into
     the config file (preserve any existing servers), then prompt: *"Restart
     <host> to load NiChart."* (Desktop apps read config only at startup.)
   - Claude Code → shell out to the `claude mcp add … --scope user` command.
   - After writing, run the **self-test (§4)** and show ✓/✗ with the tool count.
3. **"Copy config" fallback** — always offer the exact snippet + the config-file
   path for manual paste, for hosts we can't detect or write safely.
4. **Status chip** — reflect the layered checks: container up → config present →
   self-test passed.

---

## Gotchas
- Use `docker exec -i` (interactive), **never `-t`**.
- The `nichart-api` container must be running whenever the LLM host invokes the
  server. If the user quits NiChart, calls fail with a clear message
  (`nichart-mcp` already says *"Is the server running?"*) — surface that.
- Desktop hosts cache config at startup → a **restart** is required after writing.
- Deep-merge configs; do not clobber the user's other MCP servers.
- Local mode has **no auth** — keep the API (and any HTTP MCP port) on loopback.
- The image pins `mcp<2` (v1 FastMCP API). If you rebuild with a looser pin and
  `nichart-mcp` starts erroring about `FastMCP`/`MCPServer`, that's mcp 2.x — keep
  the pin.
