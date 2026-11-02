# Plan: MCP Streamable-HTTP endpoint on the API server (with Cognito OAuth)

**Status:** design, not yet implemented.
**Audience:** NiChart API maintainers.

## Goal & locked requirements

Serve MCP over **Streamable HTTP from the API server itself** (path `/mcp`), so
**remote** LLM connector hosts (ChatGPT, remote Claude, etc.) can drive NiChart
**without NiChart running on the user's device**. Today MCP is stdio-only
(`nichart-mcp`), which requires a co-located server.

Decisions already made:
- **Full MCP OAuth 2.0**, using the **existing Cognito user pool** as the
  authorization server (no second identity system).
- The endpoint is **opt-in**: a new setting `NICHART_MCP_HTTP_ENABLED` defaults
  to **false**; MCP is only mounted when explicitly enabled.
- **stdio `nichart-mcp` stays** for local/desktop hosts — this is additive.
- Mounted tools execute **in-process** (call the service layer directly), not via
  loopback self-HTTP. (See "Tool execution backend" below.)

## Grounding (current code)

- `app/mcp_server.py` defines a `FastMCP("NiChart")` server with 5 tools
  (`list_pipelines`, `check_readiness`, `run_pipeline`, `get_run_status`,
  `get_results`) and `_build_server()`; it already supports a standalone
  `--transport http`.
- The installed FastMCP (mcp v1, pinned `<2`) exposes `streamable_http_app()`
  (a mountable ASGI app) and `session_manager` (must run inside a lifespan).
- `app/main.py::create_app()` builds `FastAPI(lifespan=_lifespan)` and includes
  routers; auth is the Cognito `session` cookie in cloud mode
  (`app/auth/dependencies.py::require_auth`, `CognitoVerifier`), bypassed to
  `LOCAL_USER` in local mode.

---

## Architecture

```
Remote MCP client ──OAuth(code+PKCE)──▶ Cognito (Authorization Server)
       │                                       │ issues access token (JWT)
       └── Authorization: Bearer <token> ──▶ /mcp  (Resource Server, in NiChart API)
                                               │ validate JWT via Cognito JWKS
                                               │ → CurrentUser(sub)
                                               └─ MCP tools run in-process,
                                                  scoped to that user's projects
```

- **`/mcp`** = an OAuth 2.0 **Resource Server** that accepts bearer access tokens.
- **Cognito** = the **Authorization Server** (already configured; issuer/JWKS/OIDC
  discovery already used by `CognitoVerifier`).
- Tools run **in-process** against the service layer as the authenticated user.

---

## Remote tool surface: full set + a cloud-upload tool (decided)

The existing tools assume the **caller's local filesystem** — `run_pipeline`
takes local NIfTI paths and uploads them from the machine running the MCP server.
A **remote** connector has no access to those files, so processing can't start
until the client has gotten data into the user's cloud project.

**Decision: keep the full tool set and add a cloud-upload tool.** Nothing is
omitted; the remote client gets what it needs to upload if necessary. Remote tool
set:
- `list_projects`, `list_pipelines`, `check_readiness(project, pipeline)`
- **`upload` (new)** — ingest imaging data into the user's cloud project so
  processing has something to run on
- `run_pipeline(project, pipeline)` — runs on the uploaded cloud-project data
  (not a local path)
- `get_run_status(run_id)`, `get_results(project, pipeline)`

Structure: **one tool module** (single source of truth). The local/stdio build
and the remote/HTTP build share the list/readiness/status/results tools; the
remote build adds `upload` and `run_pipeline` operates on the cloud project
rather than a local path.

**Design detail to settle during implementation (not a blocker):** how the
`upload` tool moves bytes. MCP tool arguments are JSON, so a large NIfTI can't go
inline comfortably. Options: (a) the tool returns a short-lived **presigned
upload URL / staging ticket** the host/user uploads to, then a commit tool
finalizes; (b) chunked/base64 inline for small inputs only. Lean toward (a).

---

## Work breakdown (phased)

### Phase A — Mount `/mcp` (local mode, no auth)
1. `NICHART_MCP_HTTP_ENABLED` setting (default false). When true, `create_app()`
   builds the FastMCP server and `app.mount("/mcp", mcp.streamable_http_app())`.
2. **Lifespan wiring (known gotcha):** FastMCP's `StreamableHTTPSessionManager`
   must run inside the app lifespan. Extend `_lifespan` to
   `async with mcp.session_manager.run(): <existing startup>; yield`. Without this
   the endpoint 500s.
3. **Refactor tools to a backend abstraction** so one definition serves both
   transports: a `Backend` with an in-process implementation (calls
   `catalog_service` / `job_service` / `file_service`) and the existing
   HTTP-client implementation (for the standalone `nichart-mcp` process).
4. Verify locally with the MCP Inspector / a python MCP client: `initialize` +
   `tools/list` + a tool call, in local mode (no auth), scoped to `LOCAL_USER`.

### Phase B — Resource-server auth (Cognito bearer)
5. On `/mcp`, require `Authorization: Bearer <token>` and validate a Cognito
   **access token** (decided). This differs from the existing `CognitoVerifier`
   (built for ID tokens: `token_use=id`, `audience=client_id`), so add an
   access-token verifier variant: validate `token_use=access`, `iss`, signature
   (JWKS), and the custom resource-server **scope** (access tokens have no `aud`).
   Resolve a `CurrentUser(sub)` from the validated token.
6. Add the **`upload` tool** (remote build) + its ingestion mechanism (lean:
   presigned upload URL / staging ticket, then a commit — see the tool-surface
   section). This is what lets a remote client get data in before `run_pipeline`.
7. Per-request user context: carry the resolved `CurrentUser` into tool execution
   (FastMCP request context) so every tool scopes to that user's storage
   (`{data_root}/{sub}/...`).
8. Serve **Protected Resource Metadata** (RFC 9728) at
   `/.well-known/oauth-protected-resource` pointing at the Cognito issuer, and
   return `WWW-Authenticate: Bearer resource_metadata="..."` on 401 so clients can
   discover the authorization server.

### Phase C — Full OAuth discovery for turnkey connectors
9. Cognito already serves `/.well-known/openid-configuration` and
   `/.well-known/oauth-authorization-server` (RFC 8414) → clients can discover
   endpoints and do code+PKCE.
10. Define an MCP **resource server + custom scope** in Cognito (e.g.
    `nichart-mcp/invoke`) so issued access tokens are audience-scoped to `/mcp`.
11. **DCR — de-risk first (decided).** MCP's turnkey flow expects clients to
    self-register (RFC 7591), which **Cognito does not support**. Before building
    anything here, point a real ChatGPT connector at a throwaway Cognito client on
    a staging deploy and observe what it requires:
    - if it **accepts a manual `client_id`** → pre-register one "NiChart MCP" app
      client in Cognito and ship that (no extra infra);
    - if it **mandates DCR** → build a thin RFC 7591 shim in front of Cognito
      (returns a pre-registered client / proxies authorize + token to Cognito).
    Do **not** build the shim speculatively — the test decides.
12. End-to-end test with the actual target connector (ChatGPT) against staging.

---

## Cross-cutting

- **Config/flags:** `NICHART_MCP_HTTP_ENABLED` (off), `NICHART_MCP_HTTP_PATH`
  (`/mcp`), and the Cognito MCP resource-server id/scope.
- **Infra:** no new container — the existing API image serves `/mcp`. Confirm the
  ALB/ingress **streams** (Streamable HTTP uses chunked/SSE; don't buffer; raise
  idle timeout). Add `/mcp` + the metadata routes to CORS if browser hosts connect.
- **Security:** opt-in default; never enable without auth on a public bind; all
  token validation via Cognito JWKS; no secrets baked into the image.
- **Local vs cloud:** in local mode the endpoint can run auth-free (`LOCAL_USER`);
  in cloud mode auth (Phase B/C) is mandatory. Gate accordingly.

## Decisions (resolved)
1. **DCR handling:** de-risk by testing a real ChatGPT connector first (Phase C
   #11); then a pre-registered client if manual `client_id` is accepted, else a
   DCR shim. Do not build the shim until the test says it's required.
2. **Token:** validate Cognito **access tokens** with a custom resource-server
   scope (`token_use=access`; access tokens carry no `aud`).
3. **Tool surface / remote run:** one tool module; keep the full set and **add an
   `upload` tool**; remote `run_pipeline` operates on the uploaded cloud project
   (no local path).

## Remaining design details (settle during implementation; not blockers)
- `upload` ingestion mechanism — presigned upload URL / staging ticket (leaning)
  vs inline/chunked bytes for small inputs.
- Pre-registered-client vs DCR-shim — determined by the Phase C #11 connector test.

## Suggested sequencing
Phase A is small and unblocks internal/local testing. Phase B delivers a usable
authenticated endpoint for custom clients. **De-risk the DCR question (Phase C
#11) early** — test a real ChatGPT connector against a throwaway Cognito client —
because it may dictate whether a DCR shim is required, which is the largest piece
of work.
