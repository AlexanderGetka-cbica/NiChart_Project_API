FROM python:3.12-slim AS base
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir --upgrade pip && \
    mkdir /certs

# ── dev target: includes dev deps, entire project mounted as volume ──────────
FROM base AS dev
COPY pyproject.toml .

# app/ and resources/ must exist before pip install: hatchling registers app/
# in the editable .pth, and its force-include for resources/ (see pyproject.toml)
# requires the source directory to exist even for an editable install.
COPY app/ app/
COPY resources/ resources/
RUN pip install --no-cache-dir -e ".[dev,mcp]"

# Source is bind-mounted at runtime; copy here only so the image is self-contained
COPY . .

# ── prod target: only runtime deps, minimal footprint ───────────────────────
FROM base AS prod
COPY pyproject.toml .
# resources/ must exist before pip install so the wheel force-include can find it.
COPY app/ app/
COPY resources/ resources/

# [mcp] ships the MCP SDK so `nichart-mcp` runs in the container (for `docker exec`
# integration with desktop LLM apps — see docs/mcp-desktop-integration.md).
RUN pip install --no-cache-dir -e ".[mcp]"

