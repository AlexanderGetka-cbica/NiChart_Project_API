"""
Shared pytest fixtures.

Two client fixtures are provided:

``local_client``
    Runs the app in local mode. No auth header needed.

``cloud_client``
    Runs the app in cloud mode with a real ``CognitoVerifier`` pre-seeded with
    test JWKS. Send JWTs minted by ``make_id_token(rsa_key, ...)`` to trigger
    the full JWT parsing and verification path.

``make_id_token``
    Factory fixture. Call it to generate a signed test JWT:
    ``token = make_id_token(sub="alice", email="a@b.com")``
"""

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
import pytest
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm

from app.auth.dependencies import CurrentUser, require_auth
from app.backends.base import JobBackend, JobHandle, ToolSpec

# Fixed test identity — keeps path assertions deterministic regardless of the
# OS user running inside the test container.
_TEST_USER = CurrentUser(sub="LOCAL_USER", token="")


async def _override_require_auth() -> CurrentUser:
    return _TEST_USER


# ── Mock backend (no Docker / AWS required in tests) ─────────────────────────

class _MockJobHandle(JobHandle):
    @property
    def job_id(self) -> str:
        return "mock-job-001"

    async def status(self) -> str:
        return "succeeded"

    async def logs(self, tail: int = 200) -> str:
        return "Mock job completed successfully."

    async def cancel(self) -> None:
        pass


class MockBackend(JobBackend):
    """Immediately-succeeding backend for use in unit tests."""

    async def submit(
        self,
        tool_spec: ToolSpec,
        mount_paths: dict[str, str],
        params: dict[str, Any],
        num_subjects: int = 1,
        user_token: str | None = None,
        extra_readonly_mounts: list[str] | None = None,
        study_id: str | None = None,
    ) -> _MockJobHandle:
        return _MockJobHandle()

TEST_ISSUER    = "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_BSBhcKA66"
TEST_KID       = "test-key-1"
TEST_CLIENT_ID = "test-client-id"


# ── RSA key pair (generated once per session) ─────────────────────────────────

@pytest.fixture(scope="session")
def rsa_key():
    """RSA-2048 private key for signing test JWTs."""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.asymmetric import rsa

    return rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend(),
    )


@pytest.fixture(scope="session")
def jwks(rsa_key):
    """Fake JWKS containing the test public key."""
    jwk = json.loads(RSAAlgorithm.to_jwk(rsa_key.public_key()))
    jwk["kid"] = TEST_KID
    jwk["use"] = "sig"
    return {"keys": [jwk]}


# ── Token factory ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def make_id_token(rsa_key):
    """
    Returns a callable that mints signed Cognito-style ID tokens.

    Usage::

        token = make_id_token(sub="user-123", email="u@example.com")
        token_expired = make_id_token(expired=True)
        token_access  = make_id_token(token_use="access")  # wrong type → 401
    """

    def _make(
        sub: str = "test-sub",
        email: str = "test@example.com",
        groups: list[str] | None = None,
        token_use: str = "id",
        expired: bool = False,
        issuer: str = TEST_ISSUER,
    ) -> str:
        now = datetime.now(timezone.utc)
        exp = now - timedelta(hours=1) if expired else now + timedelta(hours=1)
        payload = {
            "sub": sub,
            "email": email,
            "cognito:username": sub,
            "cognito:groups": groups or [],
            "token_use": token_use,
            "iss": issuer,
            "aud": TEST_CLIENT_ID,
            "iat": int(now.timestamp()),
            "exp": int(exp.timestamp()),
        }
        return jwt.encode(payload, rsa_key, algorithm="RS256", headers={"kid": TEST_KID})

    return _make


# ── App clients ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def local_client():
    """TestClient with NICHART_EXECUTION_MODE=local (no auth)."""
    from app.config import Settings, get_settings
    from app.main import create_app

    app = create_app()
    app.dependency_overrides[get_settings] = lambda: Settings(execution_mode="local")
    app.dependency_overrides[require_auth] = _override_require_auth

    with TestClient(app, raise_server_exceptions=True) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture
def job_client(tmp_path):
    """
    Local-mode TestClient with MockBackend and an isolated data root.

    Background tasks complete synchronously within each request, so tests can
    check final run state immediately after a submit call.
    """
    from app.backends import get_backend
    from app.config import Settings, get_settings
    from app.main import create_app
    from app.services import job_service

    app = create_app()
    app.dependency_overrides[get_settings] = lambda: Settings(
        execution_mode="local",
        data_root=tmp_path,
    )
    app.dependency_overrides[get_backend] = lambda: MockBackend()
    app.dependency_overrides[require_auth] = _override_require_auth

    with TestClient(app, raise_server_exceptions=True) as client:
        # The lifespan calls get_settings() directly (bypassing DI), so _data_root
        # may point at the real default. Override it here so _save_run/_load_run
        # use the test-isolated tmp_path.
        job_service._data_root = tmp_path
        yield client

    app.dependency_overrides.clear()


@pytest.fixture
def data_client(tmp_path):
    """
    Local-mode TestClient with an isolated, writable data root at ``tmp_path``.

    The authenticated user is always ``LOCAL_USER`` (local mode bypasses auth).
    Project directories live at ``tmp_path / "LOCAL_USER" / {project_name}/``.
    """
    from app.config import Settings, get_settings
    from app.main import create_app

    app = create_app()
    app.dependency_overrides[get_settings] = lambda: Settings(
        execution_mode="local",
        data_root=tmp_path,
    )
    app.dependency_overrides[require_auth] = _override_require_auth

    with TestClient(app, raise_server_exceptions=True) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture
def cloud_client(rsa_key, jwks):
    """
    TestClient with NICHART_EXECUTION_MODE=cloud and a pre-seeded test verifier.

    Auth is cookie-based (BFF pattern). Pass a session cookie per request:

        token = make_id_token(sub="alice")
        resp = cloud_client.get("/projects", cookies={"session": token})

    The verifier's JWKS cache is pre-populated so no network calls are made.
    """
    from app.auth.cognito import CognitoVerifier
    from app.auth.dependencies import get_verifier
    from app.config import Settings, get_settings
    from app.main import create_app

    app = create_app()
    app.dependency_overrides[get_settings] = lambda: Settings(
        execution_mode="cloud",
        cognito_domain="https://test.auth.us-east-1.amazoncognito.com",
        cognito_client_id=TEST_CLIENT_ID,
        cookie_secure=False,  # allow plain-HTTP test requests
    )

    verifier = CognitoVerifier(jwks_url="http://unused-in-tests", issuer=TEST_ISSUER, client_id=TEST_CLIENT_ID)
    verifier._cache.update(jwks)

    app.dependency_overrides[get_verifier] = lambda: verifier

    with TestClient(app, raise_server_exceptions=True) as client:
        yield client

    app.dependency_overrides.clear()
