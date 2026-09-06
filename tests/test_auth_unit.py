from datetime import UTC, datetime, timedelta

import pytest

from app.core.config import Settings
from app.security.auth import (
    AuthenticationError,
    _encode,
    authenticate_bearer_token,
    create_access_token,
)
from app.security.models import Role


def settings(**overrides) -> Settings:
    values = {
        "auth_enabled": True,
        "jwt_secret": "unit-secret",
        "jwt_issuer": "fluxion",
        "jwt_audience": "fluxion-api",
    }
    values.update(overrides)
    return Settings(**values)


def bearer(token: str) -> str:
    return f"Bearer {token}"


def test_valid_token_returns_principal() -> None:
    token = create_access_token("user-1", Role.OPERATOR, settings=settings())

    principal = authenticate_bearer_token(bearer(token), settings=settings())

    assert principal.subject == "user-1"
    assert principal.role == Role.OPERATOR


def test_missing_malformed_bad_signature_and_expired_tokens_rejected() -> None:
    token = create_access_token(
        "user-1",
        Role.VIEWER,
        settings=settings(),
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )

    with pytest.raises(AuthenticationError):
        authenticate_bearer_token(None, settings=settings())
    with pytest.raises(AuthenticationError):
        authenticate_bearer_token("Bearer nope", settings=settings())
    with pytest.raises(AuthenticationError):
        authenticate_bearer_token(bearer(f"{token}x"), settings=settings())
    with pytest.raises(AuthenticationError):
        authenticate_bearer_token(
            bearer(token),
            settings=settings(),
            now=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=61),
        )


def test_wrong_issuer_and_audience_rejected() -> None:
    wrong_issuer = create_access_token(
        "user-1",
        Role.VIEWER,
        settings=settings(jwt_issuer="other"),
    )
    wrong_audience = create_access_token(
        "user-1",
        Role.VIEWER,
        settings=settings(jwt_audience="other"),
    )

    with pytest.raises(AuthenticationError):
        authenticate_bearer_token(bearer(wrong_issuer), settings=settings())
    with pytest.raises(AuthenticationError):
        authenticate_bearer_token(bearer(wrong_audience), settings=settings())


def test_unsupported_role_rejected() -> None:
    token = _encode(
        {
            "sub": "user-1",
            "role": "superuser",
            "iss": "fluxion",
            "aud": "fluxion-api",
            "iat": 1,
            "exp": 4_102_444_800,
        },
        settings(),
    )

    with pytest.raises(AuthenticationError):
        authenticate_bearer_token(bearer(token), settings=settings())


def test_auth_disabled_returns_internal_admin() -> None:
    principal = authenticate_bearer_token(None, settings=settings(auth_enabled=False))

    assert principal.subject == "internal"
    assert principal.role == Role.ADMIN
