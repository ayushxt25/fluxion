import base64
import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.config import Settings, get_settings
from app.security.models import Principal, Role


class AuthenticationError(Exception):
    """Raised when a bearer token is missing or invalid."""


class AuthorizationError(Exception):
    """Raised when an authenticated principal lacks permission."""


def create_access_token(
    subject: str,
    role: Role | str,
    *,
    settings: Settings | None = None,
    now: datetime | None = None,
) -> str:
    settings = settings or get_settings()
    issued_at = now or datetime.now(UTC)
    expires_at = issued_at + timedelta(minutes=settings.jwt_access_token_minutes)
    role_value = Role(role).value
    payload = {
        "sub": subject,
        "role": role_value,
        "iss": settings.jwt_issuer,
        "aud": settings.jwt_audience,
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    return _encode(payload, settings)


def authenticate_bearer_token(
    authorization: str | None,
    *,
    settings: Settings | None = None,
    now: datetime | None = None,
) -> Principal:
    settings = settings or get_settings()
    if not settings.auth_enabled:
        return Principal(subject="internal", role=Role.ADMIN)
    if not authorization or not authorization.startswith("Bearer "):
        raise AuthenticationError("Missing bearer token.")
    token = authorization.removeprefix("Bearer ").strip()
    payload = _decode(token, settings)
    current_time = now or datetime.now(UTC)
    try:
        role = Role(payload["role"])
        subject = str(payload["sub"])
        issued_at = datetime.fromtimestamp(int(payload["iat"]), UTC)
        expires_at = datetime.fromtimestamp(int(payload["exp"]), UTC)
    except (KeyError, TypeError, ValueError) as exc:
        raise AuthenticationError("Token claims are invalid.") from exc
    if payload.get("iss") != settings.jwt_issuer:
        raise AuthenticationError("Token issuer is invalid.")
    if payload.get("aud") != settings.jwt_audience:
        raise AuthenticationError("Token audience is invalid.")
    if expires_at <= current_time:
        raise AuthenticationError("Token is expired.")
    return Principal(
        subject=subject,
        role=role,
        issued_at=issued_at,
        expires_at=expires_at,
    )


def _encode(payload: dict[str, Any], settings: Settings) -> str:
    secret = _secret(settings)
    header = {"alg": settings.jwt_algorithm, "typ": "JWT"}
    signing_input = ".".join(
        (
            _b64_json(header),
            _b64_json(payload),
        )
    )
    signature = _sign(signing_input, secret)
    return f"{signing_input}.{signature}"


def _decode(token: str, settings: Settings) -> dict[str, Any]:
    secret = _secret(settings)
    try:
        header_part, payload_part, signature = token.split(".")
    except ValueError as exc:
        raise AuthenticationError("Token is malformed.") from exc
    signing_input = f"{header_part}.{payload_part}"
    expected = _sign(signing_input, secret)
    if not hmac.compare_digest(signature, expected):
        raise AuthenticationError("Token signature is invalid.")
    try:
        header = _b64_decode_json(header_part)
        payload = _b64_decode_json(payload_part)
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
        raise AuthenticationError("Token is malformed.") from exc
    if header.get("alg") != "HS256":
        raise AuthenticationError("Token algorithm is unsupported.")
    return payload


def _secret(settings: Settings) -> bytes:
    if not settings.jwt_secret:
        raise AuthenticationError("JWT secret is not configured.")
    return settings.jwt_secret.encode("utf-8")


def _sign(signing_input: str, secret: bytes) -> str:
    digest = hmac.new(secret, signing_input.encode("utf-8"), hashlib.sha256).digest()
    return _b64(digest)


def _b64_json(value: dict[str, Any]) -> str:
    return _b64(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )


def _b64_decode_json(value: str) -> dict[str, Any]:
    return json.loads(base64.urlsafe_b64decode(_pad(value)).decode("utf-8"))


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _pad(value: str) -> bytes:
    return (value + "=" * (-len(value) % 4)).encode("ascii")
