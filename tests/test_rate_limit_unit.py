import pytest

from app.core.config import Settings
from app.security.models import Principal, Role
from app.security.rate_limit import limit_for_role


def test_role_rate_limits_and_ops_limit() -> None:
    settings = Settings(
        jwt_secret="secret",
        rate_limit_viewer_per_minute=10,
        rate_limit_operator_per_minute=9,
        rate_limit_admin_per_minute=8,
        rate_limit_ops_per_minute=2,
    )

    assert limit_for_role(Principal("viewer", Role.VIEWER), settings=settings) == 10
    assert limit_for_role(Principal("operator", Role.OPERATOR), settings=settings) == 9
    assert limit_for_role(Principal("admin", Role.ADMIN), settings=settings) == 8
    assert (
        limit_for_role(Principal("admin", Role.ADMIN), ops=True, settings=settings)
        == 2
    )


def test_rate_limit_settings_must_be_positive() -> None:
    with pytest.raises(ValueError):
        Settings(jwt_secret="secret", rate_limit_viewer_per_minute=0)
    with pytest.raises(ValueError):
        Settings(jwt_secret="secret", rate_limit_ops_per_minute=0)
    with pytest.raises(ValueError):
        Settings(jwt_secret="secret", max_request_body_bytes=0)
