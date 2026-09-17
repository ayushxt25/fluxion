import pytest

from app.services.webhooks import _retry_after, validate_webhook_target


def test_webhook_target_rejects_insecure_and_loopback_by_default() -> None:
    with pytest.raises(ValueError):
        validate_webhook_target("http://example.com")
    with pytest.raises(ValueError):
        validate_webhook_target("https://127.0.0.1/hook")


def test_retry_after_delta_seconds_is_parsed() -> None:
    assert _retry_after("12") == 12
    assert _retry_after("invalid") is None
