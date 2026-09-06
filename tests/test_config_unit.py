import pytest
from pydantic import ValidationError

from app.core.config import Settings


def test_worker_lease_configuration_accepts_valid_values() -> None:
    settings = Settings(worker_lease_seconds=30, worker_heartbeat_seconds=10)

    assert settings.worker_lease_seconds == 30
    assert settings.worker_heartbeat_seconds == 10


def test_worker_lease_configuration_rejects_invalid_values() -> None:
    with pytest.raises(ValidationError):
        Settings(worker_lease_seconds=0)
    with pytest.raises(ValidationError):
        Settings(worker_heartbeat_seconds=0)
    with pytest.raises(ValidationError):
        Settings(worker_lease_seconds=10, worker_heartbeat_seconds=10)


def test_service_loop_configuration_rejects_invalid_values() -> None:
    invalid_values = (
        {"scheduler_poll_seconds": 0},
        {"outbox_poll_seconds": 0},
        {"outbox_batch_size": 0},
        {"outbox_claim_seconds": 0},
        {"lease_reaper_interval_seconds": 0},
    )

    for values in invalid_values:
        with pytest.raises(ValidationError):
            Settings(**values)


def test_observability_configuration_validation() -> None:
    settings = Settings(
        log_level="debug",
        log_format="TEXT",
        readiness_timeout_seconds=1,
    )

    assert settings.log_level == "DEBUG"
    assert settings.log_format == "text"

    for values in (
        {"log_level": "TRACE"},
        {"log_format": "xml"},
        {"readiness_timeout_seconds": 0},
    ):
        with pytest.raises(ValidationError):
            Settings(**values)
