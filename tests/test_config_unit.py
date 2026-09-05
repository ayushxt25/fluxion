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
