import pytest
from pydantic import ValidationError

from app.core.config import Settings


def test_worker_lease_configuration_accepts_valid_values() -> None:
    settings = Settings(worker_lease_seconds=30, worker_heartbeat_seconds=10)

    assert settings.worker_lease_seconds == 30
    assert settings.worker_heartbeat_seconds == 10


def test_debug_defaults_to_disabled() -> None:
    assert Settings().debug is False


def test_production_configuration_rejects_unsafe_auth_and_defaults() -> None:
    production = {
        "app_env": "production",
        "database_url": "postgresql+asyncpg://user:pass@db:5432/fluxion",
        "redis_url": "redis://redis:6379/0",
        "jwt_secret": "a" * 32,
    }
    assert Settings(**production).app_env == "production"

    for values in (
        {**production, "auth_enabled": False},
        {**production, "debug": True},
        {**production, "jwt_secret": "short"},
        {**production, "log_format": "text"},
        {**production, "database_url": "postgresql+asyncpg://db/fluxion_bench"},
    ):
        with pytest.raises(ValidationError):
            Settings(**values)


def test_environment_and_connection_configuration_are_validated() -> None:
    with pytest.raises(ValidationError):
        Settings(app_env="staging")
    with pytest.raises(ValidationError):
        Settings(database_url="sqlite:///fluxion")
    with pytest.raises(ValidationError):
        Settings(redis_url="http://redis:6379")
    with pytest.raises(ValidationError):
        Settings(api_port=65536)
    with pytest.raises(ValidationError):
        Settings(app_env="benchmark", database_url="postgresql+asyncpg://db/fluxion")

    for values in (
        {"database_pool_size": 0},
        {"database_max_overflow": -1},
        {"database_pool_timeout_seconds": 0},
    ):
        with pytest.raises(ValidationError):
            Settings(**values)


def test_render_port_is_used_when_api_port_is_not_set(monkeypatch) -> None:
    monkeypatch.delenv("API_PORT", raising=False)
    monkeypatch.setenv("PORT", "10000")

    assert Settings(_env_file=None).api_port == 10000


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
        {"dispatch_reconcile_after_seconds": 0},
        {"dispatch_reconcile_batch_size": 0},
        {"dispatch_reconcile_poll_interval_seconds": 0},
        {"lease_reaper_interval_seconds": 0},
        {"worker_concurrency": 0},
        {"worker_shutdown_grace_seconds": 0},
        {"scheduler_max_dispatch_per_run": 0},
        {"scheduler_max_dispatch_per_tick": 0},
        {"scheduler_max_dispatch_per_run": 2, "scheduler_max_dispatch_per_tick": 1},
        {"dispatch_queue_high_watermark": 0},
    )

    for values in invalid_values:
        with pytest.raises(ValidationError):
            Settings(**values)


def test_worker_and_scheduler_configuration_accepts_bounded_values() -> None:
    settings = Settings(
        worker_concurrency=2,
        worker_shutdown_grace_seconds=5,
        scheduler_max_dispatch_per_run=2,
        scheduler_max_dispatch_per_tick=4,
        dispatch_queue_high_watermark=10,
    )

    assert settings.worker_concurrency == 2
    assert settings.scheduler_max_dispatch_per_run == 2
    assert settings.scheduler_max_dispatch_per_tick == 4


def test_dispatch_reconciliation_settings_have_safe_defaults() -> None:
    settings = Settings()

    assert settings.dispatch_reconcile_after_seconds > 0
    assert settings.dispatch_reconcile_batch_size > 0
    assert settings.dispatch_reconcile_poll_interval_seconds > 0


def test_run_coordinator_settings_require_a_heartbeat_before_expiry() -> None:
    settings = Settings()

    assert settings.run_coordinator_lease_seconds > 0
    assert settings.run_coordinator_heartbeat_seconds > 0
    assert (
        settings.run_coordinator_heartbeat_seconds
        < settings.run_coordinator_lease_seconds
    )
    assert settings.run_coordinator_poll_interval_seconds > 0
    assert settings.run_coordinator_batch_size > 0

    with pytest.raises(ValidationError):
        Settings(run_coordinator_lease_seconds=10, run_coordinator_heartbeat_seconds=10)


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


def test_demo_configuration_validation() -> None:
    settings = Settings(
        fluxion_api_url="http://localhost:8001",
        fluxion_api_token="token",
        demo_timeout_seconds=5,
        demo_poll_seconds=0.1,
        max_task_result_bytes=1024,
        max_workflow_input_bytes=2048,
    )

    assert settings.fluxion_api_url == "http://localhost:8001"
    assert settings.fluxion_api_token == "token"
    assert settings.max_task_result_bytes == 1024
    assert settings.max_workflow_input_bytes == 2048

    for values in (
        {"demo_timeout_seconds": 0},
        {"demo_poll_seconds": 0},
        {"max_task_result_bytes": 0},
        {"max_workflow_input_bytes": 0},
    ):
        with pytest.raises(ValidationError):
            Settings(**values)
