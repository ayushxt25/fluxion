from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Fluxion"
    app_env: str = "development"
    debug: bool = True
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    database_url: str = "postgresql+asyncpg://fluxion:fluxion@localhost:5432/fluxion"
    test_database_url: str | None = None
    redis_url: str = "redis://localhost:6379/0"
    dispatch_queue_name: str = "fluxion:dispatch"
    worker_lease_seconds: float = 30
    worker_heartbeat_seconds: float = 10
    scheduler_poll_seconds: float = 1.0
    outbox_poll_seconds: float = 1.0
    outbox_batch_size: int = 100
    outbox_claim_seconds: float = 30
    lease_reaper_interval_seconds: float = 5
    auth_enabled: bool = True
    jwt_secret: str | None = None
    jwt_algorithm: str = "HS256"
    jwt_issuer: str = "fluxion"
    jwt_audience: str = "fluxion-api"
    jwt_access_token_minutes: int = 60
    rate_limit_enabled: bool = True
    rate_limit_viewer_per_minute: int = 120
    rate_limit_operator_per_minute: int = 90
    rate_limit_admin_per_minute: int = 60
    rate_limit_ops_per_minute: int = 20
    max_request_body_bytes: int = 1048576
    log_level: str = "INFO"
    log_format: str = "json"
    readiness_timeout_seconds: float = 2

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @model_validator(mode="after")
    def validate_worker_lease_settings(self) -> "Settings":
        if self.worker_lease_seconds <= 0:
            raise ValueError("WORKER_LEASE_SECONDS must be positive.")
        if self.api_port <= 0:
            raise ValueError("API_PORT must be positive.")
        if self.worker_heartbeat_seconds <= 0:
            raise ValueError("WORKER_HEARTBEAT_SECONDS must be positive.")
        if self.worker_heartbeat_seconds >= self.worker_lease_seconds:
            raise ValueError(
                "WORKER_HEARTBEAT_SECONDS must be less than WORKER_LEASE_SECONDS."
            )
        if self.scheduler_poll_seconds <= 0:
            raise ValueError("SCHEDULER_POLL_SECONDS must be positive.")
        if self.outbox_poll_seconds <= 0:
            raise ValueError("OUTBOX_POLL_SECONDS must be positive.")
        if self.outbox_batch_size <= 0:
            raise ValueError("OUTBOX_BATCH_SIZE must be positive.")
        if self.outbox_claim_seconds <= 0:
            raise ValueError("OUTBOX_CLAIM_SECONDS must be positive.")
        if self.lease_reaper_interval_seconds <= 0:
            raise ValueError("LEASE_REAPER_INTERVAL_SECONDS must be positive.")
        if self.jwt_algorithm != "HS256":
            raise ValueError("JWT_ALGORITHM must be HS256.")
        if self.jwt_access_token_minutes <= 0:
            raise ValueError("JWT_ACCESS_TOKEN_MINUTES must be positive.")
        if self.rate_limit_viewer_per_minute <= 0:
            raise ValueError("RATE_LIMIT_VIEWER_PER_MINUTE must be positive.")
        if self.rate_limit_operator_per_minute <= 0:
            raise ValueError("RATE_LIMIT_OPERATOR_PER_MINUTE must be positive.")
        if self.rate_limit_admin_per_minute <= 0:
            raise ValueError("RATE_LIMIT_ADMIN_PER_MINUTE must be positive.")
        if self.rate_limit_ops_per_minute <= 0:
            raise ValueError("RATE_LIMIT_OPS_PER_MINUTE must be positive.")
        if self.max_request_body_bytes <= 0:
            raise ValueError("MAX_REQUEST_BODY_BYTES must be positive.")
        if self.log_level.upper() not in {
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
        }:
            raise ValueError("LOG_LEVEL must be a valid logging level.")
        self.log_level = self.log_level.upper()
        self.log_format = self.log_format.lower()
        if self.log_format not in {"json", "text"}:
            raise ValueError("LOG_FORMAT must be either json or text.")
        if self.readiness_timeout_seconds <= 0:
            raise ValueError("READINESS_TIMEOUT_SECONDS must be positive.")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
