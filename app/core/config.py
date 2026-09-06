from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Fluxion"
    app_env: str = "development"
    debug: bool = True
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

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @model_validator(mode="after")
    def validate_worker_lease_settings(self) -> "Settings":
        if self.worker_lease_seconds <= 0:
            raise ValueError("WORKER_LEASE_SECONDS must be positive.")
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
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
