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
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
