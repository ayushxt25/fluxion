from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Fluxion"
    app_env: str = "development"
    debug: bool = True
    database_url: str = "postgresql+asyncpg://fluxion:fluxion@localhost:5432/fluxion"
    test_database_url: str | None = None
    redis_url: str = "redis://localhost:6379/0"
    dispatch_queue_name: str = "fluxion:dispatch"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
