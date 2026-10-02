from functools import lru_cache
from typing import Literal
from urllib.parse import urlparse

from pydantic import ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Fluxion"
    app_env: Literal["development", "test", "benchmark", "production"] = "development"
    debug: bool = False
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    database_url: str = "postgresql+asyncpg://fluxion:fluxion@localhost:5432/fluxion"
    test_database_url: str | None = None
    redis_url: str = "redis://localhost:6379/0"
    dispatch_queue_name: str = "fluxion:dispatch"
    worker_lease_seconds: float = 30
    worker_heartbeat_seconds: float = 10
    worker_concurrency: int = 4
    worker_shutdown_grace_seconds: float = 30
    scheduler_poll_seconds: float = 1.0
    scheduler_max_dispatch_per_run: int = 10
    scheduler_max_dispatch_per_tick: int = 100
    dispatch_queue_high_watermark: int = 1000
    outbox_poll_seconds: float = 1.0
    outbox_batch_size: int = 100
    outbox_claim_seconds: float = 30
    dispatch_reconcile_after_seconds: float = 120
    dispatch_reconcile_batch_size: int = 100
    dispatch_reconcile_poll_interval_seconds: float = 10
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
    fluxion_api_url: str = "http://localhost:8000"
    fluxion_api_token: str | None = None
    demo_timeout_seconds: float = 30
    demo_poll_seconds: float = 1.0
    max_task_result_bytes: int = 262144
    max_workflow_input_bytes: int = 262144
    sse_poll_interval_seconds: float = 0.5
    sse_heartbeat_seconds: float = 15
    sse_event_batch_size: int = 100
    task_log_buffer_size: int = 50
    task_log_max_message_bytes: int = 8192
    task_log_max_fields_bytes: int = 16384
    task_log_max_entries_per_attempt: int = 10000
    webhook_enabled: bool = True
    webhook_request_timeout_seconds: float = 10
    webhook_max_attempts: int = 8
    webhook_initial_backoff_seconds: float = 1
    webhook_backoff_multiplier: float = 2
    webhook_max_backoff_seconds: float = 300
    webhook_claim_seconds: float = 30
    webhook_batch_size: int = 50
    webhook_poll_interval_seconds: float = 1
    webhook_allow_insecure_http: bool = False
    webhook_allow_private_networks: bool = False
    retention_enabled: bool = False
    retention_completed_run_days: int = 30
    retention_task_log_days: int = 14
    retention_run_event_days: int = 30
    retention_audit_event_days: int = 90
    retention_webhook_delivery_days: int = 30
    retention_outbox_days: int = 7
    retention_batch_size: int = 500
    retention_poll_interval_seconds: float = 3600
    schedule_runner_poll_seconds: float = 1
    run_coordinator_lease_seconds: float = 30
    run_coordinator_heartbeat_seconds: float = 10
    run_coordinator_poll_interval_seconds: float = 1
    run_coordinator_batch_size: int = 100

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @model_validator(mode="after")
    def validate_worker_lease_settings(self) -> "Settings":
        database = urlparse(self.database_url)
        redis = urlparse(self.redis_url)
        if database.scheme not in {"postgresql", "postgresql+asyncpg"}:
            raise ValueError("DATABASE_URL must use PostgreSQL.")
        if redis.scheme not in {"redis", "rediss"}:
            raise ValueError("REDIS_URL must use redis or rediss.")
        if not database.path or database.path == "/":
            raise ValueError("DATABASE_URL must include a database name.")
        if not redis.hostname:
            raise ValueError("REDIS_URL must include a host.")
        if self.worker_lease_seconds <= 0:
            raise ValueError("WORKER_LEASE_SECONDS must be positive.")
        if not self.api_host.strip():
            raise ValueError("API_HOST must not be empty.")
        if not 0 < self.api_port <= 65535:
            raise ValueError("API_PORT must be between 1 and 65535.")
        if self.worker_heartbeat_seconds <= 0:
            raise ValueError("WORKER_HEARTBEAT_SECONDS must be positive.")
        if self.worker_heartbeat_seconds >= self.worker_lease_seconds:
            raise ValueError(
                "WORKER_HEARTBEAT_SECONDS must be less than WORKER_LEASE_SECONDS."
            )
        if self.worker_concurrency < 1:
            raise ValueError("WORKER_CONCURRENCY must be at least 1.")
        if self.worker_shutdown_grace_seconds <= 0:
            raise ValueError("WORKER_SHUTDOWN_GRACE_SECONDS must be positive.")
        if self.scheduler_poll_seconds <= 0:
            raise ValueError("SCHEDULER_POLL_SECONDS must be positive.")
        if self.scheduler_max_dispatch_per_run < 1:
            raise ValueError("SCHEDULER_MAX_DISPATCH_PER_RUN must be at least 1.")
        if self.scheduler_max_dispatch_per_tick < 1:
            raise ValueError("SCHEDULER_MAX_DISPATCH_PER_TICK must be at least 1.")
        if self.scheduler_max_dispatch_per_run > self.scheduler_max_dispatch_per_tick:
            raise ValueError(
                "SCHEDULER_MAX_DISPATCH_PER_RUN must not exceed "
                "SCHEDULER_MAX_DISPATCH_PER_TICK."
            )
        if self.dispatch_queue_high_watermark < 1:
            raise ValueError("DISPATCH_QUEUE_HIGH_WATERMARK must be at least 1.")
        if self.outbox_poll_seconds <= 0:
            raise ValueError("OUTBOX_POLL_SECONDS must be positive.")
        if self.outbox_batch_size <= 0:
            raise ValueError("OUTBOX_BATCH_SIZE must be positive.")
        if self.outbox_claim_seconds <= 0:
            raise ValueError("OUTBOX_CLAIM_SECONDS must be positive.")
        if self.dispatch_reconcile_after_seconds <= 0:
            raise ValueError("DISPATCH_RECONCILE_AFTER_SECONDS must be positive.")
        if self.dispatch_reconcile_batch_size < 1:
            raise ValueError("DISPATCH_RECONCILE_BATCH_SIZE must be at least 1.")
        if self.dispatch_reconcile_poll_interval_seconds <= 0:
            raise ValueError(
                "DISPATCH_RECONCILE_POLL_INTERVAL_SECONDS must be positive."
            )
        if self.lease_reaper_interval_seconds <= 0:
            raise ValueError("LEASE_REAPER_INTERVAL_SECONDS must be positive.")
        if self.schedule_runner_poll_seconds <= 0:
            raise ValueError("SCHEDULE_RUNNER_POLL_SECONDS must be positive.")
        if self.run_coordinator_lease_seconds <= 0:
            raise ValueError("RUN_COORDINATOR_LEASE_SECONDS must be positive.")
        if self.run_coordinator_heartbeat_seconds <= 0:
            raise ValueError("RUN_COORDINATOR_HEARTBEAT_SECONDS must be positive.")
        if self.run_coordinator_heartbeat_seconds >= self.run_coordinator_lease_seconds:
            raise ValueError(
                "RUN_COORDINATOR_HEARTBEAT_SECONDS must be less than "
                "RUN_COORDINATOR_LEASE_SECONDS."
            )
        if self.run_coordinator_poll_interval_seconds <= 0:
            raise ValueError("RUN_COORDINATOR_POLL_INTERVAL_SECONDS must be positive.")
        if self.run_coordinator_batch_size < 1:
            raise ValueError("RUN_COORDINATOR_BATCH_SIZE must be at least 1.")
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
        if self.demo_timeout_seconds <= 0:
            raise ValueError("DEMO_TIMEOUT_SECONDS must be positive.")
        if self.demo_poll_seconds <= 0:
            raise ValueError("DEMO_POLL_SECONDS must be positive.")
        if self.max_task_result_bytes <= 0:
            raise ValueError("MAX_TASK_RESULT_BYTES must be positive.")
        if self.max_workflow_input_bytes <= 0:
            raise ValueError("MAX_WORKFLOW_INPUT_BYTES must be positive.")
        if self.sse_poll_interval_seconds <= 0:
            raise ValueError("SSE_POLL_INTERVAL_SECONDS must be positive.")
        if self.sse_heartbeat_seconds <= 0:
            raise ValueError("SSE_HEARTBEAT_SECONDS must be positive.")
        if self.sse_event_batch_size < 1:
            raise ValueError("SSE_EVENT_BATCH_SIZE must be at least 1.")
        if self.task_log_buffer_size < 1:
            raise ValueError("TASK_LOG_BUFFER_SIZE must be at least 1.")
        if self.task_log_max_message_bytes < 1:
            raise ValueError("TASK_LOG_MAX_MESSAGE_BYTES must be at least 1.")
        if self.task_log_max_fields_bytes < 1:
            raise ValueError("TASK_LOG_MAX_FIELDS_BYTES must be at least 1.")
        if self.task_log_max_entries_per_attempt < 1:
            raise ValueError("TASK_LOG_MAX_ENTRIES_PER_ATTEMPT must be at least 1.")
        if self.webhook_request_timeout_seconds <= 0:
            raise ValueError("WEBHOOK_REQUEST_TIMEOUT_SECONDS must be positive.")
        if self.webhook_max_attempts < 1 or self.webhook_initial_backoff_seconds <= 0:
            raise ValueError("Webhook retry settings must be positive.")
        if self.webhook_backoff_multiplier < 1 or self.webhook_max_backoff_seconds <= 0:
            raise ValueError("Webhook backoff settings are invalid.")
        if self.webhook_claim_seconds <= 0 or self.webhook_batch_size < 1:
            raise ValueError("Webhook claim settings are invalid.")
        if self.webhook_poll_interval_seconds <= 0:
            raise ValueError("WEBHOOK_POLL_INTERVAL_SECONDS must be positive.")
        if any(
            value < 1
            for value in (
                self.retention_completed_run_days,
                self.retention_task_log_days,
                self.retention_run_event_days,
                self.retention_audit_event_days,
                self.retention_webhook_delivery_days,
                self.retention_outbox_days,
            )
        ):
            raise ValueError("Retention day settings must be at least one.")
        if self.retention_batch_size < 1 or self.retention_poll_interval_seconds <= 0:
            raise ValueError("Retention batch and poll settings are invalid.")
        database_name = database.path.rstrip("/").rsplit("/", 1)[-1]
        if self.app_env == "benchmark" and not database_name.endswith("_bench"):
            raise ValueError("Benchmark deployments require a *_bench DATABASE_URL.")
        if self.app_env == "production":
            if not self.auth_enabled:
                raise ValueError("AUTH_ENABLED must be true in production.")
            if self.debug:
                raise ValueError("DEBUG must be false in production.")
            if self.log_format != "json":
                raise ValueError("LOG_FORMAT must be json in production.")
            if (
                not self.jwt_secret
                or len(self.jwt_secret) < 32
                or self.jwt_secret.lower()
                in {
                    "replace-with-a-strong-development-secret",
                    "secret",
                    "changeme",
                }
            ):
                raise ValueError(
                    "A strong non-default JWT_SECRET is required in production."
                )
            if database_name.endswith("_bench"):
                raise ValueError(
                    "Production DATABASE_URL must not target a benchmark DB."
                )
            if self.database_url == (
                "postgresql+asyncpg://fluxion:fluxion@localhost:5432/fluxion"
            ):
                raise ValueError(
                    "Production DATABASE_URL must not use the development default."
                )
            if self.redis_url == "redis://localhost:6379/0":
                raise ValueError(
                    "Production REDIS_URL must not use the development default."
                )
        return self


@lru_cache
def get_settings() -> Settings:
    try:
        return Settings()
    except ValidationError as exc:
        reasons = "; ".join(
            error["msg"]
            for error in exc.errors()
        )
        raise RuntimeError(
            f"Fluxion configuration is invalid: {reasons}"
        ) from None
