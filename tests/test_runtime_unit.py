import asyncio
import tomllib
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from app import cli
from app.core.config import Settings
from app.engine.registry import TaskRegistry
from app.runtime import bootstrap, coordinator, demo, preflight, retention
from app.tasks.registry import build_task_registry

ROOT = Path(__file__).resolve().parents[1]


def test_console_scripts_are_registered() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())

    assert data["project"]["scripts"] == {
        "fluxion": "app.cli:main",
        "fluxion-api": "app.runtime.api:main",
        "fluxion-scheduler": "app.runtime.scheduler:main",
        "fluxion-publisher": "app.runtime.publisher:main",
        "fluxion-reconciler": "app.runtime.reconciler:main",
        "fluxion-coordinator": "app.runtime.coordinator:main",
        "fluxion-reaper": "app.runtime.reaper:main",
        "fluxion-worker": "app.runtime.worker:main",
        "fluxion-webhook": "app.runtime.webhooks:main",
        "fluxion-webhooks": "app.runtime.webhooks:main",
        "fluxion-retention": "app.runtime.retention:main",
        "fluxion-schedule-runner": "app.runtime.schedule_runner:main",
        "fluxion-demo": "app.runtime.demo:cli",
        "fluxion-benchmark": "app.runtime.benchmark:cli",
    }


def test_grouped_cli_registers_coordinator(monkeypatch) -> None:
    called: list[bool] = []
    monkeypatch.setattr("sys.argv", ["fluxion", "coordinator"])
    monkeypatch.setattr(coordinator, "main", lambda: called.append(True))

    cli.main()

    assert called == [True]


def test_grouped_cli_forwards_demo_arguments(monkeypatch) -> None:
    forwarded: list[list[str] | None] = []
    monkeypatch.setattr("sys.argv", ["fluxion", "demo", "--portfolio"])
    monkeypatch.setattr(demo, "cli", lambda argv=None: forwarded.append(argv))

    cli.main()

    assert forwarded == [["--portfolio"]]


def test_task_registry_hook_builds_registry_once_per_call() -> None:
    first = build_task_registry()
    second = build_task_registry()

    assert isinstance(first, TaskRegistry)
    assert isinstance(second, TaskRegistry)
    assert first is not second


def test_runtime_bootstrap_creates_and_closes_resources(monkeypatch) -> None:
    class FakeEngine:
        def __init__(self) -> None:
            self.disposed = False

        async def dispose(self) -> None:
            self.disposed = True

    class FakeRedisResource:
        def __init__(self, *args) -> None:
            self.args = args
            self.closed = False

        async def aclose(self) -> None:
            self.closed = True

    engine = FakeEngine()
    dispatcher = FakeRedisResource()
    limiter = FakeRedisResource()

    monkeypatch.setattr(bootstrap, "create_async_engine", lambda url: engine)
    monkeypatch.setattr(bootstrap, "RedisTaskDispatcher", lambda *args: dispatcher)
    monkeypatch.setattr(bootstrap, "RedisRateLimiter", lambda *args: limiter)

    async def scenario() -> tuple[bool, bool, bool]:
        settings = Settings(
            database_url="postgresql+asyncpg://user:pass@db:5432/fluxion",
            redis_url="redis://redis:6379/0",
        )
        async with bootstrap.runtime_resources(settings) as resources:
            assert resources.dispatcher is dispatcher
            assert resources.rate_limiter is limiter
        return engine.disposed, dispatcher.closed, limiter.closed

    assert asyncio.run(scenario()) == (True, True, True)


def test_startup_preflight_reports_sanitized_dependency_failure(monkeypatch) -> None:
    class FailingEngine:
        def connect(self):
            raise AssertionError("connection should be awaited through context manager")

    async def fail_database(*_args) -> None:
        raise preflight.StartupPreflightError("PostgreSQL is unavailable.")

    monkeypatch.setattr(preflight, "check_database", fail_database)
    settings = Settings(
        database_url="postgresql+asyncpg://user:password@db:5432/fluxion",
        redis_url="redis://redis:6379/0",
    )

    with pytest.raises(preflight.StartupPreflightError, match="PostgreSQL"):
        asyncio.run(
            preflight.preflight(
                role="worker",
                settings=settings,
                engine=FailingEngine(),
            )
        )


def test_docker_compose_defines_required_services_and_migration() -> None:
    compose = (ROOT / "docker-compose.yml").read_text()
    for service in (
        "postgres:",
        "redis:",
        "migrate:",
        "api:",
        "scheduler:",
        "publisher:",
        "reconciler:",
        "coordinator:",
        "reaper:",
        "worker:",
    ):
        assert f"  {service}" in compose
    assert 'command: ["alembic", "upgrade", "head"]' in compose
    assert "condition: service_completed_successfully" in compose
    assert "/health" in compose
    assert "JWT_SECRET: ${JWT_SECRET:?JWT_SECRET must be set}" in compose
    assert "APP_ENV: ${APP_ENV:-development}" in compose
    assert "restart: unless-stopped" in compose
    assert "FLUSHDB" not in compose
    assert "FLUSHALL" not in compose
    assert "retention:" in compose
    assert 'command: ["fluxion-reconciler"]' in compose
    assert 'command: ["fluxion-coordinator"]' in compose
    assert 'profiles: ["maintenance"]' in compose


def test_dockerfile_uses_single_non_root_runtime_image() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()

    assert "FROM python:3.11-slim" in dockerfile
    assert "USER fluxion" in dockerfile
    assert "--uid 10001 fluxion" in dockerfile
    assert 'CMD ["fluxion-api"]' in dockerfile
    assert "JWT_SECRET" not in dockerfile


def test_retention_runtime_disabled_does_not_create_database_engine(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        retention,
        "create_async_engine",
        lambda _: (_ for _ in ()).throw(AssertionError()),
    )

    asyncio.run(retention.run(settings=Settings(retention_enabled=False)))


def test_retention_runtime_runs_one_bounded_pass_without_redis(monkeypatch) -> None:
    stop_event = asyncio.Event()
    calls: list[str] = []

    class FakeEngine:
        async def dispose(self) -> None:
            calls.append("dispose")

    @asynccontextmanager
    async def fake_session():
        yield object()

    class FakeService:
        def __init__(self, repository, settings) -> None:
            assert repository._session is not None
            assert settings.retention_enabled

        async def run_retention(self):
            calls.append("run")
            stop_event.set()
            return type("Summary", (), {"total_deleted": 0})()

    monkeypatch.setattr(retention, "create_async_engine", lambda _: FakeEngine())
    monkeypatch.setattr(retention, "async_sessionmaker", lambda *_, **__: fake_session)
    monkeypatch.setattr(retention, "RetentionService", FakeService)
    
    async def fake_preflight(**_kwargs) -> None:
        return None

    monkeypatch.setattr(retention, "preflight", fake_preflight)

    asyncio.run(retention.run(stop_event, Settings(retention_enabled=True)))
    assert calls == ["run", "dispose"]
