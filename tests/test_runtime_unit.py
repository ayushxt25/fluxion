import asyncio
import tomllib
from pathlib import Path

from app.core.config import Settings
from app.engine.registry import TaskRegistry
from app.runtime import bootstrap
from app.runtime.worker import run_worker_loop
from app.tasks.registry import build_task_registry

ROOT = Path(__file__).resolve().parents[1]


def test_console_scripts_are_registered() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())

    assert data["project"]["scripts"] == {
        "fluxion": "app.cli:main",
        "fluxion-api": "app.runtime.api:main",
        "fluxion-scheduler": "app.runtime.scheduler:main",
        "fluxion-publisher": "app.runtime.publisher:main",
        "fluxion-reaper": "app.runtime.reaper:main",
        "fluxion-worker": "app.runtime.worker:main",
        "fluxion-demo": "app.runtime.demo:cli",
    }


def test_task_registry_hook_builds_registry_once_per_call() -> None:
    first = build_task_registry()
    second = build_task_registry()

    assert isinstance(first, TaskRegistry)
    assert isinstance(second, TaskRegistry)
    assert first is not second


def test_worker_loop_exits_without_accepting_after_stop() -> None:
    class FakeWorker:
        def __init__(self) -> None:
            self.calls = 0

        async def run_once(self, timeout: float | None = None):
            assert timeout == 1
            self.calls += 1
            stop_event.set()

    async def scenario() -> int:
        await run_worker_loop(worker, stop_event)
        await run_worker_loop(worker, stop_event)
        return worker.calls

    stop_event = asyncio.Event()
    worker = FakeWorker()

    assert asyncio.run(scenario()) == 1


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


def test_docker_compose_defines_required_services_and_migration() -> None:
    compose = (ROOT / "docker-compose.yml").read_text()
    for service in (
        "postgres:",
        "redis:",
        "migrate:",
        "api:",
        "scheduler:",
        "publisher:",
        "reaper:",
        "worker:",
    ):
        assert f"  {service}" in compose
    assert 'command: ["alembic", "upgrade", "head"]' in compose
    assert "condition: service_completed_successfully" in compose
    assert "/health" in compose
    assert "JWT_SECRET: ${JWT_SECRET:?JWT_SECRET must be set}" in compose
    assert "FLUSHDB" not in compose
    assert "FLUSHALL" not in compose


def test_dockerfile_uses_single_non_root_runtime_image() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()

    assert "FROM python:3.11-slim" in dockerfile
    assert "USER fluxion" in dockerfile
    assert 'CMD ["fluxion-api"]' in dockerfile
    assert "JWT_SECRET" not in dockerfile
