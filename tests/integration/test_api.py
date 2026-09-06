import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import urlparse

import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
if not TEST_DATABASE_URL:
    pytest.skip("TEST_DATABASE_URL is not set", allow_module_level=True)

test_database_name = urlparse(TEST_DATABASE_URL).path.rsplit("/", maxsplit=1)[-1]
if not test_database_name.endswith("_test"):
    pytest.skip(
        "TEST_DATABASE_URL must point to a *_test database",
        allow_module_level=True,
    )

# ruff: noqa: E402
from datetime import UTC, datetime

from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.dependencies import get_db_session
from app.core.config import get_settings
from app.db import models  # noqa: F401
from app.db.base import Base
from app.dispatch.transport import InMemoryTaskDispatcher
from app.engine.execution import WorkflowRun
from app.main import create_app
from app.observability.context import get_log_context
from app.observability.metrics import reset_metrics_for_tests
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.security.auth import create_access_token
from app.security.models import Role
from app.security.rate_limit import RateLimitDecision
from app.services.outbox import DispatchOutboxPublisher
from app.services.repositories import (
    DispatchOutboxRepository,
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.scheduler import WorkflowScheduler

pytestmark = pytest.mark.asyncio


class FakeRateLimiter:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.counts: dict[tuple[str, str], int] = {}

    async def check(self, principal, *, scope: str, limit: int, now=None):
        from app.security.auth import RateLimitExceededError, RateLimitUnavailableError

        if self.fail:
            raise RateLimitUnavailableError()
        key = (principal.subject, scope)
        self.counts[key] = self.counts.get(key, 0) + 1
        remaining = max(limit - self.counts[key], 0)
        if self.counts[key] > limit:
            raise RateLimitExceededError(60, limit, remaining)
        return RateLimitDecision(limit=limit, remaining=remaining, retry_after=60)


class FailingDispatcher(InMemoryTaskDispatcher):
    async def ping(self) -> None:
        raise RuntimeError("redis password=secret unreachable")


class FailingSession:
    async def execute(self, statement):
        raise RuntimeError("postgresql://user:password@localhost/db")


async def reset_schema(engine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def api_client(
    *,
    auth_enabled: bool = True,
    rate_limiter: FakeRateLimiter | None = None,
    dispatcher: InMemoryTaskDispatcher | None = None,
    postgres_ready: bool = True,
) -> AsyncIterator[tuple[AsyncClient, async_sessionmaker]]:
    env_keys = (
        "AUTH_ENABLED",
        "JWT_SECRET",
        "RATE_LIMIT_VIEWER_PER_MINUTE",
        "RATE_LIMIT_OPERATOR_PER_MINUTE",
        "RATE_LIMIT_ADMIN_PER_MINUTE",
        "RATE_LIMIT_OPS_PER_MINUTE",
        "MAX_REQUEST_BODY_BYTES",
        "LOG_LEVEL",
        "LOG_FORMAT",
        "READINESS_TIMEOUT_SECONDS",
    )
    previous_env = {key: os.environ.get(key) for key in env_keys}
    os.environ["AUTH_ENABLED"] = "true" if auth_enabled else "false"
    os.environ["JWT_SECRET"] = "test-secret"
    os.environ.setdefault("RATE_LIMIT_VIEWER_PER_MINUTE", "120")
    os.environ.setdefault("RATE_LIMIT_OPERATOR_PER_MINUTE", "90")
    os.environ.setdefault("RATE_LIMIT_ADMIN_PER_MINUTE", "60")
    os.environ.setdefault("RATE_LIMIT_OPS_PER_MINUTE", "20")
    get_settings.cache_clear()
    reset_metrics_for_tests()
    engine = create_async_engine(TEST_DATABASE_URL)
    await reset_schema(engine)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app = create_app()

    async def override_session():
        if not postgres_ready:
            yield FailingSession()
            return
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_session
    app.state.redis_dispatcher = dispatcher or InMemoryTaskDispatcher()
    app.state.rate_limiter = rate_limiter or FakeRateLimiter()
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            yield client, session_factory
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()
        for key, value in previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        get_settings.cache_clear()
        reset_metrics_for_tests()


def auth_headers(role: Role = Role.ADMIN) -> dict[str, str]:
    token = create_access_token(f"{role.value}-user", role)
    return {"Authorization": f"Bearer {token}"}


def workflow_payload(workflow_id: str = "wf-api") -> dict:
    return {
        "id": workflow_id,
        "name": "API Workflow",
        "tasks": [
            {"id": "a", "name": "A", "depends_on": []},
            {"id": "b", "name": "B", "depends_on": ["a"]},
        ],
    }


async def test_workflow_create_read_list_and_conflict() -> None:
    async with api_client() as (client, _):
        response = await client.post(
            "/api/v1/workflows",
            json=workflow_payload(),
            headers=auth_headers(Role.OPERATOR),
        )
        assert response.status_code == 201

        duplicate = await client.post(
            "/api/v1/workflows",
            json=workflow_payload(),
            headers=auth_headers(Role.OPERATOR),
        )
        assert duplicate.status_code == 409
        assert "error" in duplicate.json()

        fetched = await client.get(
            "/api/v1/workflows/wf-api",
            headers=auth_headers(Role.VIEWER),
        )
        listed = await client.get(
            "/api/v1/workflows?limit=10&offset=0",
            headers=auth_headers(Role.VIEWER),
        )
        missing = await client.get(
            "/api/v1/workflows/missing",
            headers=auth_headers(Role.VIEWER),
        )

        assert fetched.status_code == 200
        assert fetched.json()["id"] == "wf-api"
        assert listed.status_code == 200
        assert [item["id"] for item in listed.json()["items"]] == ["wf-api"]
        assert missing.status_code == 404


async def test_invalid_dag_returns_422() -> None:
    async with api_client() as (client, _):
        payload = {
            "id": "wf-invalid",
            "name": "Invalid",
            "tasks": [{"id": "a", "depends_on": ["missing"]}],
        }

        response = await client.post(
            "/api/v1/workflows",
            json=payload,
            headers=auth_headers(Role.OPERATOR),
        )

        assert response.status_code == 422
        assert response.json()["error"]["code"]


async def test_run_create_inspect_tasks_attempts_and_cancel() -> None:
    async with api_client() as (client, _):
        response = await client.post(
            "/api/v1/workflows",
            json=workflow_payload(),
            headers=auth_headers(Role.OPERATOR),
        )
        response.raise_for_status()
        created = await client.post(
            "/api/v1/workflows/wf-api/runs",
            json={"run_id": "run-api"},
            headers=auth_headers(Role.OPERATOR),
        )
        assert created.status_code == 201
        assert created.headers["Location"] == "/api/v1/runs/run-api"

        run = await client.get("/api/v1/runs/run-api", headers=auth_headers())
        tasks = await client.get("/api/v1/runs/run-api/tasks", headers=auth_headers())
        task = await client.get(
            "/api/v1/runs/run-api/tasks/a",
            headers=auth_headers(),
        )
        attempts = await client.get(
            "/api/v1/runs/run-api/tasks/a/attempts",
            headers=auth_headers(),
        )
        listed = await client.get(
            "/api/v1/runs?workflow_id=wf-api&status=PENDING",
            headers=auth_headers(),
        )
        cancelled = await client.post(
            "/api/v1/runs/run-api/cancel",
            headers=auth_headers(Role.OPERATOR),
        )

        assert run.json()["tasks"][0]["idempotency_key"] == "run-api:a"
        assert tasks.status_code == 200
        assert task.json()["task_id"] == "a"
        assert attempts.json() == {"items": [], "count": 0}
        assert listed.json()["items"][0]["run_id"] == "run-api"
        assert cancelled.json()["status"] == "CANCELLED"


async def test_attempt_response_excludes_lease_token() -> None:
    async def seed(session_factory) -> None:
        async with session_factory() as session:
            definition = WorkflowDefinition(
                id="wf-attempt-api",
                name="Workflow",
                tasks=(TaskDefinition(id="a"),),
            )
            await WorkflowRepository(session).save(definition)
            run = WorkflowRun.create("run-1", definition)
            await WorkflowRunRepository(session).create(run)
            run.dispatch_task("a")
            attempt = await TaskAttemptRepository(session).create_dispatched_attempt(
                run,
                "a",
                1,
            )
            run.start_dispatched_task("a")
            await TaskAttemptRepository(session).claim_dispatched_attempt(
                run,
                attempt,
                "worker-1",
                "secret-token",
                datetime.now(UTC),
                30,
            )

    async with api_client() as (client, session_factory):
        await seed(session_factory)
        response = await client.get(
            "/api/v1/runs/run-1/tasks/a/attempts",
            headers=auth_headers(Role.VIEWER),
        )

        assert response.status_code == 200
        assert "lease_token" not in response.text


async def test_cancelled_dispatched_outbox_is_discarded_not_published() -> None:
    async def seed(session_factory) -> str:
        async with session_factory() as session:
            definition = WorkflowDefinition(
                id="wf-cancel-dispatch",
                name="Workflow",
                tasks=(TaskDefinition(id="a"),),
            )
            await WorkflowRepository(session).save(definition)
            run = WorkflowRun.create("run-1", definition)
            await WorkflowRunRepository(session).create(run)
            await WorkflowScheduler(
                WorkflowRepository(session),
                WorkflowRunRepository(session),
                TaskAttemptRepository(session),
                outbox_repository=DispatchOutboxRepository(session),
            ).dispatch_ready(run.run_id)
            return run.run_id

    async def publish(session_factory) -> tuple[int, int, bool]:
        dispatcher = InMemoryTaskDispatcher()
        async with session_factory() as session:
            result = await DispatchOutboxPublisher(
                DispatchOutboxRepository(session),
                dispatcher,
            ).publish_pending()
            message = await dispatcher.receive(timeout=0.01)
            return result.published, len(result.discarded_event_ids), message is None

    async with api_client() as (client, session_factory):
        run_id = await seed(session_factory)
        response = await client.post(
            f"/api/v1/runs/{run_id}/cancel",
            headers=auth_headers(Role.OPERATOR),
        )
        response.raise_for_status()
        published, discarded, no_message = await publish(session_factory)

        assert published == 0
        assert discarded == 1
        assert no_message


async def test_authentication_and_rbac_policy() -> None:
    async with api_client() as (client, _):
        no_token = await client.get("/api/v1/workflows")
        malformed = await client.get(
            "/api/v1/workflows",
            headers={"Authorization": "Bearer not-a-jwt"},
        )
        viewer_create = await client.post(
            "/api/v1/workflows",
            json=workflow_payload("wf-viewer"),
            headers=auth_headers(Role.VIEWER),
        )
        operator_ops = await client.post(
            "/api/v1/ops/scheduler/tick",
            headers=auth_headers(Role.OPERATOR),
        )
        admin_ops = await client.post(
            "/api/v1/ops/scheduler/tick",
            headers=auth_headers(Role.ADMIN),
        )

        assert no_token.status_code == 401
        assert no_token.headers["WWW-Authenticate"] == "Bearer"
        assert malformed.status_code == 401
        assert viewer_create.status_code == 403
        assert operator_ops.status_code == 403
        assert admin_ops.status_code == 200
        assert "error" in viewer_create.json()


async def test_auth_disabled_mode_uses_internal_admin() -> None:
    async with api_client(auth_enabled=False) as (client, _):
        response = await client.post(
            "/api/v1/workflows",
            json=workflow_payload("wf-no-auth"),
        )

        assert response.status_code == 201


async def test_rate_limit_exceeded_shape_and_separate_principals() -> None:
    previous_limit = os.environ.get("RATE_LIMIT_VIEWER_PER_MINUTE")
    os.environ["RATE_LIMIT_VIEWER_PER_MINUTE"] = "1"
    get_settings.cache_clear()
    try:
        async with api_client() as (client, _):
            first = await client.get(
                "/api/v1/workflows",
                headers=auth_headers(Role.VIEWER),
            )
            second = await client.get(
                "/api/v1/workflows",
                headers=auth_headers(Role.VIEWER),
            )
            other = await client.get(
                "/api/v1/workflows",
                headers={
                    "Authorization": (
                        "Bearer "
                        f"{create_access_token('another-viewer', Role.VIEWER)}"
                    )
                },
            )

            assert first.status_code == 200
            assert second.status_code == 429
            assert second.json()["error"]["code"] == "rate_limit_exceeded"
            assert "Retry-After" in second.headers
            assert other.status_code == 200
    finally:
        if previous_limit is None:
            os.environ.pop("RATE_LIMIT_VIEWER_PER_MINUTE", None)
        else:
            os.environ["RATE_LIMIT_VIEWER_PER_MINUTE"] = previous_limit
        get_settings.cache_clear()


async def test_rate_limit_infrastructure_failure_returns_503() -> None:
    async with api_client(rate_limiter=FakeRateLimiter(fail=True)) as (client, _):
        response = await client.get(
            "/api/v1/workflows",
            headers=auth_headers(Role.VIEWER),
        )

        assert response.status_code == 503
        assert response.json()["error"]["code"] == "rate_limit_unavailable"


async def test_audit_success_denial_and_admin_listing() -> None:
    async with api_client() as (client, _):
        create_response = await client.post(
            "/api/v1/workflows",
            json=workflow_payload("wf-audit"),
            headers={
                **auth_headers(Role.OPERATOR),
                "X-Request-ID": "request-audit-create",
            },
        )
        denied_response = await client.post(
            "/api/v1/ops/scheduler/tick",
            headers={
                **auth_headers(Role.OPERATOR),
                "X-Request-ID": "request-audit-denied",
            },
        )
        operator_audit = await client.get(
            "/api/v1/ops/audit",
            headers=auth_headers(Role.OPERATOR),
        )
        admin_audit = await client.get(
            "/api/v1/ops/audit?limit=10&offset=0",
            headers=auth_headers(Role.ADMIN),
        )
        filtered = await client.get(
            "/api/v1/ops/audit?action=workflow.create",
            headers=auth_headers(Role.ADMIN),
        )

        assert create_response.status_code == 201
        assert denied_response.status_code == 403
        assert operator_audit.status_code == 403
        assert admin_audit.status_code == 200
        actions = {item["action"] for item in admin_audit.json()["items"]}
        assert "workflow.create" in actions
        assert "authorization.denied" in actions
        create_item = filtered.json()["items"][0]
        assert create_item["request_id"] == "request-audit-create"
        assert create_item["principal_subject"] == "operator-user"
        assert create_item["principal_role"] == "operator"
        assert "test-secret" not in admin_audit.text
        assert "lease_token" not in admin_audit.text


async def test_auth_disabled_principal_is_still_rate_limited() -> None:
    previous_limit = os.environ.get("RATE_LIMIT_ADMIN_PER_MINUTE")
    os.environ["RATE_LIMIT_ADMIN_PER_MINUTE"] = "1"
    get_settings.cache_clear()
    try:
        async with api_client(auth_enabled=False) as (client, _):
            first = await client.get("/api/v1/workflows")
            second = await client.get("/api/v1/workflows")

            assert first.status_code == 200
            assert second.status_code == 429
    finally:
        if previous_limit is None:
            os.environ.pop("RATE_LIMIT_ADMIN_PER_MINUTE", None)
        else:
            os.environ["RATE_LIMIT_ADMIN_PER_MINUTE"] = previous_limit
        get_settings.cache_clear()


async def test_security_headers_and_oversized_body() -> None:
    previous_limit = os.environ.get("MAX_REQUEST_BODY_BYTES")
    os.environ["MAX_REQUEST_BODY_BYTES"] = "64"
    get_settings.cache_clear()
    try:
        async with api_client() as (client, _):
            health = await client.get("/health")
            oversized = await client.post(
                "/api/v1/workflows",
                content="x" * 65,
                headers={
                    **auth_headers(Role.OPERATOR),
                    "Content-Type": "application/json",
                },
            )

            assert health.status_code == 200
            assert health.headers["X-Content-Type-Options"] == "nosniff"
            assert health.headers["X-Frame-Options"] == "DENY"
            assert oversized.status_code == 413
            assert oversized.json()["error"]["code"] == "payload_too_large"
    finally:
        if previous_limit is None:
            os.environ.pop("MAX_REQUEST_BODY_BYTES", None)
        else:
            os.environ["MAX_REQUEST_BODY_BYTES"] = previous_limit
        get_settings.cache_clear()


async def test_readiness_and_metrics_endpoints() -> None:
    async with api_client() as (client, _):
        health = await client.get("/health")
        ready = await client.get("/ready")
        missing_run = await client.get(
            "/api/v1/runs/run-observe-123",
            headers=auth_headers(Role.VIEWER),
        )
        metrics = await client.get("/metrics")

        assert health.status_code == 200
        assert health.json() == {"status": "ok", "service": "fluxion"}
        assert ready.status_code == 200
        assert ready.json() == {
            "status": "ready",
            "checks": {"postgres": "ok", "redis": "ok"},
        }
        assert missing_run.status_code == 404
        assert metrics.status_code == 200
        assert "text/plain" in metrics.headers["content-type"]
        body = metrics.text
        assert "fluxion_http_requests_total" in body
        assert "fluxion_http_request_duration_seconds" in body
        assert 'path="/api/v1/runs/{run_id}"' in body
        assert "run-observe-123" not in body
        assert get_log_context().request_id is None


async def test_redis_readiness_failure_does_not_leak_connection_details() -> None:
    async with api_client(dispatcher=FailingDispatcher()) as (client, _):
        response = await client.get("/ready")

        assert response.status_code == 503
        assert response.json() == {
            "status": "not_ready",
            "checks": {"postgres": "ok", "redis": "failed"},
        }
        assert "secret" not in response.text


async def test_postgres_readiness_failure_does_not_leak_connection_details() -> None:
    async with api_client(postgres_ready=False) as (client, _):
        response = await client.get("/ready")

        assert response.status_code == 503
        assert response.json() == {
            "status": "not_ready",
            "checks": {"postgres": "failed", "redis": "ok"},
        }
        assert "password" not in response.text


async def test_metrics_cover_security_and_run_creation() -> None:
    previous_limit = os.environ.get("RATE_LIMIT_VIEWER_PER_MINUTE")
    os.environ["RATE_LIMIT_VIEWER_PER_MINUTE"] = "1"
    get_settings.cache_clear()
    try:
        async with api_client() as (client, _):
            unauthorized = await client.get("/api/v1/workflows")
            await client.get("/api/v1/workflows", headers=auth_headers(Role.VIEWER))
            limited = await client.get(
                "/api/v1/workflows",
                headers=auth_headers(Role.VIEWER),
            )
            workflow = await client.post(
                "/api/v1/workflows",
                json=workflow_payload("wf-metrics"),
                headers=auth_headers(Role.OPERATOR),
            )
            run = await client.post(
                "/api/v1/workflows/wf-metrics/runs",
                json={"run_id": "run-metrics"},
                headers=auth_headers(Role.OPERATOR),
            )
            metrics = await client.get("/metrics")

            assert unauthorized.status_code == 401
            assert limited.status_code == 429
            assert workflow.status_code == 201
            assert run.status_code == 201
            body = metrics.text
            assert "fluxion_auth_denied_total 1" in body
            assert "fluxion_rate_limit_denied_total 1" in body
            assert "fluxion_workflow_runs_created_total 1" in body
    finally:
        if previous_limit is None:
            os.environ.pop("RATE_LIMIT_VIEWER_PER_MINUTE", None)
        else:
            os.environ["RATE_LIMIT_VIEWER_PER_MINUTE"] = previous_limit
        get_settings.cache_clear()
