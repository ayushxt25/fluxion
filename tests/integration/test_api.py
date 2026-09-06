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
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.security.auth import create_access_token
from app.security.models import Role
from app.services.outbox import DispatchOutboxPublisher
from app.services.repositories import (
    DispatchOutboxRepository,
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.scheduler import WorkflowScheduler

pytestmark = pytest.mark.asyncio


async def reset_schema(engine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def api_client(
    *,
    auth_enabled: bool = True,
) -> AsyncIterator[tuple[AsyncClient, async_sessionmaker]]:
    previous_auth_enabled = os.environ.get("AUTH_ENABLED")
    previous_jwt_secret = os.environ.get("JWT_SECRET")
    os.environ["AUTH_ENABLED"] = "true" if auth_enabled else "false"
    os.environ["JWT_SECRET"] = "test-secret"
    get_settings.cache_clear()
    engine = create_async_engine(TEST_DATABASE_URL)
    await reset_schema(engine)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app = create_app()

    async def override_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_session
    app.state.redis_dispatcher = InMemoryTaskDispatcher()
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
        if previous_auth_enabled is None:
            os.environ.pop("AUTH_ENABLED", None)
        else:
            os.environ["AUTH_ENABLED"] = previous_auth_enabled
        if previous_jwt_secret is None:
            os.environ.pop("JWT_SECRET", None)
        else:
            os.environ["JWT_SECRET"] = previous_jwt_secret
        get_settings.cache_clear()


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
