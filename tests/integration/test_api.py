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
from app.db import models  # noqa: F401
from app.db.base import Base
from app.dispatch.transport import InMemoryTaskDispatcher
from app.engine.execution import WorkflowRun
from app.main import create_app
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
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
async def api_client() -> AsyncIterator[tuple[AsyncClient, async_sessionmaker]]:
    engine = create_async_engine(TEST_DATABASE_URL)
    await reset_schema(engine)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app = create_app()

    async def override_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_session
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
        response = await client.post("/api/v1/workflows", json=workflow_payload())
        assert response.status_code == 201

        duplicate = await client.post("/api/v1/workflows", json=workflow_payload())
        assert duplicate.status_code == 409
        assert "error" in duplicate.json()

        fetched = await client.get("/api/v1/workflows/wf-api")
        listed = await client.get("/api/v1/workflows?limit=10&offset=0")
        missing = await client.get("/api/v1/workflows/missing")

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

        response = await client.post("/api/v1/workflows", json=payload)

        assert response.status_code == 422
        assert response.json()["error"]["code"]


async def test_run_create_inspect_tasks_attempts_and_cancel() -> None:
    async with api_client() as (client, _):
        response = await client.post("/api/v1/workflows", json=workflow_payload())
        response.raise_for_status()
        created = await client.post(
            "/api/v1/workflows/wf-api/runs",
            json={"run_id": "run-api"},
        )
        assert created.status_code == 201
        assert created.headers["Location"] == "/api/v1/runs/run-api"

        run = await client.get("/api/v1/runs/run-api")
        tasks = await client.get("/api/v1/runs/run-api/tasks")
        task = await client.get("/api/v1/runs/run-api/tasks/a")
        attempts = await client.get("/api/v1/runs/run-api/tasks/a/attempts")
        listed = await client.get("/api/v1/runs?workflow_id=wf-api&status=PENDING")
        cancelled = await client.post("/api/v1/runs/run-api/cancel")

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
        response = await client.get("/api/v1/runs/run-1/tasks/a/attempts")

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
        response = await client.post(f"/api/v1/runs/{run_id}/cancel")
        response.raise_for_status()
        published, discarded, no_message = await publish(session_factory)

        assert published == 0
        assert discarded == 1
        assert no_message
