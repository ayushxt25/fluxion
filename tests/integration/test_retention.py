import asyncio
import os
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.config import Settings
from app.db import models  # noqa: F401
from app.db.base import Base
from app.db.models.audit import AuditEventRecord
from app.db.models.events import RunEventRecord
from app.db.models.execution import (
    DispatchOutboxRecord,
    TaskAttemptRecord,
    TaskRunRecord,
    WorkflowRunRecord,
)
from app.db.models.logs import TaskLogRecord
from app.db.models.webhooks import WebhookDeliveryRecord, WebhookSubscriptionRecord
from app.db.models.workflow import (
    TaskDefinitionRecord,
    WorkflowDefinitionRecord,
    WorkflowRevisionRecord,
)
from app.services.retention import RetentionRepository, RetentionService

URL = os.environ.get("TEST_DATABASE_URL")
if not URL:
    pytest.skip("TEST_DATABASE_URL is required", allow_module_level=True)


def in_db(body):
    async def scenario():
        engine = create_async_engine(URL)
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
            await connection.run_sync(Base.metadata.create_all)
        try:
            async with async_sessionmaker(engine, expire_on_commit=False)() as session:
                await body(session)
        finally:
            await engine.dispose()

    asyncio.run(scenario())


async def add_run(
    session, run_id: str, *, status: str = "SUCCEEDED", completed_at=None
) -> None:
    async with session.begin():
        session.add(WorkflowDefinitionRecord(id="wf", name="retention"))
    async with session.begin():
        session.add(
            WorkflowRunRecord(
                run_id=run_id,
                workflow_id="wf",
                status=status,
                completed_at=completed_at,
                input=None,
                input_present=False,
            )
        )


async def add_task_run(session, run_id: str) -> None:
    async with session.begin():
        session.add(TaskDefinitionRecord(workflow_id="wf", task_id="task"))
    async with session.begin():
        session.add(
            TaskRunRecord(
                run_id=run_id,
                workflow_id="wf",
                task_id="task",
                status="SUCCEEDED",
                idempotency_key=f"{run_id}:task",
                result=None,
                result_present=False,
            )
        )


async def add_attempt(session, run_id: str) -> None:
    async with session.begin():
        session.add(
            TaskAttemptRecord(
                run_id=run_id,
                workflow_id="wf",
                task_id="task",
                attempt_number=1,
                status="SUCCEEDED",
            )
        )


def retention(session, **settings):
    return RetentionService(RetentionRepository(session), Settings(**settings))


@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED", "CANCELLED"])
def test_old_terminal_runs_are_deleted_and_definition_survives(status):
    async def body(session):
        old = datetime.now(UTC) - timedelta(days=31)
        await add_run(session, "old", status=status, completed_at=old)
        summary = await retention(session).run_retention(categories=("workflow_runs",))
        assert summary.total_deleted == 1
        assert await session.get(WorkflowRunRecord, "old") is None
        assert await session.get(WorkflowDefinitionRecord, "wf") is not None

    in_db(body)


@pytest.mark.parametrize(
    ("status", "completed_at"),
    [
        ("PENDING", datetime.now(UTC) - timedelta(days=31)),
        ("RUNNING", datetime.now(UTC) - timedelta(days=31)),
        ("SUCCEEDED", datetime.now(UTC) - timedelta(days=1)),
        ("SUCCEEDED", None),
    ],
)
def test_ineligible_runs_are_retained(status, completed_at):
    async def body(session):
        await add_run(session, "retained", status=status, completed_at=completed_at)
        summary = await retention(session).run_retention(categories=("workflow_runs",))
        assert summary.total_deleted == 0
        assert await session.get(WorkflowRunRecord, "retained") is not None

    in_db(body)


def test_retention_deletes_old_pinned_run_without_deleting_revisions():
    async def body(session):
        old = datetime.now(UTC) - timedelta(days=31)
        await add_run(session, "old", completed_at=old)
        async with session.begin():
            session.add_all(
                [
                    WorkflowRevisionRecord(
                        workflow_id="wf", revision=1, name="revision one"
                    ),
                    WorkflowRevisionRecord(
                        workflow_id="wf", revision=2, name="revision two"
                    ),
                ]
            )

        summary = await retention(session).run_retention(categories=("workflow_runs",))

        assert summary.total_deleted == 1
        assert await session.get(WorkflowRunRecord, "old") is None
        assert await session.get(WorkflowRevisionRecord, ("wf", 1)) is not None
        assert await session.get(WorkflowRevisionRecord, ("wf", 2)) is not None

    in_db(body)


def test_task_logs_expire_independently_and_preview_does_not_mutate():
    async def body(session):
        old = datetime.now(UTC) - timedelta(days=31)
        await add_run(session, "run", completed_at=datetime.now(UTC))
        await add_task_run(session, "run")
        await add_attempt(session, "run")
        async with session.begin():
            session.add_all(
                [
                    TaskLogRecord(
                        run_id="run",
                        workflow_id="wf",
                        task_id="task",
                        attempt_number=1,
                        sequence_number=1,
                        level="INFO",
                        message="old",
                        fields=None,
                        created_at=old,
                    ),
                    TaskLogRecord(
                        run_id="run",
                        workflow_id="wf",
                        task_id="task",
                        attempt_number=1,
                        sequence_number=2,
                        level="INFO",
                        message="new",
                        fields=None,
                    ),
                ]
            )
        service = retention(session)
        preview = await service.preview_retention(categories=("task_logs",))
        assert preview.categories["task_logs"].eligible == 1
        assert (
            len((await session.execute(select(TaskLogRecord.id))).scalars().all()) == 2
        )
        await session.rollback()
        summary = await service.run_retention(categories=("task_logs",))
        assert summary.total_deleted == 1
        assert (
            len((await session.execute(select(TaskLogRecord.id))).scalars().all()) == 1
        )

    in_db(body)


@pytest.mark.parametrize("delivery_status", ["PENDING", "CLAIMED", "RETRY_WAITING"])
def test_active_webhook_deliveries_protect_events_and_runs(delivery_status):
    async def body(session):
        old = datetime.now(UTC) - timedelta(days=31)
        await add_run(session, "run", completed_at=old)
        async with session.begin():
            subscription = WebhookSubscriptionRecord(
                id="sub",
                name="sub",
                target_url="https://example.com",
                secret="secret",
                enabled=True,
                event_types=["run.succeeded"],
                workflow_id=None,
            )
            event = RunEventRecord(
                run_id="run",
                workflow_id="wf",
                event_type="run.succeeded",
                version=1,
                payload={},
                created_at=old,
            )
            session.add_all((subscription, event))
            await session.flush()
            session.add(
                WebhookDeliveryRecord(
                    id="delivery",
                    subscription_id="sub",
                    run_event_id=event.id,
                    delivery_key="key",
                    status=delivery_status,
                    attempt_count=0,
                    created_at=old,
                )
            )
        service = retention(session)
        assert (
            await service.run_retention(categories=("run_events",))
        ).total_deleted == 0
        assert (
            await service.run_retention(categories=("workflow_runs",))
        ).total_deleted == 0
        assert await session.get(WorkflowRunRecord, "run") is not None

    in_db(body)


@pytest.mark.parametrize("delivery_status", ["DELIVERED", "DEAD"])
def test_terminal_webhook_deliveries_are_deleted(delivery_status):
    async def body(session):
        old = datetime.now(UTC) - timedelta(days=31)
        await add_run(session, "run", completed_at=datetime.now(UTC))
        async with session.begin():
            session.add(
                WebhookSubscriptionRecord(
                    id="sub",
                    name="sub",
                    target_url="https://example.com",
                    secret="secret",
                    enabled=True,
                    event_types=["run.succeeded"],
                    workflow_id=None,
                )
            )
            event = RunEventRecord(
                run_id="run",
                workflow_id="wf",
                event_type="run.succeeded",
                version=1,
                payload={},
                created_at=old,
            )
            session.add(event)
            await session.flush()
            session.add(
                WebhookDeliveryRecord(
                    id="delivery",
                    subscription_id="sub",
                    run_event_id=event.id,
                    delivery_key="key",
                    status=delivery_status,
                    attempt_count=0,
                    created_at=old,
                )
            )
        assert (
            await retention(session).run_retention(categories=("webhook_deliveries",))
        ).total_deleted == 1
        assert await session.get(WebhookDeliveryRecord, "delivery") is None

    in_db(body)


def test_actionable_outbox_blocks_run_cleanup_until_discarded():
    async def body(session):
        old = datetime.now(UTC) - timedelta(days=31)
        await add_run(session, "run", completed_at=old)
        await add_task_run(session, "run")
        async with session.begin():
            session.add(
                DispatchOutboxRecord(
                    id="outbox",
                    event_type="task.dispatch",
                    payload={},
                    run_id="run",
                    workflow_id="wf",
                    task_id="task",
                    attempt_number=1,
                    created_at=old,
                )
            )
        service = retention(session)
        assert (
            await service.preview_retention(categories=("workflow_runs",))
        ).total_deleted == 0
        assert (
            await service.run_retention(categories=("workflow_runs",))
        ).total_deleted == 0
        async with session.begin():
            (
                await session.get(DispatchOutboxRecord, "outbox")
            ).discarded_at = datetime.now(UTC)
        assert (
            await service.run_retention(categories=("workflow_runs",))
        ).total_deleted == 1

    in_db(body)


def test_audit_and_outbox_retention_are_bounded_and_idempotent():
    async def body(session):
        old = datetime.now(UTC) - timedelta(days=91)
        async with session.begin():
            session.add(
                AuditEventRecord(
                    id="audit",
                    request_id="r",
                    principal_subject=None,
                    principal_role=None,
                    action="test",
                    resource_type=None,
                    resource_id=None,
                    outcome="SUCCESS",
                    event_metadata=None,
                    occurred_at=old,
                )
            )
        service = retention(session)
        assert (
            await service.run_retention(categories=("audit_events",))
        ).total_deleted == 1
        assert (
            await service.run_retention(categories=("audit_events",))
        ).total_deleted == 0

    in_db(body)
