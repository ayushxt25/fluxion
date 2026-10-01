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
from app.db.models.interventions import TaskInterventionRecord
from app.db.models.logs import TaskLogRecord
from app.db.models.webhooks import WebhookDeliveryRecord, WebhookSubscriptionRecord
from app.db.models.workflow import (
    TaskDefinitionRecord,
    WorkflowDefinitionRecord,
    WorkflowRevisionRecord,
    WorkflowRevisionTaskRecord,
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
        if await session.get(WorkflowRevisionRecord, ("wf", 1)) is None:
            session.add(
                WorkflowRevisionRecord(
                    workflow_id="wf",
                    revision=1,
                    name="retention",
                )
            )
            await session.flush()
        if await session.get(WorkflowRevisionTaskRecord, ("wf", 1, "task")) is None:
            session.add(
                WorkflowRevisionTaskRecord(
                    workflow_id="wf",
                    revision=1,
                    task_id="task",
                    retry_max_attempts=1,
                    retry_initial_backoff_seconds=0,
                    retry_backoff_multiplier=1,
                    retry_max_backoff_seconds=None,
                    parameters=[],
                )
            )
        if await session.get(TaskDefinitionRecord, ("wf", "task")) is None:
            session.add(TaskDefinitionRecord(workflow_id="wf", task_id="task"))
        await session.flush()
        session.add(
            TaskRunRecord(
                run_id=run_id,
                workflow_id="wf",
                workflow_revision=1,
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


def test_active_coordinator_lease_does_not_make_a_nonterminal_run_retention_eligible():
    async def body(session):
        old = datetime.now(UTC) - timedelta(days=31)
        await add_run(session, "owned", status="RUNNING", completed_at=old)
        async with session.begin():
            record = await session.get(WorkflowRunRecord, "owned")
            record.coordinator_id = "coordinator-a"
            record.coordinator_lease_token = "internal-token"
            record.coordinator_lease_expires_at = datetime.now(UTC) + timedelta(
                seconds=30
            )
        summary = await retention(session).run_retention(categories=("workflow_runs",))

        assert summary.total_deleted == 0
        assert await session.get(WorkflowRunRecord, "owned") is not None

    in_db(body)


def test_terminal_run_with_stale_coordinator_metadata_is_still_retained_normally():
    async def body(session):
        old = datetime.now(UTC) - timedelta(days=31)
        await add_run(session, "terminal-owned", completed_at=old)
        async with session.begin():
            record = await session.get(WorkflowRunRecord, "terminal-owned")
            record.coordinator_id = "former-coordinator"
            record.coordinator_lease_token = "old-internal-token"
            record.coordinator_lease_expires_at = old
        summary = await retention(session).run_retention(categories=("workflow_runs",))

        assert summary.total_deleted == 1
        assert await session.get(WorkflowRunRecord, "terminal-owned") is None

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


def test_reconciled_actionable_outbox_preserves_execution_tree():
    async def body(session):
        old = datetime.now(UTC) - timedelta(days=31)
        reconciled_at = datetime.now(UTC) - timedelta(minutes=5)
        await add_run(session, "run", status="RUNNING")
        await add_task_run(session, "run")
        await add_attempt(session, "run")
        async with session.begin():
            session.add(
                DispatchOutboxRecord(
                    id="reconciled-outbox",
                    event_type="task.dispatch",
                    payload={},
                    run_id="run",
                    workflow_id="wf",
                    task_id="task",
                    attempt_number=1,
                    created_at=old,
                    published_at=None,
                    last_reconciled_at=reconciled_at,
                    reconcile_count=1,
                )
            )

        summary = await retention(session).run_retention(
            categories=("dispatch_outbox", "workflow_runs")
        )
        outbox = await session.get(DispatchOutboxRecord, "reconciled-outbox")

        assert summary.total_deleted == 0
        assert outbox is not None
        assert outbox.reconcile_count == 1
        assert outbox.last_reconciled_at == reconciled_at
        assert await session.get(WorkflowRunRecord, "run") is not None
        assert await session.get(TaskRunRecord, ("run", "task")) is not None
        assert await session.get(TaskAttemptRecord, ("run", "task", 1)) is not None

    in_db(body)


def test_old_discarded_outbox_is_still_deleted_by_retention():
    async def body(session):
        old = datetime.now(UTC) - timedelta(days=31)
        await add_run(session, "run", completed_at=old)
        await add_task_run(session, "run")
        async with session.begin():
            session.add(
                DispatchOutboxRecord(
                    id="discarded-outbox",
                    event_type="task.dispatch",
                    payload={},
                    run_id="run",
                    workflow_id="wf",
                    task_id="task",
                    attempt_number=1,
                    created_at=old,
                    published_at=old,
                    discarded_at=old,
                    reconcile_count=1,
                    last_reconciled_at=old,
                )
            )

        summary = await retention(session).run_retention(
            categories=("dispatch_outbox",)
        )

        assert summary.total_deleted == 1
        assert await session.get(DispatchOutboxRecord, "discarded-outbox") is None

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


def test_pending_intervention_protects_then_resolved_allows_run_cleanup():
    async def body(session):
        old = datetime.now(UTC) - timedelta(days=31)
        await add_run(session, "intervention-run", status="FAILED", completed_at=old)
        await add_task_run(session, "intervention-run")
        await add_attempt(session, "intervention-run")
        async with session.begin():
            task = await session.get(TaskRunRecord, ("intervention-run", "task"))
            attempt = await session.get(
                TaskAttemptRecord,
                ("intervention-run", "task", 1),
            )
            task.status = "INTERRUPTED"
            attempt.status = "INTERRUPTED"
            session.add(
                TaskInterventionRecord(
                    id="pending-intervention",
                    workflow_id="wf",
                    run_id="intervention-run",
                    task_id="task",
                    interrupted_attempt_number=1,
                    resolution="PENDING",
                )
            )

        service = retention(session)
        assert (
            await service.run_retention(categories=("workflow_runs",))
        ).total_deleted == 0
        assert await session.get(WorkflowRunRecord, "intervention-run") is not None
        assert await session.get(TaskAttemptRecord, ("intervention-run", "task", 1))
        assert await session.get(TaskInterventionRecord, "pending-intervention")

        async with session.begin():
            intervention = await session.get(
                TaskInterventionRecord,
                "pending-intervention",
            )
            intervention.resolution = "FAIL"
            intervention.resolved_at = datetime.now(UTC)
        assert (
            await service.run_retention(categories=("workflow_runs",))
        ).total_deleted == 1
        assert await session.get(WorkflowRunRecord, "intervention-run") is None
        assert await session.get(TaskInterventionRecord, "pending-intervention") is None

    in_db(body)


def test_run_delete_rechecks_pending_intervention_after_candidate_selection():
    async def body(session):
        old = datetime.now(UTC) - timedelta(days=31)
        await add_run(session, "retention-race", status="FAILED", completed_at=old)
        await add_task_run(session, "retention-race")
        await add_attempt(session, "retention-race")
        repository = RetentionRepository(session)
        async with session.begin():
            candidate_ids = await repository._ids(
                "workflow_runs",
                datetime.now(UTC) - timedelta(days=30),
                10,
                lock=True,
            )
            assert candidate_ids == ("retention-race",)
            task = await session.get(TaskRunRecord, ("retention-race", "task"))
            attempt = await session.get(
                TaskAttemptRecord,
                ("retention-race", "task", 1),
            )
            task.status = "INTERRUPTED"
            attempt.status = "INTERRUPTED"
            session.add(
                TaskInterventionRecord(
                    id="race-pending-intervention",
                    workflow_id="wf",
                    run_id="retention-race",
                    task_id="task",
                    interrupted_attempt_number=1,
                    resolution="PENDING",
                )
            )
            assert await repository._delete("workflow_runs", candidate_ids) == 0

        assert await session.get(WorkflowRunRecord, "retention-race") is not None
        assert await session.get(TaskAttemptRecord, ("retention-race", "task", 1))
        assert await session.get(TaskInterventionRecord, "race-pending-intervention")

    in_db(body)
