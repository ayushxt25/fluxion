import asyncio
import logging
import os
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.db.models.audit import AuditEventRecord
from app.db.models.execution import DispatchOutboxRecord, TaskAttemptRecord
from app.db.models.interventions import TaskInterventionRecord
from app.dispatch.transport import InMemoryTaskDispatcher
from app.engine.execution import WorkflowRun
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.observability.metrics import render_prometheus, reset_metrics_for_tests
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.security.models import Role
from app.services.interventions import (
    InterventionConflictError,
    TaskInterventionService,
)
from app.services.leases import LeaseReaper
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.scheduler import WorkflowScheduler
from tests.integration.test_api import api_client, auth_headers

URL = os.environ.get("TEST_DATABASE_URL")
if not URL:
    pytest.skip("TEST_DATABASE_URL is required", allow_module_level=True)

database_name = urlparse(URL).path.rsplit("/", maxsplit=1)[-1]
if not database_name.endswith("_test"):
    pytest.skip(
        "TEST_DATABASE_URL must point to a *_test database", allow_module_level=True
    )


async def _reset(engine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)


def _workflow(workflow_id: str) -> WorkflowDefinition:
    return WorkflowDefinition(
        id=workflow_id,
        name="Intervention workflow",
        tasks=[TaskDefinition(id="task")],
    )


async def _interrupted(
    session, *, run_id: str = "run-1"
) -> tuple[WorkflowDefinition, str]:
    definition = _workflow(f"wf-{run_id}")
    await WorkflowRepository(session).save(definition)
    run = WorkflowRun.create(run_id, definition)
    await WorkflowRunRepository(session).create(run)
    await WorkflowScheduler(
        WorkflowRepository(session),
        WorkflowRunRepository(session),
        TaskAttemptRepository(session),
        InMemoryTaskDispatcher(),
    ).dispatch_ready(run_id)
    attempt = (await TaskAttemptRepository(session).list_attempts(run_id, "task"))[0]
    run = await WorkflowRunRepository(session).get(run_id, definition)
    run.start_dispatched_task("task")
    await TaskAttemptRepository(session).claim_dispatched_attempt(
        run,
        attempt,
        "worker-1",
        "worker-lease-token",
        datetime.now(UTC) - timedelta(minutes=5),
        1,
    )
    reclaimed = await LeaseReaper(
        WorkflowRepository(session),
        WorkflowRunRepository(session),
        TaskAttemptRepository(session),
    ).reclaim_expired()
    assert len(reclaimed) == 1
    return definition, run_id


def _in_db(test):
    async def scenario() -> None:
        engine = create_async_engine(URL)
        await _reset(engine)
        try:
            async with async_sessionmaker(engine, expire_on_commit=False)() as session:
                await test(session)
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_expired_worker_creates_one_pending_intervention() -> None:
    async def body(session) -> None:
        reset_metrics_for_tests()
        definition, run_id = await _interrupted(session)
        await LeaseReaper(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
        ).reclaim_expired()
        loaded = await WorkflowRunRepository(session).get(run_id, definition)
        intervention = (
            await session.execute(select(TaskInterventionRecord))
        ).scalar_one()
        attempts = (
            (
                await session.execute(
                    select(TaskAttemptRecord).where(TaskAttemptRecord.run_id == run_id)
                )
            )
            .scalars()
            .all()
        )

        assert intervention.resolution == "PENDING"
        assert (intervention.run_id, intervention.task_id) == (run_id, "task")
        assert intervention.interrupted_attempt_number == 1
        assert len(attempts) == 1
        assert attempts[0].status == AttemptStatus.INTERRUPTED.value
        assert loaded.status is WorkflowStatus.FAILED
        assert loaded.get_task_status("task") is TaskStatus.INTERRUPTED
        rendered = render_prometheus()
        assert "fluxion_task_interventions_created_total 1" in rendered
        assert "run_id" not in rendered and "lease_token" not in rendered

    _in_db(body)


def test_intervention_retry_creates_one_new_attempt_and_outbox() -> None:
    async def body(session) -> None:
        reset_metrics_for_tests()
        _, run_id = await _interrupted(session)
        intervention = (
            await session.execute(select(TaskInterventionRecord))
        ).scalar_one()
        await session.rollback()

        result = await TaskInterventionService(session).resolve(
            intervention.id,
            action="RETRY",
            subject="admin-1",
            role="ADMIN",
        )
        attempts = (
            (
                await session.execute(
                    select(TaskAttemptRecord)
                    .where(TaskAttemptRecord.run_id == run_id)
                    .order_by(TaskAttemptRecord.attempt_number)
                )
            )
            .scalars()
            .all()
        )
        outbox = (
            (
                await session.execute(
                    select(DispatchOutboxRecord).where(
                        DispatchOutboxRecord.run_id == run_id
                    )
                )
            )
            .scalars()
            .all()
        )

        assert result.resolution == "RETRY"
        assert result.resulting_attempt_number == 2
        assert [attempt.status for attempt in attempts] == [
            AttemptStatus.INTERRUPTED.value,
            AttemptStatus.DISPATCHED.value,
        ]
        assert attempts[0].attempt_key != attempts[1].attempt_key
        assert len(outbox) == 2
        assert outbox[-1].attempt_number == 2
        assert (
            'fluxion_task_intervention_resolutions_total{action="retry",'
            'outcome="success"} 1'
        ) in render_prometheus()

    _in_db(body)


def test_intervention_fail_preserves_interrupted_attempt_without_dispatch() -> None:
    async def body(session) -> None:
        reset_metrics_for_tests()
        _, run_id = await _interrupted(session)
        intervention = (
            await session.execute(select(TaskInterventionRecord))
        ).scalar_one()
        await session.rollback()

        result = await TaskInterventionService(session).resolve(
            intervention.id,
            action="FAIL",
            subject="admin-1",
            role="ADMIN",
        )
        with pytest.raises(InterventionConflictError):
            await TaskInterventionService(session).resolve(
                intervention.id,
                action="FAIL",
                subject="admin-1",
                role="ADMIN",
            )
        attempts = (
            (
                await session.execute(
                    select(TaskAttemptRecord).where(TaskAttemptRecord.run_id == run_id)
                )
            )
            .scalars()
            .all()
        )
        outbox = (
            (
                await session.execute(
                    select(DispatchOutboxRecord).where(
                        DispatchOutboxRecord.run_id == run_id
                    )
                )
            )
            .scalars()
            .all()
        )

        assert result.resolution == "FAIL"
        assert len(attempts) == 1
        assert attempts[0].status == AttemptStatus.INTERRUPTED.value
        assert len(outbox) == 1
        rendered = render_prometheus()
        assert (
            'fluxion_task_intervention_resolutions_total{action="fail",'
            'outcome="success"} 1'
        ) in rendered
        assert (
            'fluxion_task_intervention_conflicts_total{action="fail"} 1'
        ) in rendered

    _in_db(body)


def test_concurrent_retry_resolution_creates_one_attempt() -> None:
    async def scenario() -> None:
        engine = create_async_engine(URL)
        await _reset(engine)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as setup:
                _, run_id = await _interrupted(setup)
                intervention_id = (
                    await setup.execute(select(TaskInterventionRecord.id))
                ).scalar_one()

            async def retry() -> str:
                async with factory() as session:
                    try:
                        await TaskInterventionService(session).resolve(
                            intervention_id,
                            action="RETRY",
                            subject="admin",
                            role="ADMIN",
                        )
                    except InterventionConflictError:
                        return "conflict"
                    return "success"

            results = await asyncio.gather(retry(), retry())
            async with factory() as verify:
                attempts = (
                    (
                        await verify.execute(
                            select(TaskAttemptRecord).where(
                                TaskAttemptRecord.run_id == run_id
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                outbox = (
                    (
                        await verify.execute(
                            select(DispatchOutboxRecord).where(
                                DispatchOutboxRecord.run_id == run_id
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                assert results.count("success") == 1
                assert results.count("conflict") == 1
                assert len(attempts) == 2
                assert len(outbox) == 2
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_concurrent_retry_and_fail_choose_one_resolution() -> None:
    async def scenario() -> None:
        engine = create_async_engine(URL)
        await _reset(engine)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as setup:
                _, run_id = await _interrupted(setup, run_id="race-run")
                intervention_id = (
                    await setup.execute(select(TaskInterventionRecord.id))
                ).scalar_one()

            async def resolve(action: str) -> str:
                async with factory() as session:
                    try:
                        await TaskInterventionService(session).resolve(
                            intervention_id,
                            action=action,
                            subject="admin",
                            role="ADMIN",
                        )
                    except InterventionConflictError:
                        return "conflict"
                    return action

            results = await asyncio.gather(resolve("RETRY"), resolve("FAIL"))
            async with factory() as verify:
                intervention = await verify.get(
                    TaskInterventionRecord,
                    intervention_id,
                )
                attempts = (
                    (
                        await verify.execute(
                            select(TaskAttemptRecord).where(
                                TaskAttemptRecord.run_id == run_id
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                outbox = (
                    (
                        await verify.execute(
                            select(DispatchOutboxRecord).where(
                                DispatchOutboxRecord.run_id == run_id
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                assert results.count("conflict") == 1
                assert intervention.resolution in {"RETRY", "FAIL"}
                if intervention.resolution == "RETRY":
                    assert len(attempts) == len(outbox) == 2
                else:
                    assert len(attempts) == len(outbox) == 1
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_stale_intervention_cannot_create_another_attempt() -> None:
    async def body(session) -> None:
        _, run_id = await _interrupted(session, run_id="stale-run")
        intervention = (
            await session.execute(select(TaskInterventionRecord))
        ).scalar_one()
        await session.rollback()
        async with session.begin():
            session.add(
                TaskAttemptRecord(
                    run_id=run_id,
                    workflow_id=intervention.workflow_id,
                    task_id="task",
                    attempt_number=2,
                    status=AttemptStatus.DISPATCHED.value,
                )
            )
        with pytest.raises(InterventionConflictError):
            await TaskInterventionService(session).resolve(
                intervention.id,
                action="RETRY",
                subject="admin",
                role="ADMIN",
            )
        attempts = (
            (
                await session.execute(
                    select(TaskAttemptRecord).where(TaskAttemptRecord.run_id == run_id)
                )
            )
            .scalars()
            .all()
        )
        assert len(attempts) == 2

    _in_db(body)


@pytest.mark.asyncio
async def test_intervention_ops_are_admin_only_and_audited() -> None:
    async with api_client() as (client, factory):
        async with factory() as session:
            _, run_id = await _interrupted(session, run_id="api-run")
            intervention_id = (
                await session.execute(
                    select(TaskInterventionRecord.id).where(
                        TaskInterventionRecord.run_id == run_id
                    )
                )
            ).scalar_one()

        unauthenticated = await client.get("/api/v1/ops/interventions")
        viewer = await client.get(
            "/api/v1/ops/interventions",
            headers=auth_headers(Role.VIEWER),
        )
        operator = await client.get(
            "/api/v1/ops/interventions",
            headers=auth_headers(Role.OPERATOR),
        )
        listing = await client.get(
            "/api/v1/ops/interventions?limit=1&offset=0",
            headers=auth_headers(Role.ADMIN),
        )
        item = await client.get(
            f"/api/v1/ops/interventions/{intervention_id}",
            headers=auth_headers(Role.ADMIN),
        )
        retry = await client.post(
            f"/api/v1/ops/interventions/{intervention_id}/retry",
            json={"reason": "operator approved"},
            headers=auth_headers(Role.ADMIN),
        )
        async with factory() as session:
            _, fail_run = await _interrupted(session, run_id="api-fail-run")
            fail_id = (
                await session.execute(
                    select(TaskInterventionRecord.id).where(
                        TaskInterventionRecord.run_id == fail_run
                    )
                )
            ).scalar_one()
        fail = await client.post(
            f"/api/v1/ops/interventions/{fail_id}/fail",
            json={"reason": "operator accepted failure"},
            headers=auth_headers(Role.ADMIN),
        )
        duplicate = await client.post(
            f"/api/v1/ops/interventions/{intervention_id}/retry",
            json={},
            headers=auth_headers(Role.ADMIN),
        )
        unknown = await client.get(
            "/api/v1/ops/interventions/missing",
            headers=auth_headers(Role.ADMIN),
        )
        invalid = await client.post(
            "/api/v1/ops/interventions/missing/fail",
            json={"reason": "x" * 1025},
            headers=auth_headers(Role.ADMIN),
        )
        audit = await client.get(
            "/api/v1/ops/audit?action=intervention.retry",
            headers=auth_headers(Role.ADMIN),
        )
        fail_audit = await client.get(
            "/api/v1/ops/audit?action=intervention.fail",
            headers=auth_headers(Role.ADMIN),
        )

        assert unauthenticated.status_code == 401
        assert viewer.status_code == operator.status_code == 403
        assert listing.status_code == item.status_code == retry.status_code == 200
        assert listing.json()["count"] == 1
        assert item.json()["id"] == intervention_id
        assert retry.json()["resolution"] == "RETRY"
        assert retry.json()["resulting_attempt_number"] == 2
        assert fail.status_code == 200
        assert fail.json()["resolution"] == "FAIL"
        assert "lease_token" not in retry.text
        assert duplicate.status_code == 409
        assert unknown.status_code == 404
        assert invalid.status_code == 422
        assert audit.status_code == 200
        assert fail_audit.status_code == 200
        assert audit.json()["count"] == fail_audit.json()["count"] == 1
        audit_item = audit.json()["items"][0]
        assert audit_item["action"] == "intervention.retry"
        assert audit_item["outcome"] == "SUCCESS"
        assert fail_audit.json()["items"][0]["outcome"] == "SUCCESS"
        assert "lease_token" not in audit.text


@pytest.mark.asyncio
async def test_stale_intervention_retry_api_conflicts_without_audit_success() -> None:
    async with api_client() as (client, factory):
        async with factory() as session:
            _, run_id = await _interrupted(session, run_id="api-stale-run")
            intervention = (
                await session.execute(
                    select(TaskInterventionRecord).where(
                        TaskInterventionRecord.run_id == run_id
                    )
                )
            ).scalar_one()
            await session.rollback()
            async with session.begin():
                session.add(
                    TaskAttemptRecord(
                        run_id=run_id,
                        workflow_id=intervention.workflow_id,
                        task_id=intervention.task_id,
                        attempt_number=2,
                        status=AttemptStatus.DISPATCHED.value,
                    )
                )

        response = await client.post(
            f"/api/v1/ops/interventions/{intervention.id}/retry",
            json={"reason": "approved"},
            headers=auth_headers(Role.ADMIN),
        )

        async with factory() as session:
            current = await session.get(TaskInterventionRecord, intervention.id)
            attempts = (
                (
                    await session.execute(
                        select(TaskAttemptRecord)
                        .where(TaskAttemptRecord.run_id == run_id)
                        .order_by(TaskAttemptRecord.attempt_number)
                    )
                )
                .scalars()
                .all()
            )
            outbox = (
                (
                    await session.execute(
                        select(DispatchOutboxRecord).where(
                            DispatchOutboxRecord.run_id == run_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            successful_audits = (
                (
                    await session.execute(
                        select(AuditEventRecord).where(
                            AuditEventRecord.action == "intervention.retry",
                            AuditEventRecord.outcome == "SUCCESS",
                        )
                    )
                )
                .scalars()
                .all()
            )

        assert response.status_code == 409
        assert current.resolution == "PENDING"
        assert [attempt.attempt_number for attempt in attempts] == [1, 2]
        assert attempts[-1].status == AttemptStatus.DISPATCHED.value
        assert len(outbox) == 1
        assert successful_audits == []
        assert "worker-lease-token" not in response.text
        assert "coordinator_lease_token" not in response.text
        assert "postgresql://" not in response.text


def test_intervention_transition_logs_are_sanitized(caplog) -> None:
    async def body(session) -> None:
        _, retry_run = await _interrupted(session, run_id="retry-log")
        retry_id = (
            await session.execute(
                select(TaskInterventionRecord.id).where(
                    TaskInterventionRecord.run_id == retry_run
                )
            )
        ).scalar_one()
        await session.rollback()
        await TaskInterventionService(session).resolve(
            retry_id,
            action="RETRY",
            subject="admin",
            role="ADMIN",
        )
        with pytest.raises(InterventionConflictError):
            await TaskInterventionService(session).resolve(
                retry_id,
                action="RETRY",
                subject="admin",
                role="ADMIN",
            )
        _, fail_run = await _interrupted(session, run_id="fail-log")
        fail_id = (
            await session.execute(
                select(TaskInterventionRecord.id).where(
                    TaskInterventionRecord.run_id == fail_run
                )
            )
        ).scalar_one()
        await session.rollback()
        await TaskInterventionService(session).resolve(
            fail_id,
            action="FAIL",
            subject="admin",
            role="ADMIN",
        )

    caplog.set_level(logging.INFO)
    _in_db(body)
    events = {getattr(record, "event", None) for record in caplog.records}
    assert {
        "task.intervention.required",
        "task.intervention.retry",
        "task.intervention.fail",
        "task.intervention.conflict",
    }.issubset(events)
    rendered = caplog.text
    assert "worker-lease-token" not in rendered
    assert "postgresql://" not in rendered
