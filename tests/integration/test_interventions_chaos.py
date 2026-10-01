import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models.execution import DispatchOutboxRecord, TaskAttemptRecord
from app.db.models.interventions import TaskInterventionRecord
from app.dispatch.transport import InMemoryTaskDispatcher
from app.engine.execution import WorkflowRun
from app.services.interventions import TaskInterventionService
from app.services.leases import LeaseReaper
from app.services.outbox import DispatchOutboxPublisher
from app.services.repositories import (
    DispatchOutboxRepository,
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.scheduler import WorkflowScheduler
from tests.integration.test_interventions import URL, _interrupted, _reset, _workflow
from tests.support.faults import InjectedFault

pytestmark = pytest.mark.chaos


async def _running_expired(session, run_id: str) -> None:
    definition = _workflow(f"wf-{run_id}")
    await WorkflowRepository(session).save(definition)
    await WorkflowRunRepository(session).create(WorkflowRun.create(run_id, definition))
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
        "worker",
        "worker-lease-token",
        datetime.now(UTC) - timedelta(minutes=5),
        1,
    )


def test_reaper_intervention_transaction_rolls_back_before_commit() -> None:
    async def scenario() -> None:
        engine = create_async_engine(URL)
        await _reset(engine)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as session:
                await _running_expired(session, "reaper-fault")
                failed = False

                def fail_after_intervention(sync_session) -> None:
                    nonlocal failed
                    if not failed and any(
                        isinstance(item, TaskInterventionRecord)
                        for item in sync_session.new
                    ):
                        failed = True
                        raise InjectedFault("before.task_intervention.reaper.commit")

                event.listen(
                    session.sync_session, "before_commit", fail_after_intervention
                )
                with pytest.raises(InjectedFault):
                    await LeaseReaper(
                        WorkflowRepository(session),
                        WorkflowRunRepository(session),
                        TaskAttemptRepository(session),
                    ).reclaim_expired()
                event.remove(
                    session.sync_session, "before_commit", fail_after_intervention
                )

            async with factory() as verify:
                attempts = (
                    (
                        await verify.execute(
                            select(TaskAttemptRecord).where(
                                TaskAttemptRecord.run_id == "reaper-fault"
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                interventions = (
                    (await verify.execute(select(TaskInterventionRecord)))
                    .scalars()
                    .all()
                )
                assert attempts[0].status == "RUNNING"
                assert interventions == []
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_retry_resolution_transaction_rolls_back_before_commit() -> None:
    async def scenario() -> None:
        engine = create_async_engine(URL)
        await _reset(engine)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as session:
                _, run_id = await _interrupted(session, run_id="retry-fault")
                intervention_id = (
                    await session.execute(select(TaskInterventionRecord.id))
                ).scalar_one()
                await session.rollback()
                failed = False

                def fail_after_retry_staged(sync_session) -> None:
                    nonlocal failed
                    if not failed and any(
                        isinstance(item, TaskAttemptRecord) and item.attempt_number == 2
                        for item in sync_session.new
                    ):
                        failed = True
                        raise InjectedFault("before.task_intervention.retry.commit")

                event.listen(
                    session.sync_session, "before_commit", fail_after_retry_staged
                )
                with pytest.raises(InjectedFault):
                    await TaskInterventionService(session).resolve(
                        intervention_id,
                        action="RETRY",
                        subject="admin",
                        role="ADMIN",
                    )
                event.remove(
                    session.sync_session, "before_commit", fail_after_retry_staged
                )

            async with factory() as verify:
                intervention = await verify.get(TaskInterventionRecord, intervention_id)
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
                assert intervention.resolution == "PENDING"
                assert len(attempts) == len(outbox) == 1
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_committed_retry_survives_process_loss_before_publisher() -> None:
    async def scenario() -> None:
        engine = create_async_engine(URL)
        await _reset(engine)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        dispatcher = InMemoryTaskDispatcher()
        try:
            async with factory() as session:
                _, run_id = await _interrupted(session, run_id="retry-process-loss")
                intervention_id = (
                    await session.execute(select(TaskInterventionRecord.id))
                ).scalar_one()
                await session.rollback()
                await TaskInterventionService(session).resolve(
                    intervention_id,
                    action="RETRY",
                    subject="admin",
                    role="ADMIN",
                )

            async with factory() as publisher_session:
                result = await DispatchOutboxPublisher(
                    DispatchOutboxRepository(publisher_session),
                    dispatcher,
                ).publish_pending()
                attempts = (
                    (
                        await publisher_session.execute(
                            select(TaskAttemptRecord).where(
                                TaskAttemptRecord.run_id == run_id
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                assert result.published == 2
                assert len(attempts) == 2
        finally:
            await engine.dispose()

    asyncio.run(scenario())
