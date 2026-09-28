import asyncio
import os
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.db.models.execution import WorkflowRunRecord
from app.db.models.schedules import WorkflowScheduleFiringRecord, WorkflowScheduleRecord
from app.schemas.schedules import MisfirePolicy, ScheduleCreate, ScheduleType
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.repositories import WorkflowRepository, WorkflowRunRepository
from app.services.schedules import ScheduleRepository, ScheduleRunner

URL = os.environ.get("TEST_DATABASE_URL")
if not URL:
    pytest.skip("TEST_DATABASE_URL is required", allow_module_level=True)


def definition(name: str = "one") -> WorkflowDefinition:
    return WorkflowDefinition(
        id="scheduled-workflow", name=name, tasks=(TaskDefinition(id="task"),)
    )


def in_db(body):
    async def scenario():
        engine = create_async_engine(URL)
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
            await connection.run_sync(Base.metadata.create_all)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            await body(factory)
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_explicit_and_latest_revision_are_resolved_at_fire_time():
    async def body(factory):
        now = datetime.now(UTC)
        async with factory() as session:
            workflows = WorkflowRepository(session)
            await workflows.save(definition())
            explicit = await ScheduleRepository(session).create(
                ScheduleCreate(
                    workflow_id="scheduled-workflow",
                    workflow_revision=1,
                    schedule_type=ScheduleType.INTERVAL,
                    interval_seconds=60,
                    next_fire_at=now,
                ),
                subject=None,
                role=None,
            )
            latest = await ScheduleRepository(session).create(
                ScheduleCreate(
                    workflow_id="scheduled-workflow",
                    schedule_type=ScheduleType.INTERVAL,
                    interval_seconds=60,
                    next_fire_at=now,
                ),
                subject=None,
                role=None,
            )
            await workflows.publish(definition("two"))
            assert await ScheduleRunner(session).tick(now) == 2
            rows = (await session.execute(select(WorkflowRunRecord))).scalars()
            rows = tuple(rows)
            revisions = sorted(row.workflow_revision for row in rows)
            assert revisions == [1, 2]
            assert all(row.schedule_id for row in rows)
            assert all(row.scheduled_for == now for row in rows)
            assert explicit.workflow_revision == 1
            assert latest.workflow_revision is None

    in_db(body)


def test_due_schedule_concurrent_runners_create_one_firing():
    async def body(factory):
        now = datetime.now(UTC)
        async with factory() as session:
            await WorkflowRepository(session).save(definition())
            schedule = await ScheduleRepository(session).create(
                ScheduleCreate(
                    workflow_id="scheduled-workflow",
                    schedule_type=ScheduleType.INTERVAL,
                    interval_seconds=60,
                    next_fire_at=now,
                ),
                subject=None,
                role=None,
            )

        async def run_one():
            async with factory() as session:
                return await ScheduleRunner(session).tick(now)

        assert sum(await asyncio.gather(run_one(), run_one())) == 1
        async with factory() as session:
            firings = await session.scalar(
                select(func.count()).select_from(WorkflowScheduleFiringRecord)
            )
            runs = await session.scalar(
                select(func.count()).select_from(WorkflowRunRecord)
            )
            schedule_row = await session.get(WorkflowScheduleRecord, schedule.id)
            assert (firings, runs) == (1, 1)
            assert schedule_row.next_fire_at > now

    in_db(body)


def test_misfire_skip_and_fire_once_are_bounded():
    async def body(factory):
        now = datetime.now(UTC)
        async with factory() as session:
            await WorkflowRepository(session).save(definition())
            for policy in (MisfirePolicy.SKIP, MisfirePolicy.FIRE_ONCE):
                await ScheduleRepository(session).create(
                    ScheduleCreate(
                        workflow_id="scheduled-workflow",
                        schedule_type=ScheduleType.INTERVAL,
                        interval_seconds=60,
                        misfire_policy=policy,
                        next_fire_at=now - timedelta(minutes=5),
                    ),
                    subject=None,
                    role=None,
                )
            assert await ScheduleRunner(session).tick(now) == 1
            run_count_query = select(func.count()).select_from(WorkflowRunRecord)
            assert await session.scalar(run_count_query) == 1

    in_db(body)


def test_pause_resume_delete_and_firing_identity_are_durable():
    async def body(factory):
        now = datetime.now(UTC)
        async with factory() as session:
            await WorkflowRepository(session).save(definition())
            repository = ScheduleRepository(session)
            schedule = await repository.create(
                ScheduleCreate(
                    workflow_id="scheduled-workflow",
                    schedule_type=ScheduleType.CRON,
                    cron_expression="0 9 * * *",
                    timezone="Asia/Kolkata",
                    next_fire_at=now,
                ),
                subject="operator",
                role="OPERATOR",
            )
            paused = await repository.set_enabled(schedule.id, False)
            assert paused.enabled is False
            assert await ScheduleRunner(session).tick(now) == 0
            resumed = await repository.set_enabled(schedule.id, True)
            assert resumed.next_fire_at > now
            await repository.delete(schedule.id)
            assert (await repository.get(schedule.id)).enabled is False
            assert await ScheduleRunner(session).tick(now + timedelta(days=1)) == 0

            async with session.begin():
                session.add(
                    WorkflowScheduleFiringRecord(
                        id="firing-one", schedule_id=schedule.id,
                        scheduled_for=now, run_id="run-one",
                    )
                )
            with pytest.raises(IntegrityError):
                async with session.begin():
                    session.add(
                        WorkflowScheduleFiringRecord(
                            id="firing-two", schedule_id=schedule.id,
                            scheduled_for=now, run_id="run-two",
                        )
                    )
            assert await session.scalar(
                select(func.count()).select_from(WorkflowScheduleFiringRecord)
            ) == 1

    in_db(body)


def test_firing_transaction_rolls_back_before_and_after_run_staging(monkeypatch):
    async def body(factory):
        now = datetime.now(UTC)
        async with factory() as session:
            await WorkflowRepository(session).save(definition())
            schedule = await ScheduleRepository(session).create(
                ScheduleCreate(
                    workflow_id="scheduled-workflow",
                    schedule_type=ScheduleType.INTERVAL,
                    interval_seconds=60, next_fire_at=now,
                ), subject=None, role=None,
            )

        async def fail_run(*_args, **_kwargs):
            raise RuntimeError("before run commit")

        original_create = WorkflowRunRepository.create_in_transaction
        monkeypatch.setattr(
            "app.services.schedules.WorkflowRunRepository.create_in_transaction",
            fail_run,
        )
        async with factory() as session:
            with pytest.raises(RuntimeError, match="before run commit"):
                await ScheduleRunner(session).tick(now)
        async with factory() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(WorkflowRunRecord)
                )
                == 0
            )
            assert (
                await session.scalar(
                    select(func.count()).select_from(WorkflowScheduleFiringRecord)
                )
                == 0
            )
            assert (
                await session.get(WorkflowScheduleRecord, schedule.id)
            ).next_fire_at == now
        monkeypatch.setattr(
            "app.services.schedules.WorkflowRunRepository.create_in_transaction",
            original_create,
        )
        async with factory() as session:
            assert await ScheduleRunner(session).tick(now) == 1
            assert (
                await session.scalar(
                    select(func.count()).select_from(WorkflowRunRecord)
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count()).select_from(WorkflowScheduleFiringRecord)
                )
                == 1
            )

        async with factory() as session:
            second = await ScheduleRepository(session).create(
                ScheduleCreate(
                    workflow_id="scheduled-workflow",
                    schedule_type=ScheduleType.INTERVAL,
                    interval_seconds=60,
                    next_fire_at=now,
                ),
                subject=None,
                role=None,
            )
        from app.services import schedules as schedule_service

        original_next_fire = schedule_service._next_fire
        monkeypatch.setattr(
            schedule_service, "_next_fire",
            lambda *_args: (_ for _ in ()).throw(RuntimeError("before advancement")),
        )
        async with factory() as session:
            with pytest.raises(RuntimeError, match="before advancement"):
                await ScheduleRunner(session).tick(now)
        monkeypatch.setattr(schedule_service, "_next_fire", original_next_fire)
        async with factory() as session:
            assert (
                await session.get(WorkflowScheduleRecord, second.id)
            ).next_fire_at == now
            assert (
                await session.scalar(
                    select(func.count()).select_from(WorkflowRunRecord)
                )
                == 1
            )
            await session.rollback()
            assert await ScheduleRunner(session).tick(now) == 1

    in_db(body)
