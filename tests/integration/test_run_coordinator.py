import asyncio
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
if not TEST_DATABASE_URL:
    pytest.skip("TEST_DATABASE_URL is not set", allow_module_level=True)

database_name = urlparse(TEST_DATABASE_URL).path.rsplit("/", maxsplit=1)[-1]
if not database_name.endswith("_test"):
    pytest.skip(
        "TEST_DATABASE_URL must point to a *_test database",
        allow_module_level=True,
    )

# ruff: noqa: E402
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.db.models.execution import WorkflowRunRecord
from app.engine.exceptions import CoordinatorLeaseLostError
from app.engine.execution import WorkflowRun
from app.engine.status import WorkflowStatus
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.coordinator import RunCoordinatorRepository
from app.services.recovery import WorkflowRecoveryService
from app.services.repositories import WorkflowRepository, WorkflowRunRepository


async def _reset(engine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)


async def _seed(
    factory,
    run_id: str,
    status: WorkflowStatus = WorkflowStatus.PENDING,
) -> None:
    definition = WorkflowDefinition(
        id=f"workflow-{run_id}",
        name="Coordinator workflow",
        tasks=(TaskDefinition(id="task"),),
    )
    async with factory() as session:
        await WorkflowRepository(session).save(definition)
        run = WorkflowRun.create(run_id, definition)
        await WorkflowRunRepository(session).create(run)
        if status is not WorkflowStatus.PENDING:
            async with session.begin():
                await session.execute(
                    update(WorkflowRunRecord)
                    .where(WorkflowRunRecord.run_id == run_id)
                    .values(status=status.value)
                )


@pytest.mark.chaos
def test_run_coordinator_fences_takeover_and_excludes_terminals() -> None:
    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            await _reset(engine)
            await _seed(factory, "active")
            await _seed(factory, "succeeded", WorkflowStatus.SUCCEEDED)
            await _seed(factory, "failed", WorkflowStatus.FAILED)
            await _seed(factory, "cancelled", WorkflowStatus.CANCELLED)

            async with factory() as first_session:
                first = RunCoordinatorRepository(first_session)
                first_claim = await first.claim_available(
                    "coordinator-a", lease_seconds=30, limit=10
                )
            assert [lease.run_id for lease in first_claim] == ["active"]
            lease_a = first_claim[0]

            async with factory() as second_session:
                second = RunCoordinatorRepository(second_session)
                assert (
                    await second.claim_available(
                        "coordinator-b", lease_seconds=30, limit=10
                    )
                    == ()
                )

            async with factory() as expiry_session, expiry_session.begin():
                await expiry_session.execute(
                    update(WorkflowRunRecord)
                    .where(WorkflowRunRecord.run_id == "active")
                    .values(
                        coordinator_lease_expires_at=datetime.now(UTC)
                        - timedelta(seconds=1)
                    )
                )

            async with factory() as takeover_session:
                takeover = RunCoordinatorRepository(takeover_session)
                claim_b = await takeover.claim_available(
                    "coordinator-b", lease_seconds=30, limit=10
                )
                assert len(claim_b) == 1
                lease_b = claim_b[0]
                assert lease_b.token != lease_a.token

            async with factory() as stale_session:
                stale = RunCoordinatorRepository(stale_session)
                assert await stale.renew(lease_a, lease_seconds=30) is None
                assert not await stale.release(lease_a)

            async with factory() as current_session:
                current = RunCoordinatorRepository(current_session)
                assert await current.renew(lease_b, lease_seconds=30) is not None
                assert await current.release(lease_b)
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_concurrent_coordinators_claim_distinct_runs() -> None:
    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            await _reset(engine)
            await _seed(factory, "one")
            await _seed(factory, "two")

            async def claim(coordinator_id: str):
                async with factory() as session:
                    return await RunCoordinatorRepository(session).claim_available(
                        coordinator_id,
                        lease_seconds=30,
                        limit=1,
                    )

            first, second = await asyncio.gather(
                claim("coordinator-a"),
                claim("coordinator-b"),
            )
            claimed = (*first, *second)
            assert len(claimed) == 2
            assert {lease.run_id for lease in claimed} == {"one", "two"}
            assert len({lease.token for lease in claimed}) == 2
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.chaos
def test_recovery_is_fenced_by_the_current_coordinator_lease() -> None:
    async def recover(factory, lease) -> object:
        async with factory() as session:
            return await WorkflowRecoveryService(
                WorkflowRepository(session),
                WorkflowRunRepository(session),
            ).recover_run(
                lease.run_id,
                lease.workflow_id,
                lease.workflow_revision,
                coordinator_id=lease.coordinator_id,
                coordinator_lease_token=lease.token,
            )

    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            await _reset(engine)
            await _seed(factory, "resumable")

            async with factory() as session:
                first = await RunCoordinatorRepository(session).claim_available(
                    "coordinator-a", lease_seconds=30, limit=1
                )
            lease_a = first[0]
            result = await recover(factory, lease_a)
            assert result.resumable
            assert result.interrupted_task_ids == ()

            async with factory() as session, session.begin():
                await session.execute(
                    update(WorkflowRunRecord)
                    .where(WorkflowRunRecord.run_id == "resumable")
                    .values(
                        coordinator_lease_expires_at=datetime.now(UTC)
                        - timedelta(seconds=1)
                    )
                )

            async with factory() as session:
                second = await RunCoordinatorRepository(session).claim_available(
                    "coordinator-b", lease_seconds=30, limit=1
                )
            lease_b = second[0]
            assert lease_a.token != lease_b.token

            with pytest.raises(CoordinatorLeaseLostError):
                await recover(factory, lease_a)
            assert (await recover(factory, lease_b)).resumable
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_two_coordinators_cannot_both_run_fenced_recovery() -> None:
    async def recover(factory, lease) -> object:
        async with factory() as session:
            return await WorkflowRecoveryService(
                WorkflowRepository(session),
                WorkflowRunRepository(session),
            ).recover_run(
                lease.run_id,
                lease.workflow_id,
                lease.workflow_revision,
                coordinator_id=lease.coordinator_id,
                coordinator_lease_token=lease.token,
            )

    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            await _reset(engine)
            await _seed(factory, "raced")
            async with factory() as session:
                leases = await RunCoordinatorRepository(session).claim_available(
                    "coordinator-a", lease_seconds=30, limit=1
                )
            lease_a = leases[0]

            stale_b = replace(
                lease_a,
                coordinator_id="coordinator-b",
                token="stale-token",
            )
            accepted, rejected = await asyncio.gather(
                recover(factory, lease_a),
                recover(factory, stale_b),
                return_exceptions=True,
            )
            assert accepted.resumable
            assert isinstance(rejected, CoordinatorLeaseLostError)
        finally:
            await engine.dispose()

    asyncio.run(scenario())
