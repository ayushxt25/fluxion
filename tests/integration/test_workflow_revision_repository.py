import asyncio
import os

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.engine.exceptions import WorkflowNotFoundError
from app.schemas.workflow import (
    DependencyResultParameter,
    LiteralParameter,
    RetryPolicy,
    TaskDefinition,
    WorkflowDefinition,
    WorkflowInputParameter,
)
from app.services.management import WorkflowRunManagementService
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)

URL = os.environ.get("TEST_DATABASE_URL")
if not URL:
    pytest.skip("TEST_DATABASE_URL is required", allow_module_level=True)


def workflow(workflow_id: str = "revision-workflow") -> WorkflowDefinition:
    return WorkflowDefinition(
        id=workflow_id,
        name="Revision workflow",
        tasks=(
            TaskDefinition(
                id="prepare",
                retry_policy=RetryPolicy(max_attempts=3, initial_backoff_seconds=2),
                parameters={"source": WorkflowInputParameter(path=("source",))},
            ),
            TaskDefinition(
                id="process",
                depends_on=("prepare",),
                parameters={
                    "previous": DependencyResultParameter(task_id="prepare"),
                    "constant": LiteralParameter(value={"mode": "safe"}),
                },
            ),
        ),
    )


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


def test_revision_publish_reads_fidelity_and_immutability():
    async def body(session):
        repository = WorkflowRepository(session)
        original = workflow()
        first = await repository.publish(original)
        second = await repository.publish(original.model_copy(update={"revision": 99}))
        changed = workflow().model_copy(update={"name": "Changed"})
        third = await repository.publish(changed)
        assert (first.revision, second.revision, third.revision) == (1, 2, 3)
        assert await repository.get_revision(original.id, 1) == original
        assert (await repository.get_revision(original.id, 2)).revision == 2
        assert (await repository.get_latest(original.id)).revision == 3
        assert [
            item.revision for item in await repository.list_revisions(original.id)
        ] == [1, 2, 3]
        with pytest.raises(WorkflowNotFoundError):
            await repository.get_revision(original.id, 99)

    in_db(body)


def test_revision_sequences_are_independent():
    async def body(session):
        repository = WorkflowRepository(session)
        assert (await repository.publish(workflow("a"))).revision == 1
        assert (await repository.publish(workflow("b"))).revision == 1
        assert (await repository.publish(workflow("a"))).revision == 2

    in_db(body)


def test_same_workflow_concurrent_publishes_are_contiguous():
    async def body(session):
        repository = WorkflowRepository(session)
        await repository.publish(workflow())
        factory = async_sessionmaker(session.bind, expire_on_commit=False)

        async def publish_one():
            async with factory() as concurrent_session:
                return await WorkflowRepository(concurrent_session).publish(workflow())

        published = await asyncio.gather(*(publish_one() for _ in range(4)))
        assert sorted(item.revision for item in published) == [2, 3, 4, 5]
        assert [
            item.revision
            for item in await repository.list_revisions("revision-workflow")
        ] == [1, 2, 3, 4, 5]

    in_db(body)


def test_child_insert_failure_rolls_back_header_and_reuses_revision(monkeypatch):
    async def body(session):
        repository = WorkflowRepository(session)
        await repository.publish(workflow())
        original = session.add_all

        def fail_children(rows):
            raise RuntimeError("simulated child insert failure")

        monkeypatch.setattr(session, "add_all", fail_children)
        with pytest.raises(RuntimeError, match="child insert"):
            await repository.publish(workflow())
        monkeypatch.setattr(session, "add_all", original)
        assert [
            item.revision
            for item in await repository.list_revisions("revision-workflow")
        ] == [1]
        assert (await repository.publish(workflow())).revision == 2

    in_db(body)


def test_new_and_existing_runs_remain_pinned_to_immutable_revisions():
    async def body(session):
        workflow_repository = WorkflowRepository(session)
        # The legacy row remains the transitional FK anchor until Phase 30A's
        # storage cutover; immutable publishing itself is append-only.
        await workflow_repository.save(workflow())
        first = await workflow_repository.publish(workflow())
        service = WorkflowRunManagementService(
            workflow_repository,
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
        )
        await service.create_run(first.id, run_id="latest-one")
        second = await workflow_repository.publish(
            workflow().model_copy(
                update={
                    "name": "Revision two",
                    "tasks": (workflow().tasks[0],),
                }
            )
        )
        await service.create_run(first.id, run_id="explicit-one", workflow_revision=1)
        await service.create_run(first.id, run_id="latest-two")

        run_repository = WorkflowRunRepository(session)
        assert await run_repository.get_workflow_reference("latest-one") == (
            first.id,
            1,
        )
        assert await run_repository.get_workflow_reference("explicit-one") == (
            first.id,
            1,
        )
        assert await run_repository.get_workflow_reference("latest-two") == (
            first.id,
            second.revision,
        )
        reloaded = await run_repository.get(
            "latest-one", await workflow_repository.get_revision(first.id, 1)
        )
        assert reloaded.workflow_revision == 1
        assert set(reloaded.task_runs) == {"prepare", "process"}
        with pytest.raises(WorkflowNotFoundError):
            await service.create_run(first.id, run_id="unknown", workflow_revision=99)

    in_db(body)
