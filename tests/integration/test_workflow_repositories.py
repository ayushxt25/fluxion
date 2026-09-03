import os
import warnings
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
import asyncio

from sqlalchemy.exc import IntegrityError, SAWarning
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.db.models.workflow import (
    TaskDefinitionRecord,
    TaskDependencyRecord,
    WorkflowDefinitionRecord,
)
from app.engine.dag import WorkflowDAG
from app.engine.exceptions import (
    WorkflowAlreadyExistsError,
    WorkflowNotFoundError,
    WorkflowRunNotFoundError,
    WorkflowValidationError,
)
from app.engine.execution import WorkflowRun
from app.engine.status import TaskStatus, WorkflowStatus
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.repositories import WorkflowRepository, WorkflowRunRepository


def task(task_id: str, depends_on: tuple[str, ...] = ()) -> TaskDefinition:
    return TaskDefinition(id=task_id, name=f"Task {task_id}", depends_on=depends_on)


def workflow(workflow_id: str, *tasks: TaskDefinition) -> WorkflowDefinition:
    return WorkflowDefinition(id=workflow_id, name="Workflow", tasks=tasks)


async def reset_schema(engine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)


def run_in_db(test_body):
    async def scenario():
        engine = create_async_engine(TEST_DATABASE_URL)
        await reset_schema(engine)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                await test_body(session)
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_save_load_single_task_workflow() -> None:
    async def body(session):
        definition = workflow("wf-single", task("a"))
        repository = WorkflowRepository(session)

        await repository.save(definition)
        loaded = await repository.get("wf-single")

        assert loaded == definition

    run_in_db(body)


def test_save_load_branching_workflow() -> None:
    async def body(session):
        definition = workflow(
            "wf-branch",
            task("a"),
            task("b", ("a",)),
            task("c", ("a",)),
        )
        repository = WorkflowRepository(session)

        await repository.save(definition)
        loaded = await repository.get("wf-branch")

        assert loaded == definition

    run_in_db(body)


def test_dependency_relationships_survive_round_trip() -> None:
    async def body(session):
        definition = workflow("wf-deps", task("a"), task("b", ("a",)))
        repository = WorkflowRepository(session)

        await repository.save(definition)
        loaded = await repository.get("wf-deps")

        assert loaded.tasks[1].depends_on == ("a",)

    run_in_db(body)


def test_disconnected_workflow_survives_round_trip() -> None:
    async def body(session):
        definition = workflow(
            "wf-disconnected",
            task("a"),
            task("b", ("a",)),
            task("c"),
        )
        repository = WorkflowRepository(session)

        await repository.save(definition)
        loaded = await repository.get("wf-disconnected")

        assert loaded == definition

    run_in_db(body)


def test_duplicate_workflow_id_rejected() -> None:
    async def body(session):
        definition = workflow("wf-dup", task("a"))
        repository = WorkflowRepository(session)

        await repository.save(definition)
        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter("always")
            with pytest.raises(WorkflowAlreadyExistsError):
                await repository.save(definition)

        assert not [
            warning for warning in caught_warnings if warning.category is SAWarning
        ]
        loaded = await repository.get("wf-dup")
        assert loaded == definition

    run_in_db(body)


def test_create_workflow_run_and_initial_task_statuses_persist() -> None:
    async def body(session):
        definition = workflow("wf-run", task("a"), task("b", ("a",)))
        await WorkflowRepository(session).save(definition)
        workflow_run = WorkflowRun.create("run-1", definition)

        await WorkflowRunRepository(session).create(workflow_run)
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert loaded.status == WorkflowStatus.PENDING
        assert loaded.get_task_status("a") == TaskStatus.READY
        assert loaded.get_task_status("b") == TaskStatus.BLOCKED

    run_in_db(body)


def test_task_and_workflow_status_updates_persist() -> None:
    async def body(session):
        definition = workflow("wf-update", task("a"))
        await WorkflowRepository(session).save(definition)
        workflow_run = WorkflowRun.create("run-1", definition)
        await WorkflowRunRepository(session).create(workflow_run)

        workflow_run.start_task("a")
        workflow_run.complete_task("a")
        await WorkflowRunRepository(session).save_state(workflow_run)
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert loaded.status == WorkflowStatus.SUCCEEDED
        assert loaded.get_task_status("a") == TaskStatus.SUCCEEDED

    run_in_db(body)


def test_partially_executed_run_reloads_exactly() -> None:
    async def body(session):
        definition = workflow(
            "wf-partial",
            task("extract"),
            task("transform", ("extract",)),
        )
        await WorkflowRepository(session).save(definition)
        workflow_run = WorkflowRun.create("run-1", definition)
        workflow_run.start_task("extract")
        workflow_run.complete_task("extract")
        workflow_run.start_task("transform")

        await WorkflowRunRepository(session).create(workflow_run)
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert loaded.get_task_status("extract") == TaskStatus.SUCCEEDED
        assert loaded.get_task_status("transform") == TaskStatus.RUNNING

    run_in_db(body)


def test_failed_and_cancelled_runs_reload_exactly() -> None:
    async def body(session):
        definition = workflow("wf-terminal", task("a"), task("b"))
        await WorkflowRepository(session).save(definition)
        failed = WorkflowRun.create("failed", definition)
        failed.start_task("a")
        failed.fail_task("a")
        cancelled = WorkflowRun.create("cancelled", definition)
        cancelled.cancel_workflow()

        repository = WorkflowRunRepository(session)
        await repository.create(failed)
        await repository.create(cancelled)

        loaded_failed = await repository.get("failed", definition)
        loaded_cancelled = await repository.get("cancelled", definition)
        assert loaded_failed.status == WorkflowStatus.FAILED
        assert loaded_cancelled.status == WorkflowStatus.CANCELLED

    run_in_db(body)


def test_separate_runs_of_same_workflow_remain_independent() -> None:
    async def body(session):
        definition = workflow("wf-independent", task("a"))
        await WorkflowRepository(session).save(definition)
        first = WorkflowRun.create("run-1", definition)
        second = WorkflowRun.create("run-2", definition)
        first.start_task("a")
        first.complete_task("a")

        repository = WorkflowRunRepository(session)
        await repository.create(first)
        await repository.create(second)

        loaded_first = await repository.get("run-1", definition)
        loaded_second = await repository.get("run-2", definition)
        assert loaded_first.status == WorkflowStatus.SUCCEEDED
        assert loaded_second.status == WorkflowStatus.PENDING

    run_in_db(body)


def test_unknown_workflow_and_run_lookup() -> None:
    async def body(session):
        with pytest.raises(WorkflowNotFoundError):
            await WorkflowRepository(session).get("missing")

        with pytest.raises(WorkflowRunNotFoundError):
            await WorkflowRunRepository(session).get(
                "missing",
                workflow("wf", task("a")),
            )

    run_in_db(body)


def test_run_lookup_requires_matching_workflow() -> None:
    async def body(session):
        first = workflow("wf-a", task("a"))
        second = workflow("wf-b", task("a"))
        workflow_repository = WorkflowRepository(session)
        await workflow_repository.save(first)
        await workflow_repository.save(second)
        await WorkflowRunRepository(session).create(WorkflowRun.create("run-1", first))

        with pytest.raises(WorkflowRunNotFoundError):
            await WorkflowRunRepository(session).get("run-1", second)

    run_in_db(body)


def test_transaction_rollback_on_invalid_workflow_persistence() -> None:
    async def body(session):
        repository = WorkflowRepository(session)
        with pytest.raises(WorkflowValidationError):
            await repository.save(workflow("wf-invalid", task("a", ("missing",))))

        with pytest.raises(WorkflowNotFoundError):
            await repository.get("wf-invalid")

    run_in_db(body)


def test_atomic_rollback_after_flushed_workflow_and_tasks() -> None:
    async def body(session):
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(WorkflowDefinitionRecord(id="wf-atomic", name="Workflow"))
                session.add(
                    TaskDefinitionRecord(
                        workflow_id="wf-atomic",
                        task_id="a",
                        name="Task a",
                    )
                )
                await session.flush()
                session.add(
                    TaskDependencyRecord(
                        workflow_id="wf-atomic",
                        task_id="a",
                        depends_on_task_id="missing",
                    )
                )

        with pytest.raises(WorkflowNotFoundError):
            await WorkflowRepository(session).get("wf-atomic")

    run_in_db(body)


def test_domain_mutation_after_save_does_not_mutate_db_state() -> None:
    async def body(session):
        definition = workflow("wf-copy", task("a"))
        repository = WorkflowRepository(session)
        await repository.save(definition)

        changed = definition.model_copy(update={"name": "Changed"})
        loaded = await repository.get("wf-copy")

        assert changed.name == "Changed"
        assert loaded.name == "Workflow"

    run_in_db(body)


def test_repositories_return_domain_objects_and_loaded_workflow_is_valid() -> None:
    async def body(session):
        definition = workflow("wf-domain", task("a"), task("b", ("a",)))
        repository = WorkflowRepository(session)

        await repository.save(definition)
        loaded = await repository.get("wf-domain")

        assert isinstance(loaded, WorkflowDefinition)
        assert WorkflowDAG(loaded).topological_order() == ("a", "b")

    run_in_db(body)


def test_run_reconstruction_does_not_reinitialize_task_statuses() -> None:
    async def body(session):
        definition = workflow("wf-restore", task("a"), task("b", ("a",)))
        await WorkflowRepository(session).save(definition)
        workflow_run = WorkflowRun.create("run-1", definition)
        workflow_run.start_task("a")
        workflow_run.complete_task("a")
        workflow_run.start_task("b")
        await WorkflowRunRepository(session).create(workflow_run)

        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert loaded.get_task_status("a") == TaskStatus.SUCCEEDED
        assert loaded.get_task_status("b") == TaskStatus.RUNNING

    run_in_db(body)
