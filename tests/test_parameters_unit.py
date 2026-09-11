import asyncio
from types import MappingProxyType

import pytest

from app.engine.context import TaskExecutionContext
from app.engine.exceptions import TaskParameterResolutionError
from app.engine.parameters import resolve_task_parameters
from app.engine.status import WorkflowStatus
from app.schemas.workflow import (
    DependencyResultParameter,
    LiteralParameter,
    TaskDefinition,
    WorkflowDefinition,
    WorkflowInputParameter,
)
from app.services.execution import (
    PersistentWorkflowExecutor,
    _InMemoryTaskAttemptRepository,
)


class FakeWorkflowRepository:
    async def exists(self, workflow_id: str) -> bool:
        return True


class RecordingRunRepository:
    async def create(self, workflow_run) -> None:
        self.workflow_run = workflow_run

    async def save_state(self, workflow_run) -> None:
        self.workflow_run = workflow_run


def workflow(*tasks: TaskDefinition) -> WorkflowDefinition:
    return WorkflowDefinition(id="workflow", name="Workflow", tasks=tasks)


def execute(definition, implementations, *, workflow_input=None, input_present=False):
    run_repository = RecordingRunRepository()
    result = asyncio.run(
        PersistentWorkflowExecutor(
            definition,
            implementations,
            FakeWorkflowRepository(),
            run_repository,
            _InMemoryTaskAttemptRepository(run_repository),
            run_id="run-1",
            workflow_input=workflow_input,
            workflow_input_present=input_present,
        ).run()
    )
    return result, run_repository.workflow_run


def test_literal_workflow_input_and_dependency_parameters_resolve() -> None:
    task = TaskDefinition(
        id="b",
        depends_on=("a",),
        parameters={
            "literal": LiteralParameter(source="literal", value=100),
            "seed": WorkflowInputParameter(source="workflow_input", path=("seed",)),
            "value": DependencyResultParameter(
                source="dependency_result",
                task_id="a",
                path=("value",),
            ),
        },
    )

    resolved = resolve_task_parameters(
        task,
        workflow_input={"seed": 21},
        workflow_input_present=True,
        dependency_results={"a": {"value": 42}},
    )

    assert resolved == {"literal": 100, "seed": 21, "value": 42}


def test_empty_path_returns_entire_source_and_list_index_works() -> None:
    task = TaskDefinition(
        id="a",
        parameters={
            "whole": WorkflowInputParameter(source="workflow_input"),
            "sku": WorkflowInputParameter(source="workflow_input", path=("items", 0)),
        },
    )

    resolved = resolve_task_parameters(
        task,
        workflow_input={"items": ["sku-1"]},
        workflow_input_present=True,
        dependency_results={},
    )

    assert resolved == {"whole": {"items": ["sku-1"]}, "sku": "sku-1"}


def test_missing_path_fails_safely() -> None:
    task = TaskDefinition(
        id="a",
        parameters={
            "seed": WorkflowInputParameter(source="workflow_input", path=("x",))
        },
    )

    with pytest.raises(TaskParameterResolutionError):
        resolve_task_parameters(
            task,
            workflow_input={},
            workflow_input_present=True,
            dependency_results={},
        )


def test_keyword_parameter_task_execution_and_context_input() -> None:
    observed = []

    def prepare(*, seed: int) -> dict[str, int]:
        return {"value": seed}

    def process(
        context: TaskExecutionContext,
        *,
        value: int,
        multiplier: int,
    ) -> dict[str, int]:
        observed.append((context.workflow_input, context.workflow_input_present))
        return {"value": value * multiplier}

    result, run = execute(
        workflow(
            TaskDefinition(
                id="a",
                parameters={
                    "seed": WorkflowInputParameter(
                        source="workflow_input",
                        path=("seed",),
                    ),
                },
            ),
            TaskDefinition(
                id="b",
                depends_on=("a",),
                parameters={
                    "value": DependencyResultParameter(
                        source="dependency_result",
                        task_id="a",
                        path=("value",),
                    ),
                    "multiplier": WorkflowInputParameter(
                        source="workflow_input",
                        path=("multiplier",),
                    ),
                },
            ),
        ),
        {"a": prepare, "b": process},
        workflow_input={"seed": 21, "multiplier": 2},
        input_present=True,
    )

    assert result.status == WorkflowStatus.SUCCEEDED
    assert run.task_runs["b"].result == {"value": 42}
    assert observed == [({"seed": 21, "multiplier": 2}, True)]


def test_context_workflow_input_is_immutable_snapshot() -> None:
    context = TaskExecutionContext(
        workflow_id="workflow",
        run_id="run-1",
        task_id="a",
        attempt_number=1,
        attempt_key="run-1:a:1",
        idempotency_key="run-1:a",
        workflow_input={"items": [1]},
        workflow_input_present=True,
    )

    assert isinstance(context.dependency_results, MappingProxyType)
    assert context.workflow_input["items"] == (1,)
    with pytest.raises(TypeError):
        context.workflow_input["x"] = 2
