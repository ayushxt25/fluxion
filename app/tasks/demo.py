from collections.abc import Callable
from uuid import uuid4

from app.engine.context import TaskExecutionContext
from app.schemas.workflow import (
    DependencyResultParameter,
    TaskDefinition,
    WorkflowDefinition,
    WorkflowInputParameter,
)

DEMO_TASK_IDS = ("demo.prepare", "demo.process", "demo.finalize")


async def demo_prepare(
    context: TaskExecutionContext,
    *,
    seed: int,
) -> dict[str, int | str]:
    """Context-aware deterministic root task used by the smoke test."""
    _ = (
        context.workflow_id,
        context.run_id,
        context.attempt_number,
        context.attempt_key,
        context.idempotency_key,
    )
    return {"task": context.task_id, "value": seed}


def demo_process(*, value: int, multiplier: int) -> dict[str, int | str]:
    """Read mapped parameters and return a derived value."""
    return {"task": "demo.process", "value": value * multiplier}


async def demo_finalize(
    context: TaskExecutionContext,
    *,
    processed: int,
) -> dict[str, object]:
    """Return a final deterministic summary from the process result."""
    return {
        "task": context.task_id,
        "processed": processed,
        "status": "complete",
    }


def build_demo_tasks() -> dict[str, Callable[..., object]]:
    return {
        "demo.prepare": demo_prepare,
        "demo.process": demo_process,
        "demo.finalize": demo_finalize,
    }


def build_demo_workflow(workflow_id: str) -> WorkflowDefinition:
    return WorkflowDefinition(
        id=workflow_id,
        name="Fluxion Demo Workflow",
        tasks=(
            TaskDefinition(
                id="demo.prepare",
                name="Prepare",
                parameters={
                    "seed": WorkflowInputParameter(
                        source="workflow_input",
                        path=("seed",),
                    ),
                },
            ),
            TaskDefinition(
                id="demo.process",
                name="Process",
                depends_on=("demo.prepare",),
                parameters={
                    "value": DependencyResultParameter(
                        source="dependency_result",
                        task_id="demo.prepare",
                        path=("value",),
                    ),
                    "multiplier": WorkflowInputParameter(
                        source="workflow_input",
                        path=("multiplier",),
                    ),
                },
            ),
            TaskDefinition(
                id="demo.finalize",
                name="Finalize",
                depends_on=("demo.process",),
                parameters={
                    "processed": DependencyResultParameter(
                        source="dependency_result",
                        task_id="demo.process",
                        path=("value",),
                    ),
                },
            ),
        ),
    )


def unique_demo_ids() -> tuple[str, str]:
    suffix = uuid4().hex[:12]
    return f"demo-workflow-{suffix}", f"demo-run-{suffix}"
