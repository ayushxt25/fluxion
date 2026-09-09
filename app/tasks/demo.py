from collections.abc import Callable
from uuid import uuid4

from app.engine.context import TaskExecutionContext
from app.schemas.workflow import TaskDefinition, WorkflowDefinition

DEMO_TASK_IDS = ("demo.prepare", "demo.process", "demo.finalize")


async def demo_prepare(context: TaskExecutionContext) -> None:
    """Context-aware no-op used to prove worker context delivery."""
    _ = (
        context.workflow_id,
        context.run_id,
        context.task_id,
        context.attempt_number,
        context.attempt_key,
        context.idempotency_key,
    )


def demo_process() -> None:
    """Deterministic local no-op used by the distributed smoke test."""
    return None


async def demo_finalize(context: TaskExecutionContext) -> None:
    """Context-aware final step for the built-in demo workflow."""
    _ = context.idempotency_key


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
            TaskDefinition(id="demo.prepare", name="Prepare"),
            TaskDefinition(
                id="demo.process",
                name="Process",
                depends_on=("demo.prepare",),
            ),
            TaskDefinition(
                id="demo.finalize",
                name="Finalize",
                depends_on=("demo.process",),
            ),
        ),
    )


def unique_demo_ids() -> tuple[str, str]:
    suffix = uuid4().hex[:12]
    return f"demo-workflow-{suffix}", f"demo-run-{suffix}"
