from collections.abc import Callable
from uuid import uuid4

from app.engine.context import TaskExecutionContext
from app.schemas.workflow import TaskDefinition, WorkflowDefinition

DEMO_TASK_IDS = ("demo.prepare", "demo.process", "demo.finalize")


async def demo_prepare(context: TaskExecutionContext) -> dict[str, int | str]:
    """Context-aware deterministic root task used by the smoke test."""
    _ = (
        context.workflow_id,
        context.run_id,
        context.attempt_number,
        context.attempt_key,
        context.idempotency_key,
    )
    return {"task": context.task_id, "value": 21}


def demo_process(context: TaskExecutionContext) -> dict[str, int | str]:
    """Read the direct dependency result and return a derived value."""
    prepare = context.dependency_results["demo.prepare"]
    if not isinstance(prepare, dict) or prepare.get("value") != 21:
        raise RuntimeError("demo.prepare result was not available.")
    return {"task": context.task_id, "value": int(prepare["value"]) * 2}


async def demo_finalize(context: TaskExecutionContext) -> dict[str, object]:
    """Return a final deterministic summary from the process result."""
    processed = context.dependency_results["demo.process"]
    if not isinstance(processed, dict) or processed.get("value") != 42:
        raise RuntimeError("demo.process result was not available.")
    return {
        "task": context.task_id,
        "prepared": 21,
        "processed": processed["value"],
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
