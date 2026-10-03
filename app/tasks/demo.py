import asyncio
from collections.abc import Callable
from uuid import uuid4

from app.engine.context import TaskExecutionContext
from app.schemas.workflow import WorkflowDefinition
from app.sdk.workflow import WorkflowBuilder, dependency_result, literal, workflow_input

SMOKE_DEMO_TASK_IDS = ("demo.prepare", "demo.process", "demo.finalize")
PORTFOLIO_DEMO_TASK_IDS = (
    "demo.ingest",
    "demo.validate",
    "demo.transform_a",
    "demo.transform_b",
    "demo.aggregate",
    "demo.publish",
)
DEMO_TASK_IDS = SMOKE_DEMO_TASK_IDS + PORTFOLIO_DEMO_TASK_IDS

_PORTFOLIO_TASK_DELAY_SECONDS = 0.5


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


async def demo_ingest(*, records: list[int]) -> dict[str, object]:
    """Return a small deterministic input payload for the portfolio DAG."""
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {"records": list(records), "count": len(records)}


async def demo_validate(*, records: list[int]) -> dict[str, object]:
    """Validate the demo input without mutating it or invoking external systems."""
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    valid_records = [record for record in records if isinstance(record, int)]
    return {"records": valid_records, "valid_count": len(valid_records)}


async def demo_transform_a(*, records: list[int], branch: str) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {"branch": branch, "values": [record * 2 for record in records]}


async def demo_transform_b(*, records: list[int], branch: str) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {"branch": branch, "values": [record * 3 for record in records]}


async def demo_aggregate(
    *,
    values_a: list[int],
    values_b: list[int],
) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {"combined": [*values_a, *values_b], "branch_count": 2}


async def demo_publish(*, combined: list[int], branch_count: int) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {
        "published": True,
        "summary": {"record_count": len(combined), "branch_count": branch_count},
    }


def build_demo_tasks() -> dict[str, Callable[..., object]]:
    return {
        "demo.prepare": demo_prepare,
        "demo.process": demo_process,
        "demo.finalize": demo_finalize,
        "demo.ingest": demo_ingest,
        "demo.validate": demo_validate,
        "demo.transform_a": demo_transform_a,
        "demo.transform_b": demo_transform_b,
        "demo.aggregate": demo_aggregate,
        "demo.publish": demo_publish,
    }


def build_demo_workflow(workflow_id: str) -> WorkflowDefinition:
    workflow = (
        WorkflowBuilder(workflow_id, name="Fluxion Demo Workflow")
        .task(
            "demo.prepare",
            name="Prepare",
            parameters={"seed": workflow_input("seed")},
        )
        .task(
            "demo.process",
            name="Process",
            depends_on=("demo.prepare",),
            parameters={
                "value": dependency_result("demo.prepare", "value"),
                "multiplier": workflow_input("multiplier"),
            },
        )
        .task(
            "demo.finalize",
            name="Finalize",
            depends_on=("demo.process",),
            parameters={"processed": dependency_result("demo.process", "value")},
        )
        .build()
    )
    return WorkflowDefinition.model_validate(workflow.model_dump(by_alias=True))


def build_portfolio_demo_workflow(workflow_id: str) -> WorkflowDefinition:
    """Build the real fan-out/fan-in workflow shown by the dashboard demo."""
    workflow = (
        WorkflowBuilder(workflow_id, name="Fluxion Portfolio DAG Demo")
        .task(
            "demo.ingest",
            name="Ingest",
            parameters={"records": workflow_input("records")},
        )
        .task(
            "demo.validate",
            name="Validate",
            depends_on=("demo.ingest",),
            parameters={"records": dependency_result("demo.ingest", "records")},
        )
        .task(
            "demo.transform_a",
            name="Transform A",
            depends_on=("demo.validate",),
            parameters={
                "records": dependency_result("demo.validate", "records"),
                "branch": literal("a"),
            },
        )
        .task(
            "demo.transform_b",
            name="Transform B",
            depends_on=("demo.validate",),
            parameters={
                "records": dependency_result("demo.validate", "records"),
                "branch": literal("b"),
            },
        )
        .task(
            "demo.aggregate",
            name="Aggregate",
            depends_on=("demo.transform_a", "demo.transform_b"),
            parameters={
                "values_a": dependency_result("demo.transform_a", "values"),
                "values_b": dependency_result("demo.transform_b", "values"),
            },
        )
        .task(
            "demo.publish",
            name="Publish",
            depends_on=("demo.aggregate",),
            parameters={
                "combined": dependency_result("demo.aggregate", "combined"),
                "branch_count": dependency_result("demo.aggregate", "branch_count"),
            },
        )
        .build()
    )
    return WorkflowDefinition.model_validate(workflow.model_dump(by_alias=True))


def unique_demo_ids() -> tuple[str, str]:
    suffix = uuid4().hex[:12]
    return f"demo-workflow-{suffix}", f"demo-run-{suffix}"


def unique_portfolio_demo_ids() -> tuple[str, str]:
    suffix = uuid4().hex[:12]
    return f"portfolio-demo-workflow-{suffix}", f"portfolio-demo-run-{suffix}"
