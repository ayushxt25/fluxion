import asyncio
from collections.abc import Callable
from uuid import uuid4

from app.engine.context import TaskExecutionContext
from app.schemas.workflow import WorkflowDefinition
from app.sdk.models import Retry
from app.sdk.workflow import WorkflowBuilder, dependency_result, workflow_input

SMOKE_DEMO_TASK_IDS = ("demo.prepare", "demo.process", "demo.finalize")
DOCUMENT_DEMO_TASK_IDS = (
    "demo.document.ingest",
    "demo.document.validate",
    "demo.document.extract_text",
    "demo.document.extract_metadata",
    "demo.document.aggregate",
    "demo.document.summarize",
    "demo.document.persist",
)
ETL_DEMO_TASK_IDS = (
    "demo.etl.fetch",
    "demo.etl.validate",
    "demo.etl.clean",
    "demo.etl.features",
    "demo.etl.merge",
    "demo.etl.persist",
)
PORTFOLIO_DEMO_TASK_IDS = DOCUMENT_DEMO_TASK_IDS
DEMO_TASK_IDS = SMOKE_DEMO_TASK_IDS + DOCUMENT_DEMO_TASK_IDS + ETL_DEMO_TASK_IDS

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


async def document_ingest(*, document: dict[str, object]) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {"document": document, "document_id": document["id"]}


async def document_validate(*, document: dict[str, object]) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {
        "document": document,
        "valid": bool(document.get("title") and document.get("body")),
    }


async def document_extract_text(*, document: dict[str, object]) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    body = str(document["body"])
    return {"text": body, "word_count": len(body.split())}


async def document_extract_metadata(
    *, document: dict[str, object]
) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {
        "title": document["title"],
        "author": document["author"],
        "tags": document["tags"],
    }


async def document_aggregate(
    *, text: str, metadata: dict[str, object]
) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {"text": text, "metadata": metadata}


async def document_summarize(*, text: str, title: str) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {
        "summary": f"{title}: {len(text.split())} words processed.",
        "word_count": len(text.split()),
    }


async def document_persist(*, summary: dict[str, object]) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {"demo_persisted": True, "summary": summary}


async def etl_fetch(*, dataset: list[dict[str, int]]) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {"records": dataset, "source": "synthetic-demo"}


async def etl_validate(
    context: TaskExecutionContext, *, records: list[dict[str, int]]
) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    if context.attempt_number == 1:
        raise RuntimeError("Simulated transient dataset dependency failure.")
    return {"records": records, "valid_count": len(records)}


async def etl_clean(*, records: list[dict[str, int]]) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {
        "records": [
            {**record, "value": max(record["value"], 0)} for record in records
        ]
    }


async def etl_features(*, records: list[dict[str, int]]) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {
        "features": [
            {"id": record["id"], "squared": record["value"] ** 2}
            for record in records
        ]
    }


async def etl_merge(
    *, records: list[dict[str, int]], features: list[dict[str, int]]
) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {"records": records, "features": features, "record_count": len(records)}


async def etl_persist(*, merged: dict[str, object]) -> dict[str, object]:
    await asyncio.sleep(_PORTFOLIO_TASK_DELAY_SECONDS)
    return {"demo_persisted": True, "record_count": merged["record_count"]}


def build_demo_tasks() -> dict[str, Callable[..., object]]:
    return {
        "demo.prepare": demo_prepare,
        "demo.process": demo_process,
        "demo.finalize": demo_finalize,
        "demo.document.ingest": document_ingest,
        "demo.document.validate": document_validate,
        "demo.document.extract_text": document_extract_text,
        "demo.document.extract_metadata": document_extract_metadata,
        "demo.document.aggregate": document_aggregate,
        "demo.document.summarize": document_summarize,
        "demo.document.persist": document_persist,
        "demo.etl.fetch": etl_fetch,
        "demo.etl.validate": etl_validate,
        "demo.etl.clean": etl_clean,
        "demo.etl.features": etl_features,
        "demo.etl.merge": etl_merge,
        "demo.etl.persist": etl_persist,
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


def build_document_demo_workflow(workflow_id: str) -> WorkflowDefinition:
    """Build the deterministic document-processing fan-out/fan-in demo."""
    workflow = (
        WorkflowBuilder(workflow_id, name="Document Processing Pipeline")
        .task(
            "demo.document.ingest",
            name="Ingest Document",
            parameters={"document": workflow_input("document")},
        )
        .task(
            "demo.document.validate",
            name="Validate Document",
            depends_on=("demo.document.ingest",),
            parameters={
                "document": dependency_result("demo.document.ingest", "document")
            },
        )
        .task(
            "demo.document.extract_text",
            name="Extract Text",
            depends_on=("demo.document.validate",),
            parameters={
                "document": dependency_result("demo.document.validate", "document")
            },
        )
        .task(
            "demo.document.extract_metadata",
            name="Extract Metadata",
            depends_on=("demo.document.validate",),
            parameters={
                "document": dependency_result("demo.document.validate", "document")
            },
        )
        .task(
            "demo.document.aggregate",
            name="Aggregate",
            depends_on=("demo.document.extract_text", "demo.document.extract_metadata"),
            parameters={
                "text": dependency_result("demo.document.extract_text", "text"),
                "metadata": dependency_result("demo.document.extract_metadata"),
            },
        )
        .task(
            "demo.document.summarize",
            name="Generate Summary",
            depends_on=("demo.document.aggregate",),
            parameters={
                "text": dependency_result("demo.document.aggregate", "text"),
                "title": dependency_result(
                    "demo.document.aggregate", "metadata", "title"
                ),
            },
        )
        .task(
            "demo.document.persist",
            name="Persist Result",
            depends_on=("demo.document.summarize",),
            parameters={"summary": dependency_result("demo.document.summarize")},
        )
        .build()
    )
    return WorkflowDefinition.model_validate(workflow.model_dump(by_alias=True))


def build_etl_demo_workflow(workflow_id: str) -> WorkflowDefinition:
    """Build the deterministic ETL demo with one engine-managed retry."""
    workflow = (
        WorkflowBuilder(workflow_id, name="Resilient ETL Pipeline")
        .task(
            "demo.etl.fetch",
            name="Fetch Dataset",
            parameters={"dataset": workflow_input("dataset")},
        )
        .task(
            "demo.etl.validate",
            name="Validate Schema",
            depends_on=("demo.etl.fetch",),
            parameters={"records": dependency_result("demo.etl.fetch", "records")},
            retry=Retry(max_attempts=2, initial_delay_seconds=1),
        )
        .task(
            "demo.etl.clean",
            name="Clean Records",
            depends_on=("demo.etl.validate",),
            parameters={"records": dependency_result("demo.etl.validate", "records")},
        )
        .task(
            "demo.etl.features",
            name="Compute Features",
            depends_on=("demo.etl.validate",),
            parameters={"records": dependency_result("demo.etl.validate", "records")},
        )
        .task(
            "demo.etl.merge",
            name="Merge",
            depends_on=("demo.etl.clean", "demo.etl.features"),
            parameters={
                "records": dependency_result("demo.etl.clean", "records"),
                "features": dependency_result("demo.etl.features", "features"),
            },
        )
        .task(
            "demo.etl.persist",
            name="Persist Dataset",
            depends_on=("demo.etl.merge",),
            parameters={"merged": dependency_result("demo.etl.merge")},
        )
        .build()
    )
    return WorkflowDefinition.model_validate(workflow.model_dump(by_alias=True))


def build_portfolio_demo_workflow(workflow_id: str) -> WorkflowDefinition:
    """Compatibility name for the document-processing portfolio workflow."""
    return build_document_demo_workflow(workflow_id)


def unique_demo_ids() -> tuple[str, str]:
    suffix = uuid4().hex[:12]
    return f"demo-workflow-{suffix}", f"demo-run-{suffix}"


def unique_portfolio_demo_ids() -> tuple[str, str]:
    return unique_document_demo_ids()


def unique_document_demo_ids() -> tuple[str, str]:
    suffix = uuid4().hex[:12]
    return f"document-demo-workflow-{suffix}", f"document-demo-run-{suffix}"


def unique_etl_demo_ids() -> tuple[str, str]:
    suffix = uuid4().hex[:12]
    return f"etl-demo-workflow-{suffix}", f"etl-demo-run-{suffix}"
