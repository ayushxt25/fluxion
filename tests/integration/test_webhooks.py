import asyncio
import os
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.db.models.events import RunEventRecord
from app.db.models.execution import WorkflowRunRecord
from app.db.models.webhooks import WebhookDeliveryRecord
from app.db.models.workflow import WorkflowDefinitionRecord
from app.services.webhooks import WebhookRepository

URL = os.environ.get("TEST_DATABASE_URL")
if not URL:
    pytest.skip("TEST_DATABASE_URL is required", allow_module_level=True)


def in_db(body):
    async def scenario():
        engine = create_async_engine(URL)
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
            await connection.run_sync(Base.metadata.create_all)
        try:
            async with async_sessionmaker(engine, expire_on_commit=False)() as session:
                async with session.begin():
                    session.add_all(
                        [
                            WorkflowDefinitionRecord(id="wf-a", name="a"),
                            WorkflowDefinitionRecord(id="wf-b", name="b"),
                        ]
                    )
                assert set(
                    (
                        await session.execute(select(WorkflowDefinitionRecord.id))
                    ).scalars()
                ) == {"wf-a", "wf-b"}
                await session.commit()
                async with session.begin():
                    session.add_all(
                        [
                            WorkflowRunRecord(
                                run_id="run-a",
                                workflow_id="wf-a",
                                status="SUCCEEDED",
                                input=None,
                                input_present=False,
                            ),
                            WorkflowRunRecord(
                                run_id="run-b",
                                workflow_id="wf-b",
                                status="SUCCEEDED",
                                input=None,
                                input_present=False,
                            ),
                        ]
                    )
                await body(session)
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def event(run="run-a", workflow="wf-a", kind="run.succeeded", created=None):
    return RunEventRecord(
        run_id=run,
        workflow_id=workflow,
        event_type=kind,
        version=1,
        payload={},
        created_at=created or datetime.now(UTC),
    )


def test_reconciliation_filters_idempotency_and_unique_delivery():
    async def body(session):
        repo = WebhookRepository(session)
        await repo.create_subscription(
            "match", "https://example.com", "s", ("run.succeeded",), "wf-a"
        )
        await repo.create_subscription(
            "wrong-event", "https://example.com", "s", ("task.failed",), "wf-a"
        )
        await repo.create_subscription(
            "wrong-workflow", "https://example.com", "s", ("run.succeeded",), "wf-b"
        )
        async with session.begin():
            session.add(event())
        assert await repo.reconcile() == 1
        assert await repo.reconcile() == 0
        rows = list((await session.execute(select(WebhookDeliveryRecord))).scalars())
        assert len(rows) == 1

    in_db(body)


def test_historical_and_disabled_subscriptions_do_not_create_intents():
    async def body(session):
        async with session.begin():
            session.add(event(created=datetime.now(UTC) - timedelta(seconds=10)))
        repo = WebhookRepository(session)
        late = await repo.create_subscription(
            "late", "https://example.com", "s", ("run.succeeded",)
        )
        disabled = await repo.create_subscription(
            "disabled", "https://example.com", "s", ("run.succeeded",)
        )
        await repo.disable(disabled.id)
        assert await repo.reconcile() == 0
        assert late.enabled

    in_db(body)
