import asyncio
import os

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.db.models.execution import WorkflowRunRecord
from app.db.models.triggers import WorkflowEventFiringRecord, WorkflowTriggerEventRecord
from app.schemas.triggers import EventIngestRequest, EventSubscriptionCreate
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.repositories import WorkflowRepository
from app.services.triggers import EventIngestionService, EventSubscriptionRepository

URL = os.environ.get("TEST_DATABASE_URL")
if not URL:
    pytest.skip("TEST_DATABASE_URL is required", allow_module_level=True)


def test_event_trigger_idempotency_and_revision_pinning() -> None:
    async def scenario() -> None:
        engine = create_async_engine(URL)
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
            await connection.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as session:
                workflow = WorkflowDefinition(
                    id="event-workflow", name="one", tasks=(TaskDefinition(id="one"),)
                )
                repository = WorkflowRepository(session)
                await repository.save(workflow)
                pinned = await EventSubscriptionRepository(session).create(
                    EventSubscriptionCreate(workflow_id=workflow.id, workflow_revision=1, event_type="deploy", filter_json={"environment": "prod"}),
                    subject="operator", role="OPERATOR",
                )
                latest = await EventSubscriptionRepository(session).create(
                    EventSubscriptionCreate(workflow_id=workflow.id, event_type="deploy", pass_event_payload_as_input=True),
                    subject=None, role=None,
                )
                await repository.publish(WorkflowDefinition(id=workflow.id, name="two", tasks=(TaskDefinition(id="two"),)))
                request = EventIngestRequest(source="ci", external_event_id="evt-1", event_type="deploy", payload={"environment": "prod"})
                first = await EventIngestionService(session).ingest(request)
                duplicate = await EventIngestionService(session).ingest(request)
                assert first.created and not duplicate.created
                assert set(first.run_ids) == set(duplicate.run_ids)
                rows = tuple((await session.execute(select(WorkflowRunRecord))).scalars())
                assert sorted(row.workflow_revision for row in rows) == [1, 2]
                assert any(row.event_subscription_id == latest.id for row in rows)
                assert await session.scalar(select(func.count()).select_from(WorkflowTriggerEventRecord)) == 1
                assert await session.scalar(select(func.count()).select_from(WorkflowEventFiringRecord)) == 2
        finally:
            await engine.dispose()

    asyncio.run(scenario())
