"""Exercise the SDK over an ASGI HTTP transport, not repositories or services."""

import httpx
import pytest

from app.sdk import AsyncFluxionClient, WorkflowBuilder
from app.security.models import Role
from tests.integration.test_api import api_client, auth_headers

pytestmark = pytest.mark.asyncio


async def test_sdk_creates_and_reads_workflow_runs_over_public_api() -> None:
    workflow = (
        WorkflowBuilder("sdk-api-workflow", name="SDK API Workflow")
        .task("demo.prepare")
        .build()
    )
    async with (
        api_client() as (server, _),
        AsyncFluxionClient(
            "http://testserver",
            token=auth_headers(Role.OPERATOR)["Authorization"].removeprefix("Bearer "),
            transport=httpx.ASGITransport(app=server._transport.app),
        ) as client,
    ):
        created = await client.create_workflow(workflow)
        published = await client.publish_workflow(
            workflow.model_copy(update={"name": "SDK API Workflow revision two"})
        )
        latest = await client.get_workflow(created.id)
        first = await client.get_workflow(created.id, revision=1)
        revisions = await client.list_workflow_revisions(created.id)
        run = await client.create_run(created.id, run_id="sdk-api-run", input=None)
        pinned = await client.create_run(
            created.id, run_id="sdk-api-rev1", revision=1
        )
        fetched = await client.get_run(run.run_id)

    assert created.id == workflow.id
    assert (created.revision, published.revision, latest.revision, first.revision) == (
        1,
        2,
        2,
        1,
    )
    assert [item.revision for item in revisions] == [1, 2]
    assert (run.workflow_revision, pinned.workflow_revision) == (2, 1)
    assert fetched.has_input
    assert fetched.input is None
