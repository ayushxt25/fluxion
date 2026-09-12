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
    async with api_client() as (server, _), AsyncFluxionClient(
        "http://testserver",
        token=auth_headers(Role.OPERATOR)["Authorization"].removeprefix("Bearer "),
        transport=httpx.ASGITransport(app=server._transport.app),
    ) as client:
        created = await client.create_workflow(workflow)
        run = await client.create_run(created.id, run_id="sdk-api-run", input=None)
        fetched = await client.get_run(run.run_id)

    assert created.id == workflow.id
    assert fetched.has_input
    assert fetched.input is None
