"""Create and inspect a workflow using the async Fluxion SDK client."""

import asyncio
import os
from uuid import uuid4

from app.sdk import AsyncFluxionClient, WorkflowBuilder, workflow_input


async def main() -> None:
    workflow = (
        WorkflowBuilder(f"sdk-async-{uuid4().hex[:8]}", name="Async SDK Demo")
        .task("demo.prepare", parameters={"seed": workflow_input("seed")})
        .build()
    )
    async with AsyncFluxionClient(
        os.getenv("FLUXION_API_URL", "http://localhost:8000"),
        token=os.getenv("FLUXION_API_TOKEN"),
    ) as client:
        await client.create_workflow(workflow)
        run = await client.run_workflow(workflow.id, input={"seed": 21}, wait=True)
        print(run.status)


if __name__ == "__main__":
    asyncio.run(main())
