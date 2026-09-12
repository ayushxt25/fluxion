"""Create and run a parameterized workflow through the public Fluxion SDK."""

import os
from uuid import uuid4

from app.sdk import (
    FluxionClient,
    Retry,
    WorkflowBuilder,
    dependency_result,
    workflow_input,
)

workflow = (
    WorkflowBuilder(f"sdk-demo-{uuid4().hex[:8]}", name="SDK Demo Workflow")
    .task(
        "demo.prepare",
        parameters={"seed": workflow_input("seed")},
    )
    .task(
        "demo.process",
        depends_on=["demo.prepare"],
        retry=Retry(max_attempts=3, initial_delay_seconds=1),
        parameters={
            "value": dependency_result("demo.prepare", "value"),
            "multiplier": workflow_input("multiplier"),
        },
    )
    .task(
        "demo.finalize",
        depends_on=["demo.process"],
        parameters={"processed": dependency_result("demo.process", "value")},
    )
    .build()
)

with FluxionClient(
    os.getenv("FLUXION_API_URL", "http://localhost:8000"),
    token=os.getenv("FLUXION_API_TOKEN"),
) as client:
    client.create_workflow(workflow)
    run = client.run_workflow(
        workflow.id,
        input={"seed": 21, "multiplier": 2},
        wait=True,
    )
    for task in run.tasks:
        print(task.task_id, task.status, task.result)
