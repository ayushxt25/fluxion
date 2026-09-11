import asyncio

import pytest

from app.engine.context import TaskExecutionContext
from app.runtime.demo import (
    DemoFailedError,
    DemoOptions,
    DemoTimeoutError,
    UrlLibApiClient,
    run_demo,
)
from app.tasks.demo import (
    DEMO_TASK_IDS,
    build_demo_tasks,
    build_demo_workflow,
    unique_demo_ids,
)
from app.tasks.registry import build_task_registry


class FakeApiClient:
    def __init__(self, run_statuses: list[str]) -> None:
        self.run_statuses = run_statuses
        self.calls: list[tuple[str, str, object]] = []

    def request_json(self, method: str, path: str, payload=None):
        self.calls.append((method, path, payload))
        if method == "POST" and path == "/api/v1/workflows":
            return {"id": payload["id"]}
        if method == "POST" and path.endswith("/runs"):
            return {"run_id": payload["run_id"]}
        if method == "GET" and path.startswith("/api/v1/runs/"):
            status = self.run_statuses.pop(0)
            return {
                "status": status,
                "tasks": [
                    {"task_id": "demo.prepare", "status": status},
                ],
            }
        raise AssertionError(f"unexpected request {method} {path}")


def options() -> DemoOptions:
    return DemoOptions(
        api_url="http://api.example.test",
        token=None,
        timeout_seconds=10,
        poll_seconds=0.01,
    )


def test_demo_task_registry_contains_expected_tasks() -> None:
    registry = build_task_registry()

    assert tuple(sorted(registry.implementations)) == tuple(sorted(DEMO_TASK_IDS))


def test_context_aware_demo_tasks_work() -> None:
    prepare_context = TaskExecutionContext(
        workflow_id="wf",
        run_id="run",
        task_id="demo.prepare",
        attempt_number=1,
        attempt_key="run:demo.prepare:1",
        idempotency_key="run:demo.prepare",
    )
    tasks = build_demo_tasks()

    prepare_result = asyncio.run(tasks["demo.prepare"](prepare_context, seed=21))
    process_result = tasks["demo.process"](
        value=prepare_result["value"],
        multiplier=2,
    )
    finalize_result = asyncio.run(
        tasks["demo.finalize"](
            TaskExecutionContext(
                workflow_id="wf",
                run_id="run",
                task_id="demo.finalize",
                attempt_number=1,
                attempt_key="run:demo.finalize:1",
                idempotency_key="run:demo.finalize",
                dependency_results={"demo.process": process_result},
            ),
            processed=process_result["value"],
        )
    )

    assert prepare_result == {"task": "demo.prepare", "value": 21}
    assert process_result == {"task": "demo.process", "value": 42}
    assert finalize_result == {
        "task": "demo.finalize",
        "processed": 42,
        "status": "complete",
    }


def test_unique_demo_ids() -> None:
    first = unique_demo_ids()
    second = unique_demo_ids()

    assert first != second
    assert first[0].startswith("demo-workflow-")
    assert first[1].startswith("demo-run-")


def test_demo_cli_builds_existing_api_requests_and_stops_on_success() -> None:
    client = FakeApiClient(["RUNNING", "SUCCEEDED"])
    lines: list[str] = []
    result = run_demo(
        client,
        options(),
        id_factory=lambda: ("wf-demo", "run-demo"),
        sleep=lambda seconds: None,
        output=lines.append,
    )

    assert result.status == "SUCCEEDED"
    assert client.calls[0] == (
        "POST",
        "/api/v1/workflows",
        build_demo_workflow("wf-demo").model_dump(
            mode="json",
            exclude_defaults=True,
        ),
    )
    assert client.calls[1] == (
        "POST",
        "/api/v1/workflows/wf-demo/runs",
        {"run_id": "run-demo", "input": {"seed": 21, "multiplier": 2}},
    )
    assert client.calls[-1] == ("GET", "/api/v1/runs/run-demo", None)
    assert "Workflow: SUCCEEDED" in lines


def test_demo_auth_header_is_omitted_without_token() -> None:
    assert "Authorization" not in UrlLibApiClient("http://api", None).headers()


def test_demo_auth_header_is_included_with_token_but_not_printed() -> None:
    client = UrlLibApiClient("http://api", "secret-token")

    assert client.headers()["Authorization"] == "Bearer secret-token"


def test_demo_polling_fails_on_failed_workflow() -> None:
    with pytest.raises(DemoFailedError):
        run_demo(
            FakeApiClient(["FAILED"]),
            options(),
            id_factory=lambda: ("wf-demo", "run-demo"),
            sleep=lambda seconds: None,
            output=lambda line: None,
        )


def test_demo_polling_timeout() -> None:
    ticks = iter((0.0, 0.1, 0.2))

    with pytest.raises(DemoTimeoutError):
        run_demo(
            FakeApiClient(["RUNNING"]),
            DemoOptions(
                api_url="http://api",
                token=None,
                timeout_seconds=0.05,
                poll_seconds=0.01,
            ),
            id_factory=lambda: ("wf-demo", "run-demo"),
            sleep=lambda seconds: None,
            monotonic=lambda: next(ticks),
            output=lambda line: None,
        )
