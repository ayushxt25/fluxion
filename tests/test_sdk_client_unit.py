import json

import httpx
import pytest

from app.sdk import (
    AuthenticationError,
    ConflictError,
    FluxionAPIError,
    FluxionClient,
    FluxionConnectionError,
    FluxionTimeoutError,
    NotFoundError,
    PermissionDeniedError,
    RateLimitError,
    ServiceUnavailableError,
    ValidationError,
    WorkflowBuilder,
)


def run_payload(status: str = "RUNNING") -> dict[str, object]:
    return {
        "run_id": "run-1",
        "workflow_id": "workflow-1",
        "status": status,
        "created_at": "2026-09-01T00:00:00Z",
        "has_input": False,
        "tasks": [],
    }


def client(handler) -> FluxionClient:
    return FluxionClient(
        "https://fluxion.example/",
        token="secret-token",
        transport=httpx.MockTransport(handler),
    )


def test_sync_client_normalizes_url_and_sends_auth_and_user_agent() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://fluxion.example/health"
        assert request.headers["authorization"] == "Bearer secret-token"
        assert request.headers["user-agent"].startswith("Fluxion-Python/")
        return httpx.Response(200, json={"status": "ok", "service": "fluxion"})

    with client(handler) as sdk:
        assert sdk.base_url == "https://fluxion.example"
        assert sdk.health().status == "ok"
    assert sdk._client.is_closed


def test_create_run_preserves_omitted_and_explicit_null_input() -> None:
    payloads: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        return httpx.Response(201, json=run_payload())

    with client(handler) as sdk:
        sdk.create_run("workflow-1", run_id="run-1")
        sdk.create_run("workflow-1", run_id="run-2", input=None)
        sdk.create_run("workflow-1", input={"value": 42})

    assert payloads == [
        {"run_id": "run-1"},
        {"run_id": "run-2", "input": None},
        {"input": {"value": 42}},
    ]


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (401, AuthenticationError),
        (403, PermissionDeniedError),
        (404, NotFoundError),
        (409, ConflictError),
        (422, ValidationError),
        (503, ServiceUnavailableError),
    ],
)
def test_http_errors_are_typed(status: int, error_type: type[FluxionAPIError]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status,
            json={"error": {"code": "example", "message": "safe message"}},
            headers={"X-Request-ID": "request-1"},
        )

    with client(handler) as sdk, pytest.raises(error_type) as raised:
        sdk.health()
    assert raised.value.request_id == "request-1"


def test_rate_limit_exposes_retry_after_and_malformed_json_is_safe() -> None:
    responses = iter(
        (
            httpx.Response(
                429,
                json={"error": {"code": "rate_limit_exceeded", "message": "slow down"}},
                headers={"Retry-After": "12"},
            ),
            httpx.Response(200, content=b"not-json"),
        )
    )

    with client(lambda request: next(responses)) as sdk:
        with pytest.raises(RateLimitError) as raised:
            sdk.health()
        assert raised.value.retry_after == 12
        with pytest.raises(FluxionAPIError, match="malformed JSON"):
            sdk.health()


@pytest.mark.parametrize(
    ("transport_error", "error_type"),
    [
        (httpx.ConnectError("offline"), FluxionConnectionError),
        (httpx.ReadTimeout("slow"), FluxionTimeoutError),
    ],
)
def test_transport_errors_are_typed(
    transport_error: httpx.HTTPError,
    error_type: type[Exception],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise transport_error

    with client(handler) as sdk, pytest.raises(error_type):
        sdk.health()


def test_workflow_creation_task_and_wait_helpers() -> None:
    workflow = WorkflowBuilder("workflow-1").task("task-1").build()
    responses = iter(
        (
            httpx.Response(201, json=workflow.model_dump(mode="json", by_alias=True)),
            httpx.Response(200, json=run_payload("SUCCEEDED")),
            httpx.Response(
                200,
                json=[
                    {
                        "task_id": "task-1",
                        "status": "SUCCEEDED",
                        "next_retry_at": None,
                        "idempotency_key": "run-1:task-1",
                        "has_result": True,
                        "result": {"value": 42},
                        "attempt_count": 1,
                        "latest_attempt_status": "SUCCEEDED",
                    }
                ],
            ),
        )
    )
    with client(lambda request: next(responses)) as sdk:
        assert sdk.create_workflow(workflow).id == "workflow-1"
        run = sdk.wait_for_run("run-1", timeout=1, poll_interval=0.01)
        assert run.status == "SUCCEEDED"
        assert sdk.list_run_tasks("run-1")[0].result == {"value": 42}
