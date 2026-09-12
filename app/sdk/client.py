import time
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel, TypeAdapter
from pydantic import ValidationError as PydanticValidationError

from app.sdk.errors import FluxionAPIError, FluxionTimeoutError
from app.sdk.http import (
    build_headers,
    map_http_error,
    normalize_base_url,
    parse_json,
    raise_for_response,
)
from app.sdk.models import (
    AuditEventList,
    Health,
    LeaseReapResult,
    OutboxPublishResult,
    Readiness,
    RecoveryResult,
    SchedulerTickResult,
    TaskAttemptList,
    TaskRun,
    Workflow,
    WorkflowList,
    WorkflowRun,
    WorkflowRunList,
)

_MISSING = object()
_T = TypeVar("_T", bound=BaseModel)


class OperationsClient:
    def __init__(self, client: "FluxionClient") -> None:
        self._client = client

    def scheduler_tick(self) -> SchedulerTickResult:
        return self._client._model(
            "POST", "/api/v1/ops/scheduler/tick", SchedulerTickResult
        )

    def publish_outbox(self) -> OutboxPublishResult:
        return self._client._model(
            "POST", "/api/v1/ops/outbox/publish", OutboxPublishResult
        )

    def reap_leases(self) -> LeaseReapResult:
        return self._client._model("POST", "/api/v1/ops/leases/reap", LeaseReapResult)

    def list_audit(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        action: str | None = None,
        principal_subject: str | None = None,
        outcome: str | None = None,
    ) -> AuditEventList:
        return self._client._model(
            "GET",
            "/api/v1/ops/audit",
            AuditEventList,
            params={
                "limit": limit,
                "offset": offset,
                "action": action,
                "principal_subject": principal_subject,
                "outcome": outcome,
            },
        )


class FluxionClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        timeout: float | httpx.Timeout = 10.0,
        user_agent: str | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = normalize_base_url(base_url)
        self.timeout = timeout
        self._client = httpx.Client(
            base_url=self.base_url,
            headers=build_headers(token, user_agent),
            timeout=timeout,
            transport=transport,
        )
        self.ops = OperationsClient(self)

    def __enter__(self) -> "FluxionClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def health(self) -> Health:
        return self._model("GET", "/health", Health)

    def readiness(self) -> Readiness:
        return self._model("GET", "/ready", Readiness)

    def create_workflow(self, workflow: Workflow) -> Workflow:
        return self._model(
            "POST",
            "/api/v1/workflows",
            Workflow,
            json=workflow.model_dump(mode="json", by_alias=True),
        )

    def get_workflow(self, workflow_id: str) -> Workflow:
        return self._model("GET", f"/api/v1/workflows/{workflow_id}", Workflow)

    def list_workflows(self, *, limit: int = 50, offset: int = 0) -> WorkflowList:
        return self._model(
            "GET",
            "/api/v1/workflows",
            WorkflowList,
            params={"limit": limit, "offset": offset},
        )

    def create_run(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        input: Any = _MISSING,
    ) -> WorkflowRun:
        payload: dict[str, Any] = {}
        if run_id is not None:
            payload["run_id"] = run_id
        if input is not _MISSING:
            payload["input"] = input
        return self._model(
            "POST", f"/api/v1/workflows/{workflow_id}/runs", WorkflowRun, json=payload
        )

    def get_run(self, run_id: str) -> WorkflowRun:
        return self._model("GET", f"/api/v1/runs/{run_id}", WorkflowRun)

    def list_runs(
        self,
        *,
        workflow_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> WorkflowRunList:
        return self._model(
            "GET",
            "/api/v1/runs",
            WorkflowRunList,
            params={
                "workflow_id": workflow_id,
                "status": status,
                "limit": limit,
                "offset": offset,
            },
        )

    def list_run_tasks(self, run_id: str) -> tuple[TaskRun, ...]:
        try:
            return TypeAdapter(tuple[TaskRun, ...]).validate_python(
                self._request("GET", f"/api/v1/runs/{run_id}/tasks")
            )
        except PydanticValidationError as exc:
            raise FluxionAPIError("Fluxion API returned an invalid response.") from exc

    def get_task(self, run_id: str, task_id: str) -> TaskRun:
        return self._model("GET", f"/api/v1/runs/{run_id}/tasks/{task_id}", TaskRun)

    def list_task_attempts(self, run_id: str, task_id: str) -> TaskAttemptList:
        return self._model(
            "GET", f"/api/v1/runs/{run_id}/tasks/{task_id}/attempts", TaskAttemptList
        )

    def cancel_run(self, run_id: str) -> WorkflowRun:
        return self._model("POST", f"/api/v1/runs/{run_id}/cancel", WorkflowRun)

    def recover_run(self, run_id: str) -> RecoveryResult:
        return self._model("POST", f"/api/v1/runs/{run_id}/recover", RecoveryResult)

    def resume_run(self, run_id: str) -> WorkflowRun:
        return self._model("POST", f"/api/v1/runs/{run_id}/resume", WorkflowRun)

    def run_workflow(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        input: Any = _MISSING,
        wait: bool = False,
        timeout: float = 60.0,
        poll_interval: float = 0.5,
    ) -> WorkflowRun:
        run = self.create_run(workflow_id, run_id=run_id, input=input)
        if wait:
            return self.wait_for_run(
                run.run_id, timeout=timeout, poll_interval=poll_interval
            )
        return run

    def wait_for_run(
        self,
        run_id: str,
        *,
        timeout: float = 60.0,
        poll_interval: float = 0.5,
    ) -> WorkflowRun:
        if timeout <= 0 or poll_interval <= 0:
            raise ValueError("timeout and poll_interval must be positive.")
        deadline = time.monotonic() + timeout
        while True:
            run = self.get_run(run_id)
            if run.status in {"SUCCEEDED", "FAILED", "CANCELLED", "INTERRUPTED"}:
                return run
            if time.monotonic() >= deadline:
                raise FluxionTimeoutError(
                    f"Run '{run_id}' did not finish within {timeout:g}s."
                )
            time.sleep(min(poll_interval, max(0, deadline - time.monotonic())))

    def _model(self, method: str, path: str, model: type[_T], **kwargs: Any) -> _T:
        try:
            return model.model_validate(self._request(method, path, **kwargs))
        except PydanticValidationError as exc:
            raise FluxionAPIError("Fluxion API returned an invalid response.") from exc

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            response = self._client.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise map_http_error(exc) from exc
        raise_for_response(response)
        return parse_json(response)
