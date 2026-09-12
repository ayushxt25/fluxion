import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel, TypeAdapter
from pydantic import ValidationError as PydanticValidationError

from app.sdk.client import _MISSING
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
    RunEvent,
    RunEventList,
    SchedulerTickResult,
    TaskAttemptList,
    TaskRun,
    Workflow,
    WorkflowList,
    WorkflowRun,
    WorkflowRunList,
)

_T = TypeVar("_T", bound=BaseModel)


class AsyncOperationsClient:
    def __init__(self, client: "AsyncFluxionClient") -> None:
        self._client = client

    async def scheduler_tick(self) -> SchedulerTickResult:
        return await self._client._model(
            "POST", "/api/v1/ops/scheduler/tick", SchedulerTickResult
        )

    async def publish_outbox(self) -> OutboxPublishResult:
        return await self._client._model(
            "POST", "/api/v1/ops/outbox/publish", OutboxPublishResult
        )

    async def reap_leases(self) -> LeaseReapResult:
        return await self._client._model(
            "POST", "/api/v1/ops/leases/reap", LeaseReapResult
        )

    async def list_audit(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        action: str | None = None,
        principal_subject: str | None = None,
        outcome: str | None = None,
    ) -> AuditEventList:
        return await self._client._model(
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


class AsyncFluxionClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        timeout: float | httpx.Timeout = 10.0,
        user_agent: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = normalize_base_url(base_url)
        self.timeout = timeout
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=build_headers(token, user_agent),
            timeout=timeout,
            transport=transport,
        )
        self.ops = AsyncOperationsClient(self)

    async def __aenter__(self) -> "AsyncFluxionClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def health(self) -> Health:
        return await self._model("GET", "/health", Health)

    async def readiness(self) -> Readiness:
        return await self._model("GET", "/ready", Readiness)

    async def create_workflow(self, workflow: Workflow) -> Workflow:
        return await self._model(
            "POST",
            "/api/v1/workflows",
            Workflow,
            json=workflow.model_dump(mode="json", by_alias=True),
        )

    async def get_workflow(self, workflow_id: str) -> Workflow:
        return await self._model("GET", f"/api/v1/workflows/{workflow_id}", Workflow)

    async def list_workflows(self, *, limit: int = 50, offset: int = 0) -> WorkflowList:
        return await self._model(
            "GET",
            "/api/v1/workflows",
            WorkflowList,
            params={"limit": limit, "offset": offset},
        )

    async def create_run(
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
        return await self._model(
            "POST", f"/api/v1/workflows/{workflow_id}/runs", WorkflowRun, json=payload
        )

    async def get_run(self, run_id: str) -> WorkflowRun:
        return await self._model("GET", f"/api/v1/runs/{run_id}", WorkflowRun)

    async def list_runs(
        self,
        *,
        workflow_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> WorkflowRunList:
        return await self._model(
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

    async def list_run_tasks(self, run_id: str) -> tuple[TaskRun, ...]:
        try:
            return TypeAdapter(tuple[TaskRun, ...]).validate_python(
                await self._request("GET", f"/api/v1/runs/{run_id}/tasks")
            )
        except PydanticValidationError as exc:
            raise FluxionAPIError("Fluxion API returned an invalid response.") from exc

    async def get_task(self, run_id: str, task_id: str) -> TaskRun:
        return await self._model(
            "GET", f"/api/v1/runs/{run_id}/tasks/{task_id}", TaskRun
        )

    async def list_task_attempts(self, run_id: str, task_id: str) -> TaskAttemptList:
        return await self._model(
            "GET", f"/api/v1/runs/{run_id}/tasks/{task_id}/attempts", TaskAttemptList
        )

    async def list_run_events(
        self, run_id: str, *, after: int | None = None, limit: int = 100
    ) -> RunEventList:
        return await self._model(
            "GET", f"/api/v1/runs/{run_id}/events/history", RunEventList,
            params={"after": after, "limit": limit},
        )

    async def watch_run(
        self, run_id: str, *, after: int | None = None, timeout: float | None = None
    ) -> AsyncIterator[RunEvent]:
        headers = {"Last-Event-ID": str(after)} if after is not None else {}
        try:
            async with self._client.stream(
                "GET", f"/api/v1/runs/{run_id}/events", headers=headers,
                timeout=timeout or self.timeout,
            ) as response:
                raise_for_response(response)
                data: list[str] = []
                async for line in response.aiter_lines():
                    if not line:
                        if data:
                            try:
                                yield RunEvent.model_validate_json("\n".join(data))
                            except PydanticValidationError as exc:
                                raise FluxionAPIError(
                                    "Fluxion SSE stream contained an invalid event."
                                ) from exc
                            data.clear()
                        continue
                    if line.startswith("data:"):
                        data.append(line[5:].lstrip())
        except httpx.HTTPError as exc:
            raise map_http_error(exc) from exc

    async def cancel_run(self, run_id: str) -> WorkflowRun:
        return await self._model("POST", f"/api/v1/runs/{run_id}/cancel", WorkflowRun)

    async def recover_run(self, run_id: str) -> RecoveryResult:
        return await self._model(
            "POST", f"/api/v1/runs/{run_id}/recover", RecoveryResult
        )

    async def resume_run(self, run_id: str) -> WorkflowRun:
        return await self._model("POST", f"/api/v1/runs/{run_id}/resume", WorkflowRun)

    async def run_workflow(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        input: Any = _MISSING,
        wait: bool = False,
        timeout: float = 60.0,
        poll_interval: float = 0.5,
    ) -> WorkflowRun:
        run = await self.create_run(workflow_id, run_id=run_id, input=input)
        if wait:
            return await self.wait_for_run(
                run.run_id, timeout=timeout, poll_interval=poll_interval
            )
        return run

    async def wait_for_run(
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
            run = await self.get_run(run_id)
            if run.status in {"SUCCEEDED", "FAILED", "CANCELLED", "INTERRUPTED"}:
                return run
            if time.monotonic() >= deadline:
                raise FluxionTimeoutError(
                    f"Run '{run_id}' did not finish within {timeout:g}s."
                )
            await asyncio.sleep(min(poll_interval, max(0, deadline - time.monotonic())))

    async def _model(
        self,
        method: str,
        path: str,
        model: type[_T],
        **kwargs: Any,
    ) -> _T:
        try:
            return model.model_validate(await self._request(method, path, **kwargs))
        except PydanticValidationError as exc:
            raise FluxionAPIError("Fluxion API returned an invalid response.") from exc

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            response = await self._client.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise map_http_error(exc) from exc
        raise_for_response(response)
        return parse_json(response)
