import argparse
import json
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from app.tasks.demo import build_demo_workflow, unique_demo_ids

TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED"}


class DemoError(RuntimeError):
    pass


class DemoTimeoutError(DemoError):
    pass


class DemoFailedError(DemoError):
    pass


@dataclass(frozen=True)
class DemoOptions:
    api_url: str
    token: str | None
    timeout_seconds: float
    poll_seconds: float


@dataclass(frozen=True)
class DemoResult:
    workflow_id: str
    run_id: str
    status: str


class UrlLibApiClient:
    def __init__(self, api_url: str, token: str | None = None) -> None:
        self._api_url = api_url.rstrip("/") + "/"
        self._token = token

    def headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        request = Request(
            urljoin(self._api_url, path.lstrip("/")),
            data=data,
            headers=self.headers(),
            method=method,
        )
        try:
            with urlopen(request, timeout=10) as response:  # noqa: S310
                body = response.read().decode("utf-8")
        except HTTPError as exc:
            body = exc.read().decode("utf-8")
            raise DemoError(f"API request failed with HTTP {exc.code}: {body}") from exc
        return json.loads(body) if body else {}


def run_demo(
    client: UrlLibApiClient,
    options: DemoOptions,
    *,
    id_factory: Callable[[], tuple[str, str]] = unique_demo_ids,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    output: Callable[[str], None] = print,
) -> DemoResult:
    workflow_id, run_id = id_factory()
    workflow = build_demo_workflow(workflow_id).model_dump(
        mode="json",
        exclude_defaults=True,
    )

    client.request_json("POST", "/api/v1/workflows", workflow)
    output(f"Created workflow: {workflow_id}")
    client.request_json(
        "POST",
        f"/api/v1/workflows/{workflow_id}/runs",
        {"run_id": run_id},
    )
    output(f"Created run: {run_id}")

    deadline = monotonic() + options.timeout_seconds
    last_status = None
    while monotonic() < deadline:
        run = client.request_json("GET", f"/api/v1/runs/{run_id}")
        status = run["status"]
        if status != last_status:
            output(f"Run status: {status}")
            last_status = status
        for task in run.get("tasks", ()):
            output(f"{task['task_id']}: {task['status']}")
        if status == "SUCCEEDED":
            output("Workflow: SUCCEEDED")
            return DemoResult(workflow_id=workflow_id, run_id=run_id, status=status)
        if status in TERMINAL_STATUSES:
            raise DemoFailedError(f"Workflow finished with status {status}.")
        sleep(options.poll_seconds)

    raise DemoTimeoutError(
        f"Demo run '{run_id}' did not finish within {options.timeout_seconds:g}s."
    )


def _options_from_args(
    argv: list[str] | None,
) -> tuple[argparse.Namespace, DemoOptions]:
    from app.core.config import get_settings

    settings = get_settings()
    parser = argparse.ArgumentParser(prog="fluxion demo")
    parser.add_argument("--api-url", default=settings.fluxion_api_url)
    parser.add_argument("--token", default=settings.fluxion_api_token)
    parser.add_argument("--timeout", type=float, default=settings.demo_timeout_seconds)
    parser.add_argument("--poll", type=float, default=settings.demo_poll_seconds)
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.poll <= 0:
        parser.error("--poll must be positive")
    return args, DemoOptions(
        api_url=args.api_url,
        token=args.token,
        timeout_seconds=args.timeout,
        poll_seconds=args.poll,
    )


def main(argv: list[str] | None = None) -> int:
    _, options = _options_from_args(argv)
    try:
        run_demo(UrlLibApiClient(options.api_url, options.token), options)
    except DemoError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def cli() -> None:
    raise SystemExit(main())
