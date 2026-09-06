from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from threading import Lock

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
_DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = Lock()
        self._counters: defaultdict[tuple[str, tuple[tuple[str, str], ...]], float] = (
            defaultdict(float)
        )
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._histograms: dict[
            tuple[str, tuple[tuple[str, str], ...]],
            list[float],
        ] = defaultdict(list)

    def inc_counter(
        self,
        name: str,
        labels: dict[str, str] | None = None,
        amount: float = 1,
    ) -> None:
        with self._lock:
            self._counters[(name, _labels(labels))] += amount

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> None:
        with self._lock:
            self._gauges[(name, _labels(labels))] = value

    def inc_gauge(
        self,
        name: str,
        labels: dict[str, str] | None = None,
        amount: float = 1,
    ) -> None:
        with self._lock:
            key = (name, _labels(labels))
            self._gauges[key] = self._gauges.get(key, 0) + amount

    def observe_histogram(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> None:
        with self._lock:
            self._histograms[(name, _labels(labels))].append(value)

    def render(self) -> str:
        with self._lock:
            lines: list[str] = []
            typed: set[str] = set()
            for (name, labels), value in sorted(self._counters.items()):
                if name not in typed:
                    lines.append(f"# TYPE {name} counter")
                    typed.add(name)
                lines.append(f"{name}{_format_labels(labels)} {_format_value(value)}")
            for (name, labels), value in sorted(self._gauges.items()):
                if name not in typed:
                    lines.append(f"# TYPE {name} gauge")
                    typed.add(name)
                lines.append(f"{name}{_format_labels(labels)} {_format_value(value)}")
            for (name, labels), values in sorted(self._histograms.items()):
                if name not in typed:
                    lines.append(f"# TYPE {name} histogram")
                    typed.add(name)
                for bucket in _DEFAULT_BUCKETS:
                    bucket_labels = (*labels, ("le", _format_bucket(bucket)))
                    count = sum(value <= bucket for value in values)
                    lines.append(
                        f"{name}_bucket{_format_labels(bucket_labels)} {count}"
                    )
                infinite_labels = (*labels, ("le", "+Inf"))
                lines.append(
                    f"{name}_bucket{_format_labels(infinite_labels)} {len(values)}"
                )
                lines.append(f"{name}_count{_format_labels(labels)} {len(values)}")
                lines.append(
                    f"{name}_sum{_format_labels(labels)} {_format_value(sum(values))}"
                )
            return "\n".join(lines) + ("\n" if lines else "")

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()


registry = MetricsRegistry()


def reset_metrics_for_tests() -> None:
    registry.reset()


def render_prometheus() -> str:
    return registry.render()


def record_http_request(
    *,
    method: str,
    path: str,
    status_code: int,
    duration_seconds: float,
) -> None:
    labels = {"method": method, "path": path, "status": str(status_code)}
    registry.inc_counter("fluxion_http_requests_total", labels)
    registry.observe_histogram(
        "fluxion_http_request_duration_seconds",
        duration_seconds,
        labels,
    )


def record_workflow_run_created() -> None:
    registry.inc_counter("fluxion_workflow_runs_created_total")


def record_task_dispatches(count: int = 1) -> None:
    if count:
        registry.inc_counter("fluxion_task_dispatches_total", amount=count)


def record_task_attempt_started() -> None:
    registry.inc_counter("fluxion_task_attempts_started_total")
    registry.inc_gauge("fluxion_worker_active_tasks", amount=1)


def record_task_attempt_succeeded(duration_seconds: float) -> None:
    registry.inc_counter("fluxion_task_attempts_succeeded_total")
    registry.observe_histogram(
        "fluxion_task_execution_duration_seconds",
        duration_seconds,
    )
    registry.inc_gauge("fluxion_worker_active_tasks", amount=-1)


def record_task_attempt_failed(duration_seconds: float) -> None:
    registry.inc_counter("fluxion_task_attempts_failed_total")
    registry.observe_histogram(
        "fluxion_task_execution_duration_seconds",
        duration_seconds,
    )
    registry.inc_gauge("fluxion_worker_active_tasks", amount=-1)


def record_task_retry() -> None:
    registry.inc_counter("fluxion_task_retries_total")


def record_outbox_publish(
    *,
    outcome: str,
    duration_seconds: float,
    count: int = 1,
) -> None:
    registry.inc_counter(
        "fluxion_outbox_publish_total",
        {"outcome": outcome},
        amount=count,
    )
    registry.observe_histogram(
        "fluxion_outbox_publish_duration_seconds",
        duration_seconds,
        {"outcome": outcome},
    )


def record_lease_reclaims(count: int) -> None:
    if count:
        registry.inc_counter("fluxion_lease_reclaims_total", amount=count)


def record_auth_denied() -> None:
    registry.inc_counter("fluxion_auth_denied_total")


def record_rate_limit_denied() -> None:
    registry.inc_counter("fluxion_rate_limit_denied_total")


def _labels(labels: dict[str, str] | None) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((labels or {}).items()))


def _format_labels(labels: Iterable[tuple[str, str]]) -> str:
    items = tuple(labels)
    if not items:
        return ""
    return "{" + ",".join(f'{key}="{_escape(value)}"' for key, value in items) + "}"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _format_value(value: float) -> str:
    if value == int(value):
        return str(int(value))
    return repr(value)


def _format_bucket(value: float) -> str:
    return _format_value(value)
