import json
import logging

from app.observability.context import (
    clear_log_context,
    get_log_context,
    set_principal,
    set_request_id,
)
from app.observability.logging import JsonLogFormatter, TextLogFormatter
from app.observability.metrics import (
    record_http_request,
    record_task_attempt_failed,
    record_task_attempt_started,
    record_task_attempt_succeeded,
    record_task_dispatches,
    record_task_retry,
    render_prometheus,
    reset_metrics_for_tests,
)


def test_json_logging_includes_context_and_redacts_sensitive_extra() -> None:
    set_request_id("request-1")
    set_principal("user-1", "operator")
    record = logging.LogRecord(
        "fluxion.test",
        logging.INFO,
        __file__,
        1,
        "request complete",
        (),
        None,
    )
    record.authorization = "Bearer secret.jwt"
    record.lease_token = "lease-secret"
    record.run_id = "run-1"

    payload = json.loads(JsonLogFormatter().format(record))

    assert payload["request_id"] == "request-1"
    assert payload["principal_subject"] == "user-1"
    assert payload["principal_role"] == "operator"
    assert payload["run_id"] == "run-1"
    assert "authorization" not in payload
    assert "lease_token" not in payload
    assert "secret.jwt" not in json.dumps(payload)
    clear_log_context()


def test_text_logging_works_and_context_is_cleared() -> None:
    set_request_id("request-2")
    record = logging.LogRecord(
        "fluxion.test",
        logging.INFO,
        __file__,
        1,
        "hello",
        (),
        None,
    )

    line = TextLogFormatter().format(record)
    clear_log_context()

    assert "request_id=request-2" in line
    assert get_log_context().request_id is None


def test_metrics_use_low_cardinality_labels() -> None:
    reset_metrics_for_tests()

    record_http_request(
        method="GET",
        path="/api/v1/runs/{run_id}",
        status_code=200,
        duration_seconds=0.01,
    )
    record_task_dispatches(2)
    record_task_attempt_started()
    record_task_attempt_succeeded(0.02)
    record_task_attempt_started()
    record_task_attempt_failed(0.03)
    record_task_retry()

    payload = render_prometheus()

    assert 'path="/api/v1/runs/{run_id}"' in payload
    assert "run-123" not in payload
    assert "fluxion_task_dispatches_total 2" in payload
    assert "fluxion_task_attempts_started_total 2" in payload
    assert "fluxion_task_attempts_succeeded_total 1" in payload
    assert "fluxion_task_attempts_failed_total 1" in payload
    assert "fluxion_task_retries_total 1" in payload
    reset_metrics_for_tests()
