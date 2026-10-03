import asyncio
import logging

from fastapi import Request

from app.api.errors import unexpected_error_handler


def test_unexpected_error_is_logged_with_request_id_and_is_sanitized(caplog) -> None:
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/v1/runs/run-1",
            "headers": [],
        }
    )
    request.state.request_id = "request-1"

    with caplog.at_level(logging.ERROR, logger="app.api.errors"):
        response = asyncio.run(
            unexpected_error_handler(
                request,
                RuntimeError("postgresql://user:password@db/fluxion"),
            )
        )

    assert response.status_code == 500
    assert response.body == (
        b'{"error":{"code":"internal_server_error","message":'
        b'"An unexpected server error occurred."}}'
    )
    assert caplog.records[0].exc_info is not None
    assert caplog.records[0].request_id == "request-1"
    assert "password" not in str(caplog.records[0].exc_info)
