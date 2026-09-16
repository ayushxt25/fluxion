import asyncio

import pytest

from app.engine.context import TaskExecutionContext
from app.engine.task_logging import (
    FluxionTaskLogger,
    TaskLogLimitError,
    TaskLogValidationError,
)
from app.services.task_logs import TaskLogFlusher


def _logger(*, entries: int = 3) -> FluxionTaskLogger:
    return FluxionTaskLogger(
        max_message_bytes=16,
        max_fields_bytes=64,
        max_entries=entries,
    )


def test_task_logger_orders_levels_and_redacts_sensitive_fields() -> None:
    logger = _logger()
    logger.debug("start")
    logger.info("process", records=2, token="not-persisted")
    logger.warning("finish")

    entries = logger.drain()

    assert [entry.sequence_number for entry in entries] == [1, 2, 3]
    assert [entry.level for entry in entries] == ["DEBUG", "INFO", "WARNING"]
    assert entries[1].fields == {"records": 2, "token": "[REDACTED]"}
    assert logger.drain() == ()


def test_task_logger_rejects_oversized_invalid_and_excess_entries() -> None:
    logger = _logger(entries=1)
    with pytest.raises(TaskLogValidationError):
        logger.info("x" * 17)
    with pytest.raises(TaskLogValidationError):
        logger.info("valid", invalid=object())
    logger.error("valid")
    with pytest.raises(TaskLogLimitError):
        logger.error("next")


@pytest.mark.asyncio
async def test_threshold_and_final_flush_preserve_attempt_sequence() -> None:
    class FakeRepository:
        def __init__(self) -> None:
            self.entries = []

        async def append(self, **kwargs) -> None:
            self.entries.extend(kwargs["entries"])

    task_logger = FluxionTaskLogger(
        max_message_bytes=100,
        max_fields_bytes=100,
        max_entries=10,
        buffer_size=2,
    )
    repository = FakeRepository()
    flusher = TaskLogFlusher(
        task_logger,
        repository,
        run_id="run",
        workflow_id="workflow",
        task_id="task",
        attempt_number=1,
    )
    task_logger.set_threshold_callback(flusher.flush_soon)

    task_logger.info("one")
    task_logger.info("two")
    await asyncio.sleep(0)
    task_logger.info("three")
    await flusher.close()

    assert [entry.sequence_number for entry in repository.entries] == [1, 2, 3]


def test_context_equality_excludes_attempt_logger_identity() -> None:
    first = TaskExecutionContext(
        workflow_id="workflow",
        run_id="run",
        task_id="task",
        attempt_number=1,
        attempt_key="run:task:1",
        idempotency_key="run:task",
    )
    second = TaskExecutionContext(
        workflow_id="workflow",
        run_id="run",
        task_id="task",
        attempt_number=1,
        attempt_key="run:task:1",
        idempotency_key="run:task",
    )
    different_attempt = TaskExecutionContext(
        workflow_id="workflow",
        run_id="run",
        task_id="task",
        attempt_number=2,
        attempt_key="run:task:2",
        idempotency_key="run:task",
    )

    assert first.logger is not second.logger
    assert first == second
    assert first != different_attempt
