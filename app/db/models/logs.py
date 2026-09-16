from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class TaskLogRecord(Base):
    __tablename__ = "task_logs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["run_id", "task_id", "attempt_number"],
            [
                "task_attempts.run_id",
                "task_attempts.task_id",
                "task_attempts.attempt_number",
            ],
            ondelete="RESTRICT",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    task_id: Mapped[str] = mapped_column(String(255), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)
    level: Mapped[str] = mapped_column(String(16), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    fields: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


Index(
    "uq_task_logs_attempt_sequence",
    TaskLogRecord.run_id,
    TaskLogRecord.task_id,
    TaskLogRecord.attempt_number,
    TaskLogRecord.sequence_number,
    unique=True,
)
Index("ix_task_logs_run_id_id", TaskLogRecord.run_id, TaskLogRecord.id)
