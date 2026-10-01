from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class TaskInterventionRecord(Base):
    __tablename__ = "task_interventions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["run_id", "task_id", "interrupted_attempt_number"],
            [
                "task_attempts.run_id",
                "task_attempts.task_id",
                "task_attempts.attempt_number",
            ],
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "run_id",
            "task_id",
            "interrupted_attempt_number",
            name="uq_task_interventions_interrupted_attempt",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    task_id: Mapped[str] = mapped_column(String(255), nullable=False)
    interrupted_attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    resolution: Mapped[str] = mapped_column(
        String(16), nullable=False, default="PENDING"
    )
    reason: Mapped[str | None] = mapped_column(Text)
    resolver_subject: Mapped[str | None] = mapped_column(String(255))
    resolver_role: Mapped[str | None] = mapped_column(String(64))
    resulting_attempt_number: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


Index(
    "ix_task_interventions_pending_created",
    TaskInterventionRecord.created_at,
    TaskInterventionRecord.id,
    postgresql_where=TaskInterventionRecord.resolution == "PENDING",
)
