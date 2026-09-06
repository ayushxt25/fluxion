from datetime import datetime

from sqlalchemy import DateTime, Index, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class AuditEventRecord(Base):
    __tablename__ = "audit_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    request_id: Mapped[str] = mapped_column(String(255), nullable=False)
    principal_subject: Mapped[str | None] = mapped_column(String(255))
    principal_role: Mapped[str | None] = mapped_column(String(32))
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    resource_type: Mapped[str | None] = mapped_column(String(64))
    resource_id: Mapped[str | None] = mapped_column(String(255))
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    event_metadata: Mapped[dict | None] = mapped_column("metadata", JSONB)


Index("ix_audit_events_occurred_at", AuditEventRecord.occurred_at)
Index("ix_audit_events_action", AuditEventRecord.action)
Index("ix_audit_events_principal_subject", AuditEventRecord.principal_subject)
