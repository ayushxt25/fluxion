from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class WebhookSubscriptionRecord(Base):
    __tablename__ = "webhook_subscriptions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    target_url: Mapped[str] = mapped_column(Text, nullable=False)
    secret: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    event_types: Mapped[list] = mapped_column(JSONB, nullable=False)
    workflow_id: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class WebhookDeliveryRecord(Base):
    __tablename__ = "webhook_deliveries"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    subscription_id: Mapped[str] = mapped_column(
        ForeignKey("webhook_subscriptions.id"), nullable=False
    )
    run_event_id: Mapped[int] = mapped_column(
        ForeignKey("run_events.id"), nullable=False
    )
    delivery_key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    claimed_by: Mapped[str | None] = mapped_column(String(255))
    claim_token: Mapped[str | None] = mapped_column(String(255))
    claim_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_status_code: Mapped[int | None] = mapped_column(Integer)
    last_error_type: Mapped[str | None] = mapped_column(String(255))
    last_error_message: Mapped[str | None] = mapped_column(Text)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


Index(
    "uq_webhook_delivery_subscription_event",
    WebhookDeliveryRecord.subscription_id,
    WebhookDeliveryRecord.run_event_id,
    unique=True,
)
Index(
    "ix_webhook_deliveries_due",
    WebhookDeliveryRecord.status,
    WebhookDeliveryRecord.next_attempt_at,
)
