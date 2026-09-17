"""Add durable webhook subscriptions and deliveries.

Revision ID: 20260916_0013
Revises: 20260916_0012
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260916_0013"
down_revision: str | None = "20260916_0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "webhook_subscriptions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("target_url", sa.Text(), nullable=False),
        sa.Column("secret", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column(
            "event_types", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column("workflow_id", sa.String(255)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_table(
        "webhook_deliveries",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("subscription_id", sa.String(36), nullable=False),
        sa.Column("run_event_id", sa.Integer(), nullable=False),
        sa.Column("delivery_key", sa.String(128), nullable=False, unique=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True)),
        sa.Column("claimed_by", sa.String(255)),
        sa.Column("claim_token", sa.String(255)),
        sa.Column("claim_expires_at", sa.DateTime(timezone=True)),
        sa.Column("last_status_code", sa.Integer()),
        sa.Column("last_error_type", sa.String(255)),
        sa.Column("last_error_message", sa.Text()),
        sa.Column("delivered_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(["subscription_id"], ["webhook_subscriptions.id"]),
        sa.ForeignKeyConstraint(["run_event_id"], ["run_events.id"]),
    )
    op.create_index(
        "uq_webhook_delivery_subscription_event",
        "webhook_deliveries",
        ["subscription_id", "run_event_id"],
        unique=True,
    )
    op.create_index(
        "ix_webhook_deliveries_due", "webhook_deliveries", ["status", "next_attempt_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_webhook_deliveries_due", table_name="webhook_deliveries")
    op.drop_index(
        "uq_webhook_delivery_subscription_event", table_name="webhook_deliveries"
    )
    op.drop_table("webhook_deliveries")
    op.drop_table("webhook_subscriptions")
