"""add worker attempt leases

Revision ID: 20260905_0004
Revises: 20260903_0003
Create Date: 2026-09-05 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260905_0004"
down_revision: str | None = "20260903_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("task_attempts", sa.Column("worker_id", sa.String(length=255)))
    op.add_column("task_attempts", sa.Column("lease_token", sa.String(length=255)))
    op.add_column(
        "task_attempts",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
    )
    op.add_column(
        "task_attempts",
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True)),
    )
    op.create_index(
        "ix_task_attempts_status_lease_expires_at",
        "task_attempts",
        ["status", "lease_expires_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_task_attempts_status_lease_expires_at",
        table_name="task_attempts",
    )
    op.drop_column("task_attempts", "last_heartbeat_at")
    op.drop_column("task_attempts", "lease_expires_at")
    op.drop_column("task_attempts", "lease_token")
    op.drop_column("task_attempts", "worker_id")
