"""Add dispatch outbox discard metadata.

Revision ID: 20260906_0007
Revises: 20260905_0006
Create Date: 2026-09-06 00:07:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260906_0007"
down_revision: str | None = "20260905_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "dispatch_outbox",
        sa.Column("discarded_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "dispatch_outbox",
        sa.Column("discard_reason", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("dispatch_outbox", "discard_reason")
    op.drop_column("dispatch_outbox", "discarded_at")
