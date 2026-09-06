"""Add dispatch outbox publication claims.

Revision ID: 20260905_0006
Revises: 20260905_0005
Create Date: 2026-09-05 00:06:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260905_0006"
down_revision: str | None = "20260905_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "dispatch_outbox",
        sa.Column("claimed_by", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "dispatch_outbox",
        sa.Column("claim_token", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "dispatch_outbox",
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "dispatch_outbox",
        sa.Column("claim_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_dispatch_outbox_claimable",
        "dispatch_outbox",
        ["claim_expires_at", "created_at", "id"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_dispatch_outbox_claimable",
        table_name="dispatch_outbox",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_column("dispatch_outbox", "claim_expires_at")
    op.drop_column("dispatch_outbox", "claimed_at")
    op.drop_column("dispatch_outbox", "claim_token")
    op.drop_column("dispatch_outbox", "claimed_by")
