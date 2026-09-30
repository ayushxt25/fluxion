"""Add durable dispatch reconciliation metadata."""

import sqlalchemy as sa

from alembic import op

revision = "20260929_0022"
down_revision = "20260928_0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "dispatch_outbox",
        sa.Column("last_reconciled_at", sa.DateTime(timezone=True)),
    )
    op.add_column(
        "dispatch_outbox",
        sa.Column(
            "reconcile_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.create_index(
        "ix_dispatch_outbox_reconcile_published",
        "dispatch_outbox",
        ["published_at", "id"],
        postgresql_where=sa.text("published_at IS NOT NULL AND discarded_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_dispatch_outbox_reconcile_published",
        table_name="dispatch_outbox",
    )
    op.drop_column("dispatch_outbox", "reconcile_count")
    op.drop_column("dispatch_outbox", "last_reconciled_at")
