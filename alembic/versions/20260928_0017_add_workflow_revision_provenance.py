"""Add immutable workflow revision provenance."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260928_0017"
down_revision: str | None = "20260924_0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "workflow_revisions",
        sa.Column("created_by_subject", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "workflow_revisions",
        sa.Column("created_by_role", sa.String(length=32), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("workflow_revisions", "created_by_role")
    op.drop_column("workflow_revisions", "created_by_subject")
