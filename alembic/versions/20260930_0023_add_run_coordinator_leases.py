"""Add durable, fenced workflow-run coordinator leases.

Revision ID: 20260930_0023
Revises: 20260929_0022
"""

import sqlalchemy as sa

from alembic import op

revision = "20260930_0023"
down_revision = "20260929_0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "workflow_runs",
        sa.Column("coordinator_id", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "workflow_runs",
        sa.Column("coordinator_lease_token", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "workflow_runs",
        sa.Column(
            "coordinator_lease_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "workflow_runs",
        sa.Column(
            "coordinator_last_heartbeat_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_workflow_runs_coordinator_claimable",
        "workflow_runs",
        ["coordinator_lease_expires_at", "run_id"],
        postgresql_where=sa.text("status IN ('PENDING', 'RUNNING')"),
    )


def downgrade() -> None:
    op.drop_index("ix_workflow_runs_coordinator_claimable", "workflow_runs")
    op.drop_column("workflow_runs", "coordinator_last_heartbeat_at")
    op.drop_column("workflow_runs", "coordinator_lease_expires_at")
    op.drop_column("workflow_runs", "coordinator_lease_token")
    op.drop_column("workflow_runs", "coordinator_id")
