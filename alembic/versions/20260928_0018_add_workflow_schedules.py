"""Add durable workflow schedules and firing identities."""
# ruff: noqa: E501

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260928_0018"
down_revision: str | None = "20260928_0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workflow_schedules",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "workflow_id",
            sa.String(255),
            sa.ForeignKey("workflow_definitions.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("workflow_revision", sa.Integer()),
        sa.Column("schedule_type", sa.String(16), nullable=False),
        sa.Column("cron_expression", sa.String(128)),
        sa.Column("interval_seconds", sa.Integer()),
        sa.Column("timezone", sa.String(64), nullable=False),
        sa.Column("misfire_policy", sa.String(16), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("next_fire_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_fire_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("created_by_subject", sa.String(255)),
        sa.Column("created_by_role", sa.String(32)),
    )
    op.create_index(
        "ix_workflow_schedules_due", "workflow_schedules", ["enabled", "next_fire_at"]
    )
    op.create_index(
        "ix_workflow_schedules_workflow", "workflow_schedules", ["workflow_id"]
    )
    op.create_table(
        "workflow_schedule_firings",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "schedule_id",
            sa.String(36),
            sa.ForeignKey("workflow_schedules.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run_id", sa.String(255), nullable=False, unique=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("schedule_id", "scheduled_for"),
    )


def downgrade() -> None:
    op.drop_table("workflow_schedule_firings")
    op.drop_index("ix_workflow_schedules_workflow", table_name="workflow_schedules")
    op.drop_index("ix_workflow_schedules_due", table_name="workflow_schedules")
    op.drop_table("workflow_schedules")
