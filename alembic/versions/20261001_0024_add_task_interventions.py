"""Add durable ambiguous-execution interventions."""

import sqlalchemy as sa

from alembic import op

revision = "20261001_0024"
down_revision = "20260930_0023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "task_interventions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("workflow_id", sa.String(length=255), nullable=False),
        sa.Column("run_id", sa.String(length=255), nullable=False),
        sa.Column("task_id", sa.String(length=255), nullable=False),
        sa.Column("interrupted_attempt_number", sa.Integer(), nullable=False),
        sa.Column(
            "resolution",
            sa.String(length=16),
            nullable=False,
            server_default="PENDING",
        ),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("resolver_subject", sa.String(length=255), nullable=True),
        sa.Column("resolver_role", sa.String(length=64), nullable=True),
        sa.Column("resulting_attempt_number", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["run_id", "workflow_id", "task_id", "interrupted_attempt_number"],
            [
                "task_attempts.run_id",
                "task_attempts.workflow_id",
                "task_attempts.task_id",
                "task_attempts.attempt_number",
            ],
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "run_id",
            "task_id",
            "interrupted_attempt_number",
            name="uq_task_interventions_interrupted_attempt",
        ),
    )
    op.create_index(
        "ix_task_interventions_pending_created",
        "task_interventions",
        ["created_at", "id"],
        postgresql_where=sa.text("resolution = 'PENDING'"),
    )


def downgrade() -> None:
    op.drop_index("ix_task_interventions_pending_created", "task_interventions")
    op.drop_table("task_interventions")
