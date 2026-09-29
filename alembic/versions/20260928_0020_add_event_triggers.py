"""Add durable event-trigger subscriptions and firing provenance."""

import sqlalchemy as sa

from alembic import op

revision = "20260928_0020"
down_revision = "20260928_0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workflow_event_subscriptions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "workflow_id",
            sa.String(255),
            sa.ForeignKey("workflow_definitions.id"),
            nullable=False,
        ),
        sa.Column("workflow_revision", sa.Integer()),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("filter_json", sa.JSON()),
        sa.Column("pass_event_payload_as_input", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by_subject", sa.String(255)),
        sa.Column("created_by_role", sa.String(32)),
    )
    op.create_index(
        "ix_event_subscriptions_type_enabled",
        "workflow_event_subscriptions",
        ["event_type", "enabled"],
    )
    op.create_table(
        "workflow_trigger_events",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column("source", sa.String(255), nullable=False),
        sa.Column("external_event_id", sa.String(255), nullable=False),
        sa.Column("payload_json", sa.JSON()),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("source", "external_event_id"),
    )
    op.create_table(
        "workflow_event_firings",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "subscription_id",
            sa.String(36),
            sa.ForeignKey("workflow_event_subscriptions.id"),
            nullable=False,
        ),
        sa.Column(
            "trigger_event_id",
            sa.String(36),
            sa.ForeignKey("workflow_trigger_events.id"),
            nullable=False,
        ),
        sa.Column("run_id", sa.String(255), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("subscription_id", "trigger_event_id"),
    )
    op.add_column("workflow_runs", sa.Column("trigger_event_id", sa.String(36)))
    op.add_column("workflow_runs", sa.Column("event_subscription_id", sa.String(36)))


def downgrade() -> None:
    op.drop_column("workflow_runs", "event_subscription_id")
    op.drop_column("workflow_runs", "trigger_event_id")
    op.drop_table("workflow_event_firings")
    op.drop_table("workflow_trigger_events")
    op.drop_index(
        "ix_event_subscriptions_type_enabled", table_name="workflow_event_subscriptions"
    )
    op.drop_table("workflow_event_subscriptions")
