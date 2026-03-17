"""Add Issue 17 value/timing fields to wallet feature snapshots."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260317_0004"
down_revision = "20260317_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add value-at-entry and true timing fields used by Issue 17 scoring."""

    with op.batch_alter_table("wallet_feature_snapshots") as batch_op:
        batch_op.add_column(sa.Column("value_at_entry_score", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("timing_drift_score", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("timing_positive_capture_score", sa.Float(), nullable=True))


def downgrade() -> None:
    """Remove the Issue 17 value-at-entry and timing fields."""

    with op.batch_alter_table("wallet_feature_snapshots") as batch_op:
        batch_op.drop_column("timing_positive_capture_score")
        batch_op.drop_column("timing_drift_score")
        batch_op.drop_column("value_at_entry_score")
