"""Add adjusted_score to wallet feature snapshots."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260316_0002"
down_revision = "20260315_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add the adjusted score column used by the scoring engine."""

    with op.batch_alter_table("wallet_feature_snapshots") as batch_op:
        batch_op.add_column(sa.Column("adjusted_score", sa.Float(), nullable=True))


def downgrade() -> None:
    """Remove the adjusted score column."""

    with op.batch_alter_table("wallet_feature_snapshots") as batch_op:
        batch_op.drop_column("adjusted_score")
