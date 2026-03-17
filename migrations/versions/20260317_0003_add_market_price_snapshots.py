"""Add market_price_snapshots table."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260317_0003"
down_revision = "20260316_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the market price snapshot table and supporting indexes."""

    op.create_table(
        "market_price_snapshots",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("snapshot_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("best_bid", sa.Float(), nullable=True),
        sa.Column("best_ask", sa.Float(), nullable=True),
        sa.Column("mid_price", sa.Float(), nullable=True),
        sa.Column("last_price", sa.Float(), nullable=True),
        sa.Column("volume", sa.Float(), nullable=True),
        sa.Column("liquidity", sa.Float(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("raw_json", sa.Text(), nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["market_id"], ["markets.market_id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "market_id",
            "snapshot_time",
            "source",
            name="uq_market_price_snapshots_market_time_source",
        ),
    )
    op.create_index(
        "ix_market_price_snapshots_market_id_snapshot_time",
        "market_price_snapshots",
        ["market_id", "snapshot_time"],
        unique=False,
    )
    op.create_index(
        "ix_market_price_snapshots_snapshot_time",
        "market_price_snapshots",
        ["snapshot_time"],
        unique=False,
    )


def downgrade() -> None:
    """Drop the market price snapshot table."""

    op.drop_index(
        "ix_market_price_snapshots_snapshot_time",
        table_name="market_price_snapshots",
    )
    op.drop_index(
        "ix_market_price_snapshots_market_id_snapshot_time",
        table_name="market_price_snapshots",
    )
    op.drop_table("market_price_snapshots")
