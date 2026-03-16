"""Initial database schema."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260315_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Apply the initial schema."""

    op.create_table(
        "wallets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("wallet_address", sa.Text(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=True),
        sa.Column("profile_slug", sa.Text(), nullable=True),
        sa.Column("is_flagged", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("flag_status", sa.Text(), nullable=False, server_default=sa.text("'unflagged'")),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("wallet_address", name="uq_wallets_wallet_address"),
    )
    op.create_index("ix_wallets_is_flagged", "wallets", ["is_flagged"])
    op.create_index("ix_wallets_flag_status", "wallets", ["flag_status"])

    op.create_table(
        "events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("category", sa.Text(), nullable=True),
        sa.Column("slug", sa.Text(), nullable=True),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("raw_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("event_id", name="uq_events_event_id"),
    )

    op.create_table(
        "markets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=True),
        sa.Column("slug", sa.Text(), nullable=True),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("category", sa.Text(), nullable=True),
        sa.Column("subcategory", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("resolution_outcome", sa.Text(), nullable=True),
        sa.Column("resolution_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("close_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("liquidity", sa.Float(), nullable=True),
        sa.Column("volume", sa.Float(), nullable=True),
        sa.Column("raw_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["events.event_id"]),
        sa.UniqueConstraint("market_id", name="uq_markets_market_id"),
    )
    op.create_index("ix_markets_event_id", "markets", ["event_id"])
    op.create_index("ix_markets_category", "markets", ["category"])
    op.create_index("ix_markets_status", "markets", ["status"])

    op.create_table(
        "trades",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("trade_id", sa.Text(), nullable=False),
        sa.Column("wallet_address", sa.Text(), nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=True),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("notional", sa.Float(), nullable=False),
        sa.Column("trade_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.Text(), nullable=False, server_default=sa.text("'rest'")),
        sa.Column("raw_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["wallet_address"], ["wallets.wallet_address"]),
        sa.ForeignKeyConstraint(["market_id"], ["markets.market_id"]),
        sa.ForeignKeyConstraint(["event_id"], ["events.event_id"]),
        sa.UniqueConstraint("trade_id", name="uq_trades_trade_id"),
    )
    op.create_index(
        "ix_trades_wallet_address_trade_time",
        "trades",
        ["wallet_address", "trade_time"],
    )
    op.create_index("ix_trades_market_id_trade_time", "trades", ["market_id", "trade_time"])
    op.create_index("ix_trades_event_id", "trades", ["event_id"])

    op.create_table(
        "positions_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("wallet_address", sa.Text(), nullable=False),
        sa.Column("snapshot_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=True),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("avg_entry_price", sa.Float(), nullable=True),
        sa.Column("current_value", sa.Float(), nullable=True),
        sa.Column("unrealized_pnl", sa.Float(), nullable=True),
        sa.Column("realized_pnl", sa.Float(), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("raw_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["wallet_address"], ["wallets.wallet_address"]),
        sa.ForeignKeyConstraint(["market_id"], ["markets.market_id"]),
        sa.ForeignKeyConstraint(["event_id"], ["events.event_id"]),
    )
    op.create_index(
        "ix_positions_snapshots_wallet_address_snapshot_time",
        "positions_snapshots",
        ["wallet_address", "snapshot_time"],
    )
    op.create_index(
        "ix_positions_snapshots_wallet_market_outcome_snapshot_time",
        "positions_snapshots",
        ["wallet_address", "market_id", "outcome", "snapshot_time"],
    )

    op.create_table(
        "closed_positions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("wallet_address", sa.Text(), nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=True),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("entry_price_avg", sa.Float(), nullable=True),
        sa.Column("exit_price_avg", sa.Float(), nullable=True),
        sa.Column("quantity", sa.Float(), nullable=True),
        sa.Column("realized_pnl", sa.Float(), nullable=True),
        sa.Column("roi", sa.Float(), nullable=True),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolution_outcome", sa.Text(), nullable=True),
        sa.Column("raw_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["wallet_address"], ["wallets.wallet_address"]),
        sa.ForeignKeyConstraint(["market_id"], ["markets.market_id"]),
        sa.ForeignKeyConstraint(["event_id"], ["events.event_id"]),
    )
    op.create_index(
        "ix_closed_positions_wallet_address_closed_at",
        "closed_positions",
        ["wallet_address", "closed_at"],
    )
    op.create_index(
        "ix_closed_positions_wallet_address_market_id",
        "closed_positions",
        ["wallet_address", "market_id"],
    )

    op.create_table(
        "wallet_feature_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("wallet_address", sa.Text(), nullable=False),
        sa.Column("as_of_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_markets_count", sa.Integer(), nullable=False),
        sa.Column("trades_count", sa.Integer(), nullable=False),
        sa.Column("win_rate", sa.Float(), nullable=True),
        sa.Column("avg_roi", sa.Float(), nullable=True),
        sa.Column("median_roi", sa.Float(), nullable=True),
        sa.Column("realized_pnl_total", sa.Float(), nullable=True),
        sa.Column("early_entry_edge", sa.Float(), nullable=True),
        sa.Column("specialization_score", sa.Float(), nullable=True),
        sa.Column("conviction_score", sa.Float(), nullable=True),
        sa.Column("consistency_score", sa.Float(), nullable=True),
        sa.Column("timing_score", sa.Float(), nullable=True),
        sa.Column("composite_score", sa.Float(), nullable=True),
        sa.Column("confidence_score", sa.Float(), nullable=True),
        sa.Column(
            "explanations_json",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["wallet_address"], ["wallets.wallet_address"]),
    )
    op.create_index(
        "ix_wallet_feature_snapshots_wallet_address_as_of_time",
        "wallet_feature_snapshots",
        ["wallet_address", "as_of_time"],
    )
    op.create_index(
        "ix_wallet_feature_snapshots_composite_score",
        "wallet_feature_snapshots",
        ["composite_score"],
    )

    op.create_table(
        "watchlist",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("wallet_address", sa.Text(), nullable=False),
        sa.Column("watch_status", sa.Text(), nullable=False, server_default=sa.text("'active'")),
        sa.Column("added_reason", sa.Text(), nullable=False),
        sa.Column("added_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=False, server_default=sa.text("100")),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["wallet_address"], ["wallets.wallet_address"]),
        sa.UniqueConstraint("wallet_address", name="uq_watchlist_wallet_address"),
    )
    op.create_index("ix_watchlist_watch_status", "watchlist", ["watch_status"])
    op.create_index("ix_watchlist_priority", "watchlist", ["priority"])

    op.create_table(
        "alerts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("wallet_address", sa.Text(), nullable=False),
        sa.Column("alert_type", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("market_id", sa.Text(), nullable=True),
        sa.Column("event_id", sa.Text(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("details_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("is_read", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.ForeignKeyConstraint(["wallet_address"], ["wallets.wallet_address"]),
        sa.ForeignKeyConstraint(["market_id"], ["markets.market_id"]),
        sa.ForeignKeyConstraint(["event_id"], ["events.event_id"]),
    )
    op.create_index(
        "ix_alerts_wallet_address_detected_at",
        "alerts",
        ["wallet_address", "detected_at"],
    )
    op.create_index("ix_alerts_alert_type", "alerts", ["alert_type"])
    op.create_index("ix_alerts_severity", "alerts", ["severity"])

    op.create_table(
        "ingestion_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_type", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("records_written", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("metadata_json", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
    )


def downgrade() -> None:
    """Revert the initial schema."""

    op.drop_table("ingestion_runs")
    op.drop_index("ix_alerts_severity", table_name="alerts")
    op.drop_index("ix_alerts_alert_type", table_name="alerts")
    op.drop_index("ix_alerts_wallet_address_detected_at", table_name="alerts")
    op.drop_table("alerts")
    op.drop_index("ix_watchlist_priority", table_name="watchlist")
    op.drop_index("ix_watchlist_watch_status", table_name="watchlist")
    op.drop_table("watchlist")
    op.drop_index(
        "ix_wallet_feature_snapshots_composite_score",
        table_name="wallet_feature_snapshots",
    )
    op.drop_index(
        "ix_wallet_feature_snapshots_wallet_address_as_of_time",
        table_name="wallet_feature_snapshots",
    )
    op.drop_table("wallet_feature_snapshots")
    op.drop_index("ix_closed_positions_wallet_address_market_id", table_name="closed_positions")
    op.drop_index("ix_closed_positions_wallet_address_closed_at", table_name="closed_positions")
    op.drop_table("closed_positions")
    op.drop_index(
        "ix_positions_snapshots_wallet_market_outcome_snapshot_time",
        table_name="positions_snapshots",
    )
    op.drop_index(
        "ix_positions_snapshots_wallet_address_snapshot_time",
        table_name="positions_snapshots",
    )
    op.drop_table("positions_snapshots")
    op.drop_index("ix_trades_event_id", table_name="trades")
    op.drop_index("ix_trades_market_id_trade_time", table_name="trades")
    op.drop_index("ix_trades_wallet_address_trade_time", table_name="trades")
    op.drop_table("trades")
    op.drop_index("ix_markets_status", table_name="markets")
    op.drop_index("ix_markets_category", table_name="markets")
    op.drop_index("ix_markets_event_id", table_name="markets")
    op.drop_table("markets")
    op.drop_table("events")
    op.drop_index("ix_wallets_flag_status", table_name="wallets")
    op.drop_index("ix_wallets_is_flagged", table_name="wallets")
    op.drop_table("wallets")
