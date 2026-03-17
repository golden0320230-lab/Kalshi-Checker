"""SQLAlchemy ORM models for the local tracker database."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Index, Integer, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from polymarket_anomaly_tracker.db.base import Base, CreatedAtMixin, TimestampMixin, utc_now
from polymarket_anomaly_tracker.db.enums import (
    AlertSeverity,
    AlertType,
    IngestionRunStatus,
    TradeSource,
    WalletFlagStatus,
    WatchStatus,
)


class Wallet(TimestampMixin, Base):
    """One row per public wallet."""

    __tablename__ = "wallets"
    __table_args__ = (
        UniqueConstraint("wallet_address", name="uq_wallets_wallet_address"),
        Index("ix_wallets_is_flagged", "is_flagged"),
        Index("ix_wallets_flag_status", "flag_status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    wallet_address: Mapped[str] = mapped_column(Text, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    profile_slug: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_flagged: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    flag_status: Mapped[str] = mapped_column(
        Text,
        default=WalletFlagStatus.UNFLAGGED.value,
        nullable=False,
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class Event(TimestampMixin, Base):
    """Optional event-level metadata."""

    __tablename__ = "events"
    __table_args__ = (UniqueConstraint("event_id", name="uq_events_event_id"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str | None] = mapped_column(Text, nullable=True)
    slug: Mapped[str | None] = mapped_column(Text, nullable=True)
    start_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    end_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    raw_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)


class Market(TimestampMixin, Base):
    """Market-level metadata."""

    __tablename__ = "markets"
    __table_args__ = (
        UniqueConstraint("market_id", name="uq_markets_market_id"),
        Index("ix_markets_event_id", "event_id"),
        Index("ix_markets_category", "category"),
        Index("ix_markets_status", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    market_id: Mapped[str] = mapped_column(Text, nullable=False)
    event_id: Mapped[str | None] = mapped_column(ForeignKey("events.event_id"), nullable=True)
    slug: Mapped[str | None] = mapped_column(Text, nullable=True)
    question: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str | None] = mapped_column(Text, nullable=True)
    subcategory: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    resolution_outcome: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolution_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    close_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    liquidity: Mapped[float | None] = mapped_column(Float, nullable=True)
    volume: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)


class MarketPriceSnapshot(CreatedAtMixin, Base):
    """Point-in-time market quote and liquidity snapshots."""

    __tablename__ = "market_price_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "market_id",
            "snapshot_time",
            "source",
            name="uq_market_price_snapshots_market_time_source",
        ),
        Index(
            "ix_market_price_snapshots_market_id_snapshot_time",
            "market_id",
            "snapshot_time",
        ),
        Index("ix_market_price_snapshots_snapshot_time", "snapshot_time"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.market_id"), nullable=False)
    snapshot_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    best_bid: Mapped[float | None] = mapped_column(Float, nullable=True)
    best_ask: Mapped[float | None] = mapped_column(Float, nullable=True)
    mid_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    volume: Mapped[float | None] = mapped_column(Float, nullable=True)
    liquidity: Mapped[float | None] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    raw_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)


class Trade(CreatedAtMixin, Base):
    """Normalized public trade rows."""

    __tablename__ = "trades"
    __table_args__ = (
        UniqueConstraint("trade_id", name="uq_trades_trade_id"),
        Index("ix_trades_wallet_address_trade_time", "wallet_address", "trade_time"),
        Index("ix_trades_market_id_trade_time", "market_id", "trade_time"),
        Index("ix_trades_event_id", "event_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    trade_id: Mapped[str] = mapped_column(Text, nullable=False)
    wallet_address: Mapped[str] = mapped_column(
        ForeignKey("wallets.wallet_address"),
        nullable=False,
    )
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.market_id"), nullable=False)
    event_id: Mapped[str | None] = mapped_column(ForeignKey("events.event_id"), nullable=True)
    outcome: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    size: Mapped[float] = mapped_column(Float, nullable=False)
    notional: Mapped[float] = mapped_column(Float, nullable=False)
    trade_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source: Mapped[str] = mapped_column(Text, default=TradeSource.REST.value, nullable=False)
    raw_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)


class PositionSnapshot(CreatedAtMixin, Base):
    """Point-in-time wallet position snapshots."""

    __tablename__ = "positions_snapshots"
    __table_args__ = (
        Index(
            "ix_positions_snapshots_wallet_address_snapshot_time",
            "wallet_address",
            "snapshot_time",
        ),
        Index(
            "ix_positions_snapshots_wallet_market_outcome_snapshot_time",
            "wallet_address",
            "market_id",
            "outcome",
            "snapshot_time",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    wallet_address: Mapped[str] = mapped_column(
        ForeignKey("wallets.wallet_address"),
        nullable=False,
    )
    snapshot_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.market_id"), nullable=False)
    event_id: Mapped[str | None] = mapped_column(ForeignKey("events.event_id"), nullable=True)
    outcome: Mapped[str] = mapped_column(Text, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    avg_entry_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    current_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    unrealized_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)


class ClosedPosition(CreatedAtMixin, Base):
    """Normalized historical closed positions."""

    __tablename__ = "closed_positions"
    __table_args__ = (
        Index("ix_closed_positions_wallet_address_closed_at", "wallet_address", "closed_at"),
        Index("ix_closed_positions_wallet_address_market_id", "wallet_address", "market_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    wallet_address: Mapped[str] = mapped_column(
        ForeignKey("wallets.wallet_address"),
        nullable=False,
    )
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.market_id"), nullable=False)
    event_id: Mapped[str | None] = mapped_column(ForeignKey("events.event_id"), nullable=True)
    outcome: Mapped[str] = mapped_column(Text, nullable=False)
    entry_price_avg: Mapped[float | None] = mapped_column(Float, nullable=True)
    exit_price_avg: Mapped[float | None] = mapped_column(Float, nullable=True)
    quantity: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    roi: Mapped[float | None] = mapped_column(Float, nullable=True)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolution_outcome: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)


class WalletFeatureSnapshot(CreatedAtMixin, Base):
    """Persisted feature values for reproducible ranking."""

    __tablename__ = "wallet_feature_snapshots"
    __table_args__ = (
        Index(
            "ix_wallet_feature_snapshots_wallet_address_as_of_time",
            "wallet_address",
            "as_of_time",
        ),
        Index("ix_wallet_feature_snapshots_composite_score", "composite_score"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    wallet_address: Mapped[str] = mapped_column(
        ForeignKey("wallets.wallet_address"),
        nullable=False,
    )
    as_of_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    resolved_markets_count: Mapped[int] = mapped_column(Integer, nullable=False)
    trades_count: Mapped[int] = mapped_column(Integer, nullable=False)
    win_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    avg_roi: Mapped[float | None] = mapped_column(Float, nullable=True)
    median_roi: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_pnl_total: Mapped[float | None] = mapped_column(Float, nullable=True)
    early_entry_edge: Mapped[float | None] = mapped_column(Float, nullable=True)
    specialization_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    conviction_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    consistency_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    timing_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    composite_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    adjusted_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    explanations_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)


class WatchlistEntry(Base):
    """Tracked wallets that require tighter polling."""

    __tablename__ = "watchlist"
    __table_args__ = (
        UniqueConstraint("wallet_address", name="uq_watchlist_wallet_address"),
        Index("ix_watchlist_watch_status", "watch_status"),
        Index("ix_watchlist_priority", "priority"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    wallet_address: Mapped[str] = mapped_column(
        ForeignKey("wallets.wallet_address"),
        nullable=False,
    )
    watch_status: Mapped[str] = mapped_column(
        Text,
        default=WatchStatus.ACTIVE.value,
        nullable=False,
    )
    added_reason: Mapped[str] = mapped_column(Text, nullable=False)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    priority: Mapped[int] = mapped_column(Integer, default=100, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class Alert(Base):
    """Persisted local alert events for watchlisted wallets."""

    __tablename__ = "alerts"
    __table_args__ = (
        Index("ix_alerts_wallet_address_detected_at", "wallet_address", "detected_at"),
        Index("ix_alerts_alert_type", "alert_type"),
        Index("ix_alerts_severity", "severity"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    wallet_address: Mapped[str] = mapped_column(
        ForeignKey("wallets.wallet_address"),
        nullable=False,
    )
    alert_type: Mapped[str] = mapped_column(
        Text,
        default=AlertType.POSITION_CHANGED.value,
        nullable=False,
    )
    severity: Mapped[str] = mapped_column(Text, default=AlertSeverity.INFO.value, nullable=False)
    market_id: Mapped[str | None] = mapped_column(ForeignKey("markets.market_id"), nullable=True)
    event_id: Mapped[str | None] = mapped_column(ForeignKey("events.event_id"), nullable=True)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    details_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)


class IngestionRun(Base):
    """Operational metadata for ingest, scoring, and tracking runs."""

    __tablename__ = "ingestion_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    run_type: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(
        Text,
        default=IngestionRunStatus.RUNNING.value,
        nullable=False,
    )
    records_written: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
