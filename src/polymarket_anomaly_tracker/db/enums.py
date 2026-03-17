"""Enum-like constants for database-backed workflow state."""

from __future__ import annotations

from enum import StrEnum


class WalletFlagStatus(StrEnum):
    """Lifecycle states for tracked wallets."""

    UNFLAGGED = "unflagged"
    CANDIDATE = "candidate"
    FLAGGED = "flagged"


class WatchStatus(StrEnum):
    """Lifecycle states for watchlist entries."""

    ACTIVE = "active"
    PAUSED = "paused"
    REMOVED = "removed"


class TradeSource(StrEnum):
    """Supported trade ingestion sources."""

    REST = "rest"
    WEBSOCKET = "websocket"


class AlertType(StrEnum):
    """Supported alert categories."""

    POSITION_OPENED = "position_opened"
    POSITION_CLOSED = "position_closed"
    POSITION_CHANGED = "position_changed"
    MULTIPLE_FLAGGED_WALLETS_SAME_MARKET = "multiple_flagged_wallets_same_market"


class AlertSeverity(StrEnum):
    """Supported alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class IngestionRunStatus(StrEnum):
    """Statuses for ingestion and scoring runs."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class IngestionRunType(StrEnum):
    """Top-level run categories tracked in the database."""

    LEADERBOARD = "leaderboard"
    MARKET_PRICES = "market_prices"
    WALLET_ENRICHMENT = "wallet_enrichment"
    FEATURES = "features"
    WATCH = "watch"
