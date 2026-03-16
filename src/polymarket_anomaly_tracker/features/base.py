"""Shared types for wallet feature dataset assembly and computation."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

WALLET_DATASET_COLUMNS = (
    "wallet_address",
    "display_name",
    "first_seen_at",
    "last_seen_at",
)
TRADE_DATASET_COLUMNS = (
    "wallet_address",
    "trade_id",
    "market_id",
    "price",
    "size",
    "notional",
    "trade_time",
)
CLOSED_POSITION_DATASET_COLUMNS = (
    "wallet_address",
    "market_id",
    "outcome",
    "quantity",
    "realized_pnl",
    "roi",
    "closed_at",
)
CORE_PNL_FEATURE_COLUMNS = (
    "wallet_address",
    "display_name",
    "resolved_markets_count",
    "trades_count",
    "win_rate",
    "avg_roi",
    "median_roi",
    "realized_pnl_total",
)


@dataclass(frozen=True)
class WalletAnalysisDataset:
    """Normalized frames used to compute wallet-level features."""

    wallets: pd.DataFrame
    trades: pd.DataFrame
    closed_positions: pd.DataFrame


@dataclass(frozen=True)
class WalletCorePnLFeatures:
    """Core closed-position and trade-count metrics for one wallet."""

    wallet_address: str
    display_name: str | None
    resolved_markets_count: int
    trades_count: int
    win_rate: float | None
    avg_roi: float | None
    median_roi: float | None
    realized_pnl_total: float | None


def empty_frame(columns: tuple[str, ...]) -> pd.DataFrame:
    """Return an empty DataFrame with deterministic column order."""

    return pd.DataFrame(columns=list(columns))
