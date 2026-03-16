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
WALLET_INDEX_COLUMNS = (
    "wallet_address",
    "display_name",
)
TRADE_DATASET_COLUMNS = (
    "wallet_address",
    "trade_id",
    "market_id",
    "outcome",
    "side",
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
MARKET_DATASET_COLUMNS = (
    "market_id",
    "category",
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
    markets: pd.DataFrame


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


def build_wallet_index_frame(dataset: WalletAnalysisDataset) -> pd.DataFrame:
    """Build a deterministic wallet index from available dataset components."""

    display_names = {
        str(row["wallet_address"]): _normalize_optional_string(row.get("display_name"))
        for row in dataset.wallets.to_dict(orient="records")
        if row.get("wallet_address") is not None
    }
    wallet_addresses = set(display_names)
    wallet_addresses.update(
        str(wallet_address)
        for wallet_address in dataset.trades.get("wallet_address", pd.Series(dtype="object"))
        .dropna()
        .tolist()
    )
    wallet_addresses.update(
        str(wallet_address)
        for wallet_address in dataset.closed_positions.get(
            "wallet_address",
            pd.Series(dtype="object"),
        )
        .dropna()
        .tolist()
    )

    if not wallet_addresses:
        return empty_frame(WALLET_INDEX_COLUMNS)

    sorted_wallet_addresses = sorted(wallet_addresses)
    return pd.DataFrame(
        {
            "wallet_address": sorted_wallet_addresses,
            "display_name": [
                display_names.get(wallet_address) for wallet_address in sorted_wallet_addresses
            ],
        }
    )


def _normalize_optional_string(value: object) -> str | None:
    if value is None:
        return None
    return str(value)
