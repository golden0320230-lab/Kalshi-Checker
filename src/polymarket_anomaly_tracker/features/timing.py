"""Reduced timing and early-entry approximations for v1 feature scoring."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import cast

import pandas as pd

from polymarket_anomaly_tracker.features.base import WalletAnalysisDataset, build_wallet_index_frame

TIMING_FEATURE_COLUMNS = (
    "wallet_address",
    "display_name",
    "early_entry_edge",
    "timing_score",
)


@dataclass(frozen=True)
class WalletTimingFeatures:
    """Approximate timing-oriented features for one wallet."""

    wallet_address: str
    display_name: str | None
    early_entry_edge: float | None
    timing_score: float | None


def compute_timing_feature_frame(
    dataset: WalletAnalysisDataset,
    *,
    min_trades: int = 2,
) -> pd.DataFrame:
    """Compute conservative timing approximations from trade entry prices.

    v1 does not have forward price snapshots for every trade, so this uses a
    reduced proxy: join each trade to the wallet's realized outcome on the same
    market/outcome and compare entry price to the implied resolved price.
    """

    wallet_index = build_wallet_index_frame(dataset)
    if wallet_index.empty:
        return pd.DataFrame(columns=list(TIMING_FEATURE_COLUMNS))

    realized_lookup = _build_realized_outcome_lookup(dataset)
    wallet_metrics = {
        wallet_address: _compute_wallet_timing_metrics(
            trade_rows=[
                trade_row
                for trade_row in cast(
                    list[dict[str, object]],
                    dataset.trades.to_dict(orient="records"),
                )
                if trade_row["wallet_address"] == wallet_address
            ],
            realized_lookup=realized_lookup,
            min_trades=min_trades,
        )
        for wallet_address in wallet_index["wallet_address"].tolist()
    }

    feature_frame = wallet_index.copy()
    feature_frame["early_entry_edge"] = [
        wallet_metrics[wallet_address][0]
        for wallet_address in feature_frame["wallet_address"].tolist()
    ]
    feature_frame["timing_score"] = [
        wallet_metrics[wallet_address][1]
        for wallet_address in feature_frame["wallet_address"].tolist()
    ]
    return feature_frame.loc[:, list(TIMING_FEATURE_COLUMNS)]


def compute_timing_features(
    dataset: WalletAnalysisDataset,
    *,
    min_trades: int = 2,
) -> list[WalletTimingFeatures]:
    """Compute timing approximations as typed records."""

    feature_frame = compute_timing_feature_frame(dataset, min_trades=min_trades)
    return [
        WalletTimingFeatures(
            wallet_address=str(row["wallet_address"]),
            display_name=_normalize_optional_string(row["display_name"]),
            early_entry_edge=_normalize_optional_float(row["early_entry_edge"]),
            timing_score=_normalize_optional_float(row["timing_score"]),
        )
        for row in feature_frame.to_dict(orient="records")
    ]


def _compute_wallet_timing_metrics(
    *,
    trade_rows: list[dict[str, object]],
    realized_lookup: dict[tuple[str, str, str], float],
    min_trades: int,
) -> tuple[float | None, float | None]:
    weighted_edge_sum = 0.0
    weighted_positive_edge_sum = 0.0
    total_weight = 0.0
    matched_trades = 0

    for trade_row in trade_rows:
        trade_price = _normalize_optional_float(trade_row["price"])
        trade_notional = _normalize_optional_float(trade_row["notional"])
        if trade_price is None or trade_notional is None:
            continue

        lookup_key = (
            str(trade_row["wallet_address"]),
            str(trade_row["market_id"]),
            str(trade_row["outcome"]),
        )
        realized_pnl = realized_lookup.get(lookup_key)
        if realized_pnl is None:
            continue

        matched_trades += 1
        resolved_price = 1.0 if realized_pnl > 0 else 0.0 if realized_pnl < 0 else 0.5
        direction = -1.0 if str(trade_row["side"]).lower() == "sell" else 1.0
        directional_gap = direction * (resolved_price - trade_price)
        weight = abs(trade_notional)
        total_weight += weight
        weighted_edge_sum += weight * directional_gap
        weighted_positive_edge_sum += weight * max(directional_gap, 0.0)

    if matched_trades < min_trades or total_weight == 0:
        return None, None

    return (
        weighted_edge_sum / total_weight,
        weighted_positive_edge_sum / total_weight,
    )


def _build_realized_outcome_lookup(
    dataset: WalletAnalysisDataset,
) -> dict[tuple[str, str, str], float]:
    realized_lookup: dict[tuple[str, str, str], float] = {}
    for row in dataset.closed_positions.to_dict(orient="records"):
        key = (
            str(row["wallet_address"]),
            str(row["market_id"]),
            str(row["outcome"]),
        )
        realized_lookup[key] = realized_lookup.get(key, 0.0) + (
            _normalize_optional_float(row["realized_pnl"]) or 0.0
        )
    return realized_lookup


def _normalize_optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (float, int)):
        return float(value)
    return None


def _normalize_optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        return value
    return None
