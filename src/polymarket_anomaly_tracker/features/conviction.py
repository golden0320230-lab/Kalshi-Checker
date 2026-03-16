"""Conviction feature computation from trade sizing versus realized outcomes."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import cast

import pandas as pd

from polymarket_anomaly_tracker.features.base import WalletAnalysisDataset, build_wallet_index_frame

CONVICTION_FEATURE_COLUMNS = (
    "wallet_address",
    "display_name",
    "conviction_score",
)


@dataclass(frozen=True)
class WalletConvictionFeatures:
    """Conviction score for one wallet."""

    wallet_address: str
    display_name: str | None
    conviction_score: float | None


def compute_conviction_feature_frame(
    dataset: WalletAnalysisDataset,
    *,
    min_trades: int = 3,
) -> pd.DataFrame:
    """Compute a conservative sizing-versus-outcome correlation score."""

    wallet_index = build_wallet_index_frame(dataset)
    if wallet_index.empty:
        return pd.DataFrame(columns=list(CONVICTION_FEATURE_COLUMNS))

    realized_lookup = _build_realized_outcome_lookup(dataset)
    conviction_scores = {
        wallet_address: _compute_wallet_conviction_score(
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
    feature_frame["conviction_score"] = [
        conviction_scores[wallet_address]
        for wallet_address in feature_frame["wallet_address"].tolist()
    ]
    return feature_frame.loc[:, list(CONVICTION_FEATURE_COLUMNS)]


def compute_conviction_features(
    dataset: WalletAnalysisDataset,
    *,
    min_trades: int = 3,
) -> list[WalletConvictionFeatures]:
    """Compute conviction scores as typed records."""

    feature_frame = compute_conviction_feature_frame(dataset, min_trades=min_trades)
    return [
        WalletConvictionFeatures(
            wallet_address=str(row["wallet_address"]),
            display_name=_normalize_optional_string(row["display_name"]),
            conviction_score=_normalize_optional_float(row["conviction_score"]),
        )
        for row in feature_frame.to_dict(orient="records")
    ]


def _compute_wallet_conviction_score(
    *,
    trade_rows: list[dict[str, object]],
    realized_lookup: dict[tuple[str, str, str], float],
    min_trades: int,
) -> float | None:
    notionals: list[float] = []
    realized_pnls: list[float] = []
    for trade_row in trade_rows:
        trade_notional = _normalize_optional_float(trade_row["notional"])
        if trade_notional is None:
            continue
        lookup_key = (
            str(trade_row["wallet_address"]),
            str(trade_row["market_id"]),
            str(trade_row["outcome"]),
        )
        realized_pnl = realized_lookup.get(lookup_key)
        if realized_pnl is None:
            continue
        notionals.append(abs(trade_notional))
        realized_pnls.append(realized_pnl)

    if len(notionals) < min_trades:
        return None
    return _pearson_correlation(notionals, realized_pnls)


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


def _pearson_correlation(left: list[float], right: list[float]) -> float | None:
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    numerator = sum(
        (left_value - left_mean) * (right_value - right_mean)
        for left_value, right_value in zip(left, right, strict=True)
    )
    left_variance = sum((left_value - left_mean) ** 2 for left_value in left)
    right_variance = sum((right_value - right_mean) ** 2 for right_value in right)
    if left_variance == 0 or right_variance == 0:
        return None
    return numerator / math.sqrt(left_variance * right_variance)


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
