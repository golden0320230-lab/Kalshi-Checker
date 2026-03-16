"""Time-bucket consistency feature computation."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import cast

import pandas as pd

from polymarket_anomaly_tracker.features.base import WalletAnalysisDataset, build_wallet_index_frame

CONSISTENCY_FEATURE_COLUMNS = (
    "wallet_address",
    "display_name",
    "consistency_score",
)


@dataclass(frozen=True)
class WalletConsistencyFeatures:
    """Consistency score for one wallet."""

    wallet_address: str
    display_name: str | None
    consistency_score: float | None


def compute_consistency_feature_frame(
    dataset: WalletAnalysisDataset,
    *,
    min_periods: int = 2,
) -> pd.DataFrame:
    """Compute the share of positive weekly buckets for each wallet."""

    wallet_index = build_wallet_index_frame(dataset)
    if wallet_index.empty:
        return pd.DataFrame(columns=list(CONSISTENCY_FEATURE_COLUMNS))

    closed_position_rows = cast(
        list[dict[str, object]],
        dataset.closed_positions.to_dict(orient="records"),
    )
    consistency_scores = {
        wallet_address: _compute_wallet_consistency_score(
            closed_position_rows=[
                row
                for row in closed_position_rows
                if row["wallet_address"] == wallet_address
            ],
            min_periods=min_periods,
        )
        for wallet_address in wallet_index["wallet_address"].tolist()
    }

    feature_frame = wallet_index.copy()
    feature_frame["consistency_score"] = [
        consistency_scores[wallet_address]
        for wallet_address in feature_frame["wallet_address"].tolist()
    ]
    return feature_frame.loc[:, list(CONSISTENCY_FEATURE_COLUMNS)]


def compute_consistency_features(
    dataset: WalletAnalysisDataset,
    *,
    min_periods: int = 2,
) -> list[WalletConsistencyFeatures]:
    """Compute time-bucket consistency scores as typed records."""

    feature_frame = compute_consistency_feature_frame(dataset, min_periods=min_periods)
    return [
        WalletConsistencyFeatures(
            wallet_address=str(row["wallet_address"]),
            display_name=_normalize_optional_string(row["display_name"]),
            consistency_score=_normalize_optional_float(row["consistency_score"]),
        )
        for row in feature_frame.to_dict(orient="records")
    ]


def _compute_wallet_consistency_score(
    *,
    closed_position_rows: list[dict[str, object]],
    min_periods: int,
) -> float | None:
    weekly_pnl: dict[datetime, float] = {}
    for row in closed_position_rows:
        closed_at = _normalize_datetime(row["closed_at"])
        if closed_at is None:
            continue
        bucket_start = _start_of_week(closed_at)
        weekly_pnl[bucket_start] = weekly_pnl.get(bucket_start, 0.0) + (
            _normalize_optional_float(row["realized_pnl"]) or 0.0
        )

    if len(weekly_pnl) < min_periods:
        return None
    positive_periods = sum(1 for pnl in weekly_pnl.values() if pnl > 0)
    return positive_periods / len(weekly_pnl)


def _start_of_week(value: datetime) -> datetime:
    return (value - timedelta(days=value.weekday())).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=value.tzinfo,
    )


def _normalize_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value
    return None


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
