"""Magnitude-aware weekly consistency feature computation."""

from __future__ import annotations

import math
from collections.abc import Sequence
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

POSITIVE_RATIO_WEIGHT = 0.40
PROFIT_FACTOR_WEIGHT = 0.35
WORST_WEEK_PENALTY_WEIGHT = 0.25
WORST_WEEK_SMALL_CONSTANT = 1.0


@dataclass(frozen=True)
class WalletConsistencyFeatures:
    """Consistency score for one wallet."""

    wallet_address: str
    display_name: str | None
    consistency_score: float | None


@dataclass(frozen=True)
class ConsistencyComponents:
    """Explainable intermediate values used to build the final consistency score."""

    positive_ratio: float
    profit_factor_component: float
    worst_week_penalty_component: float
    consistency_score: float


def compute_consistency_feature_frame(
    dataset: WalletAnalysisDataset,
    *,
    min_periods: int = 2,
) -> pd.DataFrame:
    """Compute a magnitude-aware weekly consistency score for each wallet."""

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
    """Compute weekly consistency scores as typed records."""

    feature_frame = compute_consistency_feature_frame(dataset, min_periods=min_periods)
    return [
        WalletConsistencyFeatures(
            wallet_address=str(row["wallet_address"]),
            display_name=_normalize_optional_string(row["display_name"]),
            consistency_score=_normalize_optional_float(row["consistency_score"]),
        )
        for row in feature_frame.to_dict(orient="records")
    ]


def compute_consistency_components(
    weekly_pnl_values: Sequence[float],
) -> ConsistencyComponents | None:
    """Build explainable consistency components from weekly PnL values."""

    if len(weekly_pnl_values) < 2:
        return None

    gross_positive = sum(pnl for pnl in weekly_pnl_values if pnl > 0)
    gross_negative = sum(pnl for pnl in weekly_pnl_values if pnl < 0)
    positive_weeks = sum(1 for pnl in weekly_pnl_values if pnl > 0)
    mean_positive_week = (
        gross_positive / positive_weeks if positive_weeks > 0 else WORST_WEEK_SMALL_CONSTANT
    )
    worst_week = min(weekly_pnl_values)

    if gross_positive == 0 and gross_negative == 0:
        return ConsistencyComponents(
            positive_ratio=0.0,
            profit_factor_component=0.0,
            worst_week_penalty_component=1.0,
            consistency_score=0.0,
        )

    positive_ratio = positive_weeks / len(weekly_pnl_values)
    profit_factor_component = _compute_profit_factor_component(
        gross_positive=gross_positive,
        gross_negative=gross_negative,
    )
    worst_week_ratio = abs(min(worst_week, 0.0)) / max(
        abs(mean_positive_week),
        WORST_WEEK_SMALL_CONSTANT,
    )
    worst_week_penalty_component = 1.0 / (1.0 + worst_week_ratio)
    consistency_score = (
        POSITIVE_RATIO_WEIGHT * positive_ratio
        + PROFIT_FACTOR_WEIGHT * profit_factor_component
        + WORST_WEEK_PENALTY_WEIGHT * worst_week_penalty_component
    )
    return ConsistencyComponents(
        positive_ratio=positive_ratio,
        profit_factor_component=profit_factor_component,
        worst_week_penalty_component=worst_week_penalty_component,
        consistency_score=consistency_score,
    )


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

    components = compute_consistency_components(
        tuple(weekly_pnl[bucket_start] for bucket_start in sorted(weekly_pnl))
    )
    if components is None:
        return None
    return components.consistency_score


def _compute_profit_factor_component(
    *,
    gross_positive: float,
    gross_negative: float,
) -> float:
    if gross_positive <= 0 and gross_negative >= 0:
        return 0.0
    if gross_negative == 0:
        return 1.0 if gross_positive > 0 else 0.0

    profit_factor = gross_positive / abs(gross_negative)
    return profit_factor / (1.0 + profit_factor)


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
