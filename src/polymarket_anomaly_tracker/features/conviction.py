"""Conviction feature computation from bucketed sizing versus realized outcomes."""

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


@dataclass(frozen=True)
class _ResolvedBucketOutcome:
    realized_pnl: float
    roi: float | None


@dataclass(frozen=True)
class _ConvictionBucket:
    market_id: str
    outcome: str
    total_abs_notional: float
    trade_count: int
    net_size: float | None
    avg_entry_price: float | None
    realized_pnl: float
    roi: float | None


def compute_conviction_feature_frame(
    dataset: WalletAnalysisDataset,
    *,
    min_unique_buckets: int = 3,
) -> pd.DataFrame:
    """Compute bucket-level sizing-versus-outcome correlation for each wallet."""

    wallet_index = build_wallet_index_frame(dataset)
    if wallet_index.empty:
        return pd.DataFrame(columns=list(CONVICTION_FEATURE_COLUMNS))

    resolved_lookup = _build_resolved_bucket_lookup(dataset)
    trade_rows = cast(list[dict[str, object]], dataset.trades.to_dict(orient="records"))
    conviction_scores = {
        wallet_address: _compute_wallet_conviction_score(
            trade_rows=[
                trade_row
                for trade_row in trade_rows
                if trade_row["wallet_address"] == wallet_address
            ],
            resolved_lookup=resolved_lookup,
            min_unique_buckets=min_unique_buckets,
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
    min_unique_buckets: int = 3,
) -> list[WalletConvictionFeatures]:
    """Compute conviction scores as typed records."""

    feature_frame = compute_conviction_feature_frame(
        dataset,
        min_unique_buckets=min_unique_buckets,
    )
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
    resolved_lookup: dict[tuple[str, str, str], _ResolvedBucketOutcome],
    min_unique_buckets: int,
) -> float | None:
    conviction_buckets = _build_trade_buckets(
        trade_rows=trade_rows,
        resolved_lookup=resolved_lookup,
    )
    if len(conviction_buckets) < min_unique_buckets:
        return None

    notionals = [bucket.total_abs_notional for bucket in conviction_buckets]
    realized_pnls = [bucket.realized_pnl for bucket in conviction_buckets]
    return _pearson_correlation(notionals, realized_pnls)


def _build_trade_buckets(
    *,
    trade_rows: list[dict[str, object]],
    resolved_lookup: dict[tuple[str, str, str], _ResolvedBucketOutcome],
) -> list[_ConvictionBucket]:
    bucket_totals: dict[tuple[str, str, str], dict[str, float | int | None]] = {}
    for trade_row in trade_rows:
        market_id = _normalize_optional_string(trade_row.get("market_id"))
        outcome = _normalize_optional_string(trade_row.get("outcome"))
        wallet_address = _normalize_optional_string(trade_row.get("wallet_address"))
        trade_notional = _normalize_optional_float(trade_row.get("notional"))
        if (
            wallet_address is None
            or market_id is None
            or outcome is None
            or trade_notional is None
        ):
            continue

        lookup_key = (wallet_address, market_id, outcome)
        resolved_outcome = resolved_lookup.get(lookup_key)
        if resolved_outcome is None:
            continue

        bucket = bucket_totals.setdefault(
            lookup_key,
            {
                "total_abs_notional": 0.0,
                "trade_count": 0,
                "net_size": 0.0,
                "weighted_entry_sum": 0.0,
            },
        )
        abs_notional = abs(trade_notional)
        bucket["total_abs_notional"] = _normalize_required_float(bucket["total_abs_notional"]) + (
            abs_notional
        )
        bucket["trade_count"] = _normalize_required_int(bucket["trade_count"]) + 1

        trade_size = _normalize_optional_float(trade_row.get("size"))
        if trade_size is not None:
            signed_size = _directional_sign(trade_row.get("side")) * trade_size
            bucket["net_size"] = _normalize_required_float(bucket["net_size"]) + signed_size

        trade_price = _normalize_optional_float(trade_row.get("price"))
        if trade_price is not None:
            weighted_entry_sum = _normalize_required_float(bucket["weighted_entry_sum"])
            bucket["weighted_entry_sum"] = weighted_entry_sum + (abs_notional * trade_price)

    conviction_buckets: list[_ConvictionBucket] = []
    for (wallet_address, market_id, outcome), bucket in bucket_totals.items():
        resolved_outcome = resolved_lookup[(wallet_address, market_id, outcome)]
        total_abs_notional = _normalize_required_float(bucket["total_abs_notional"])
        avg_entry_price = None
        if total_abs_notional > 0:
            avg_entry_price = (
                _normalize_required_float(bucket["weighted_entry_sum"]) / total_abs_notional
            )

        conviction_buckets.append(
            _ConvictionBucket(
                market_id=market_id,
                outcome=outcome,
                total_abs_notional=total_abs_notional,
                trade_count=_normalize_required_int(bucket["trade_count"]),
                net_size=_normalize_optional_float(bucket["net_size"]),
                avg_entry_price=avg_entry_price,
                realized_pnl=resolved_outcome.realized_pnl,
                roi=resolved_outcome.roi,
            )
        )

    conviction_buckets.sort(key=lambda bucket: (bucket.market_id, bucket.outcome))
    return conviction_buckets


def _build_resolved_bucket_lookup(
    dataset: WalletAnalysisDataset,
) -> dict[tuple[str, str, str], _ResolvedBucketOutcome]:
    resolved_lookup: dict[tuple[str, str, str], _ResolvedBucketOutcome] = {}
    for row in cast(list[dict[str, object]], dataset.closed_positions.to_dict(orient="records")):
        key = (
            str(row["wallet_address"]),
            str(row["market_id"]),
            str(row["outcome"]),
        )
        existing_outcome = resolved_lookup.get(key)
        realized_pnl = _normalize_optional_float(row.get("realized_pnl")) or 0.0
        roi = _normalize_optional_float(row.get("roi"))
        if existing_outcome is None:
            resolved_lookup[key] = _ResolvedBucketOutcome(realized_pnl=realized_pnl, roi=roi)
            continue

        resolved_lookup[key] = _ResolvedBucketOutcome(
            realized_pnl=existing_outcome.realized_pnl + realized_pnl,
            roi=existing_outcome.roi if existing_outcome.roi is not None else roi,
        )
    return resolved_lookup


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


def _directional_sign(side: object) -> float:
    normalized_side = _normalize_optional_string(side)
    if normalized_side is not None and normalized_side.lower() == "sell":
        return -1.0
    return 1.0


def _normalize_optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (float, int)):
        return float(value)
    return None


def _normalize_required_float(value: object) -> float:
    normalized_value = _normalize_optional_float(value)
    if normalized_value is None:
        return 0.0
    return normalized_value


def _normalize_required_int(value: object) -> int:
    normalized_value = _normalize_optional_float(value)
    if normalized_value is None:
        return 0
    return int(normalized_value)


def _normalize_optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        return value
    return None
