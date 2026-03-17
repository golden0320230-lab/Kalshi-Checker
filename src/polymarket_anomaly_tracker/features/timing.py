"""Value-at-entry and true timing-drift features built from market snapshots."""

from __future__ import annotations

import math
from bisect import bisect_left
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

import pandas as pd

from polymarket_anomaly_tracker.features.base import WalletAnalysisDataset, build_wallet_index_frame

FORWARD_DRIFT_WINDOWS = (
    timedelta(hours=1),
    timedelta(hours=6),
    timedelta(hours=24),
)

TIMING_FEATURE_COLUMNS = (
    "wallet_address",
    "display_name",
    "value_at_entry_score",
    "timing_drift_score",
    "timing_positive_capture_score",
)


@dataclass(frozen=True)
class WalletTimingFeatures:
    """Timing-related features for one wallet."""

    wallet_address: str
    display_name: str | None
    value_at_entry_score: float | None
    timing_drift_score: float | None
    timing_positive_capture_score: float | None


@dataclass(frozen=True)
class _SnapshotSeries:
    snapshot_times: tuple[datetime, ...]
    reference_prices: tuple[float, ...]


def compute_timing_feature_frame(
    dataset: WalletAnalysisDataset,
    *,
    min_value_trades: int = 2,
    min_matched_trades: int = 2,
) -> pd.DataFrame:
    """Compute value-at-entry and true timing drift from trade and price history.

    `value_at_entry_score` remains a resolved-outcome proxy and is explicitly
    separated from timing. `timing_drift_score` and
    `timing_positive_capture_score` use forward market snapshots at +1h, +6h,
    and +24h after each trade. If too few trades have forward snapshot matches,
    the timing metrics return `None`.
    """

    wallet_index = build_wallet_index_frame(dataset)
    if wallet_index.empty:
        return pd.DataFrame(columns=list(TIMING_FEATURE_COLUMNS))

    realized_lookup = _build_realized_outcome_lookup(dataset)
    price_index = _build_market_price_index(dataset)
    trade_rows = cast(list[dict[str, object]], dataset.trades.to_dict(orient="records"))
    wallet_metrics = {
        wallet_address: _compute_wallet_timing_metrics(
            trade_rows=[
                trade_row
                for trade_row in trade_rows
                if trade_row["wallet_address"] == wallet_address
            ],
            realized_lookup=realized_lookup,
            price_index=price_index,
            min_value_trades=min_value_trades,
            min_matched_trades=min_matched_trades,
        )
        for wallet_address in wallet_index["wallet_address"].tolist()
    }

    feature_frame = wallet_index.copy()
    feature_frame["value_at_entry_score"] = [
        wallet_metrics[wallet_address][0]
        for wallet_address in feature_frame["wallet_address"].tolist()
    ]
    feature_frame["timing_drift_score"] = [
        wallet_metrics[wallet_address][1]
        for wallet_address in feature_frame["wallet_address"].tolist()
    ]
    feature_frame["timing_positive_capture_score"] = [
        wallet_metrics[wallet_address][2]
        for wallet_address in feature_frame["wallet_address"].tolist()
    ]
    return feature_frame.loc[:, list(TIMING_FEATURE_COLUMNS)]


def compute_timing_features(
    dataset: WalletAnalysisDataset,
    *,
    min_value_trades: int = 2,
    min_matched_trades: int = 2,
) -> list[WalletTimingFeatures]:
    """Compute value-at-entry and timing-drift features as typed rows."""

    feature_frame = compute_timing_feature_frame(
        dataset,
        min_value_trades=min_value_trades,
        min_matched_trades=min_matched_trades,
    )
    return [
        WalletTimingFeatures(
            wallet_address=str(row["wallet_address"]),
            display_name=_normalize_optional_string(row["display_name"]),
            value_at_entry_score=_normalize_optional_float(row["value_at_entry_score"]),
            timing_drift_score=_normalize_optional_float(row["timing_drift_score"]),
            timing_positive_capture_score=_normalize_optional_float(
                row["timing_positive_capture_score"]
            ),
        )
        for row in feature_frame.to_dict(orient="records")
    ]


def _compute_wallet_timing_metrics(
    *,
    trade_rows: list[dict[str, object]],
    realized_lookup: dict[tuple[str, str, str], float],
    price_index: dict[str, _SnapshotSeries],
    min_value_trades: int,
    min_matched_trades: int,
) -> tuple[float | None, float | None, float | None]:
    value_at_entry_score = _compute_value_at_entry_score(
        trade_rows=trade_rows,
        realized_lookup=realized_lookup,
        min_value_trades=min_value_trades,
    )
    timing_drift_score, timing_positive_capture_score = _compute_timing_drift_scores(
        trade_rows=trade_rows,
        price_index=price_index,
        min_matched_trades=min_matched_trades,
    )
    return (
        value_at_entry_score,
        timing_drift_score,
        timing_positive_capture_score,
    )


def _compute_value_at_entry_score(
    *,
    trade_rows: list[dict[str, object]],
    realized_lookup: dict[tuple[str, str, str], float],
    min_value_trades: int,
) -> float | None:
    weighted_gap_sum = 0.0
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
            _normalize_outcome(trade_row["outcome"]),
        )
        realized_pnl = realized_lookup.get(lookup_key)
        if realized_pnl is None:
            continue

        matched_trades += 1
        resolved_price = 1.0 if realized_pnl > 0 else 0.0 if realized_pnl < 0 else 0.5
        direction = _directional_sign(trade_row["side"])
        directional_gap = direction * (resolved_price - trade_price)
        weight = abs(trade_notional)
        total_weight += weight
        weighted_gap_sum += weight * directional_gap

    if matched_trades < min_value_trades or total_weight == 0:
        return None

    return weighted_gap_sum / total_weight


def _compute_timing_drift_scores(
    *,
    trade_rows: list[dict[str, object]],
    price_index: dict[str, _SnapshotSeries],
    min_matched_trades: int,
) -> tuple[float | None, float | None]:
    weighted_drift_sum = 0.0
    weighted_positive_capture_sum = 0.0
    total_weight = 0.0
    matched_trades = 0

    for trade_row in trade_rows:
        trade_price = _normalize_optional_float(trade_row["price"])
        trade_notional = _normalize_optional_float(trade_row["notional"])
        trade_time = _normalize_optional_datetime(trade_row["trade_time"])
        market_id = _normalize_optional_string(trade_row["market_id"])
        outcome = _normalize_optional_string(trade_row["outcome"])
        if (
            trade_price is None
            or trade_notional is None
            or trade_time is None
            or market_id is None
            or outcome is None
        ):
            continue

        snapshot_series = price_index.get(market_id)
        if snapshot_series is None:
            continue

        signed_drifts = [
            signed_drift
            for horizon in FORWARD_DRIFT_WINDOWS
            if (
                signed_drift := _compute_post_trade_drift(
                    snapshot_series=snapshot_series,
                    trade_time=trade_time,
                    trade_price=trade_price,
                    outcome=outcome,
                    side=trade_row["side"],
                    horizon=horizon,
                )
            )
            is not None
        ]
        if not signed_drifts:
            continue

        matched_trades += 1
        trade_drift_score = sum(signed_drifts) / len(signed_drifts)
        positive_capture_sum = sum(max(signed_drift, 0.0) for signed_drift in signed_drifts)
        trade_positive_capture = positive_capture_sum / len(signed_drifts)
        weight = abs(trade_notional)
        total_weight += weight
        weighted_drift_sum += weight * trade_drift_score
        weighted_positive_capture_sum += weight * trade_positive_capture

    if matched_trades < min_matched_trades or total_weight == 0:
        return None, None

    return (
        weighted_drift_sum / total_weight,
        weighted_positive_capture_sum / total_weight,
    )


def _compute_post_trade_drift(
    *,
    snapshot_series: _SnapshotSeries,
    trade_time: datetime,
    trade_price: float,
    outcome: str,
    side: object,
    horizon: timedelta,
) -> float | None:
    future_contract_price = _find_contract_price_after(
        snapshot_series=snapshot_series,
        target_time=trade_time + horizon,
        outcome=outcome,
    )
    if future_contract_price is None:
        return None
    direction = _directional_sign(side)
    return direction * (future_contract_price - trade_price)


def _find_contract_price_after(
    *,
    snapshot_series: _SnapshotSeries,
    target_time: datetime,
    outcome: str,
) -> float | None:
    normalized_target_time = _normalize_required_datetime(target_time)
    snapshot_index = bisect_left(snapshot_series.snapshot_times, normalized_target_time)
    if snapshot_index >= len(snapshot_series.snapshot_times):
        return None

    yes_price = snapshot_series.reference_prices[snapshot_index]
    normalized_outcome = _normalize_outcome(outcome)
    if normalized_outcome == "NO":
        return max(0.0, min(1.0, 1.0 - yes_price))
    return yes_price


def _build_market_price_index(dataset: WalletAnalysisDataset) -> dict[str, _SnapshotSeries]:
    snapshot_index: dict[str, list[tuple[datetime, float]]] = {}
    for row in cast(list[dict[str, object]], dataset.price_snapshots.to_dict(orient="records")):
        market_id = _normalize_optional_string(row.get("market_id"))
        snapshot_time = _normalize_optional_datetime(row.get("snapshot_time"))
        reference_price = _extract_snapshot_reference_price(row)
        if market_id is None or snapshot_time is None or reference_price is None:
            continue
        snapshot_index.setdefault(market_id, []).append((snapshot_time, reference_price))

    return {
        market_id: _SnapshotSeries(
            snapshot_times=tuple(snapshot_time for snapshot_time, _ in sorted_rows),
            reference_prices=tuple(price for _, price in sorted_rows),
        )
        for market_id, rows in snapshot_index.items()
        if (sorted_rows := sorted(rows, key=lambda row: row[0]))
    }


def _extract_snapshot_reference_price(row: dict[str, object]) -> float | None:
    mid_price = _normalize_optional_float(row.get("mid_price"))
    if mid_price is not None:
        return mid_price

    last_price = _normalize_optional_float(row.get("last_price"))
    if last_price is not None:
        return last_price

    best_bid = _normalize_optional_float(row.get("best_bid"))
    best_ask = _normalize_optional_float(row.get("best_ask"))
    if best_bid is not None and best_ask is not None:
        return (best_bid + best_ask) / 2.0
    if best_bid is not None:
        return best_bid
    if best_ask is not None:
        return best_ask
    return None


def _build_realized_outcome_lookup(
    dataset: WalletAnalysisDataset,
) -> dict[tuple[str, str, str], float]:
    realized_lookup: dict[tuple[str, str, str], float] = {}
    for row in dataset.closed_positions.to_dict(orient="records"):
        key = (
            str(row["wallet_address"]),
            str(row["market_id"]),
            _normalize_outcome(row["outcome"]),
        )
        realized_lookup[key] = realized_lookup.get(key, 0.0) + (
            _normalize_optional_float(row["realized_pnl"]) or 0.0
        )
    return realized_lookup


def _directional_sign(side: object) -> float:
    normalized_side = _normalize_optional_string(side)
    if normalized_side is not None and normalized_side.lower() == "sell":
        return -1.0
    return 1.0


def _normalize_outcome(value: object) -> str:
    normalized_value = _normalize_optional_string(value)
    if normalized_value is None:
        return "YES"
    return normalized_value.upper()


def _normalize_optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (float, int)):
        return float(value)
    return None


def _normalize_optional_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return _normalize_required_datetime(value.to_pydatetime())
    if isinstance(value, datetime):
        return _normalize_required_datetime(value)
    return None


def _normalize_required_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _normalize_optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        return value
    return None
