"""Composite anomaly scoring built on top of wallet feature modules."""

from __future__ import annotations

import json
import math
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import cast

import pandas as pd
from sqlalchemy.orm import Session

from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.features.base import WalletAnalysisDataset
from polymarket_anomaly_tracker.features.consistency import compute_consistency_features
from polymarket_anomaly_tracker.features.conviction import compute_conviction_features
from polymarket_anomaly_tracker.features.dataset import build_wallet_analysis_dataset
from polymarket_anomaly_tracker.features.pnl import compute_core_pnl_features
from polymarket_anomaly_tracker.features.specialization import compute_specialization_features
from polymarket_anomaly_tracker.features.timing import compute_timing_features
from polymarket_anomaly_tracker.scoring.explanations import build_explanation_payloads
from polymarket_anomaly_tracker.scoring.normalization import (
    add_percentile_normalized_columns,
    clamp_float,
)

RAW_SCORE_COLUMNS = (
    "wallet_address",
    "display_name",
    "as_of_time",
    "resolved_markets_count",
    "trades_count",
    "recent_trades_count_90d",
    "win_rate",
    "avg_roi",
    "median_roi",
    "realized_pnl_total",
    "early_entry_edge",
    "timing_score",
    "specialization_score",
    "specialization_category",
    "conviction_score",
    "consistency_score",
)

NORMALIZED_SCORE_COLUMNS = (
    "normalized_early_entry_edge",
    "normalized_timing_score",
    "normalized_win_rate",
    "normalized_avg_roi",
    "normalized_realized_pnl_percentile",
    "normalized_specialization_score",
    "normalized_conviction_score",
    "normalized_consistency_score",
)

SCORING_FRAME_COLUMNS = RAW_SCORE_COLUMNS + NORMALIZED_SCORE_COLUMNS + (
    "score_eligible",
    "flag_eligible",
    "composite_score",
    "confidence_score",
    "adjusted_score",
)

NORMALIZED_COLUMN_MAPPING = {
    "early_entry_edge": "normalized_early_entry_edge",
    "timing_score": "normalized_timing_score",
    "win_rate": "normalized_win_rate",
    "avg_roi": "normalized_avg_roi",
    "realized_pnl_total": "normalized_realized_pnl_percentile",
    "specialization_score": "normalized_specialization_score",
    "conviction_score": "normalized_conviction_score",
    "consistency_score": "normalized_consistency_score",
}

COMPOSITE_SCORE_WEIGHTS = {
    "normalized_early_entry_edge": 0.22,
    "normalized_timing_score": 0.18,
    "normalized_win_rate": 0.15,
    "normalized_avg_roi": 0.12,
    "normalized_realized_pnl_percentile": 0.10,
    "normalized_specialization_score": 0.10,
    "normalized_conviction_score": 0.08,
    "normalized_consistency_score": 0.05,
}


def compute_anomaly_score_frame(
    dataset: WalletAnalysisDataset,
    *,
    as_of_time: datetime | None = None,
    score_eligible_min_resolved_markets: int = 5,
    score_eligible_min_trades: int = 10,
    flag_eligible_min_resolved_markets: int = 8,
    flag_eligible_min_trades: int = 20,
    flag_eligible_min_confidence_score: float = 0.50,
) -> pd.DataFrame:
    """Compute raw features, normalized features, and composite anomaly scores."""

    effective_as_of_time = _resolve_as_of_time(dataset, explicit_as_of_time=as_of_time)
    rows = _build_raw_score_rows(dataset, as_of_time=effective_as_of_time)
    if not rows:
        return pd.DataFrame(columns=list(SCORING_FRAME_COLUMNS))

    score_frame = pd.DataFrame(rows, columns=list(RAW_SCORE_COLUMNS))
    score_frame["score_eligible"] = (
        score_frame["resolved_markets_count"].ge(score_eligible_min_resolved_markets)
        & score_frame["trades_count"].ge(score_eligible_min_trades)
    )
    score_frame = add_percentile_normalized_columns(
        score_frame,
        column_mapping=NORMALIZED_COLUMN_MAPPING,
        reference_mask=score_frame["score_eligible"],
    )

    row_dicts = cast(list[dict[str, object]], score_frame.to_dict(orient="records"))
    score_frame["confidence_score"] = [
        _compute_confidence_score(
            resolved_markets_count=_normalize_required_int(row["resolved_markets_count"]),
            trades_count=_normalize_required_int(row["trades_count"]),
            recent_trades_count_90d=_normalize_required_int(row["recent_trades_count_90d"]),
        )
        for row in row_dicts
    ]
    row_dicts = cast(list[dict[str, object]], score_frame.to_dict(orient="records"))
    score_frame["flag_eligible"] = [
        _is_flag_eligible(
            row=row,
            min_resolved_markets=flag_eligible_min_resolved_markets,
            min_trades=flag_eligible_min_trades,
            min_confidence_score=flag_eligible_min_confidence_score,
        )
        for row in row_dicts
    ]
    score_frame["composite_score"] = [
        _compute_composite_score(row)
        for row in cast(list[dict[str, object]], score_frame.to_dict(orient="records"))
    ]
    row_dicts = cast(list[dict[str, object]], score_frame.to_dict(orient="records"))
    score_frame["adjusted_score"] = [
        _compute_adjusted_score(row)
        for row in row_dicts
    ]

    score_frame = score_frame.loc[:, list(SCORING_FRAME_COLUMNS)]
    return (
        score_frame.sort_values(
            by=["adjusted_score", "composite_score", "wallet_address"],
            ascending=[False, False, True],
            na_position="last",
        )
        .reset_index(drop=True)
    )


def score_and_persist_wallets(
    session: Session,
    *,
    as_of_time: datetime | None = None,
    wallet_addresses: Sequence[str] | None = None,
    score_eligible_min_resolved_markets: int = 5,
    score_eligible_min_trades: int = 10,
    flag_eligible_min_resolved_markets: int = 8,
    flag_eligible_min_trades: int = 20,
    flag_eligible_min_confidence_score: float = 0.50,
) -> pd.DataFrame:
    """Compute and persist scored wallet feature snapshots for the current run."""

    dataset = build_wallet_analysis_dataset(session, wallet_addresses=wallet_addresses)
    score_frame = compute_anomaly_score_frame(
        dataset,
        as_of_time=as_of_time,
        score_eligible_min_resolved_markets=score_eligible_min_resolved_markets,
        score_eligible_min_trades=score_eligible_min_trades,
        flag_eligible_min_resolved_markets=flag_eligible_min_resolved_markets,
        flag_eligible_min_trades=flag_eligible_min_trades,
        flag_eligible_min_confidence_score=flag_eligible_min_confidence_score,
    )
    persist_score_frame(session, score_frame)
    return score_frame


def persist_score_frame(session: Session, score_frame: pd.DataFrame) -> None:
    """Persist scored feature snapshots using the existing repository layer."""

    repository = DatabaseRepository(session)
    explanation_payloads = build_explanation_payloads(score_frame)
    for row in cast(list[dict[str, object]], score_frame.to_dict(orient="records")):
        wallet_address = _normalize_required_string(row["wallet_address"])
        repository.upsert_wallet_feature_snapshot(
            wallet_address=wallet_address,
            as_of_time=_normalize_required_datetime(row["as_of_time"]),
            resolved_markets_count=_normalize_required_int(row["resolved_markets_count"]),
            trades_count=_normalize_required_int(row["trades_count"]),
            win_rate=_normalize_optional_float(row["win_rate"]),
            avg_roi=_normalize_optional_float(row["avg_roi"]),
            median_roi=_normalize_optional_float(row["median_roi"]),
            realized_pnl_total=_normalize_optional_float(row["realized_pnl_total"]),
            early_entry_edge=_normalize_optional_float(row["early_entry_edge"]),
            specialization_score=_normalize_optional_float(row["specialization_score"]),
            conviction_score=_normalize_optional_float(row["conviction_score"]),
            consistency_score=_normalize_optional_float(row["consistency_score"]),
            timing_score=_normalize_optional_float(row["timing_score"]),
            composite_score=_normalize_optional_float(row["composite_score"]),
            confidence_score=_normalize_optional_float(row["confidence_score"]),
            adjusted_score=_normalize_optional_float(row["adjusted_score"]),
            explanations_json=json.dumps(
                explanation_payloads[wallet_address],
                sort_keys=True,
            ),
        )


def _build_raw_score_rows(
    dataset: WalletAnalysisDataset,
    *,
    as_of_time: datetime,
) -> list[dict[str, object]]:
    core_rows = {row.wallet_address: row for row in compute_core_pnl_features(dataset)}
    timing_rows = {row.wallet_address: row for row in compute_timing_features(dataset)}
    specialization_rows = {
        row.wallet_address: row for row in compute_specialization_features(dataset)
    }
    conviction_rows = {row.wallet_address: row for row in compute_conviction_features(dataset)}
    consistency_rows = {row.wallet_address: row for row in compute_consistency_features(dataset)}
    recent_trade_counts = _build_recent_trade_counts(dataset, as_of_time=as_of_time)

    rows: list[dict[str, object]] = []
    for wallet_address in sorted(core_rows):
        core_row = core_rows[wallet_address]
        timing_row = timing_rows[wallet_address]
        specialization_row = specialization_rows[wallet_address]
        conviction_row = conviction_rows[wallet_address]
        consistency_row = consistency_rows[wallet_address]
        rows.append(
            {
                "wallet_address": core_row.wallet_address,
                "display_name": core_row.display_name,
                "as_of_time": as_of_time,
                "resolved_markets_count": core_row.resolved_markets_count,
                "trades_count": core_row.trades_count,
                "recent_trades_count_90d": recent_trade_counts.get(wallet_address, 0),
                "win_rate": core_row.win_rate,
                "avg_roi": core_row.avg_roi,
                "median_roi": core_row.median_roi,
                "realized_pnl_total": core_row.realized_pnl_total,
                "early_entry_edge": timing_row.early_entry_edge,
                "timing_score": timing_row.timing_score,
                "specialization_score": specialization_row.specialization_score,
                "specialization_category": specialization_row.specialization_category,
                "conviction_score": conviction_row.conviction_score,
                "consistency_score": consistency_row.consistency_score,
            }
        )
    return rows


def _build_recent_trade_counts(
    dataset: WalletAnalysisDataset,
    *,
    as_of_time: datetime,
) -> dict[str, int]:
    if dataset.trades.empty:
        return {}

    cutoff = as_of_time - timedelta(days=90)
    recent_trade_counts: dict[str, int] = {}
    for row in cast(list[dict[str, object]], dataset.trades.to_dict(orient="records")):
        trade_time = _normalize_optional_datetime(row["trade_time"])
        if trade_time is None or trade_time < cutoff:
            continue
        wallet_address = _normalize_required_string(row["wallet_address"])
        recent_trade_counts[wallet_address] = recent_trade_counts.get(wallet_address, 0) + 1
    return recent_trade_counts


def _resolve_as_of_time(
    dataset: WalletAnalysisDataset,
    *,
    explicit_as_of_time: datetime | None,
) -> datetime:
    if explicit_as_of_time is not None:
        return _normalize_required_datetime(explicit_as_of_time)

    candidate_values: list[datetime] = []
    candidate_values.extend(_collect_datetime_candidates(dataset.wallets, "last_seen_at"))
    candidate_values.extend(_collect_datetime_candidates(dataset.trades, "trade_time"))
    candidate_values.extend(_collect_datetime_candidates(dataset.closed_positions, "closed_at"))
    if candidate_values:
        return max(candidate_values)
    return datetime.now(UTC)


def _collect_datetime_candidates(frame: pd.DataFrame, column_name: str) -> list[datetime]:
    if frame.empty:
        return []
    normalized_values: list[datetime] = []
    for value in frame[column_name].dropna().tolist():
        normalized_value = _normalize_optional_datetime(value)
        if normalized_value is not None:
            normalized_values.append(normalized_value)
    return normalized_values


def _compute_confidence_score(
    *,
    resolved_markets_count: int,
    trades_count: int,
    recent_trades_count_90d: int,
) -> float:
    sample_factor = min(1.0, resolved_markets_count / 20.0)
    trade_factor = min(1.0, trades_count / 50.0)
    recency_factor = min(1.0, recent_trades_count_90d / 15.0)
    return clamp_float(
        0.5 * sample_factor + 0.3 * trade_factor + 0.2 * recency_factor,
    )


def _is_flag_eligible(
    *,
    row: dict[str, object],
    min_resolved_markets: int,
    min_trades: int,
    min_confidence_score: float,
) -> bool:
    resolved_markets_count = _normalize_required_int(row["resolved_markets_count"])
    trades_count = _normalize_required_int(row["trades_count"])
    confidence_score = _normalize_optional_float(row["confidence_score"])
    return (
        resolved_markets_count >= min_resolved_markets
        and trades_count >= min_trades
        and confidence_score is not None
        and confidence_score >= min_confidence_score
    )


def _compute_composite_score(row: dict[str, object]) -> float:
    composite_score = 0.0
    for column_name, weight in COMPOSITE_SCORE_WEIGHTS.items():
        composite_score += weight * _coalesce_normalized_value(row[column_name])
    return clamp_float(composite_score)


def _compute_adjusted_score(row: dict[str, object]) -> float:
    composite_score = _normalize_optional_float(row["composite_score"])
    confidence_score = _normalize_optional_float(row["confidence_score"])
    if composite_score is None or confidence_score is None:
        return 0.0
    return clamp_float(composite_score * confidence_score)


def _coalesce_normalized_value(value: object) -> float:
    normalized_value = _normalize_optional_float(value)
    if normalized_value is None:
        return 0.5
    return clamp_float(normalized_value)


def _normalize_optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (float, int)):
        return float(value)
    return None


def _normalize_required_int(value: object) -> int:
    normalized_value = _normalize_optional_float(value)
    if normalized_value is None:
        return 0
    return int(normalized_value)


def _normalize_optional_datetime(value: object) -> datetime | None:
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value
    return None


def _normalize_required_datetime(value: object) -> datetime:
    normalized_value = _normalize_optional_datetime(value)
    if normalized_value is None:
        return datetime.now(UTC)
    return normalized_value


def _normalize_required_string(value: object) -> str:
    if isinstance(value, str):
        return value
    return str(value)
