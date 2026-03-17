"""Walk-forward scoring validation and weight-profile backtests."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pandas as pd
from sqlalchemy.orm import Session

from polymarket_anomaly_tracker.features.base import (
    CLOSED_POSITION_DATASET_COLUMNS,
    MARKET_DATASET_COLUMNS,
    PRICE_SNAPSHOT_DATASET_COLUMNS,
    TRADE_DATASET_COLUMNS,
    WalletAnalysisDataset,
    empty_frame,
)
from polymarket_anomaly_tracker.features.dataset import build_wallet_analysis_dataset
from polymarket_anomaly_tracker.scoring.anomaly_score import compute_anomaly_score_frame
from polymarket_anomaly_tracker.scoring.normalization import percentile_normalize_series

BACKTEST_SUMMARY_COLUMNS = (
    "profile_name",
    "train_start",
    "train_cutoff",
    "test_end",
    "ranked_wallets",
    "evaluated_wallets",
    "top_n",
    "top_n_evaluated_wallets",
    "top_decile_wallets",
    "average_future_roi_top_n",
    "average_future_pnl_percentile_top_n",
    "top_decile_hit_rate",
    "baseline_hit_rate",
    "hit_rate_lift",
    "spearman_future_pnl_correlation",
    "weights",
)


@dataclass(frozen=True)
class WalkForwardBacktestResult:
    """Walk-forward comparison output for one or more weight profiles."""

    summary_frame: pd.DataFrame
    train_start: datetime
    train_cutoff: datetime
    test_end: datetime


@dataclass(frozen=True)
class BacktestExportPaths:
    """Export paths written by the backtest CLI."""

    json_path: Path
    csv_path: Path


def run_walk_forward_backtest(
    session: Session,
    *,
    train_days: int,
    test_days: int,
    top_n: int,
    weight_profiles: Mapping[str, Mapping[str, float]],
    evaluation_end: datetime | None = None,
    score_eligible_min_resolved_markets: int = 5,
    score_eligible_min_trades: int = 10,
    flag_eligible_min_resolved_markets: int = 8,
    flag_eligible_min_trades: int = 20,
    flag_eligible_min_confidence_score: float = 0.50,
) -> WalkForwardBacktestResult:
    """Evaluate score weight profiles on a fixed train/test walk-forward split."""

    full_dataset = build_wallet_analysis_dataset(session)
    resolved_evaluation_end = _resolve_evaluation_end(
        dataset=full_dataset,
        explicit_evaluation_end=evaluation_end,
    )
    train_cutoff = resolved_evaluation_end - timedelta(days=test_days)
    train_start = train_cutoff - timedelta(days=train_days)
    training_dataset = _filter_dataset_for_training(
        full_dataset,
        train_start=train_start,
        train_cutoff=train_cutoff,
    )
    future_outcomes = _build_future_outcome_frame(
        full_dataset,
        train_cutoff=train_cutoff,
        test_end=resolved_evaluation_end,
    )

    summary_rows = [
        _evaluate_weight_profile(
            profile_name=profile_name,
            composite_weights=composite_weights,
            training_dataset=training_dataset,
            future_outcomes=future_outcomes,
            train_start=train_start,
            train_cutoff=train_cutoff,
            test_end=resolved_evaluation_end,
            top_n=top_n,
            score_eligible_min_resolved_markets=score_eligible_min_resolved_markets,
            score_eligible_min_trades=score_eligible_min_trades,
            flag_eligible_min_resolved_markets=flag_eligible_min_resolved_markets,
            flag_eligible_min_trades=flag_eligible_min_trades,
            flag_eligible_min_confidence_score=flag_eligible_min_confidence_score,
        )
        for profile_name, composite_weights in weight_profiles.items()
    ]
    return WalkForwardBacktestResult(
        summary_frame=pd.DataFrame(summary_rows, columns=list(BACKTEST_SUMMARY_COLUMNS)),
        train_start=train_start,
        train_cutoff=train_cutoff,
        test_end=resolved_evaluation_end,
    )


def export_backtest_summary(
    result: WalkForwardBacktestResult,
    *,
    output_dir: Path,
) -> BacktestExportPaths:
    """Write walk-forward summary rows to JSON and CSV."""

    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "score_backtest_summary.json"
    csv_path = output_dir / "score_backtest_summary.csv"

    json_payload = {
        "train_start": result.train_start.isoformat(),
        "train_cutoff": result.train_cutoff.isoformat(),
        "test_end": result.test_end.isoformat(),
        "profiles": _serialize_summary_records(result.summary_frame),
    }
    json_path.write_text(
        json.dumps(json_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    csv_frame = result.summary_frame.copy()
    csv_frame["weights"] = csv_frame["weights"].apply(
        lambda value: json.dumps(cast(dict[str, float], value), sort_keys=True)
    )
    csv_frame.to_csv(csv_path, index=False)

    return BacktestExportPaths(json_path=json_path, csv_path=csv_path)


def _evaluate_weight_profile(
    *,
    profile_name: str,
    composite_weights: Mapping[str, float],
    training_dataset: WalletAnalysisDataset,
    future_outcomes: pd.DataFrame,
    train_start: datetime,
    train_cutoff: datetime,
    test_end: datetime,
    top_n: int,
    score_eligible_min_resolved_markets: int,
    score_eligible_min_trades: int,
    flag_eligible_min_resolved_markets: int,
    flag_eligible_min_trades: int,
    flag_eligible_min_confidence_score: float,
) -> dict[str, object]:
    score_frame = compute_anomaly_score_frame(
        training_dataset,
        as_of_time=train_cutoff,
        composite_weights=composite_weights,
        score_eligible_min_resolved_markets=score_eligible_min_resolved_markets,
        score_eligible_min_trades=score_eligible_min_trades,
        flag_eligible_min_resolved_markets=flag_eligible_min_resolved_markets,
        flag_eligible_min_trades=flag_eligible_min_trades,
        flag_eligible_min_confidence_score=flag_eligible_min_confidence_score,
    )
    if score_frame.empty:
        return _build_empty_profile_summary(
            profile_name=profile_name,
            composite_weights=composite_weights,
            train_start=train_start,
            train_cutoff=train_cutoff,
            test_end=test_end,
        )

    ranked_frame = _build_ranked_future_frame(score_frame, future_outcomes=future_outcomes)
    eligible_ranked_frame = ranked_frame.loc[ranked_frame["score_eligible"]].copy()
    eligible_ranked_frame = eligible_ranked_frame.sort_values(
        by=["adjusted_score", "composite_score", "confidence_score", "wallet_address"],
        ascending=[False, False, False, True],
        na_position="last",
    ).reset_index(drop=True)

    top_n_count = min(top_n, len(eligible_ranked_frame))
    top_n_frame = eligible_ranked_frame.head(top_n_count).copy()
    evaluated_frame = eligible_ranked_frame.dropna(subset=["future_realized_pnl_total"]).copy()
    top_n_evaluated_frame = top_n_frame.dropna(subset=["future_realized_pnl_total"]).copy()
    top_decile_count = 0
    top_decile_frame = eligible_ranked_frame.head(0).copy()
    if len(eligible_ranked_frame) > 0:
        top_decile_count = max(1, math.ceil(len(eligible_ranked_frame) * 0.10))
        top_decile_frame = eligible_ranked_frame.head(top_decile_count).copy()
    top_decile_evaluated_frame = top_decile_frame.dropna(
        subset=["future_realized_pnl_total"]
    ).copy()

    baseline_hit_rate = _compute_hit_rate(evaluated_frame)
    top_decile_hit_rate = _compute_hit_rate(top_decile_evaluated_frame)
    average_future_roi_top_n = _mean_optional_series(top_n_frame["future_avg_roi"])
    average_future_pnl_percentile_top_n = _mean_optional_series(
        top_n_frame["future_realized_pnl_percentile"]
    )
    return {
        "profile_name": profile_name,
        "train_start": train_start,
        "train_cutoff": train_cutoff,
        "test_end": test_end,
        "ranked_wallets": len(eligible_ranked_frame),
        "evaluated_wallets": len(evaluated_frame),
        "top_n": top_n_count,
        "top_n_evaluated_wallets": len(top_n_evaluated_frame),
        "top_decile_wallets": top_decile_count,
        "average_future_roi_top_n": average_future_roi_top_n,
        "average_future_pnl_percentile_top_n": average_future_pnl_percentile_top_n,
        "top_decile_hit_rate": top_decile_hit_rate,
        "baseline_hit_rate": baseline_hit_rate,
        "hit_rate_lift": _subtract_optional(top_decile_hit_rate, baseline_hit_rate),
        "spearman_future_pnl_correlation": _compute_spearman_future_pnl_correlation(
            evaluated_frame
        ),
        "weights": dict(composite_weights),
    }


def _build_empty_profile_summary(
    *,
    profile_name: str,
    composite_weights: Mapping[str, float],
    train_start: datetime,
    train_cutoff: datetime,
    test_end: datetime,
) -> dict[str, object]:
    return {
        "profile_name": profile_name,
        "train_start": train_start,
        "train_cutoff": train_cutoff,
        "test_end": test_end,
        "ranked_wallets": 0,
        "evaluated_wallets": 0,
        "top_n": 0,
        "top_n_evaluated_wallets": 0,
        "top_decile_wallets": 0,
        "average_future_roi_top_n": None,
        "average_future_pnl_percentile_top_n": None,
        "top_decile_hit_rate": None,
        "baseline_hit_rate": None,
        "hit_rate_lift": None,
        "spearman_future_pnl_correlation": None,
        "weights": dict(composite_weights),
    }


def _build_ranked_future_frame(
    score_frame: pd.DataFrame,
    *,
    future_outcomes: pd.DataFrame,
) -> pd.DataFrame:
    if score_frame.empty:
        return pd.DataFrame()

    ranked_frame = score_frame.merge(future_outcomes, on="wallet_address", how="left")
    ranked_frame["future_realized_pnl_percentile"] = percentile_normalize_series(
        ranked_frame["future_realized_pnl_total"]
    )
    return ranked_frame


def _build_future_outcome_frame(
    dataset: WalletAnalysisDataset,
    *,
    train_cutoff: datetime,
    test_end: datetime,
) -> pd.DataFrame:
    future_closed_positions = _filter_time_window(
        dataset.closed_positions,
        column_name="closed_at",
        start_time=train_cutoff,
        end_time=test_end,
    )
    if future_closed_positions.empty:
        return pd.DataFrame(
            columns=[
                "wallet_address",
                "future_avg_roi",
                "future_realized_pnl_total",
            ]
        )

    future_outcomes = future_closed_positions.groupby("wallet_address", as_index=False).agg(
        future_avg_roi=("roi", "mean"),
        future_realized_pnl_total=("realized_pnl", "sum"),
    )
    return future_outcomes.sort_values("wallet_address").reset_index(drop=True)


def _filter_dataset_for_training(
    dataset: WalletAnalysisDataset,
    *,
    train_start: datetime,
    train_cutoff: datetime,
) -> WalletAnalysisDataset:
    trades = _filter_time_window(
        dataset.trades,
        column_name="trade_time",
        start_time=train_start,
        end_time=train_cutoff,
    )
    closed_positions = _filter_time_window(
        dataset.closed_positions,
        column_name="closed_at",
        start_time=train_start,
        end_time=train_cutoff,
    )
    price_snapshots = _filter_time_window(
        dataset.price_snapshots,
        column_name="snapshot_time",
        start_time=train_start,
        end_time=train_cutoff,
    )
    relevant_market_ids = {
        str(market_id)
        for market_id in trades.get("market_id", pd.Series(dtype="object")).dropna().tolist()
    }
    relevant_market_ids.update(
        str(market_id)
        for market_id in closed_positions.get("market_id", pd.Series(dtype="object"))
        .dropna()
        .tolist()
    )
    markets = dataset.markets.copy()
    if relevant_market_ids:
        markets = markets.loc[markets["market_id"].isin(sorted(relevant_market_ids))].copy()
        price_snapshots = price_snapshots.loc[
            price_snapshots["market_id"].isin(sorted(relevant_market_ids))
        ].copy()
    else:
        markets = empty_frame(MARKET_DATASET_COLUMNS)
        price_snapshots = empty_frame(PRICE_SNAPSHOT_DATASET_COLUMNS)

    return WalletAnalysisDataset(
        wallets=dataset.wallets.copy(),
        trades=trades if not trades.empty else empty_frame(TRADE_DATASET_COLUMNS),
        closed_positions=(
            closed_positions
            if not closed_positions.empty
            else empty_frame(CLOSED_POSITION_DATASET_COLUMNS)
        ),
        markets=markets,
        price_snapshots=price_snapshots,
    )


def _filter_time_window(
    frame: pd.DataFrame,
    *,
    column_name: str,
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    normalized_start_time = pd.Timestamp(start_time)
    normalized_end_time = pd.Timestamp(end_time)
    mask = (
        frame[column_name].ge(normalized_start_time)
        & frame[column_name].lt(normalized_end_time)
    )
    return frame.loc[mask].copy()


def _resolve_evaluation_end(
    *,
    dataset: WalletAnalysisDataset,
    explicit_evaluation_end: datetime | None,
) -> datetime:
    if explicit_evaluation_end is not None:
        return explicit_evaluation_end

    candidate_values: list[datetime] = []
    for frame, column_name in (
        (dataset.closed_positions, "closed_at"),
        (dataset.trades, "trade_time"),
        (dataset.price_snapshots, "snapshot_time"),
        (dataset.wallets, "last_seen_at"),
    ):
        if frame.empty:
            continue
        for value in frame[column_name].dropna().tolist():
            if isinstance(value, pd.Timestamp):
                candidate_values.append(value.to_pydatetime())
            elif isinstance(value, datetime):
                candidate_values.append(value)

    if candidate_values:
        return max(candidate_values)
    return datetime.now(UTC)


def _compute_hit_rate(frame: pd.DataFrame) -> float | None:
    if frame.empty:
        return None
    return float(frame["future_realized_pnl_total"].gt(0).mean())


def _compute_spearman_future_pnl_correlation(frame: pd.DataFrame) -> float | None:
    if len(frame) < 2:
        return None

    adjusted_score_series = frame["adjusted_score"]
    future_pnl_series = frame["future_realized_pnl_total"]
    if adjusted_score_series.nunique(dropna=True) < 2 or future_pnl_series.nunique(dropna=True) < 2:
        return None

    correlation = adjusted_score_series.rank(method="average").corr(
        future_pnl_series.rank(method="average"),
        method="pearson",
    )
    if math.isnan(correlation):
        return None
    return float(correlation)


def _mean_optional_series(series: pd.Series) -> float | None:
    if series.dropna().empty:
        return None
    value = float(series.mean())
    if math.isnan(value):
        return None
    return value


def _subtract_optional(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return float(left - right)


def _serialize_summary_records(summary_frame: pd.DataFrame) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for row in cast(list[dict[str, object]], summary_frame.to_dict(orient="records")):
        records.append(
            {
                "profile_name": cast(str, row["profile_name"]),
                "train_start": cast(datetime, row["train_start"]).isoformat(),
                "train_cutoff": cast(datetime, row["train_cutoff"]).isoformat(),
                "test_end": cast(datetime, row["test_end"]).isoformat(),
                "ranked_wallets": int(cast(int, row["ranked_wallets"])),
                "evaluated_wallets": int(cast(int, row["evaluated_wallets"])),
                "top_n": int(cast(int, row["top_n"])),
                "top_n_evaluated_wallets": int(cast(int, row["top_n_evaluated_wallets"])),
                "top_decile_wallets": int(cast(int, row["top_decile_wallets"])),
                "average_future_roi_top_n": _normalize_optional_float(
                    row["average_future_roi_top_n"]
                ),
                "average_future_pnl_percentile_top_n": _normalize_optional_float(
                    row["average_future_pnl_percentile_top_n"]
                ),
                "top_decile_hit_rate": _normalize_optional_float(row["top_decile_hit_rate"]),
                "baseline_hit_rate": _normalize_optional_float(row["baseline_hit_rate"]),
                "hit_rate_lift": _normalize_optional_float(row["hit_rate_lift"]),
                "spearman_future_pnl_correlation": _normalize_optional_float(
                    row["spearman_future_pnl_correlation"]
                ),
                "weights": {
                    key: float(value)
                    for key, value in cast(dict[str, float], row["weights"]).items()
                },
            }
        )
    return records


def _normalize_optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (float, int)):
        return float(value)
    return None
