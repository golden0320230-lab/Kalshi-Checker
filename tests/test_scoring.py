"""Tests for score normalization, composite scoring, and explanations."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest
from alembic import command
from typer.testing import CliRunner

from polymarket_anomaly_tracker.config import clear_settings_cache
from polymarket_anomaly_tracker.db.init_db import build_alembic_config, init_database
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope
from polymarket_anomaly_tracker.main import app
from polymarket_anomaly_tracker.scoring.anomaly_score import (
    COMPOSITE_SCORE_WEIGHTS,
    TIMING_VALUE_SCORE_WEIGHTS,
    score_and_persist_wallets,
)
from polymarket_anomaly_tracker.scoring.explanations import build_explanation_payload
from polymarket_anomaly_tracker.scoring.normalization import percentile_normalize_series

ALPHA_WALLET = "0xaaa"
BETA_WALLET = "0xbbb"
GAMMA_WALLET = "0xccc"
DELTA_WALLET = "0xddd"
runner = CliRunner()


def test_percentile_normalize_series_handles_constant_columns() -> None:
    series = pd.Series([5.0, 5.0, None], dtype="float64")

    normalized = percentile_normalize_series(series)

    assert normalized.iloc[0] == pytest.approx(0.5)
    assert normalized.iloc[1] == pytest.approx(0.5)
    assert pd.isna(normalized.iloc[2])


def test_build_explanation_payload_tracks_reason_threshold_edges() -> None:
    payload = build_explanation_payload(
        {
            "wallet_address": ALPHA_WALLET,
            "display_name": "Alpha",
            "resolved_markets_count": 8,
            "trades_count": 20,
            "recent_trades_count_90d": 12,
            "score_eligible": True,
            "flag_eligible": True,
            "adjusted_score": 0.81,
            "composite_score": 0.90,
            "confidence_score": 0.90,
            "value_at_entry_score": 0.41,
            "timing_drift_score": 0.29,
            "timing_positive_capture_score": 0.18,
            "win_rate": 0.82,
            "avg_roi": 0.18,
            "median_roi": 0.15,
            "realized_pnl_total": 125.0,
            "specialization_score": 0.33,
            "specialization_category": "politics",
            "conviction_score": 0.77,
            "consistency_score": 0.67,
            "normalized_value_at_entry_score": 0.75,
            "normalized_timing_drift_score": 0.82,
            "normalized_timing_positive_capture_score": 0.749,
            "normalized_win_rate": 0.91,
            "normalized_avg_roi": 0.80,
            "normalized_realized_pnl_percentile": 0.88,
            "normalized_specialization_score": 0.77,
            "normalized_conviction_score": 0.70,
            "normalized_consistency_score": 0.60,
        }
    )

    assert payload["sample_size"] == {
        "resolved_markets_count": 8,
        "trades_count": 20,
        "recent_trades_count_90d": 12,
    }
    assert "normalized_value_at_entry_score" in payload["threshold_reason_keys"]
    assert "normalized_timing_positive_capture_score" not in payload["threshold_reason_keys"]
    assert "normalized_timing_drift_score" in payload["threshold_reason_keys"]
    assert payload["top_reasons"]
    assert {
        detail["message"] for detail in payload["reason_details"]
    } >= {"Unusually strong results in politics markets"}


def test_timing_value_weights_are_rebalanced_and_isolated() -> None:
    assert sum(TIMING_VALUE_SCORE_WEIGHTS.values()) == pytest.approx(0.24)
    assert sum(COMPOSITE_SCORE_WEIGHTS.values()) == pytest.approx(1.0)


def test_score_and_persist_wallets_outputs_raw_and_normalized_scores(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'scoring.db'}"
    init_database(database_url)
    seed_scoring_test_data(database_url)
    as_of_time = datetime(2026, 4, 2, 12, 0, tzinfo=UTC)

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        score_frame = score_and_persist_wallets(
            session,
            as_of_time=as_of_time,
            score_eligible_min_resolved_markets=3,
            score_eligible_min_trades=3,
            flag_eligible_min_resolved_markets=4,
            flag_eligible_min_trades=4,
            flag_eligible_min_confidence_score=0.15,
        )
        repository = DatabaseRepository(session)
        alpha_snapshot = repository.get_latest_feature_snapshot(ALPHA_WALLET)
        beta_snapshot = repository.get_latest_feature_snapshot(BETA_WALLET)
        delta_snapshot = repository.get_latest_feature_snapshot(DELTA_WALLET)

    score_rows = {
        row["wallet_address"]: row for row in score_frame.to_dict(orient="records")
    }

    assert {
        "win_rate",
        "normalized_win_rate",
        "normalized_realized_pnl_percentile",
        "value_at_entry_score",
        "timing_drift_score",
        "timing_positive_capture_score",
        "normalized_value_at_entry_score",
        "normalized_timing_drift_score",
        "normalized_timing_positive_capture_score",
        "adjusted_score",
    } <= set(score_frame.columns)
    assert score_rows[ALPHA_WALLET]["score_eligible"] is True
    assert score_rows[BETA_WALLET]["score_eligible"] is True
    assert score_rows[DELTA_WALLET]["score_eligible"] is False
    assert score_rows[ALPHA_WALLET]["recent_trades_count_90d"] == 4
    assert score_rows[BETA_WALLET]["recent_trades_count_90d"] == 3
    assert score_rows[ALPHA_WALLET]["normalized_realized_pnl_percentile"] == pytest.approx(1.0)
    assert score_rows[BETA_WALLET]["normalized_realized_pnl_percentile"] == pytest.approx(0.0)
    assert score_rows[ALPHA_WALLET]["adjusted_score"] > score_rows[BETA_WALLET]["adjusted_score"]

    assert alpha_snapshot is not None
    assert beta_snapshot is not None
    assert delta_snapshot is not None
    assert alpha_snapshot.adjusted_score == pytest.approx(
        float(score_rows[ALPHA_WALLET]["adjusted_score"])
    )
    assert beta_snapshot.adjusted_score == pytest.approx(
        float(score_rows[BETA_WALLET]["adjusted_score"])
    )
    assert delta_snapshot.adjusted_score == pytest.approx(
        float(score_rows[DELTA_WALLET]["adjusted_score"])
    )

    explanation_payload = json.loads(alpha_snapshot.explanations_json)
    assert explanation_payload["metrics"]["adjusted_score"] == pytest.approx(
        float(score_rows[ALPHA_WALLET]["adjusted_score"])
    )
    assert explanation_payload["sample_size"]["resolved_markets_count"] == 4
    assert explanation_payload["sample_size"]["trades_count"] == 4
    assert explanation_payload["normalized_features"]["normalized_win_rate"] == pytest.approx(1.0)
    assert explanation_payload["top_reasons"]


def test_score_and_persist_wallets_requires_current_scoring_migration(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'scoring-pre-issue10.db'}"
    command.upgrade(build_alembic_config(database_url), "20260315_0001")
    seed_scoring_test_data(database_url)

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        with pytest.raises(RuntimeError, match="uv run pmat init-db"):
            score_and_persist_wallets(
                session,
                as_of_time=datetime(2026, 4, 2, 12, 0, tzinfo=UTC),
                score_eligible_min_resolved_markets=3,
                score_eligible_min_trades=3,
            )


def test_score_compute_command_persists_snapshots(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'scoring-cli.db'}"
    init_database(database_url)
    seed_scoring_test_data(database_url)

    monkeypatch.setenv("PMAT_DATABASE_URL", database_url)
    clear_settings_cache()
    try:
        result = runner.invoke(app, ["score", "compute"])
    finally:
        clear_settings_cache()

    assert result.exit_code == 0
    assert "Computed wallet scores." in result.stdout
    assert "Wallets scored: 4." in result.stdout


def seed_scoring_test_data(database_url: str) -> None:
    """Seed deterministic data for scoring tests."""

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        observed_at = datetime(2026, 3, 16, 12, 0, tzinfo=UTC)
        wallet_rows = (
            (ALPHA_WALLET, "Alpha"),
            (BETA_WALLET, "Beta"),
            (GAMMA_WALLET, "Gamma"),
            (DELTA_WALLET, "Delta"),
        )
        for wallet_address, display_name in wallet_rows:
            repository.upsert_wallet(
                wallet_address=wallet_address,
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                display_name=display_name,
            )

        market_rows = (
            ("market-1", "politics"),
            ("market-2", "politics"),
            ("market-3", "crypto"),
            ("market-4", "crypto"),
            ("market-5", "macro"),
            ("market-6", "politics"),
            ("market-7", "crypto"),
            ("market-8", "politics"),
        )
        for market_id, category in market_rows:
            repository.upsert_market(
                market_id=market_id,
                question=f"Question for {market_id}",
                status="closed",
                category=category,
            )

        seed_alpha_wallet(repository)
        seed_beta_wallet(repository)
        seed_delta_wallet(repository)


def seed_alpha_wallet(repository: DatabaseRepository) -> None:
    alpha_trades = (
        ("market-1", "YES", 0.40, 120.0, datetime(2026, 3, 16, 13, 0, tzinfo=UTC)),
        ("market-2", "YES", 0.55, 100.0, datetime(2026, 3, 23, 13, 0, tzinfo=UTC)),
        ("market-6", "YES", 0.35, 160.0, datetime(2026, 3, 30, 13, 0, tzinfo=UTC)),
        ("market-7", "NO", 0.70, 40.0, datetime(2026, 3, 30, 14, 0, tzinfo=UTC)),
    )
    for index, (
        market_id,
        outcome,
        price,
        notional,
        trade_time,
    ) in enumerate(alpha_trades, start=1):
        repository.upsert_trade(
            trade_id=f"alpha-trade-{index}",
            wallet_address=ALPHA_WALLET,
            market_id=market_id,
            outcome=outcome,
            side="buy",
            price=price,
            size=100.0,
            notional=notional,
            trade_time=trade_time,
        )

    repository.upsert_closed_position(
        wallet_address=ALPHA_WALLET,
        market_id="market-1",
        outcome="YES",
        quantity=100.0,
        realized_pnl=20.0,
        roi=0.20,
        closed_at=datetime(2026, 3, 17, 10, 0, tzinfo=UTC),
    )
    repository.upsert_closed_position(
        wallet_address=ALPHA_WALLET,
        market_id="market-2",
        outcome="YES",
        quantity=100.0,
        realized_pnl=10.0,
        roi=0.10,
        closed_at=datetime(2026, 3, 24, 10, 0, tzinfo=UTC),
    )
    repository.upsert_closed_position(
        wallet_address=ALPHA_WALLET,
        market_id="market-6",
        outcome="YES",
        quantity=100.0,
        realized_pnl=30.0,
        roi=0.30,
        closed_at=datetime(2026, 3, 31, 10, 0, tzinfo=UTC),
    )
    repository.upsert_closed_position(
        wallet_address=ALPHA_WALLET,
        market_id="market-7",
        outcome="NO",
        quantity=100.0,
        realized_pnl=-10.0,
        roi=-0.10,
        closed_at=datetime(2026, 3, 31, 11, 0, tzinfo=UTC),
    )


def seed_beta_wallet(repository: DatabaseRepository) -> None:
    beta_trades = (
        ("market-3", "NO", 0.65, 140.0, datetime(2026, 3, 16, 14, 0, tzinfo=UTC)),
        ("market-4", "NO", 0.30, 110.0, datetime(2026, 3, 23, 14, 0, tzinfo=UTC)),
        ("market-8", "YES", 0.60, 90.0, datetime(2026, 3, 30, 14, 0, tzinfo=UTC)),
    )
    for index, (
        market_id,
        outcome,
        price,
        notional,
        trade_time,
    ) in enumerate(beta_trades, start=1):
        repository.upsert_trade(
            trade_id=f"beta-trade-{index}",
            wallet_address=BETA_WALLET,
            market_id=market_id,
            outcome=outcome,
            side="buy",
            price=price,
            size=100.0,
            notional=notional,
            trade_time=trade_time,
        )

    repository.upsert_closed_position(
        wallet_address=BETA_WALLET,
        market_id="market-3",
        outcome="NO",
        quantity=100.0,
        realized_pnl=-20.0,
        roi=-0.20,
        closed_at=datetime(2026, 3, 18, 12, 0, tzinfo=UTC),
    )
    repository.upsert_closed_position(
        wallet_address=BETA_WALLET,
        market_id="market-4",
        outcome="NO",
        quantity=100.0,
        realized_pnl=5.0,
        roi=0.05,
        closed_at=datetime(2026, 3, 25, 13, 0, tzinfo=UTC),
    )
    repository.upsert_closed_position(
        wallet_address=BETA_WALLET,
        market_id="market-8",
        outcome="YES",
        quantity=100.0,
        realized_pnl=-5.0,
        roi=-0.05,
        closed_at=datetime(2026, 4, 1, 13, 0, tzinfo=UTC),
    )


def seed_delta_wallet(repository: DatabaseRepository) -> None:
    repository.upsert_closed_position(
        wallet_address=DELTA_WALLET,
        market_id="market-5",
        outcome="YES",
        quantity=50.0,
        realized_pnl=5.0,
        roi=None,
        closed_at=datetime(2026, 3, 19, 8, 0, tzinfo=UTC),
    )
