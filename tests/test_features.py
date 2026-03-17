"""Tests for wallet dataset assembly and core PnL features."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from polymarket_anomaly_tracker.db.init_db import init_database
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope
from polymarket_anomaly_tracker.features.consistency import compute_consistency_features
from polymarket_anomaly_tracker.features.conviction import compute_conviction_features
from polymarket_anomaly_tracker.features.dataset import (
    build_wallet_analysis_dataset,
    load_wallet_analysis_dataset,
)
from polymarket_anomaly_tracker.features.pnl import (
    compute_core_pnl_feature_frame,
    compute_core_pnl_features,
)
from polymarket_anomaly_tracker.features.specialization import compute_specialization_features
from polymarket_anomaly_tracker.features.timing import compute_timing_features

ALPHA_WALLET = "0xaaa"
BETA_WALLET = "0xbbb"
GAMMA_WALLET = "0xccc"
DELTA_WALLET = "0xddd"


def test_build_wallet_analysis_dataset_and_compute_core_features(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'features.db'}"
    init_database(database_url)
    seed_feature_test_data(database_url)

    dataset = load_wallet_analysis_dataset(database_url)
    feature_frame = compute_core_pnl_feature_frame(dataset)
    feature_rows = {
        feature.wallet_address: feature for feature in compute_core_pnl_features(dataset)
    }

    assert list(dataset.wallets["wallet_address"]) == [
        ALPHA_WALLET,
        BETA_WALLET,
        GAMMA_WALLET,
        DELTA_WALLET,
    ]
    assert len(dataset.trades) == 7
    assert len(dataset.closed_positions) == 8
    assert len(dataset.price_snapshots) == 3
    assert set(dataset.price_snapshots["market_id"]) == {"market-1", "market-3", "market-8"}
    assert list(feature_frame["wallet_address"]) == [
        ALPHA_WALLET,
        BETA_WALLET,
        GAMMA_WALLET,
        DELTA_WALLET,
    ]

    alpha = feature_rows[ALPHA_WALLET]
    assert alpha.display_name == "Alpha"
    assert alpha.resolved_markets_count == 4
    assert alpha.trades_count == 4
    assert alpha.win_rate == pytest.approx(0.75)
    assert alpha.avg_roi == pytest.approx(0.125)
    assert alpha.median_roi == pytest.approx(0.15)
    assert alpha.realized_pnl_total == pytest.approx(50.0)

    beta = feature_rows[BETA_WALLET]
    assert beta.display_name == "Beta"
    assert beta.resolved_markets_count == 3
    assert beta.trades_count == 3
    assert beta.win_rate == pytest.approx(1 / 3)
    assert beta.avg_roi == pytest.approx(-1 / 15)
    assert beta.median_roi == pytest.approx(-0.05)
    assert beta.realized_pnl_total == pytest.approx(-20.0)

    delta = feature_rows[DELTA_WALLET]
    assert delta.display_name == "Delta"
    assert delta.resolved_markets_count == 1
    assert delta.trades_count == 0
    assert delta.win_rate == pytest.approx(1.0)
    assert delta.avg_roi is None
    assert delta.median_roi is None
    assert delta.realized_pnl_total == pytest.approx(5.0)

    gamma = feature_rows[GAMMA_WALLET]
    assert gamma.display_name == "Gamma"
    assert gamma.resolved_markets_count == 0
    assert gamma.trades_count == 0
    assert gamma.win_rate is None
    assert gamma.avg_roi is None
    assert gamma.median_roi is None
    assert gamma.realized_pnl_total is None


def test_build_wallet_analysis_dataset_filters_wallets_and_handles_sparse_data(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'features-filtered.db'}"
    init_database(database_url)
    seed_feature_test_data(database_url)

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        dataset = build_wallet_analysis_dataset(
            session,
            wallet_addresses=[ALPHA_WALLET, DELTA_WALLET],
        )

    feature_rows = {
        feature.wallet_address: feature for feature in compute_core_pnl_features(dataset)
    }

    assert list(dataset.wallets["wallet_address"]) == [ALPHA_WALLET, DELTA_WALLET]
    assert set(dataset.trades["wallet_address"]) == {ALPHA_WALLET}
    assert set(dataset.closed_positions["wallet_address"]) == {ALPHA_WALLET, DELTA_WALLET}
    assert set(dataset.price_snapshots["market_id"]) == {"market-1"}
    assert set(feature_rows) == {ALPHA_WALLET, DELTA_WALLET}
    assert feature_rows[DELTA_WALLET].avg_roi is None
    assert feature_rows[DELTA_WALLET].median_roi is None


def test_compute_advanced_features_handles_small_samples_conservatively(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'features-advanced.db'}"
    init_database(database_url)
    seed_feature_test_data(database_url)

    dataset = load_wallet_analysis_dataset(database_url)
    timing_rows = {
        feature.wallet_address: feature for feature in compute_timing_features(dataset)
    }
    specialization_rows = {
        feature.wallet_address: feature for feature in compute_specialization_features(dataset)
    }
    conviction_rows = {
        feature.wallet_address: feature for feature in compute_conviction_features(dataset)
    }
    consistency_rows = {
        feature.wallet_address: feature for feature in compute_consistency_features(dataset)
    }

    alpha_timing = timing_rows[ALPHA_WALLET]
    assert alpha_timing.early_entry_edge == pytest.approx(193 / 420)
    assert alpha_timing.timing_score == pytest.approx(221 / 420)

    beta_timing = timing_rows[BETA_WALLET]
    assert beta_timing.early_entry_edge == pytest.approx(-0.2)
    assert beta_timing.timing_score == pytest.approx(77 / 340)

    alpha_specialization = specialization_rows[ALPHA_WALLET]
    assert alpha_specialization.specialization_category == "politics"
    assert alpha_specialization.specialization_score == pytest.approx(0.25)
    assert specialization_rows[BETA_WALLET].specialization_score is None
    assert specialization_rows[DELTA_WALLET].specialization_score is None

    assert conviction_rows[ALPHA_WALLET].conviction_score is not None
    assert conviction_rows[ALPHA_WALLET].conviction_score > 0.95
    assert conviction_rows[BETA_WALLET].conviction_score is not None
    assert conviction_rows[BETA_WALLET].conviction_score < -0.6
    assert conviction_rows[DELTA_WALLET].conviction_score is None

    assert consistency_rows[ALPHA_WALLET].consistency_score == pytest.approx(1.0)
    assert consistency_rows[BETA_WALLET].consistency_score == pytest.approx(1 / 3)
    assert consistency_rows[GAMMA_WALLET].consistency_score is None
    assert consistency_rows[DELTA_WALLET].consistency_score is None


def seed_feature_test_data(database_url: str) -> None:
    """Seed deterministic wallets, trades, and closed positions for feature tests."""

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
        for market_id, snapshot_time, best_bid, best_ask, last_price in (
            ("market-1", observed_at, 0.41, 0.45, 0.44),
            ("market-3", observed_at, 0.52, 0.56, 0.54),
            ("market-8", observed_at, 0.73, 0.77, 0.75),
        ):
            repository.upsert_market_price_snapshot(
                market_id=market_id,
                snapshot_time=snapshot_time,
                source="fixture",
                best_bid=best_bid,
                best_ask=best_ask,
                mid_price=(best_bid + best_ask) / 2.0,
                last_price=last_price,
                volume=1000.0,
                liquidity=500.0,
            )

        seed_alpha_wallet(repository)
        seed_beta_wallet(repository)
        seed_delta_wallet(repository)


def seed_alpha_wallet(repository: DatabaseRepository) -> None:
    """Seed trades and closed positions for the alpha wallet."""

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
    """Seed trades and closed positions for the beta wallet."""

    beta_trades = (
        ("market-3", "NO", 0.65, 140.0, datetime(2026, 3, 16, 14, 0, tzinfo=UTC)),
        ("market-4", "NO", 0.30, 110.0, datetime(2026, 3, 23, 14, 0, tzinfo=UTC)),
        ("market-8", "YES", 0.60, 90.0, datetime(2026, 3, 30, 14, 0, tzinfo=UTC)),
    )
    for index, (market_id, outcome, price, notional, trade_time) in enumerate(beta_trades, start=1):
        repository.upsert_trade(
            trade_id=f"beta-trade-{index}",
            wallet_address=BETA_WALLET,
            market_id=market_id,
            outcome=outcome,
            side="buy",
            price=price,
            size=80.0,
            notional=notional,
            trade_time=trade_time,
        )

    repository.upsert_closed_position(
        wallet_address=BETA_WALLET,
        market_id="market-3",
        outcome="NO",
        quantity=80.0,
        realized_pnl=-20.0,
        roi=-0.20,
        closed_at=datetime(2026, 3, 18, 12, 0, tzinfo=UTC),
    )
    repository.upsert_closed_position(
        wallet_address=BETA_WALLET,
        market_id="market-4",
        outcome="NO",
        quantity=80.0,
        realized_pnl=5.0,
        roi=0.05,
        closed_at=datetime(2026, 3, 25, 13, 0, tzinfo=UTC),
    )
    repository.upsert_closed_position(
        wallet_address=BETA_WALLET,
        market_id="market-8",
        outcome="YES",
        quantity=80.0,
        realized_pnl=-5.0,
        roi=-0.05,
        closed_at=datetime(2026, 4, 1, 13, 0, tzinfo=UTC),
    )


def seed_delta_wallet(repository: DatabaseRepository) -> None:
    """Seed a sparse wallet with missing ROI data."""

    repository.upsert_closed_position(
        wallet_address=DELTA_WALLET,
        market_id="market-5",
        outcome="YES",
        quantity=20.0,
        realized_pnl=5.0,
        roi=None,
        closed_at=datetime(2026, 3, 19, 8, 0, tzinfo=UTC),
    )
