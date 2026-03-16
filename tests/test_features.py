"""Tests for wallet dataset assembly and core PnL features."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from polymarket_anomaly_tracker.db.init_db import init_database
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope
from polymarket_anomaly_tracker.features.dataset import (
    build_wallet_analysis_dataset,
    load_wallet_analysis_dataset,
)
from polymarket_anomaly_tracker.features.pnl import (
    compute_core_pnl_feature_frame,
    compute_core_pnl_features,
)

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
    assert len(dataset.trades) == 6
    assert len(dataset.closed_positions) == 6
    assert list(feature_frame["wallet_address"]) == [
        ALPHA_WALLET,
        BETA_WALLET,
        GAMMA_WALLET,
        DELTA_WALLET,
    ]

    alpha = feature_rows[ALPHA_WALLET]
    assert alpha.display_name == "Alpha"
    assert alpha.resolved_markets_count == 2
    assert alpha.trades_count == 4
    assert alpha.win_rate == pytest.approx(1.0)
    assert alpha.avg_roi == pytest.approx(0.1)
    assert alpha.median_roi == pytest.approx(0.15)
    assert alpha.realized_pnl_total == pytest.approx(30.0)

    beta = feature_rows[BETA_WALLET]
    assert beta.display_name == "Beta"
    assert beta.resolved_markets_count == 2
    assert beta.trades_count == 2
    assert beta.win_rate == pytest.approx(0.0)
    assert beta.avg_roi == pytest.approx(-0.15)
    assert beta.median_roi == pytest.approx(-0.15)
    assert beta.realized_pnl_total == pytest.approx(-12.0)

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
    assert set(feature_rows) == {ALPHA_WALLET, DELTA_WALLET}
    assert feature_rows[DELTA_WALLET].avg_roi is None
    assert feature_rows[DELTA_WALLET].median_roi is None


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
            "market-1",
            "market-2",
            "market-3",
            "market-4",
            "market-5",
        )
        for market_id in market_rows:
            repository.upsert_market(
                market_id=market_id,
                question=f"Question for {market_id}",
                status="closed",
            )

        seed_alpha_wallet(repository)
        seed_beta_wallet(repository)
        seed_delta_wallet(repository)


def seed_alpha_wallet(repository: DatabaseRepository) -> None:
    """Seed trades and closed positions for the alpha wallet."""

    trade_time = datetime(2026, 3, 16, 13, 0, tzinfo=UTC)
    for index, market_id in enumerate(("market-1", "market-1", "market-2", "market-2"), start=1):
        repository.upsert_trade(
            trade_id=f"alpha-trade-{index}",
            wallet_address=ALPHA_WALLET,
            market_id=market_id,
            outcome="YES",
            side="buy",
            price=0.5 + (index * 0.01),
            size=100.0,
            notional=(0.5 + (index * 0.01)) * 100.0,
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
        quantity=50.0,
        realized_pnl=-5.0,
        roi=-0.05,
        closed_at=datetime(2026, 3, 18, 10, 0, tzinfo=UTC),
    )
    repository.upsert_closed_position(
        wallet_address=ALPHA_WALLET,
        market_id="market-2",
        outcome="NO",
        quantity=50.0,
        realized_pnl=15.0,
        roi=0.15,
        closed_at=datetime(2026, 3, 18, 11, 0, tzinfo=UTC),
    )


def seed_beta_wallet(repository: DatabaseRepository) -> None:
    """Seed trades and closed positions for the beta wallet."""

    trade_time = datetime(2026, 3, 16, 14, 0, tzinfo=UTC)
    for index, market_id in enumerate(("market-3", "market-4"), start=1):
        repository.upsert_trade(
            trade_id=f"beta-trade-{index}",
            wallet_address=BETA_WALLET,
            market_id=market_id,
            outcome="NO",
            side="buy",
            price=0.45 + (index * 0.01),
            size=80.0,
            notional=(0.45 + (index * 0.01)) * 80.0,
            trade_time=trade_time,
        )

    repository.upsert_closed_position(
        wallet_address=BETA_WALLET,
        market_id="market-3",
        outcome="YES",
        quantity=80.0,
        realized_pnl=-12.0,
        roi=-0.30,
        closed_at=datetime(2026, 3, 18, 12, 0, tzinfo=UTC),
    )
    repository.upsert_closed_position(
        wallet_address=BETA_WALLET,
        market_id="market-4",
        outcome="NO",
        quantity=80.0,
        realized_pnl=0.0,
        roi=0.0,
        closed_at=datetime(2026, 3, 18, 13, 0, tzinfo=UTC),
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
