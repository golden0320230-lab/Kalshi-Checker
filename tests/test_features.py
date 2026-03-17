"""Tests for wallet dataset assembly and feature computation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
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

EARLY_WALLET = "0xearly"
LATE_WALLET = "0xlate"
SPARSE_TIMING_WALLET = "0xsparse"
SPLIT_FILL_WALLET = "0xsplit"
COMPACT_FILL_WALLET = "0xcompact"
CONSTANT_BUCKET_WALLET = "0xconstant"


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
    assert len(dataset.price_snapshots) == 21
    assert set(dataset.price_snapshots["market_id"]) == {
        "market-1",
        "market-2",
        "market-3",
        "market-4",
        "market-6",
        "market-7",
        "market-8",
    }
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
    assert set(dataset.price_snapshots["market_id"]) == {
        "market-1",
        "market-2",
        "market-6",
        "market-7",
    }
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
    assert alpha_timing.value_at_entry_score == pytest.approx(193 / 420)
    assert alpha_timing.timing_drift_score is not None
    assert alpha_timing.timing_positive_capture_score is not None
    assert alpha_timing.timing_drift_score > 0.10
    assert alpha_timing.timing_positive_capture_score > 0.10

    beta_timing = timing_rows[BETA_WALLET]
    assert beta_timing.value_at_entry_score == pytest.approx(-0.2)
    assert beta_timing.timing_drift_score is not None
    assert beta_timing.timing_positive_capture_score is not None
    assert beta_timing.timing_drift_score < alpha_timing.timing_drift_score
    assert beta_timing.timing_positive_capture_score < alpha_timing.timing_positive_capture_score

    gamma_timing = timing_rows[GAMMA_WALLET]
    assert gamma_timing.value_at_entry_score is None
    assert gamma_timing.timing_drift_score is None
    assert gamma_timing.timing_positive_capture_score is None

    delta_timing = timing_rows[DELTA_WALLET]
    assert delta_timing.value_at_entry_score is None
    assert delta_timing.timing_drift_score is None
    assert delta_timing.timing_positive_capture_score is None

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


def test_true_timing_prefers_early_entry_over_late_entry_even_when_resolution_is_bad(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'features-true-timing.db'}"
    init_database(database_url)
    seed_true_timing_test_data(database_url)

    dataset = load_wallet_analysis_dataset(database_url)
    timing_rows = {
        feature.wallet_address: feature
        for feature in compute_timing_features(
            dataset,
            min_value_trades=1,
            min_matched_trades=1,
        )
    }

    early = timing_rows[EARLY_WALLET]
    late = timing_rows[LATE_WALLET]

    assert early.value_at_entry_score is not None
    assert late.value_at_entry_score is not None
    assert early.value_at_entry_score < 0
    assert late.value_at_entry_score < 0
    assert early.timing_drift_score is not None
    assert late.timing_drift_score is not None
    assert early.timing_positive_capture_score is not None
    assert late.timing_positive_capture_score is not None
    assert early.timing_drift_score > late.timing_drift_score
    assert early.timing_positive_capture_score > late.timing_positive_capture_score


def test_true_timing_returns_none_when_matched_trade_threshold_is_not_met(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'features-sparse-timing.db'}"
    init_database(database_url)
    seed_sparse_timing_test_data(database_url)

    dataset = load_wallet_analysis_dataset(database_url)
    timing_rows = {
        feature.wallet_address: feature
        for feature in compute_timing_features(
            dataset,
            min_value_trades=1,
            min_matched_trades=2,
        )
    }

    sparse = timing_rows[SPARSE_TIMING_WALLET]
    assert sparse.value_at_entry_score == pytest.approx(0.65)
    assert sparse.timing_drift_score is None
    assert sparse.timing_positive_capture_score is None


def test_conviction_uses_bucket_level_aggregation_so_split_fills_do_not_change_score(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'features-conviction-buckets.db'}"
    init_database(database_url)
    seed_conviction_bucket_test_data(database_url)

    dataset = load_wallet_analysis_dataset(database_url)
    conviction_rows = {
        feature.wallet_address: feature for feature in compute_conviction_features(dataset)
    }

    split_fill = conviction_rows[SPLIT_FILL_WALLET]
    compact_fill = conviction_rows[COMPACT_FILL_WALLET]
    assert split_fill.conviction_score is not None
    assert compact_fill.conviction_score is not None
    assert split_fill.conviction_score == pytest.approx(compact_fill.conviction_score)


def test_conviction_returns_none_when_bucket_variance_is_constant(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'features-conviction-constant.db'}"
    init_database(database_url)
    seed_conviction_bucket_test_data(database_url)

    dataset = load_wallet_analysis_dataset(database_url)
    conviction_rows = {
        feature.wallet_address: feature for feature in compute_conviction_features(dataset)
    }

    constant_bucket_wallet = conviction_rows[CONSTANT_BUCKET_WALLET]
    assert constant_bucket_wallet.conviction_score is None


def seed_feature_test_data(database_url: str) -> None:
    """Seed deterministic wallets, trades, closed positions, and price history."""

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

        for market_id, snapshot_rows in FEATURE_PRICE_SNAPSHOTS.items():
            for snapshot_time, best_bid, best_ask, last_price in snapshot_rows:
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


def seed_true_timing_test_data(database_url: str) -> None:
    """Seed one favorable early trade path and one late follow trade path."""

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        observed_at = datetime(2026, 3, 1, 11, 0, tzinfo=UTC)
        for wallet_address, display_name in (
            (EARLY_WALLET, "Early"),
            (LATE_WALLET, "Late"),
        ):
            repository.upsert_wallet(
                wallet_address=wallet_address,
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                display_name=display_name,
            )

        repository.upsert_market(
            market_id="market-timing",
            question="Timing test market",
            status="closed",
            category="politics",
        )

        early_trade_time = datetime(2026, 3, 1, 12, 0, tzinfo=UTC)
        late_trade_time = datetime(2026, 3, 1, 18, 0, tzinfo=UTC)
        for index, (snapshot_time, mid_price) in enumerate(
            (
                (early_trade_time + timedelta(hours=1), 0.52),
                (early_trade_time + timedelta(hours=6), 0.67),
                (early_trade_time + timedelta(hours=7), 0.65),
                (early_trade_time + timedelta(hours=12, minutes=30), 0.60),
                (early_trade_time + timedelta(hours=24, minutes=30), 0.72),
                (early_trade_time + timedelta(hours=31), 0.55),
            ),
            start=1,
        ):
            repository.upsert_market_price_snapshot(
                market_id="market-timing",
                snapshot_time=snapshot_time,
                source=f"fixture-{index}",
                best_bid=mid_price - 0.02,
                best_ask=mid_price + 0.02,
                mid_price=mid_price,
                last_price=mid_price,
                volume=500.0,
                liquidity=250.0,
            )

        repository.upsert_trade(
            trade_id="early-trade",
            wallet_address=EARLY_WALLET,
            market_id="market-timing",
            outcome="YES",
            side="buy",
            price=0.40,
            size=100.0,
            notional=40.0,
            trade_time=early_trade_time,
        )
        repository.upsert_trade(
            trade_id="late-trade",
            wallet_address=LATE_WALLET,
            market_id="market-timing",
            outcome="YES",
            side="buy",
            price=0.68,
            size=100.0,
            notional=68.0,
            trade_time=late_trade_time,
        )
        repository.upsert_closed_position(
            wallet_address=EARLY_WALLET,
            market_id="market-timing",
            outcome="YES",
            quantity=100.0,
            realized_pnl=-10.0,
            roi=-0.10,
            closed_at=datetime(2026, 3, 3, 12, 0, tzinfo=UTC),
        )
        repository.upsert_closed_position(
            wallet_address=LATE_WALLET,
            market_id="market-timing",
            outcome="YES",
            quantity=100.0,
            realized_pnl=-8.0,
            roi=-0.08,
            closed_at=datetime(2026, 3, 3, 12, 0, tzinfo=UTC),
        )


def seed_sparse_timing_test_data(database_url: str) -> None:
    """Seed a wallet with only one matched forward trade."""

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        observed_at = datetime(2026, 3, 5, 9, 0, tzinfo=UTC)
        repository.upsert_wallet(
            wallet_address=SPARSE_TIMING_WALLET,
            first_seen_at=observed_at,
            last_seen_at=observed_at,
            display_name="Sparse",
        )
        repository.upsert_market(
            market_id="market-sparse",
            question="Sparse timing market",
            status="closed",
            category="crypto",
        )
        trade_time = datetime(2026, 3, 5, 10, 0, tzinfo=UTC)
        repository.upsert_trade(
            trade_id="sparse-trade",
            wallet_address=SPARSE_TIMING_WALLET,
            market_id="market-sparse",
            outcome="YES",
            side="buy",
            price=0.35,
            size=100.0,
            notional=35.0,
            trade_time=trade_time,
        )
        repository.upsert_closed_position(
            wallet_address=SPARSE_TIMING_WALLET,
            market_id="market-sparse",
            outcome="YES",
            quantity=100.0,
            realized_pnl=15.0,
            roi=0.15,
            closed_at=datetime(2026, 3, 7, 10, 0, tzinfo=UTC),
        )
        repository.upsert_market_price_snapshot(
            market_id="market-sparse",
            snapshot_time=trade_time + timedelta(hours=1),
            source="fixture",
            best_bid=0.43,
            best_ask=0.47,
            mid_price=0.45,
            last_price=0.45,
            volume=250.0,
            liquidity=125.0,
        )


def seed_conviction_bucket_test_data(database_url: str) -> None:
    """Seed wallets for bucket-level conviction regression tests."""

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        observed_at = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
        for wallet_address, display_name in (
            (SPLIT_FILL_WALLET, "Split"),
            (COMPACT_FILL_WALLET, "Compact"),
            (CONSTANT_BUCKET_WALLET, "Constant"),
        ):
            repository.upsert_wallet(
                wallet_address=wallet_address,
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                display_name=display_name,
            )

        for market_id in ("bucket-1", "bucket-2", "bucket-3"):
            repository.upsert_market(
                market_id=market_id,
                question=f"Bucket test {market_id}",
                status="closed",
                category="macro",
            )

        seed_compact_conviction_wallet(repository)
        seed_split_fill_conviction_wallet(repository)
        seed_constant_bucket_conviction_wallet(repository)


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


def seed_compact_conviction_wallet(repository: DatabaseRepository) -> None:
    """Seed one trade per bucket for the compact conviction wallet."""

    seed_conviction_bucket_closed_positions(repository, COMPACT_FILL_WALLET)
    for index, (market_id, outcome, notional, price) in enumerate(
        (
            ("bucket-1", "YES", 90.0, 0.45),
            ("bucket-2", "YES", 45.0, 0.58),
            ("bucket-3", "NO", 60.0, 0.32),
        ),
        start=1,
    ):
        repository.upsert_trade(
            trade_id=f"compact-trade-{index}",
            wallet_address=COMPACT_FILL_WALLET,
            market_id=market_id,
            outcome=outcome,
            side="buy",
            price=price,
            size=100.0,
            notional=notional,
            trade_time=datetime(2026, 3, 9, 12 + index, 0, tzinfo=UTC),
        )


def seed_split_fill_conviction_wallet(repository: DatabaseRepository) -> None:
    """Seed the same conviction buckets with one bucket split into many fills."""

    seed_conviction_bucket_closed_positions(repository, SPLIT_FILL_WALLET)
    split_notionals = (10.0, 8.0, 12.0, 15.0, 9.0, 14.0, 11.0, 11.0)
    for index, notional in enumerate(split_notionals, start=1):
        repository.upsert_trade(
            trade_id=f"split-trade-{index}",
            wallet_address=SPLIT_FILL_WALLET,
            market_id="bucket-1",
            outcome="YES",
            side="buy",
            price=0.45,
            size=25.0,
            notional=notional,
            trade_time=datetime(2026, 3, 9, 12, index, tzinfo=UTC),
        )
    for index, (market_id, outcome, notional, price) in enumerate(
        (
            ("bucket-2", "YES", 45.0, 0.58),
            ("bucket-3", "NO", 60.0, 0.32),
        ),
        start=1,
    ):
        repository.upsert_trade(
            trade_id=f"split-tail-trade-{index}",
            wallet_address=SPLIT_FILL_WALLET,
            market_id=market_id,
            outcome=outcome,
            side="buy",
            price=price,
            size=100.0,
            notional=notional,
            trade_time=datetime(2026, 3, 9, 14 + index, 0, tzinfo=UTC),
        )


def seed_constant_bucket_conviction_wallet(repository: DatabaseRepository) -> None:
    """Seed a wallet with constant bucket notionals so conviction is undefined."""

    for market_id, realized_pnl in (
        ("bucket-1", 15.0),
        ("bucket-2", -5.0),
        ("bucket-3", 20.0),
    ):
        repository.upsert_closed_position(
            wallet_address=CONSTANT_BUCKET_WALLET,
            market_id=market_id,
            outcome="YES",
            quantity=100.0,
            realized_pnl=realized_pnl,
            roi=realized_pnl / 100.0,
            closed_at=datetime(2026, 3, 12, 10, 0, tzinfo=UTC),
        )
        repository.upsert_trade(
            trade_id=f"constant-{market_id}",
            wallet_address=CONSTANT_BUCKET_WALLET,
            market_id=market_id,
            outcome="YES",
            side="buy",
            price=0.50,
            size=100.0,
            notional=50.0,
            trade_time=datetime(2026, 3, 10, 10, 0, tzinfo=UTC),
        )


def seed_conviction_bucket_closed_positions(
    repository: DatabaseRepository,
    wallet_address: str,
) -> None:
    """Seed identical resolved buckets used for the split-fill regression test."""

    for market_id, outcome, realized_pnl, roi in (
        ("bucket-1", "YES", 30.0, 0.30),
        ("bucket-2", "YES", -10.0, -0.10),
        ("bucket-3", "NO", 5.0, 0.05),
    ):
        repository.upsert_closed_position(
            wallet_address=wallet_address,
            market_id=market_id,
            outcome=outcome,
            quantity=100.0,
            realized_pnl=realized_pnl,
            roi=roi,
            closed_at=datetime(2026, 3, 12, 9, 0, tzinfo=UTC),
        )


def seed_beta_wallet(repository: DatabaseRepository) -> None:
    """Seed trades and closed positions for the beta wallet."""

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
        quantity=50.0,
        realized_pnl=5.0,
        roi=None,
        closed_at=datetime(2026, 3, 19, 8, 0, tzinfo=UTC),
    )


FEATURE_PRICE_SNAPSHOTS = {
    "market-1": (
        (datetime(2026, 3, 16, 14, 0, tzinfo=UTC), 0.50, 0.54, 0.53),
        (datetime(2026, 3, 16, 19, 5, tzinfo=UTC), 0.61, 0.65, 0.64),
        (datetime(2026, 3, 17, 13, 30, tzinfo=UTC), 0.70, 0.74, 0.73),
    ),
    "market-2": (
        (datetime(2026, 3, 23, 14, 5, tzinfo=UTC), 0.60, 0.64, 0.63),
        (datetime(2026, 3, 23, 19, 10, tzinfo=UTC), 0.66, 0.70, 0.69),
        (datetime(2026, 3, 24, 13, 15, tzinfo=UTC), 0.71, 0.75, 0.74),
    ),
    "market-3": (
        (datetime(2026, 3, 16, 15, 5, tzinfo=UTC), 0.48, 0.52, 0.51),
        (datetime(2026, 3, 16, 20, 10, tzinfo=UTC), 0.56, 0.60, 0.59),
        (datetime(2026, 3, 17, 14, 20, tzinfo=UTC), 0.60, 0.64, 0.63),
    ),
    "market-4": (
        (datetime(2026, 3, 23, 15, 5, tzinfo=UTC), 0.66, 0.70, 0.69),
        (datetime(2026, 3, 23, 20, 10, tzinfo=UTC), 0.61, 0.65, 0.64),
        (datetime(2026, 3, 24, 14, 15, tzinfo=UTC), 0.58, 0.62, 0.61),
    ),
    "market-6": (
        (datetime(2026, 3, 30, 14, 5, tzinfo=UTC), 0.39, 0.43, 0.42),
        (datetime(2026, 3, 30, 19, 5, tzinfo=UTC), 0.48, 0.52, 0.51),
        (datetime(2026, 3, 31, 13, 15, tzinfo=UTC), 0.56, 0.60, 0.59),
    ),
    "market-7": (
        (datetime(2026, 3, 30, 15, 5, tzinfo=UTC), 0.16, 0.20, 0.19),
        (datetime(2026, 3, 30, 20, 5, tzinfo=UTC), 0.12, 0.16, 0.15),
        (datetime(2026, 3, 31, 14, 15, tzinfo=UTC), 0.08, 0.12, 0.10),
    ),
    "market-8": (
        (datetime(2026, 3, 30, 15, 5, tzinfo=UTC), 0.53, 0.57, 0.56),
        (datetime(2026, 3, 30, 20, 5, tzinfo=UTC), 0.46, 0.50, 0.49),
        (datetime(2026, 3, 31, 14, 15, tzinfo=UTC), 0.33, 0.37, 0.36),
    ),
}
