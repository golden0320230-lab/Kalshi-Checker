"""Tests for candidate/flagged classification and watchlist sync."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import func, select
from typer.testing import CliRunner

from polymarket_anomaly_tracker.config import clear_settings_cache
from polymarket_anomaly_tracker.db.enums import WalletFlagStatus, WatchStatus
from polymarket_anomaly_tracker.db.init_db import init_database
from polymarket_anomaly_tracker.db.models import WatchlistEntry
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope
from polymarket_anomaly_tracker.main import app
from polymarket_anomaly_tracker.scoring.flagger import refresh_flag_statuses

runner = CliRunner()

FLAGGED_WALLET = "0x001"
CANDIDATE_WALLET = "0x002"
BOUNDARY_WALLET = "0x003"


def test_refresh_flag_statuses_classifies_threshold_edges_and_syncs_watchlist(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'flagging.db'}"
    init_database(database_url)
    seed_flagging_test_data(database_url)

    first_result = refresh_flag_statuses(database_url)
    classifications_by_wallet = {
        classification.wallet_address: classification
        for classification in first_result.classifications
    }

    assert first_result.wallets_evaluated == 20
    assert first_result.flagged_wallets == 1
    assert first_result.candidate_wallets == 1
    assert first_result.unflagged_wallets == 18
    assert first_result.watchlist_created == 1
    assert first_result.watchlist_updated == 0
    assert first_result.watchlist_removed == 1

    assert (
        classifications_by_wallet[FLAGGED_WALLET].new_flag_status
        == WalletFlagStatus.FLAGGED.value
    )
    assert classifications_by_wallet[FLAGGED_WALLET].threshold_reason_count == 2
    assert (
        classifications_by_wallet[CANDIDATE_WALLET].new_flag_status
        == WalletFlagStatus.CANDIDATE.value
    )
    assert (
        classifications_by_wallet[BOUNDARY_WALLET].new_flag_status
        == WalletFlagStatus.UNFLAGGED.value
    )

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        flagged_wallet = repository.get_wallet(FLAGGED_WALLET)
        candidate_wallet = repository.get_wallet(CANDIDATE_WALLET)
        boundary_wallet = repository.get_wallet(BOUNDARY_WALLET)
        flagged_watchlist_entry = repository.get_watchlist_entry(FLAGGED_WALLET)
        boundary_watchlist_entry = repository.get_watchlist_entry(BOUNDARY_WALLET)

        assert flagged_wallet is not None
        assert flagged_wallet.is_flagged is True
        assert flagged_wallet.flag_status == WalletFlagStatus.FLAGGED.value
        assert candidate_wallet is not None
        assert candidate_wallet.is_flagged is False
        assert candidate_wallet.flag_status == WalletFlagStatus.CANDIDATE.value
        assert boundary_wallet is not None
        assert boundary_wallet.is_flagged is False
        assert boundary_wallet.flag_status == WalletFlagStatus.UNFLAGGED.value

        assert flagged_watchlist_entry is not None
        assert flagged_watchlist_entry.watch_status == WatchStatus.ACTIVE.value
        assert "Flagged at adjusted score" in flagged_watchlist_entry.added_reason
        assert boundary_watchlist_entry is not None
        assert boundary_watchlist_entry.watch_status == WatchStatus.REMOVED.value

        assert session.scalar(
            select(func.count())
            .select_from(WatchlistEntry)
            .where(WatchlistEntry.wallet_address == FLAGGED_WALLET)
        ) == 1

    second_result = refresh_flag_statuses(database_url)
    assert second_result.watchlist_created == 0
    assert second_result.watchlist_updated == 1
    assert second_result.watchlist_removed == 0

    with session_scope(session_factory) as session:
        assert session.scalar(
            select(func.count())
            .select_from(WatchlistEntry)
            .where(WatchlistEntry.wallet_address == FLAGGED_WALLET)
        ) == 1


def test_flag_refresh_command_runs_and_reports_counts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'flagging-cli.db'}"
    init_database(database_url)
    seed_flagging_test_data(database_url)

    monkeypatch.setenv("PMAT_DATABASE_URL", database_url)
    clear_settings_cache()
    try:
        result = runner.invoke(app, ["flag", "refresh"])
    finally:
        clear_settings_cache()

    assert result.exit_code == 0
    assert "Refreshed wallet flags." in result.stdout
    assert "Flagged: 1." in result.stdout
    assert "Candidates: 1." in result.stdout


def seed_flagging_test_data(database_url: str) -> None:
    """Seed deterministic scored wallets for flagging tests."""

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        observed_at = datetime(2026, 4, 10, 9, 0, tzinfo=UTC)
        as_of_time = datetime(2026, 4, 10, 12, 0, tzinfo=UTC)
        wallets = [f"0x{index:03d}" for index in range(1, 21)]
        for wallet_address in wallets:
            repository.upsert_wallet(
                wallet_address=wallet_address,
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                display_name=f"Wallet {wallet_address[-3:]}",
                is_flagged=wallet_address == BOUNDARY_WALLET,
                flag_status=(
                    WalletFlagStatus.FLAGGED.value
                    if wallet_address == BOUNDARY_WALLET
                    else WalletFlagStatus.UNFLAGGED.value
                ),
            )

        score_rows = [
            (
                FLAGGED_WALLET,
                8,
                20,
                0.85,
                0.50,
                0.80,
                2,
                [
                    "Prices moved favorably after entry across forward windows",
                    "Resolved-market win rate ranked near the top of this run",
                ],
            ),
            (
                CANDIDATE_WALLET,
                5,
                10,
                0.75,
                0.60,
                0.70,
                1,
                ["Average ROI ranked near the top of this run"],
            ),
            (
                BOUNDARY_WALLET,
                5,
                10,
                0.75,
                0.60,
                0.70,
                1,
                ["Average ROI ranked near the top of this run"],
            ),
        ]
        for index in range(4, 21):
            score_rows.append(
                (
                    f"0x{index:03d}",
                    4,
                    9,
                    0.60 - (index * 0.01),
                    0.40,
                    0.60 - (index * 0.01),
                    0,
                    [],
                )
            )

        for (
            wallet_address,
            resolved_markets,
            trades,
            composite_score,
            confidence_score,
            adjusted_score,
            threshold_reason_count,
            top_reasons,
        ) in score_rows:
            repository.upsert_wallet_feature_snapshot(
                wallet_address=wallet_address,
                as_of_time=as_of_time,
                resolved_markets_count=resolved_markets,
                trades_count=trades,
                win_rate=adjusted_score,
                avg_roi=adjusted_score / 2,
                median_roi=adjusted_score / 3,
                realized_pnl_total=adjusted_score * 100,
                composite_score=composite_score,
                confidence_score=confidence_score,
                adjusted_score=adjusted_score,
                explanations_json=json.dumps(
                    build_explanation_payload(
                        adjusted_score=adjusted_score,
                        confidence_score=confidence_score,
                        resolved_markets_count=resolved_markets,
                        trades_count=trades,
                        threshold_reason_count=threshold_reason_count,
                        top_reasons=top_reasons,
                    ),
                    sort_keys=True,
                ),
            )

        repository.upsert_watchlist_entry(
            wallet_address=BOUNDARY_WALLET,
            added_reason="Previously flagged from an older run.",
            added_at=observed_at,
            watch_status=WatchStatus.ACTIVE.value,
            priority=10,
        )


def build_explanation_payload(
    *,
    adjusted_score: float,
    confidence_score: float,
    resolved_markets_count: int,
    trades_count: int,
    threshold_reason_count: int,
    top_reasons: list[str],
) -> dict[str, object]:
    threshold_reason_keys = [
        f"reason-{index}" for index in range(1, threshold_reason_count + 1)
    ]
    return {
        "top_reasons": top_reasons,
        "threshold_reason_keys": threshold_reason_keys,
        "metrics": {
            "adjusted_score": adjusted_score,
            "confidence_score": confidence_score,
        },
        "sample_size": {
            "resolved_markets_count": resolved_markets_count,
            "trades_count": trades_count,
            "recent_trades_count_90d": trades_count,
        },
    }
