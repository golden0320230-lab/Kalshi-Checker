"""Tests for report rendering and export commands."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from typer.testing import CliRunner

from polymarket_anomaly_tracker.config import clear_settings_cache
from polymarket_anomaly_tracker.db.enums import AlertSeverity, AlertType, WatchStatus
from polymarket_anomaly_tracker.db.init_db import init_database
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope
from polymarket_anomaly_tracker.main import app

runner = CliRunner()

TOP_WALLET = "0x-report-top"
SECOND_WALLET = "0x-report-second"
SPARSE_WALLET = "0x-report-sparse"


def test_report_top_wallets_command_renders_ranked_wallets_and_reasons(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'report-top-wallets.db'}"
    init_database(database_url)
    seed_reporting_test_data(database_url)

    monkeypatch.setenv("PMAT_DATABASE_URL", database_url)
    clear_settings_cache()
    try:
        result = runner.invoke(
            app,
            ["report", "top-wallets", "--limit", "2"],
            terminal_width=160,
        )
    finally:
        clear_settings_cache()

    assert result.exit_code == 0
    assert "Top Wallets as of" in result.stdout
    normalized_output = result.stdout.replace("\n", " ")
    assert TOP_WALLET in normalized_output or "Trader Alpha" in normalized_output
    assert SECOND_WALLET in normalized_output or "Trader Beta" in normalized_output
    assert "Prices moved favorably after entry across forward windows" in normalized_output


def test_report_wallet_command_handles_sparse_wallet_without_crashing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'report-wallet-sparse.db'}"
    init_database(database_url)
    seed_reporting_test_data(database_url)

    monkeypatch.setenv("PMAT_DATABASE_URL", database_url)
    clear_settings_cache()
    try:
        result = runner.invoke(
            app,
            ["report", "wallet", SPARSE_WALLET],
            terminal_width=160,
        )
    finally:
        clear_settings_cache()

    assert result.exit_code == 0
    normalized_output = result.stdout.replace("\n", " ")
    assert SPARSE_WALLET in normalized_output
    assert "No scoring snapshot available" in normalized_output
    assert "No open positions" in normalized_output
    assert "No trades" in normalized_output
    assert "No closed positions" in normalized_output
    assert "No alerts" in normalized_output


def test_report_export_creates_csv_and_json_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'report-export.db'}"
    csv_output_path = tmp_path / "top_wallets.csv"
    json_output_path = tmp_path / "wallet_detail.json"
    init_database(database_url)
    seed_reporting_test_data(database_url)

    monkeypatch.setenv("PMAT_DATABASE_URL", database_url)
    clear_settings_cache()
    try:
        csv_result = runner.invoke(
            app,
            [
                "report",
                "export",
                "--report",
                "top-wallets",
                "--format",
                "csv",
                "--output",
                str(csv_output_path),
                "--limit",
                "2",
            ],
            terminal_width=160,
        )
        json_result = runner.invoke(
            app,
            [
                "report",
                "export",
                "--report",
                "wallet",
                "--format",
                "json",
                "--output",
                str(json_output_path),
                "--wallet-address",
                TOP_WALLET,
            ],
            terminal_width=160,
        )
    finally:
        clear_settings_cache()

    assert csv_result.exit_code == 0
    assert json_result.exit_code == 0
    assert csv_output_path.exists()
    assert json_output_path.exists()

    csv_contents = csv_output_path.read_text(encoding="utf-8")
    json_payload = json.loads(json_output_path.read_text(encoding="utf-8"))

    assert "wallet_address,display_name,flag_status" in csv_contents
    assert TOP_WALLET in csv_contents
    assert json_payload["wallet_address"] == TOP_WALLET
    assert json_payload["score_summary"]["recent_trades_count_90d"] == 7
    assert (
        json_payload["score_summary"]["top_reasons"][0]
        == "Prices moved favorably after entry across forward windows"
    )
    assert json_payload["recent_trades"][0]["market_question"] == "Fed Market"
    assert json_payload["recent_alerts"][0]["summary"] == "Opened YES position in Election Market"


def seed_reporting_test_data(database_url: str) -> None:
    """Seed deterministic scores, trades, positions, and alerts for reporting tests."""

    session_factory = create_session_factory(database_url)
    observed_at = datetime(2026, 4, 15, 10, 0, tzinfo=UTC)
    as_of_time = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    trade_time = datetime(2026, 4, 14, 18, 0, tzinfo=UTC)
    closed_at = datetime(2026, 4, 10, 12, 0, tzinfo=UTC)
    alert_time = datetime(2026, 4, 15, 12, 30, tzinfo=UTC)
    position_time = datetime(2026, 4, 15, 12, 15, tzinfo=UTC)

    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        for wallet_address, display_name, flag_status, is_flagged in (
            (TOP_WALLET, "Trader Alpha", "flagged", True),
            (SECOND_WALLET, "Trader Beta", "candidate", False),
            (SPARSE_WALLET, "Trader Sparse", "unflagged", False),
        ):
            repository.upsert_wallet(
                wallet_address=wallet_address,
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                display_name=display_name,
                profile_slug=display_name.lower().replace(" ", "-"),
                flag_status=flag_status,
                is_flagged=is_flagged,
            )

        repository.upsert_event(
            event_id="event-election",
            title="Election Event",
            status="active",
            category="politics",
            slug="election-event",
        )
        repository.upsert_event(
            event_id="event-fed",
            title="Fed Event",
            status="closed",
            category="macro",
            slug="fed-event",
        )
        repository.upsert_market(
            market_id="market-election",
            event_id="event-election",
            question="Election Market",
            status="active",
            category="politics",
            slug="election-market",
        )
        repository.upsert_market(
            market_id="market-fed",
            event_id="event-fed",
            question="Fed Market",
            status="closed",
            category="macro",
            slug="fed-market",
        )

        repository.upsert_wallet_feature_snapshot(
            wallet_address=TOP_WALLET,
            as_of_time=as_of_time,
            resolved_markets_count=12,
            trades_count=40,
            win_rate=0.82,
            avg_roi=0.33,
            median_roi=0.28,
            realized_pnl_total=850.0,
            value_at_entry_score=0.78,
            specialization_score=0.63,
            conviction_score=0.71,
            consistency_score=0.69,
            timing_drift_score=0.75,
            timing_positive_capture_score=0.61,
            composite_score=0.86,
            confidence_score=0.74,
            adjusted_score=0.91,
            explanations_json=json.dumps(
                build_explanation_payload(
                    top_reasons=[
                        "Prices moved favorably after entry across forward windows",
                        "Resolved-market win rate ranked near the top of this run",
                    ],
                    recent_trades_count_90d=7,
                ),
                sort_keys=True,
            ),
        )
        repository.upsert_wallet_feature_snapshot(
            wallet_address=SECOND_WALLET,
            as_of_time=as_of_time,
            resolved_markets_count=8,
            trades_count=22,
            win_rate=0.70,
            avg_roi=0.22,
            median_roi=0.18,
            realized_pnl_total=420.0,
            value_at_entry_score=0.54,
            specialization_score=0.45,
            conviction_score=0.49,
            consistency_score=0.52,
            timing_drift_score=0.55,
            timing_positive_capture_score=0.38,
            composite_score=0.72,
            confidence_score=0.65,
            adjusted_score=0.76,
            explanations_json=json.dumps(
                build_explanation_payload(
                    top_reasons=["Average ROI ranked near the top of this run"],
                    recent_trades_count_90d=5,
                ),
                sort_keys=True,
            ),
        )

        repository.upsert_watchlist_entry(
            wallet_address=TOP_WALLET,
            added_reason="Flagged from the latest scoring run.",
            added_at=observed_at,
            watch_status=WatchStatus.ACTIVE.value,
            last_checked_at=alert_time,
            priority=3,
            notes="Priority watch wallet.",
        )
        repository.upsert_trade(
            trade_id="trade-1",
            wallet_address=TOP_WALLET,
            market_id="market-election",
            event_id="event-election",
            outcome="YES",
            side="BUY",
            price=0.60,
            size=100.0,
            notional=60.0,
            trade_time=trade_time,
        )
        repository.upsert_trade(
            trade_id="trade-2",
            wallet_address=TOP_WALLET,
            market_id="market-fed",
            event_id="event-fed",
            outcome="NO",
            side="SELL",
            price=0.45,
            size=80.0,
            notional=36.0,
            trade_time=trade_time.replace(hour=20),
        )
        repository.upsert_closed_position(
            wallet_address=TOP_WALLET,
            market_id="market-fed",
            event_id="event-fed",
            outcome="NO",
            quantity=80.0,
            realized_pnl=18.5,
            roi=0.22,
            closed_at=closed_at,
        )
        repository.upsert_position_snapshot(
            wallet_address=TOP_WALLET,
            snapshot_time=position_time,
            market_id="market-election",
            event_id="event-election",
            outcome="YES",
            quantity=120.0,
            avg_entry_price=0.62,
            current_value=74.4,
            unrealized_pnl=10.2,
            realized_pnl=0.0,
            status="open",
        )
        repository.upsert_alert(
            wallet_address=TOP_WALLET,
            alert_type=AlertType.POSITION_OPENED.value,
            severity=AlertSeverity.INFO.value,
            market_id="market-election",
            event_id="event-election",
            summary="Opened YES position in Election Market",
            detected_at=alert_time,
            details_json=json.dumps({"change_kind": "opened"}, sort_keys=True),
        )


def build_explanation_payload(
    *,
    top_reasons: list[str],
    recent_trades_count_90d: int,
) -> dict[str, object]:
    """Build a deterministic explanation payload for reporting tests."""

    return {
        "top_reasons": top_reasons,
        "threshold_reason_keys": ["normalized_timing_drift_score"],
        "sample_size": {
            "resolved_markets_count": 12,
            "trades_count": 40,
            "recent_trades_count_90d": recent_trades_count_90d,
        },
        "raw_features": {
            "avg_roi": 0.33,
            "consistency_score": 0.69,
            "conviction_score": 0.71,
            "value_at_entry_score": 0.78,
            "realized_pnl_total": 850.0,
            "specialization_score": 0.63,
            "timing_drift_score": 0.75,
            "timing_positive_capture_score": 0.61,
            "win_rate": 0.82,
        },
        "normalized_features": {
            "normalized_avg_roi": 0.81,
            "normalized_consistency_score": 0.74,
            "normalized_conviction_score": 0.78,
            "normalized_value_at_entry_score": 0.72,
            "normalized_realized_pnl_percentile": 0.85,
            "normalized_specialization_score": 0.72,
            "normalized_timing_drift_score": 0.92,
            "normalized_timing_positive_capture_score": 0.80,
            "normalized_win_rate": 0.88,
        },
    }
