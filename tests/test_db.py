"""Database initialization and ORM smoke tests."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import inspect, select
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from polymarket_anomaly_tracker.db.enums import (
    AlertSeverity,
    AlertType,
    IngestionRunStatus,
    IngestionRunType,
    TradeSource,
    WalletFlagStatus,
    WatchStatus,
)
from polymarket_anomaly_tracker.db.init_db import init_database
from polymarket_anomaly_tracker.db.models import (
    Alert,
    ClosedPosition,
    Event,
    IngestionRun,
    Market,
    PositionSnapshot,
    Trade,
    Wallet,
    WalletFeatureSnapshot,
    WatchlistEntry,
)
from polymarket_anomaly_tracker.db.session import (
    create_db_engine,
    create_session_factory,
    session_scope,
)
from polymarket_anomaly_tracker.main import app

runner = CliRunner()

EXPECTED_TABLES = {
    "alerts",
    "closed_positions",
    "events",
    "ingestion_runs",
    "markets",
    "positions_snapshots",
    "trades",
    "wallet_feature_snapshots",
    "wallets",
    "watchlist",
}

EXPECTED_INDEXES = {
    "wallets": {"ix_wallets_is_flagged", "ix_wallets_flag_status"},
    "markets": {"ix_markets_event_id", "ix_markets_category", "ix_markets_status"},
    "trades": {
        "ix_trades_wallet_address_trade_time",
        "ix_trades_market_id_trade_time",
        "ix_trades_event_id",
    },
    "positions_snapshots": {
        "ix_positions_snapshots_wallet_address_snapshot_time",
        "ix_positions_snapshots_wallet_market_outcome_snapshot_time",
    },
    "closed_positions": {
        "ix_closed_positions_wallet_address_closed_at",
        "ix_closed_positions_wallet_address_market_id",
    },
    "wallet_feature_snapshots": {
        "ix_wallet_feature_snapshots_wallet_address_as_of_time",
        "ix_wallet_feature_snapshots_composite_score",
    },
    "watchlist": {"ix_watchlist_watch_status", "ix_watchlist_priority"},
    "alerts": {
        "ix_alerts_wallet_address_detected_at",
        "ix_alerts_alert_type",
        "ix_alerts_severity",
    },
}

EXPECTED_UNIQUE_CONSTRAINTS = {
    "wallets": {"uq_wallets_wallet_address"},
    "events": {"uq_events_event_id"},
    "markets": {"uq_markets_market_id"},
    "trades": {"uq_trades_trade_id"},
    "watchlist": {"uq_watchlist_wallet_address"},
}


def build_sqlite_url(database_path: Path) -> str:
    """Create a SQLite URL for a temporary database path."""

    return f"sqlite:///{database_path}"


def test_init_database_creates_all_tables_and_indexes(tmp_path: Path) -> None:
    database_url = build_sqlite_url(tmp_path / "tracker.db")

    init_database(database_url)

    engine = create_db_engine(database_url)
    try:
        inspector = inspect(engine)
        actual_tables = set(inspector.get_table_names())
        assert EXPECTED_TABLES <= actual_tables
        assert actual_tables - EXPECTED_TABLES == {"alembic_version"}

        for table_name, expected_indexes in EXPECTED_INDEXES.items():
            actual_indexes = {index["name"] for index in inspector.get_indexes(table_name)}
            assert expected_indexes <= actual_indexes

        for table_name, expected_constraints in EXPECTED_UNIQUE_CONSTRAINTS.items():
            actual_constraints = {
                constraint["name"] for constraint in inspector.get_unique_constraints(table_name)
            }
            assert expected_constraints <= actual_constraints
    finally:
        engine.dispose()


def test_init_database_is_idempotent(tmp_path: Path) -> None:
    database_url = build_sqlite_url(tmp_path / "tracker.db")

    first_pass = init_database(database_url)
    second_pass = init_database(database_url)

    assert first_pass == second_pass
    assert set(second_pass) == EXPECTED_TABLES


def test_basic_crud_across_key_tables(tmp_path: Path) -> None:
    database_url = build_sqlite_url(tmp_path / "tracker.db")
    init_database(database_url)
    session_factory = create_session_factory(database_url)
    now = datetime.now(UTC)

    with session_scope(session_factory) as session:
        wallet = Wallet(
            wallet_address="0xabc",
            first_seen_at=now,
            last_seen_at=now,
            flag_status=WalletFlagStatus.CANDIDATE.value,
        )
        event = Event(
            event_id="event-1",
            title="Election night",
            status="open",
        )
        market = Market(
            market_id="market-1",
            event_id="event-1",
            question="Will candidate X win?",
            category="politics",
            status="open",
        )
        trade = Trade(
            trade_id="trade-1",
            wallet_address="0xabc",
            market_id="market-1",
            event_id="event-1",
            outcome="YES",
            side="buy",
            price=0.61,
            size=125.0,
            notional=76.25,
            trade_time=now,
            source=TradeSource.REST.value,
        )
        position_snapshot = PositionSnapshot(
            wallet_address="0xabc",
            snapshot_time=now,
            market_id="market-1",
            event_id="event-1",
            outcome="YES",
            quantity=125.0,
            avg_entry_price=0.61,
            current_value=84.0,
            unrealized_pnl=7.75,
        )
        closed_position = ClosedPosition(
            wallet_address="0xabc",
            market_id="market-1",
            event_id="event-1",
            outcome="YES",
            quantity=125.0,
            realized_pnl=12.5,
            roi=0.15,
            opened_at=now,
            closed_at=now,
        )
        feature_snapshot = WalletFeatureSnapshot(
            wallet_address="0xabc",
            as_of_time=now,
            resolved_markets_count=12,
            trades_count=25,
            composite_score=88.5,
            confidence_score=0.82,
        )
        watchlist_entry = WatchlistEntry(
            wallet_address="0xabc",
            watch_status=WatchStatus.ACTIVE.value,
            added_reason="High anomaly score",
            added_at=now,
            priority=10,
        )
        alert = Alert(
            wallet_address="0xabc",
            alert_type=AlertType.POSITION_CHANGED.value,
            severity=AlertSeverity.WARNING.value,
            market_id="market-1",
            event_id="event-1",
            summary="Wallet materially increased exposure.",
            detected_at=now,
        )
        ingestion_run = IngestionRun(
            run_type=IngestionRunType.LEADERBOARD.value,
            started_at=now,
            finished_at=now,
            status=IngestionRunStatus.SUCCEEDED.value,
            records_written=3,
        )

        session.add_all(
            [
                wallet,
                event,
                market,
                trade,
                position_snapshot,
                closed_position,
                feature_snapshot,
                watchlist_entry,
                alert,
                ingestion_run,
            ]
        )

    read_session = Session(bind=create_db_engine(database_url), expire_on_commit=False)
    try:
        assert read_session.scalar(select(Wallet.wallet_address)) == "0xabc"
        assert read_session.scalar(select(Market.market_id)) == "market-1"
        assert read_session.scalar(select(Trade.trade_id)) == "trade-1"
        assert read_session.scalar(select(PositionSnapshot.outcome)) == "YES"
        assert read_session.scalar(select(ClosedPosition.roi)) == 0.15
        assert read_session.scalar(select(WalletFeatureSnapshot.composite_score)) == 88.5
        assert read_session.scalar(select(WatchlistEntry.priority)) == 10
        assert read_session.scalar(select(Alert.alert_type)) == AlertType.POSITION_CHANGED.value
        assert (
            read_session.scalar(select(IngestionRun.status))
            == IngestionRunStatus.SUCCEEDED.value
        )
    finally:
        read_session.close()


def test_init_db_cli_creates_fresh_database(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = build_sqlite_url(tmp_path / "cli.db")
    monkeypatch.setenv("PMAT_DATABASE_URL", database_url)
    monkeypatch.delenv("PMAT_SETTINGS_FILE", raising=False)

    result = runner.invoke(app, ["init-db"])

    assert result.exit_code == 0
    assert "Initialized database at" in result.stdout
    assert (tmp_path / "cli.db").exists()
