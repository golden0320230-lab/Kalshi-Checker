"""Tests for the offline end-to-end fixture demo flow."""

from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import func, select
from typer.testing import CliRunner

from polymarket_anomaly_tracker.config import clear_settings_cache
from polymarket_anomaly_tracker.db.models import Alert, WatchlistEntry
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope
from polymarket_anomaly_tracker.main import app

runner = CliRunner()


def test_demo_run_command_executes_full_offline_pipeline(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'demo.db'}"
    output_dir = tmp_path / "reports"

    clear_settings_cache()
    result = runner.invoke(
        app,
        [
            "demo",
            "run",
            "--database-url",
            database_url,
            "--output-dir",
            str(output_dir),
        ],
    )
    clear_settings_cache()

    assert result.exit_code == 0
    assert "Completed offline fixture demo." in result.stdout
    assert "Flagged: 1." in result.stdout
    assert "Candidates: 0." in result.stdout
    assert "Alerts: 2." in result.stdout

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        latest_as_of_time = repository.get_latest_feature_snapshot_time()
        assert latest_as_of_time is not None
        snapshot_rows = repository.list_wallet_feature_snapshot_rows(as_of_time=latest_as_of_time)
        assert len(snapshot_rows) == 20
        assert snapshot_rows[0].flag_status == "flagged"
        assert snapshot_rows[0].is_flagged is True
        top_wallet_address = snapshot_rows[0].wallet_address

        watchlist_entry = repository.get_watchlist_entry(top_wallet_address)
        assert watchlist_entry is not None
        assert watchlist_entry.watch_status == "active"
        assert watchlist_entry.last_checked_at is not None

        assert session.scalar(select(func.count()).select_from(Alert)) == 2
        assert session.scalar(select(func.count()).select_from(WatchlistEntry)) == 1

    top_wallet_json = json.loads((output_dir / "top_wallet.json").read_text(encoding="utf-8"))
    top_wallets_json = json.loads(
        (output_dir / "top_wallets.json").read_text(encoding="utf-8")
    )
    assert top_wallet_json["wallet_address"] == top_wallet_address
    assert top_wallet_json["recent_alerts"]
    assert top_wallets_json["rows"][0]["wallet_address"] == top_wallet_address
    assert (output_dir / "top_wallet.csv").exists()
    assert (output_dir / "top_wallets.csv").exists()
