"""Tests for leaderboard seeding workflows."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from sqlalchemy import func, select
from typer.testing import CliRunner

from polymarket_anomaly_tracker.clients.polymarket_rest import (
    PolymarketRESTClient,
    make_client,
)
from polymarket_anomaly_tracker.config import clear_settings_cache
from polymarket_anomaly_tracker.db.enums import IngestionRunStatus
from polymarket_anomaly_tracker.db.init_db import init_database
from polymarket_anomaly_tracker.db.models import IngestionRun, Wallet
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope
from polymarket_anomaly_tracker.ingest.leaderboard import (
    LeaderboardSeedError,
    seed_leaderboard_wallets,
)
from polymarket_anomaly_tracker.main import app

FIXTURES_DIR = Path(__file__).parent / "fixtures"
WALLET_ADDRESS = "0x1111111111111111111111111111111111111111"

runner = CliRunner()


def load_fixture(name: str) -> object:
    """Load a JSON fixture into Python objects."""

    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def build_leaderboard_client(*, status_code: int = 200) -> PolymarketRESTClient:
    """Create a mock Polymarket client for leaderboard requests."""

    payload = load_fixture("leaderboard.json")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path != "/v1/leaderboard":
            return httpx.Response(404, request=request, json={"detail": "not found"})
        return httpx.Response(status_code, request=request, json=payload)

    return make_client(
        transport=httpx.MockTransport(handler),
        max_retries=0,
        sleep=lambda _: None,
    )


def sqlite_round_trip(value: datetime) -> datetime:
    """Match SQLite's naive datetime round-trip behavior in assertions."""

    return value.astimezone(UTC).replace(tzinfo=None)


def test_ingest_seed_command_inserts_wallets_and_run_stats(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'seed.db'}"
    init_database(database_url)

    def mock_make_client() -> PolymarketRESTClient:
        return build_leaderboard_client()

    monkeypatch.setenv("PMAT_DATABASE_URL", database_url)
    monkeypatch.setattr(
        "polymarket_anomaly_tracker.ingest.leaderboard.make_client",
        mock_make_client,
    )
    clear_settings_cache()
    try:
        result = runner.invoke(
            app,
            [
                "ingest",
                "seed",
                "--leaderboard-window",
                "all",
                "--top-wallets",
                "10",
            ],
        )
    finally:
        clear_settings_cache()

    assert result.exit_code == 0
    assert "Seeded leaderboard wallets." in result.stdout
    assert "New wallets: 1." in result.stdout

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        wallet = session.scalar(select(Wallet).where(Wallet.wallet_address == WALLET_ADDRESS))
        run = session.scalar(select(IngestionRun))
        wallet_count = session.scalar(select(func.count()).select_from(Wallet))
        run_count = session.scalar(select(func.count()).select_from(IngestionRun))

        assert wallet is not None
        assert wallet.display_name == "alpha"
        assert wallet_count == 1
        assert run_count == 1
        assert run is not None
        assert run.status == IngestionRunStatus.SUCCEEDED.value
        assert run.records_written == 1
        assert json.loads(run.metadata_json) == {
            "existing_wallets": 0,
            "fetched_entries": 1,
            "new_wallets": 1,
            "requested_limit": 10,
            "window": "all",
        }


def test_seed_leaderboard_wallets_is_idempotent(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'repeat.db'}"
    init_database(database_url)

    first_client = build_leaderboard_client()
    second_client = build_leaderboard_client()
    first_started_at = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
    second_started_at = datetime(2026, 3, 15, 12, 5, tzinfo=UTC)

    try:
        first_result = seed_leaderboard_wallets(
            database_url=database_url,
            window="all",
            limit=25,
            client=first_client,
            started_at=first_started_at,
        )
        second_result = seed_leaderboard_wallets(
            database_url=database_url,
            window="all",
            limit=25,
            client=second_client,
            started_at=second_started_at,
        )
    finally:
        first_client.close()
        second_client.close()

    assert first_result.new_wallets == 1
    assert first_result.existing_wallets == 0
    assert second_result.new_wallets == 0
    assert second_result.existing_wallets == 1

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        wallet = session.scalar(select(Wallet).where(Wallet.wallet_address == WALLET_ADDRESS))
        wallet_count = session.scalar(select(func.count()).select_from(Wallet))
        runs = session.scalars(select(IngestionRun).order_by(IngestionRun.started_at)).all()

        assert wallet is not None
        assert wallet_count == 1
        assert wallet.first_seen_at == sqlite_round_trip(first_started_at)
        assert wallet.last_seen_at == sqlite_round_trip(second_started_at)
        assert [run.status for run in runs] == [
            IngestionRunStatus.SUCCEEDED.value,
            IngestionRunStatus.SUCCEEDED.value,
        ]


def test_seed_leaderboard_wallets_marks_failed_runs(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'failed.db'}"
    init_database(database_url)
    client = build_leaderboard_client(status_code=429)

    try:
        with pytest.raises(LeaderboardSeedError):
            seed_leaderboard_wallets(
                database_url=database_url,
                window="week",
                limit=5,
                client=client,
                started_at=datetime(2026, 3, 15, 13, 0, tzinfo=UTC),
            )
    finally:
        client.close()

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        run = session.scalar(select(IngestionRun))
        wallet_count = session.scalar(select(func.count()).select_from(Wallet))

        assert run is not None
        assert run.status == IngestionRunStatus.FAILED.value
        assert run.records_written == 0
        assert run.error_message is not None
        assert "week" in run.metadata_json
        assert wallet_count == 0
