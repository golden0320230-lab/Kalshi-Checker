"""Tests for leaderboard seeding and wallet enrichment workflows."""

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
from polymarket_anomaly_tracker.db.enums import IngestionRunStatus, IngestionRunType
from polymarket_anomaly_tracker.db.init_db import init_database
from polymarket_anomaly_tracker.db.models import (
    ClosedPosition,
    Event,
    IngestionRun,
    Market,
    MarketPriceSnapshot,
    PositionSnapshot,
    Trade,
    Wallet,
)
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope
from polymarket_anomaly_tracker.ingest.leaderboard import (
    LeaderboardSeedError,
    seed_leaderboard_wallets,
)
from polymarket_anomaly_tracker.ingest.market_prices import ingest_market_price_snapshots
from polymarket_anomaly_tracker.ingest.orchestrator import enrich_seeded_wallets
from polymarket_anomaly_tracker.main import app

FIXTURES_DIR = Path(__file__).parent / "fixtures"
WALLET_ADDRESS = "0x1111111111111111111111111111111111111111"
WALLET_ADDRESS_TWO = "0x2222222222222222222222222222222222222222"

runner = CliRunner()


def load_fixture(name: str) -> object:
    """Load a JSON fixture into Python objects."""

    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def load_fixture_dict(name: str) -> dict[str, object]:
    """Load a JSON fixture and assert that it is a dictionary."""

    payload = load_fixture(name)
    assert isinstance(payload, dict)
    return payload


def load_fixture_list(name: str) -> list[dict[str, object]]:
    """Load a JSON fixture and assert that it is a list of dictionaries."""

    payload = load_fixture(name)
    assert isinstance(payload, list)
    return [dict(item) for item in payload]


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


def build_enrichment_client(
    *,
    failing_wallets: set[str] | None = None,
    failing_endpoint: str = "trades",
) -> PolymarketRESTClient:
    """Create a mock client for profile, trade, position, and market enrichment."""

    wallet_failures = failing_wallets or set()
    profiles = {
        WALLET_ADDRESS: build_profile_payload(
            wallet_address=WALLET_ADDRESS,
            name="Alpha",
            pseudonym="Trader Alpha",
            x_username="alpha_x",
        ),
        WALLET_ADDRESS_TWO: build_profile_payload(
            wallet_address=WALLET_ADDRESS_TWO,
            name="Beta",
            pseudonym="Trader Beta",
            x_username="beta_x",
        ),
    }
    trades = {
        WALLET_ADDRESS: [
            build_trade_payload(
                wallet_address=WALLET_ADDRESS,
                condition_id="condition-1",
                asset="asset-1",
                title="Will candidate X win?",
                slug="candidate-x-win",
                event_slug="election-night",
                outcome="YES",
                transaction_hash="0xaaa",
                name="Alpha",
                pseudonym="Trader Alpha",
            )
        ],
        WALLET_ADDRESS_TWO: [
            build_trade_payload(
                wallet_address=WALLET_ADDRESS_TWO,
                condition_id="condition-2",
                asset="asset-2",
                title="Will candidate Y win?",
                slug="candidate-y-win",
                event_slug="debate-night",
                outcome="NO",
                transaction_hash="0xbbb",
                name="Beta",
                pseudonym="Trader Beta",
            )
        ],
    }
    current_positions = {
        WALLET_ADDRESS: [
            build_current_position_payload(
                wallet_address=WALLET_ADDRESS,
                condition_id="condition-1",
                asset="asset-1",
                title="Will candidate X win?",
                slug="candidate-x-win",
                event_slug="election-night",
                outcome="YES",
            )
        ],
        WALLET_ADDRESS_TWO: [
            build_current_position_payload(
                wallet_address=WALLET_ADDRESS_TWO,
                condition_id="condition-2",
                asset="asset-2",
                title="Will candidate Y win?",
                slug="candidate-y-win",
                event_slug="debate-night",
                outcome="NO",
            )
        ],
    }
    closed_positions = {
        WALLET_ADDRESS: [
            build_closed_position_payload(
                wallet_address=WALLET_ADDRESS,
                condition_id="condition-1",
                asset="asset-1",
                title="Will candidate X win?",
                slug="candidate-x-win",
                event_slug="election-night",
                outcome="YES",
            )
        ],
        WALLET_ADDRESS_TWO: [
            build_closed_position_payload(
                wallet_address=WALLET_ADDRESS_TWO,
                condition_id="condition-2",
                asset="asset-2",
                title="Will candidate Y win?",
                slug="candidate-y-win",
                event_slug="debate-night",
                outcome="NO",
            )
        ],
    }
    markets = {
        "condition-1": build_market_payload(
            market_id="market-1",
            condition_id="condition-1",
            question="Will candidate X win?",
            slug="candidate-x-win",
            category="Politics",
            event_id="event-1",
            event_title="Election Night",
            event_slug="election-night",
        ),
        "condition-2": build_market_payload(
            market_id="market-2",
            condition_id="condition-2",
            question="Will candidate Y win?",
            slug="candidate-y-win",
            category="Politics",
            event_id="event-2",
            event_title="Debate Night",
            event_slug="debate-night",
        ),
    }

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/public-profile":
            wallet_address = request.url.params["address"]
            if wallet_address in wallet_failures and failing_endpoint == "profile":
                return httpx.Response(500, request=request, json={"detail": "profile failed"})
            return httpx.Response(200, request=request, json=profiles[wallet_address])
        if path == "/trades":
            wallet_address = request.url.params["user"]
            if wallet_address in wallet_failures and failing_endpoint == "trades":
                return httpx.Response(500, request=request, json={"detail": "trades failed"})
            return httpx.Response(200, request=request, json=trades[wallet_address])
        if path == "/positions":
            wallet_address = request.url.params["user"]
            if wallet_address in wallet_failures and failing_endpoint == "positions":
                return httpx.Response(500, request=request, json={"detail": "positions failed"})
            return httpx.Response(200, request=request, json=current_positions[wallet_address])
        if path == "/closed-positions":
            wallet_address = request.url.params["user"]
            if wallet_address in wallet_failures and failing_endpoint == "closed-positions":
                return httpx.Response(
                    500,
                    request=request,
                    json={"detail": "closed positions failed"},
                )
            return httpx.Response(200, request=request, json=closed_positions[wallet_address])
        if path == "/markets":
            market_ids = list(request.url.params.get_list("id"))
            payload = [markets[market_id] for market_id in market_ids if market_id in markets]
            return httpx.Response(200, request=request, json=payload)
        if path.startswith("/markets/"):
            market_id = path.rsplit("/", maxsplit=1)[-1]
            payload = markets.get(market_id)
            if payload is None:
                return httpx.Response(404, request=request, json={"detail": "not found"})
            return httpx.Response(200, request=request, json=payload)
        return httpx.Response(404, request=request, json={"detail": "not found"})

    return make_client(
        transport=httpx.MockTransport(handler),
        max_retries=0,
        sleep=lambda _: None,
    )


def build_market_price_client() -> PolymarketRESTClient:
    """Create a mock client for market price polling."""

    market_payloads = load_fixture_list("market_price_snapshots.json")
    markets_by_id = {
        str(payload["conditionId"]): dict(payload)
        for payload in market_payloads
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/markets":
            market_ids = list(request.url.params.get_list("id"))
            payload = [
                markets_by_id[market_id]
                for market_id in market_ids
                if market_id in markets_by_id
            ]
            return httpx.Response(200, request=request, json=payload)
        if request.url.path.startswith("/markets/"):
            market_id = request.url.path.rsplit("/", maxsplit=1)[-1]
            payload = markets_by_id.get(market_id)
            if payload is None:
                return httpx.Response(404, request=request, json={"detail": "not found"})
            return httpx.Response(200, request=request, json=payload)
        return httpx.Response(404, request=request, json={"detail": "not found"})

    return make_client(
        transport=httpx.MockTransport(handler),
        max_retries=0,
        sleep=lambda _: None,
    )


def build_profile_payload(
    *,
    wallet_address: str,
    name: str,
    pseudonym: str,
    x_username: str,
) -> dict[str, object]:
    """Build a profile response for a specific wallet."""

    payload = dict(load_fixture_dict("profile.json"))
    payload["proxyWallet"] = wallet_address
    payload["name"] = name
    payload["pseudonym"] = pseudonym
    payload["xUsername"] = x_username
    return payload


def build_trade_payload(
    *,
    wallet_address: str,
    condition_id: str,
    asset: str,
    title: str,
    slug: str,
    event_slug: str,
    outcome: str,
    transaction_hash: str,
    name: str,
    pseudonym: str,
) -> dict[str, object]:
    """Build a trade response row for a specific wallet."""

    payload = dict(load_fixture_list("trades.json")[0])
    payload["proxyWallet"] = wallet_address
    payload["conditionId"] = condition_id
    payload["asset"] = asset
    payload["title"] = title
    payload["slug"] = slug
    payload["eventSlug"] = event_slug
    payload["outcome"] = outcome
    payload["transactionHash"] = transaction_hash
    payload["name"] = name
    payload["pseudonym"] = pseudonym
    return payload


def build_current_position_payload(
    *,
    wallet_address: str,
    condition_id: str,
    asset: str,
    title: str,
    slug: str,
    event_slug: str,
    outcome: str,
) -> dict[str, object]:
    """Build a current-position response row for a specific wallet."""

    payload = dict(load_fixture_list("current_positions.json")[0])
    payload["proxyWallet"] = wallet_address
    payload["conditionId"] = condition_id
    payload["asset"] = asset
    payload["title"] = title
    payload["slug"] = slug
    payload["eventSlug"] = event_slug
    payload["outcome"] = outcome
    return payload


def build_closed_position_payload(
    *,
    wallet_address: str,
    condition_id: str,
    asset: str,
    title: str,
    slug: str,
    event_slug: str,
    outcome: str,
) -> dict[str, object]:
    """Build a closed-position response row for a specific wallet."""

    payload = dict(load_fixture_list("closed_positions.json")[0])
    payload["proxyWallet"] = wallet_address
    payload["conditionId"] = condition_id
    payload["asset"] = asset
    payload["title"] = title
    payload["slug"] = slug
    payload["eventSlug"] = event_slug
    payload["outcome"] = outcome
    return payload


def build_market_payload(
    *,
    market_id: str,
    condition_id: str,
    question: str,
    slug: str,
    category: str,
    event_id: str,
    event_title: str,
    event_slug: str,
) -> dict[str, object]:
    """Build a market metadata response with linked event metadata."""

    payload = dict(load_fixture_dict("market.json"))
    payload["id"] = market_id
    payload["conditionId"] = condition_id
    payload["question"] = question
    payload["slug"] = slug
    payload["category"] = category
    payload["events"] = [
        {
            "id": event_id,
            "title": event_title,
            "category": category,
            "slug": event_slug,
            "startDate": "2026-10-01T00:00:00Z",
            "endDate": "2026-11-05T00:00:00Z",
            "status": "active",
        }
    ]
    return payload


def sqlite_round_trip(value: datetime) -> datetime:
    """Match SQLite's naive datetime round-trip behavior in assertions."""

    return value.astimezone(UTC).replace(tzinfo=None)


def seed_wallet_rows(database_url: str, wallet_addresses: list[str]) -> None:
    """Insert seed wallets directly for enrichment tests."""

    session_factory = create_session_factory(database_url)
    observed_at = datetime(2026, 3, 15, 9, 0, tzinfo=UTC)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        for wallet_address in wallet_addresses:
            repository.upsert_wallet(
                wallet_address=wallet_address,
                first_seen_at=observed_at,
                last_seen_at=observed_at,
            )


def seed_market_rows(database_url: str, market_ids: list[str]) -> None:
    """Insert known markets directly for market price polling tests."""

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        for market_id in market_ids:
            repository.upsert_market(
                market_id=market_id,
                question=f"Question for {market_id}",
                status="active",
            )


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


def test_ingest_market_prices_command_snapshots_known_markets_and_run_stats(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'market-prices-cli.db'}"
    init_database(database_url)
    seed_market_rows(database_url, ["condition-1", "condition-2"])

    def mock_make_client() -> PolymarketRESTClient:
        return build_market_price_client()

    monkeypatch.setenv("PMAT_DATABASE_URL", database_url)
    monkeypatch.setattr(
        "polymarket_anomaly_tracker.ingest.market_prices.make_client",
        mock_make_client,
    )
    clear_settings_cache()
    try:
        result = runner.invoke(
            app,
            [
                "ingest",
                "market-prices",
                "--markets-from-db",
                "--max-markets",
                "10",
            ],
        )
    finally:
        clear_settings_cache()

    assert result.exit_code == 0
    assert "Snapshotted market prices." in result.stdout
    assert "Markets requested: 2." in result.stdout
    assert "Snapshots written: 2." in result.stdout

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        snapshot_count = session.scalar(select(func.count()).select_from(MarketPriceSnapshot))
        run = session.scalar(
            select(IngestionRun).where(
                IngestionRun.run_type == IngestionRunType.MARKET_PRICES.value
            )
        )
        snapshot = session.scalar(
            select(MarketPriceSnapshot).where(MarketPriceSnapshot.market_id == "condition-1")
        )

        assert snapshot_count == 2
        assert snapshot is not None
        assert snapshot.best_bid == pytest.approx(0.44)
        assert snapshot.best_ask == pytest.approx(0.48)
        assert snapshot.mid_price == pytest.approx(0.46)
        assert snapshot.last_price == pytest.approx(0.46)
        assert run is not None
        assert run.status == IngestionRunStatus.SUCCEEDED.value
        assert run.records_written == 2
        assert json.loads(run.metadata_json)["markets_requested"] == 2


def test_ingest_market_price_snapshots_is_idempotent(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'market-prices-repeat.db'}"
    init_database(database_url)
    seed_market_rows(database_url, ["condition-1", "condition-2"])

    first_client = build_market_price_client()
    second_client = build_market_price_client()
    snapshot_time = datetime(2026, 3, 20, 12, 0, tzinfo=UTC)

    try:
        ingest_market_price_snapshots(
            database_url=database_url,
            market_ids=["condition-1", "condition-2"],
            client=first_client,
            started_at=snapshot_time,
        )
        ingest_market_price_snapshots(
            database_url=database_url,
            market_ids=["condition-1", "condition-2"],
            client=second_client,
            started_at=snapshot_time,
        )
    finally:
        first_client.close()
        second_client.close()

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        snapshot_count = session.scalar(select(func.count()).select_from(MarketPriceSnapshot))
        snapshot_rows = session.scalars(
            select(MarketPriceSnapshot).order_by(
                MarketPriceSnapshot.market_id,
                MarketPriceSnapshot.snapshot_time,
            )
        ).all()

        assert snapshot_count == 2
        assert [snapshot.market_id for snapshot in snapshot_rows] == [
            "condition-1",
            "condition-2",
        ]


def test_ingest_enrich_command_persists_wallet_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'enrich.db'}"
    init_database(database_url)
    seed_wallet_rows(database_url, [WALLET_ADDRESS])

    def mock_make_client() -> PolymarketRESTClient:
        return build_enrichment_client()

    monkeypatch.setenv("PMAT_DATABASE_URL", database_url)
    monkeypatch.setattr(
        "polymarket_anomaly_tracker.ingest.orchestrator.make_client",
        mock_make_client,
    )
    clear_settings_cache()
    try:
        result = runner.invoke(
            app,
            ["ingest", "enrich", "--wallet-batch-size", "5"],
        )
    finally:
        clear_settings_cache()

    assert result.exit_code == 0
    assert "Enriched seeded wallets." in result.stdout
    assert "Succeeded: 1." in result.stdout
    assert "Trades: 1." in result.stdout

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        wallet = session.scalar(select(Wallet).where(Wallet.wallet_address == WALLET_ADDRESS))
        market = session.scalar(select(Market))
        event = session.scalar(select(Event))
        run = session.scalar(
            select(IngestionRun).where(
                IngestionRun.run_type == IngestionRunType.WALLET_ENRICHMENT.value
            )
        )

        assert wallet is not None
        assert wallet.display_name == "Alpha"
        assert session.scalar(select(func.count()).select_from(Trade)) == 1
        assert session.scalar(select(func.count()).select_from(PositionSnapshot)) == 1
        assert session.scalar(select(func.count()).select_from(ClosedPosition)) == 1
        assert session.scalar(select(func.count()).select_from(Market)) == 1
        assert session.scalar(select(func.count()).select_from(Event)) == 1
        assert market is not None
        assert market.market_id == "condition-1"
        assert market.category == "politics"
        assert event is not None
        assert event.event_id == "event-1"
        assert run is not None
        assert run.status == IngestionRunStatus.SUCCEEDED.value
        assert run.records_written == 6
        assert json.loads(run.metadata_json)["wallets_succeeded"] == 1


def test_enrich_seeded_wallets_is_idempotent_for_duplicate_batch(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'enrich-repeat.db'}"
    init_database(database_url)
    seed_wallet_rows(database_url, [WALLET_ADDRESS])
    first_client = build_enrichment_client()
    second_client = build_enrichment_client()
    started_at = datetime(2026, 3, 16, 8, 0, tzinfo=UTC)

    try:
        first_result = enrich_seeded_wallets(
            database_url=database_url,
            wallet_batch_size=10,
            client=first_client,
            started_at=started_at,
        )
        second_result = enrich_seeded_wallets(
            database_url=database_url,
            wallet_batch_size=10,
            client=second_client,
            started_at=started_at,
        )
    finally:
        first_client.close()
        second_client.close()

    assert first_result.wallets_succeeded == 1
    assert second_result.wallets_succeeded == 1
    assert first_result.records_written == 6
    assert second_result.records_written == 6

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        assert session.scalar(select(func.count()).select_from(Wallet)) == 1
        assert session.scalar(select(func.count()).select_from(Trade)) == 1
        assert session.scalar(select(func.count()).select_from(PositionSnapshot)) == 1
        assert session.scalar(select(func.count()).select_from(ClosedPosition)) == 1
        assert session.scalar(select(func.count()).select_from(Market)) == 1
        assert session.scalar(select(func.count()).select_from(Event)) == 1
        assert (
            session.scalar(
                select(func.count()).select_from(IngestionRun).where(
                    IngestionRun.run_type == IngestionRunType.WALLET_ENRICHMENT.value
                )
            )
            == 1
        )


def test_enrich_seeded_wallets_continues_when_one_wallet_fails(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'enrich-partial.db'}"
    init_database(database_url)
    seed_wallet_rows(database_url, [WALLET_ADDRESS, WALLET_ADDRESS_TWO])
    client = build_enrichment_client(failing_wallets={WALLET_ADDRESS}, failing_endpoint="trades")

    try:
        result = enrich_seeded_wallets(
            database_url=database_url,
            wallet_batch_size=10,
            client=client,
            started_at=datetime(2026, 3, 16, 9, 0, tzinfo=UTC),
        )
    finally:
        client.close()

    assert result.wallets_requested == 2
    assert result.wallets_succeeded == 1
    assert result.wallets_failed == 1
    assert result.failed_wallets[0].wallet_address == WALLET_ADDRESS

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        first_wallet = session.scalar(select(Wallet).where(Wallet.wallet_address == WALLET_ADDRESS))
        second_wallet = session.scalar(
            select(Wallet).where(Wallet.wallet_address == WALLET_ADDRESS_TWO)
        )
        run = session.scalar(
            select(IngestionRun).where(
                IngestionRun.run_type == IngestionRunType.WALLET_ENRICHMENT.value
            )
        )

        assert first_wallet is not None
        assert first_wallet.display_name is None
        assert second_wallet is not None
        assert second_wallet.display_name == "Beta"
        assert session.scalar(select(func.count()).select_from(Trade)) == 1
        assert session.scalar(select(func.count()).select_from(PositionSnapshot)) == 1
        assert session.scalar(select(func.count()).select_from(ClosedPosition)) == 1
        assert session.scalar(select(func.count()).select_from(Market)) == 1
        assert session.scalar(select(func.count()).select_from(Event)) == 1
        assert run is not None
        assert run.status == IngestionRunStatus.SUCCEEDED.value
        assert run.error_message == "1 wallet(s) failed"
        assert json.loads(run.metadata_json)["wallets_failed"] == 1
