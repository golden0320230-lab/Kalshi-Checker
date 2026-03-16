"""Tests for watch-mode snapshot diffing and alert emission."""

from __future__ import annotations

import json
from collections import Counter, deque
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select
from typer.testing import CliRunner

from polymarket_anomaly_tracker.clients.dto import CurrentPositionDto, MarketDto
from polymarket_anomaly_tracker.config import clear_settings_cache
from polymarket_anomaly_tracker.db.enums import (
    AlertType,
    IngestionRunStatus,
    IngestionRunType,
    WatchStatus,
)
from polymarket_anomaly_tracker.db.init_db import init_database
from polymarket_anomaly_tracker.db.models import Alert, IngestionRun
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope
from polymarket_anomaly_tracker.main import app
from polymarket_anomaly_tracker.tracking.monitor import WatchRunResult, run_watch_monitor

runner = CliRunner()

OPEN_WALLET = "0x-open"
INCREASE_WALLET = "0x-increase"
DECREASE_WALLET = "0x-decrease"
CLOSE_WALLET = "0x-close"
SMALL_WALLET = "0x-small"
FAIL_WALLET = "0x-fail"
REPEAT_CLOSE_WALLET = "0x-repeat-close"


class FakeWatchClient:
    """Minimal watch-mode client stub with deterministic wallet responses."""

    def __init__(
        self,
        *,
        position_responses: dict[str, list[object]],
        market_payloads: dict[str, dict[str, object]],
    ) -> None:
        self._position_responses = {
            wallet_address: deque(responses)
            for wallet_address, responses in position_responses.items()
        }
        self._market_payloads = market_payloads

    def get_current_positions(self, wallet_address: str) -> list[CurrentPositionDto]:
        response_queue = self._position_responses.get(wallet_address, deque([[]]))
        if not response_queue:
            raw_response: object = []
        elif len(response_queue) == 1:
            raw_response = response_queue[0]
        else:
            raw_response = response_queue.popleft()

        if isinstance(raw_response, Exception):
            raise raw_response
        if not isinstance(raw_response, list):
            msg = f"Unsupported position response for {wallet_address}: {raw_response!r}"
            raise TypeError(msg)

        return [CurrentPositionDto.model_validate(payload) for payload in raw_response]

    def get_markets_by_ids(self, market_ids: tuple[str, ...] | list[str]) -> list[MarketDto]:
        return [
            MarketDto.model_validate(self._market_payloads[market_id])
            for market_id in market_ids
            if market_id in self._market_payloads
        ]

    def get_market(self, market_id: str) -> MarketDto:
        payload = self._market_payloads.get(market_id)
        if payload is None:
            msg = f"No market payload configured for {market_id}"
            raise ValueError(msg)
        return MarketDto.model_validate(payload)

    def close(self) -> None:
        return None


def test_run_watch_monitor_emits_material_alerts_and_isolates_wallet_failures(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'tracking.db'}"
    init_database(database_url)
    previous_snapshot_time = seed_watch_monitor_state(database_url)
    observed_at = datetime(2026, 4, 12, 10, 0, tzinfo=UTC)

    client = FakeWatchClient(
        position_responses={
            OPEN_WALLET: [
                [
                    build_current_position_payload(
                        wallet_address=OPEN_WALLET,
                        condition_id="market-open",
                        outcome="YES",
                        size=120.0,
                        current_value=72.0,
                        title="Open Market",
                    )
                ]
            ],
            INCREASE_WALLET: [
                [
                    build_current_position_payload(
                        wallet_address=INCREASE_WALLET,
                        condition_id="market-increase",
                        outcome="YES",
                        size=160.0,
                        current_value=96.0,
                        title="Increase Market",
                    )
                ]
            ],
            DECREASE_WALLET: [
                [
                    build_current_position_payload(
                        wallet_address=DECREASE_WALLET,
                        condition_id="market-decrease",
                        outcome="NO",
                        size=70.0,
                        current_value=42.0,
                        title="Decrease Market",
                    )
                ]
            ],
            CLOSE_WALLET: [[]],
            SMALL_WALLET: [
                [
                    build_current_position_payload(
                        wallet_address=SMALL_WALLET,
                        condition_id="market-small",
                        outcome="YES",
                        size=102.0,
                        current_value=61.0,
                        title="Small Change Market",
                    )
                ]
            ],
            FAIL_WALLET: [RuntimeError("positions failed")],
        },
        market_payloads=build_market_payloads(
            "market-open",
            "market-increase",
            "market-decrease",
            "market-small",
        ),
    )

    result = run_watch_monitor(
        database_url=database_url,
        interval_seconds=0.0,
        max_cycles=1,
        client=client,
        started_at=observed_at,
    )

    assert result.cycles_completed == 1
    assert result.wallet_checks_requested == 6
    assert result.wallet_checks_succeeded == 5
    assert result.wallet_checks_failed == 1
    assert result.alerts_written == 4
    assert result.opened_alerts == 1
    assert result.increased_alerts == 1
    assert result.decreased_alerts == 1
    assert result.closed_alerts == 1
    assert result.positions_written == 4
    assert result.markets_written == 4
    assert result.events_written == 4
    assert result.failures[0].wallet_address == FAIL_WALLET
    assert result.failures[0].cycle_number == 1

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        alerts = list(session.scalars(select(Alert).order_by(Alert.summary)))
        summaries = [alert.summary for alert in alerts]
        run_rows = list(
            session.scalars(
                select(IngestionRun).where(IngestionRun.run_type == IngestionRunType.WATCH.value)
            )
        )
        repository = DatabaseRepository(session)

        assert len(alerts) == 4
        assert Counter(alert.alert_type for alert in alerts) == Counter(
            {
                AlertType.POSITION_CLOSED.value: 1,
                AlertType.POSITION_CHANGED.value: 2,
                AlertType.POSITION_OPENED.value: 1,
            }
        )
        assert "Closed YES position in Close Market" in summaries
        assert "Decreased NO position in Decrease Market" in summaries
        assert "Increased YES position in Increase Market" in summaries
        assert "Opened YES position in Open Market" in summaries
        assert all("Small Change Market" not in summary for summary in summaries)

        open_entry = repository.get_watchlist_entry(OPEN_WALLET)
        fail_entry = repository.get_watchlist_entry(FAIL_WALLET)
        assert open_entry is not None
        assert fail_entry is not None
        assert normalize_datetime(open_entry.last_checked_at) == observed_at
        assert fail_entry.last_checked_at is None

        assert len(run_rows) == 1
        assert run_rows[0].status == IngestionRunStatus.SUCCEEDED.value
        assert run_rows[0].error_message == "1 wallet check(s) failed"
        metadata = json.loads(run_rows[0].metadata_json)
        assert metadata["max_cycles"] == 1
        assert metadata["interval_seconds"] == 0.0
        assert metadata["cycles"][0]["wallet_checks_failed"] == 1

    assert previous_snapshot_time < observed_at


def test_run_watch_monitor_supports_finite_cycles_without_repeating_close_alerts(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'tracking-repeat-close.db'}"
    init_database(database_url)
    seed_repeat_close_state(database_url)
    cycle_one_time = datetime(2026, 4, 12, 13, 0, tzinfo=UTC)
    cycle_two_time = datetime(2026, 4, 12, 13, 5, tzinfo=UTC)
    finished_at = datetime(2026, 4, 12, 13, 6, tzinfo=UTC)
    sleep_calls: list[float] = []

    client = FakeWatchClient(
        position_responses={REPEAT_CLOSE_WALLET: [[], []]},
        market_payloads={},
    )

    result = run_watch_monitor(
        database_url=database_url,
        interval_seconds=0.0,
        max_cycles=2,
        client=client,
        started_at=cycle_one_time,
        clock=build_clock(cycle_two_time, finished_at),
        sleep=sleep_calls.append,
    )

    assert result.cycles_completed == 2
    assert result.wallet_checks_requested == 2
    assert result.wallet_checks_succeeded == 2
    assert result.wallet_checks_failed == 0
    assert result.alerts_written == 1
    assert result.closed_alerts == 1
    assert sleep_calls == [0.0]

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        alerts = list(session.scalars(select(Alert).order_by(Alert.detected_at)))
        repository = DatabaseRepository(session)
        watchlist_entry = repository.get_watchlist_entry(REPEAT_CLOSE_WALLET)

        assert len(alerts) == 1
        assert alerts[0].summary == "Closed YES position in Repeat Close Market"
        assert watchlist_entry is not None
        assert normalize_datetime(watchlist_entry.last_checked_at) == cycle_two_time


def test_watch_run_command_reports_counts(monkeypatch) -> None:
    def mock_run_watch_monitor(
        *,
        database_url: str,
        interval_seconds: float,
        max_cycles: int,
    ) -> WatchRunResult:
        assert database_url
        assert interval_seconds == 0.0
        assert max_cycles == 1
        return WatchRunResult(
            cycles_completed=1,
            wallet_checks_requested=3,
            wallet_checks_succeeded=2,
            wallet_checks_failed=1,
            positions_written=2,
            alerts_written=4,
            markets_written=2,
            events_written=2,
            opened_alerts=1,
            increased_alerts=1,
            decreased_alerts=1,
            closed_alerts=1,
            failures=(),
            started_at=datetime(2026, 4, 12, 9, 0, tzinfo=UTC),
            finished_at=datetime(2026, 4, 12, 9, 1, tzinfo=UTC),
        )

    monkeypatch.setattr(
        "polymarket_anomaly_tracker.cli.watch_cmd.run_watch_monitor",
        mock_run_watch_monitor,
    )

    clear_settings_cache()
    try:
        result = runner.invoke(
            app,
            ["watch", "run", "--interval-seconds", "0", "--max-cycles", "1"],
        )
    finally:
        clear_settings_cache()

    normalized_output = result.stdout.replace("\n", " ")
    assert result.exit_code == 0
    assert "Ran watch monitor." in normalized_output
    assert "Wallet checks: 3." in normalized_output
    assert "Alerts: 4." in normalized_output
    assert "Opened: 1." in normalized_output
    assert "Closed: 1." in normalized_output


def seed_watch_monitor_state(database_url: str) -> datetime:
    """Seed wallets, markets, watchlist rows, and prior snapshots for watch tests."""

    previous_snapshot_time = datetime(2026, 4, 12, 9, 0, tzinfo=UTC)
    added_at = datetime(2026, 4, 12, 8, 0, tzinfo=UTC)
    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        seed_watch_wallets(
            repository,
            wallet_addresses=(
                OPEN_WALLET,
                INCREASE_WALLET,
                DECREASE_WALLET,
                CLOSE_WALLET,
                SMALL_WALLET,
                FAIL_WALLET,
            ),
            added_at=added_at,
        )
        seed_watch_markets(
            repository,
            market_ids=(
                "market-open",
                "market-increase",
                "market-decrease",
                "market-close",
                "market-small",
                "market-fail",
            ),
            question_by_market={
                "market-open": "Open Market",
                "market-increase": "Increase Market",
                "market-decrease": "Decrease Market",
                "market-close": "Close Market",
                "market-small": "Small Change Market",
                "market-fail": "Fail Market",
            },
        )
        seed_position_snapshot(
            repository,
            wallet_address=INCREASE_WALLET,
            snapshot_time=previous_snapshot_time,
            market_id="market-increase",
            outcome="YES",
            quantity=100.0,
            current_value=60.0,
        )
        seed_position_snapshot(
            repository,
            wallet_address=DECREASE_WALLET,
            snapshot_time=previous_snapshot_time,
            market_id="market-decrease",
            outcome="NO",
            quantity=100.0,
            current_value=60.0,
        )
        seed_position_snapshot(
            repository,
            wallet_address=CLOSE_WALLET,
            snapshot_time=previous_snapshot_time,
            market_id="market-close",
            outcome="YES",
            quantity=100.0,
            current_value=55.0,
        )
        seed_position_snapshot(
            repository,
            wallet_address=SMALL_WALLET,
            snapshot_time=previous_snapshot_time,
            market_id="market-small",
            outcome="YES",
            quantity=100.0,
            current_value=60.0,
        )
    return previous_snapshot_time


def seed_repeat_close_state(database_url: str) -> None:
    """Seed one watched wallet that is about to close out entirely."""

    previous_snapshot_time = datetime(2026, 4, 12, 12, 0, tzinfo=UTC)
    added_at = datetime(2026, 4, 12, 11, 30, tzinfo=UTC)
    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        seed_watch_wallets(
            repository,
            wallet_addresses=(REPEAT_CLOSE_WALLET,),
            added_at=added_at,
        )
        seed_watch_markets(
            repository,
            market_ids=("market-repeat-close",),
            question_by_market={"market-repeat-close": "Repeat Close Market"},
        )
        seed_position_snapshot(
            repository,
            wallet_address=REPEAT_CLOSE_WALLET,
            snapshot_time=previous_snapshot_time,
            market_id="market-repeat-close",
            outcome="YES",
            quantity=80.0,
            current_value=48.0,
        )


def seed_watch_wallets(
    repository: DatabaseRepository,
    *,
    wallet_addresses: tuple[str, ...],
    added_at: datetime,
) -> None:
    for wallet_address in wallet_addresses:
        repository.upsert_wallet(
            wallet_address=wallet_address,
            first_seen_at=added_at,
            last_seen_at=added_at,
            display_name=wallet_address,
            is_flagged=True,
            flag_status="flagged",
        )
        repository.upsert_watchlist_entry(
            wallet_address=wallet_address,
            added_reason="Flagged from latest scoring run.",
            added_at=added_at,
            watch_status=WatchStatus.ACTIVE.value,
            priority=5,
        )


def seed_watch_markets(
    repository: DatabaseRepository,
    *,
    market_ids: tuple[str, ...],
    question_by_market: dict[str, str],
) -> None:
    for market_id in market_ids:
        event_id = f"event-{market_id}"
        repository.upsert_event(
            event_id=event_id,
            title=f"{question_by_market[market_id]} Event",
            status="active",
            slug=f"{market_id}-event",
        )
        repository.upsert_market(
            market_id=market_id,
            event_id=event_id,
            question=question_by_market[market_id],
            status="active",
            slug=market_id,
            category="politics",
        )


def seed_position_snapshot(
    repository: DatabaseRepository,
    *,
    wallet_address: str,
    snapshot_time: datetime,
    market_id: str,
    outcome: str,
    quantity: float,
    current_value: float,
) -> None:
    repository.upsert_position_snapshot(
        wallet_address=wallet_address,
        snapshot_time=snapshot_time,
        market_id=market_id,
        event_id=f"event-{market_id}",
        outcome=outcome,
        quantity=quantity,
        avg_entry_price=0.6,
        current_value=current_value,
        unrealized_pnl=5.0,
        realized_pnl=0.0,
        status="open",
    )


def build_market_payloads(*market_ids: str) -> dict[str, dict[str, object]]:
    """Build minimal market payloads for watch-mode metadata sync."""

    question_map = {
        "market-open": "Open Market",
        "market-increase": "Increase Market",
        "market-decrease": "Decrease Market",
        "market-small": "Small Change Market",
    }
    return {
        market_id: {
            "id": f"{market_id}-gamma",
            "conditionId": market_id,
            "question": question_map[market_id],
            "slug": market_id,
            "category": "Politics",
            "active": True,
            "closed": False,
            "archived": False,
            "events": [
                {
                    "id": f"event-{market_id}",
                    "slug": f"{market_id}-event",
                    "status": "active",
                    "title": f"{question_map[market_id]} Event",
                }
            ],
        }
        for market_id in market_ids
    }


def build_current_position_payload(
    *,
    wallet_address: str,
    condition_id: str,
    outcome: str,
    size: float,
    current_value: float,
    title: str,
) -> dict[str, object]:
    """Build a minimal current-position payload for watch tests."""

    return {
        "proxyWallet": wallet_address,
        "asset": f"asset-{condition_id}",
        "conditionId": condition_id,
        "size": size,
        "avgPrice": 0.6,
        "currentValue": current_value,
        "cashPnl": 4.0,
        "realizedPnl": 0.0,
        "title": title,
        "slug": condition_id,
        "eventSlug": f"{condition_id}-event",
        "outcome": outcome,
    }


def build_clock(*timestamps: datetime):
    """Build a deterministic clock callback for monitor tests."""

    timestamp_queue = deque(timestamps)

    def _clock() -> datetime:
        if not timestamp_queue:
            msg = "Clock was called more times than expected"
            raise AssertionError(msg)
        return timestamp_queue.popleft()

    return _clock


def normalize_datetime(value: datetime | None) -> datetime | None:
    """Normalize SQLite-loaded datetimes back to UTC-aware values."""

    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
