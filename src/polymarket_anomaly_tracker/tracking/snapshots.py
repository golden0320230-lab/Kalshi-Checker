"""Helpers for capturing and loading watch-mode position snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from polymarket_anomaly_tracker.clients.dto import CurrentPositionDto
from polymarket_anomaly_tracker.clients.polymarket_rest import PolymarketRESTClient
from polymarket_anomaly_tracker.db.models import PositionSnapshot
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.ingest.markets import (
    MarketSyncResult,
    build_market_references,
    sync_market_metadata,
)
from polymarket_anomaly_tracker.ingest.positions import persist_current_positions

PositionKey = tuple[str, str]


@dataclass(frozen=True)
class PositionState:
    """Normalized position state used for snapshot diffing."""

    wallet_address: str
    market_id: str
    event_id: str | None
    market_question: str | None
    outcome: str
    quantity: float
    avg_entry_price: float | None
    current_value: float | None
    unrealized_pnl: float | None
    realized_pnl: float | None
    status: str | None
    snapshot_time: datetime

    @property
    def key(self) -> PositionKey:
        """Return the natural key for one market outcome."""

        return (self.market_id, self.outcome)


@dataclass(frozen=True)
class WalletSnapshotCaptureResult:
    """Summary of one wallet snapshot capture during watch mode."""

    wallet_address: str
    snapshot_time: datetime
    positions_written: int
    markets_written: int
    events_written: int
    position_states: tuple[PositionState, ...]


def load_position_state_map(
    repository: DatabaseRepository,
    *,
    wallet_address: str,
    snapshot_time: datetime | None = None,
) -> dict[PositionKey, PositionState]:
    """Load a wallet's position state from one exact snapshot or the latest snapshot."""

    if snapshot_time is None:
        snapshot_rows = repository.list_latest_position_snapshots(wallet_address)
    else:
        snapshot_rows = repository.list_position_snapshots(
            wallet_address=wallet_address,
            snapshot_time=snapshot_time,
        )

    state_map: dict[PositionKey, PositionState] = {}
    for snapshot_row in snapshot_rows:
        position_state = _build_snapshot_position_state(snapshot_row, repository)
        state_map[position_state.key] = position_state
    return state_map


def capture_current_position_snapshot(
    repository: DatabaseRepository,
    client: PolymarketRESTClient,
    *,
    wallet_address: str,
    snapshot_time: datetime,
) -> WalletSnapshotCaptureResult:
    """Fetch, normalize, and persist the current position snapshot for one wallet."""

    current_positions = client.get_current_positions(wallet_address)
    market_sync_result = _sync_snapshot_markets(
        repository=repository,
        client=client,
        current_positions=current_positions,
    )
    position_states = tuple(
        _build_current_position_state(
            wallet_address=wallet_address,
            current_position=position,
            repository=repository,
            snapshot_time=snapshot_time,
        )
        for position in current_positions
    )
    positions_written = persist_current_positions(
        repository=repository,
        wallet_address=wallet_address,
        current_positions=current_positions,
        snapshot_time=snapshot_time,
    )
    return WalletSnapshotCaptureResult(
        wallet_address=wallet_address,
        snapshot_time=_normalize_datetime(snapshot_time),
        positions_written=positions_written,
        markets_written=market_sync_result.markets_written,
        events_written=market_sync_result.events_written,
        position_states=position_states,
    )


def _sync_snapshot_markets(
    *,
    repository: DatabaseRepository,
    client: PolymarketRESTClient,
    current_positions: list[CurrentPositionDto],
) -> MarketSyncResult:
    market_references = build_market_references(
        trades=[],
        current_positions=current_positions,
        closed_positions=[],
    )
    return sync_market_metadata(
        repository=repository,
        client=client,
        market_references=market_references,
    )


def _build_snapshot_position_state(
    snapshot_row: PositionSnapshot,
    repository: DatabaseRepository,
) -> PositionState:
    market = repository.get_market(snapshot_row.market_id)
    return PositionState(
        wallet_address=snapshot_row.wallet_address,
        market_id=snapshot_row.market_id,
        event_id=snapshot_row.event_id or (None if market is None else market.event_id),
        market_question=None if market is None else market.question,
        outcome=snapshot_row.outcome,
        quantity=snapshot_row.quantity,
        avg_entry_price=snapshot_row.avg_entry_price,
        current_value=snapshot_row.current_value,
        unrealized_pnl=snapshot_row.unrealized_pnl,
        realized_pnl=snapshot_row.realized_pnl,
        status=snapshot_row.status,
        snapshot_time=_normalize_datetime(snapshot_row.snapshot_time),
    )


def _build_current_position_state(
    *,
    wallet_address: str,
    current_position: CurrentPositionDto,
    repository: DatabaseRepository,
    snapshot_time: datetime,
) -> PositionState:
    market = repository.get_market(current_position.condition_id)
    outcome = current_position.outcome or "UNKNOWN"
    return PositionState(
        wallet_address=wallet_address,
        market_id=current_position.condition_id,
        event_id=None if market is None else market.event_id,
        market_question=(
            current_position.title
            or (None if market is None else market.question)
        ),
        outcome=outcome,
        quantity=current_position.size,
        avg_entry_price=current_position.avg_price,
        current_value=current_position.current_value,
        unrealized_pnl=current_position.cash_pnl,
        realized_pnl=current_position.realized_pnl,
        status=_derive_current_position_status(current_position),
        snapshot_time=_normalize_datetime(snapshot_time),
    )


def _derive_current_position_status(current_position: CurrentPositionDto) -> str:
    if current_position.redeemable:
        return "redeemable"
    if current_position.mergeable:
        return "mergeable"
    return "open"


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
