"""Position enrichment helpers."""

from __future__ import annotations

from datetime import UTC, datetime

from polymarket_anomaly_tracker.clients.dto import ClosedPositionDto, CurrentPositionDto
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository


def persist_current_positions(
    *,
    repository: DatabaseRepository,
    wallet_address: str,
    current_positions: list[CurrentPositionDto],
    snapshot_time: datetime,
) -> int:
    """Persist point-in-time current positions for one wallet."""

    for position in current_positions:
        repository.upsert_position_snapshot(
            wallet_address=wallet_address,
            snapshot_time=snapshot_time,
            market_id=position.condition_id,
            event_id=None,
            outcome=position.outcome or "UNKNOWN",
            quantity=position.size,
            avg_entry_price=position.avg_price,
            current_value=position.current_value,
            unrealized_pnl=position.cash_pnl,
            realized_pnl=position.realized_pnl,
            status=_derive_position_status(position),
            raw_json=position.model_dump_json(by_alias=True, exclude_none=True),
        )

    return len(current_positions)


def persist_closed_positions(
    *,
    repository: DatabaseRepository,
    wallet_address: str,
    closed_positions: list[ClosedPositionDto],
) -> int:
    """Persist normalized historical closed positions for one wallet."""

    for position in closed_positions:
        quantity = None
        if (
            position.total_bought is not None
            and position.avg_price is not None
            and position.avg_price != 0
        ):
            quantity = position.total_bought / position.avg_price

        roi = None
        if (
            position.total_bought is not None
            and position.total_bought != 0
            and position.realized_pnl is not None
        ):
            roi = position.realized_pnl / position.total_bought

        repository.upsert_closed_position(
            wallet_address=wallet_address,
            market_id=position.condition_id,
            event_id=None,
            outcome=position.outcome or "UNKNOWN",
            entry_price_avg=position.avg_price,
            exit_price_avg=position.cur_price,
            quantity=quantity,
            realized_pnl=position.realized_pnl,
            roi=roi,
            opened_at=None,
            closed_at=datetime.fromtimestamp(position.timestamp, UTC),
            resolution_outcome=None,
            raw_json=position.model_dump_json(by_alias=True, exclude_none=True),
        )

    return len(closed_positions)


def _derive_position_status(position: CurrentPositionDto) -> str:
    if position.redeemable:
        return "redeemable"
    if position.mergeable:
        return "mergeable"
    return "open"
