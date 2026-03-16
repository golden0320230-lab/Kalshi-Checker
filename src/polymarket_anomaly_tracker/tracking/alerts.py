"""Alert persistence helpers for watch-mode position diffs."""

from __future__ import annotations

import json
from dataclasses import dataclass

from polymarket_anomaly_tracker.db.enums import AlertSeverity, AlertType
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.tracking.diffing import PositionChangeEvent


@dataclass(frozen=True)
class AlertPersistenceResult:
    """Summary of alerts written for one wallet snapshot diff."""

    alerts_written: int
    opened_alerts: int
    increased_alerts: int
    decreased_alerts: int
    closed_alerts: int


def persist_position_change_alerts(
    repository: DatabaseRepository,
    position_changes: list[PositionChangeEvent],
) -> AlertPersistenceResult:
    """Persist watch-mode alerts for material position changes."""

    opened_alerts = 0
    increased_alerts = 0
    decreased_alerts = 0
    closed_alerts = 0

    for position_change in position_changes:
        market = repository.get_market(position_change.market_id)
        market_question = position_change.market_question or (
            position_change.market_id if market is None else market.question
        )
        repository.upsert_alert(
            wallet_address=position_change.wallet_address,
            alert_type=_map_alert_type(position_change),
            severity=_derive_alert_severity(position_change),
            market_id=position_change.market_id,
            event_id=position_change.event_id or (None if market is None else market.event_id),
            summary=_build_alert_summary(
                position_change=position_change,
                market_question=market_question,
            ),
            detected_at=position_change.detected_at,
            details_json=json.dumps(
                _build_alert_details(position_change),
                sort_keys=True,
            ),
        )

        if position_change.change_kind == "opened":
            opened_alerts += 1
        elif position_change.change_kind == "increased":
            increased_alerts += 1
        elif position_change.change_kind == "decreased":
            decreased_alerts += 1
        else:
            closed_alerts += 1

    return AlertPersistenceResult(
        alerts_written=len(position_changes),
        opened_alerts=opened_alerts,
        increased_alerts=increased_alerts,
        decreased_alerts=decreased_alerts,
        closed_alerts=closed_alerts,
    )


def _map_alert_type(position_change: PositionChangeEvent) -> str:
    if position_change.change_kind == "opened":
        return AlertType.POSITION_OPENED.value
    if position_change.change_kind == "closed":
        return AlertType.POSITION_CLOSED.value
    return AlertType.POSITION_CHANGED.value


def _derive_alert_severity(position_change: PositionChangeEvent) -> str:
    magnitude_candidates = [
        abs(value)
        for value in (
            position_change.previous_value,
            position_change.current_value,
            position_change.value_delta,
        )
        if value is not None
    ]
    max_magnitude = max(magnitude_candidates, default=0.0)
    if max_magnitude >= 500.0:
        return AlertSeverity.WARNING.value
    return AlertSeverity.INFO.value


def _build_alert_summary(
    *,
    position_change: PositionChangeEvent,
    market_question: str,
) -> str:
    if position_change.change_kind == "opened":
        action = "Opened"
    elif position_change.change_kind == "increased":
        action = "Increased"
    elif position_change.change_kind == "decreased":
        action = "Decreased"
    else:
        action = "Closed"

    return f"{action} {position_change.outcome} position in {market_question}"


def _build_alert_details(position_change: PositionChangeEvent) -> dict[str, object]:
    return {
        "change_kind": position_change.change_kind,
        "current_quantity": position_change.current_quantity,
        "current_value": position_change.current_value,
        "detected_at": position_change.detected_at.isoformat(),
        "market_id": position_change.market_id,
        "outcome": position_change.outcome,
        "previous_quantity": position_change.previous_quantity,
        "previous_value": position_change.previous_value,
        "quantity_delta": position_change.quantity_delta,
        "value_delta": position_change.value_delta,
        "wallet_address": position_change.wallet_address,
    }
