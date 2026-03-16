"""Snapshot diffing helpers for watch-mode alert generation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from polymarket_anomaly_tracker.tracking.snapshots import PositionKey, PositionState

PositionChangeKind = Literal["opened", "increased", "decreased", "closed"]

_EPSILON = 1e-9


@dataclass(frozen=True)
class WatchAlertThresholds:
    """Materiality thresholds for position-change alerts."""

    min_position_value_usd: float = 25.0
    min_position_change_usd: float = 25.0
    min_quantity_change_ratio: float = 0.05

    def __post_init__(self) -> None:
        if self.min_position_value_usd <= 0:
            msg = "Minimum position value threshold must be positive"
            raise ValueError(msg)
        if self.min_position_change_usd <= 0:
            msg = "Minimum position change threshold must be positive"
            raise ValueError(msg)
        if self.min_quantity_change_ratio < 0:
            msg = "Minimum quantity change ratio must be non-negative"
            raise ValueError(msg)


@dataclass(frozen=True)
class PositionChangeEvent:
    """A material position lifecycle change between two snapshots."""

    wallet_address: str
    change_kind: PositionChangeKind
    detected_at: datetime
    market_id: str
    event_id: str | None
    market_question: str | None
    outcome: str
    previous_quantity: float
    current_quantity: float
    quantity_delta: float
    previous_value: float | None
    current_value: float | None
    value_delta: float | None


def diff_position_state_maps(
    *,
    wallet_address: str,
    previous_states: dict[PositionKey, PositionState],
    current_states: dict[PositionKey, PositionState],
    detected_at: datetime,
    thresholds: WatchAlertThresholds | None = None,
) -> list[PositionChangeEvent]:
    """Return material position changes between two wallet snapshots."""

    active_thresholds = thresholds or WatchAlertThresholds()
    position_changes: list[PositionChangeEvent] = []
    for position_key in sorted(set(previous_states) | set(current_states)):
        previous_state = previous_states.get(position_key)
        current_state = current_states.get(position_key)

        if previous_state is None and current_state is not None:
            if _is_material_position_state(
                current_state,
                min_position_value_usd=active_thresholds.min_position_value_usd,
            ):
                position_changes.append(
                    _build_change_event(
                        wallet_address=wallet_address,
                        change_kind="opened",
                        detected_at=detected_at,
                        previous_state=None,
                        current_state=current_state,
                    )
                )
            continue

        if previous_state is not None and current_state is None:
            if _is_material_position_state(
                previous_state,
                min_position_value_usd=active_thresholds.min_position_value_usd,
            ):
                position_changes.append(
                    _build_change_event(
                        wallet_address=wallet_address,
                        change_kind="closed",
                        detected_at=detected_at,
                        previous_state=previous_state,
                        current_state=None,
                    )
                )
            continue

        if previous_state is None or current_state is None:
            continue

        quantity_delta = current_state.quantity - previous_state.quantity
        if abs(quantity_delta) <= _EPSILON:
            continue
        if not _is_material_position_delta(
            previous_state=previous_state,
            current_state=current_state,
            thresholds=active_thresholds,
        ):
            continue

        position_changes.append(
            _build_change_event(
                wallet_address=wallet_address,
                change_kind="increased" if quantity_delta > 0 else "decreased",
                detected_at=detected_at,
                previous_state=previous_state,
                current_state=current_state,
            )
        )

    return position_changes


def _build_change_event(
    *,
    wallet_address: str,
    change_kind: PositionChangeKind,
    detected_at: datetime,
    previous_state: PositionState | None,
    current_state: PositionState | None,
) -> PositionChangeEvent:
    anchor_state = current_state or previous_state
    if anchor_state is None:
        msg = "Position change event requires at least one snapshot state"
        raise ValueError(msg)

    previous_value = None if previous_state is None else _estimate_position_value(previous_state)
    current_value = None if current_state is None else _estimate_position_value(current_state)
    value_delta = None
    if previous_value is not None and current_value is not None:
        value_delta = current_value - previous_value

    return PositionChangeEvent(
        wallet_address=wallet_address,
        change_kind=change_kind,
        detected_at=detected_at,
        market_id=anchor_state.market_id,
        event_id=anchor_state.event_id,
        market_question=anchor_state.market_question,
        outcome=anchor_state.outcome,
        previous_quantity=0.0 if previous_state is None else previous_state.quantity,
        current_quantity=0.0 if current_state is None else current_state.quantity,
        quantity_delta=(
            (0.0 if current_state is None else current_state.quantity)
            - (0.0 if previous_state is None else previous_state.quantity)
        ),
        previous_value=previous_value,
        current_value=current_value,
        value_delta=value_delta,
    )


def _is_material_position_delta(
    *,
    previous_state: PositionState,
    current_state: PositionState,
    thresholds: WatchAlertThresholds,
) -> bool:
    quantity_delta = current_state.quantity - previous_state.quantity
    if abs(quantity_delta) <= _EPSILON:
        return False

    baseline_quantity = max(abs(previous_state.quantity), _EPSILON)
    quantity_change_ratio = abs(quantity_delta) / baseline_quantity
    if quantity_change_ratio >= thresholds.min_quantity_change_ratio:
        return True

    previous_value = _estimate_position_value(previous_state)
    current_value = _estimate_position_value(current_state)
    if previous_value is None or current_value is None:
        return False
    return abs(current_value - previous_value) >= thresholds.min_position_change_usd


def _is_material_position_state(
    state: PositionState,
    *,
    min_position_value_usd: float,
) -> bool:
    estimated_value = _estimate_position_value(state)
    if estimated_value is None:
        return True
    return estimated_value >= min_position_value_usd


def _estimate_position_value(state: PositionState) -> float | None:
    if state.current_value is not None:
        return abs(state.current_value)
    if state.avg_entry_price is not None:
        return abs(state.quantity * state.avg_entry_price)
    return None
