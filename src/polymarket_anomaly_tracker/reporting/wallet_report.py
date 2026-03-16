"""Detailed single-wallet reporting helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

from polymarket_anomaly_tracker.db.models import (
    Alert,
    ClosedPosition,
    PositionSnapshot,
    Trade,
    WalletFeatureSnapshot,
)
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository


@dataclass(frozen=True)
class WalletReportScoreSummary:
    """Latest score and explanation context for one wallet."""

    as_of_time: datetime
    adjusted_score: float | None
    composite_score: float | None
    confidence_score: float | None
    resolved_markets_count: int
    trades_count: int
    recent_trades_count_90d: int
    top_reasons: tuple[str, ...]
    threshold_reason_keys: tuple[str, ...]
    raw_features: dict[str, object]
    normalized_features: dict[str, object]


@dataclass(frozen=True)
class WalletReportPositionRow:
    """Latest open-position row for one wallet."""

    market_id: str
    market_question: str
    outcome: str
    quantity: float
    avg_entry_price: float | None
    current_value: float | None
    unrealized_pnl: float | None
    realized_pnl: float | None
    snapshot_time: datetime
    status: str | None


@dataclass(frozen=True)
class WalletReportTradeRow:
    """Recent trade row for one wallet."""

    trade_time: datetime
    market_id: str
    market_question: str
    outcome: str
    side: str
    price: float
    size: float
    notional: float


@dataclass(frozen=True)
class WalletReportClosedPositionRow:
    """Recent closed-position row for one wallet."""

    closed_at: datetime | None
    market_id: str
    market_question: str
    outcome: str
    quantity: float | None
    realized_pnl: float | None
    roi: float | None


@dataclass(frozen=True)
class WalletReportAlertRow:
    """Recent alert row for one wallet."""

    detected_at: datetime
    alert_type: str
    severity: str
    market_id: str | None
    market_question: str | None
    summary: str


@dataclass(frozen=True)
class WalletDetailReport:
    """Full drill-down report for one wallet."""

    wallet_address: str
    display_name: str | None
    profile_slug: str | None
    first_seen_at: datetime
    last_seen_at: datetime
    flag_status: str
    is_flagged: bool
    notes: str | None
    watch_status: str | None
    watch_priority: int | None
    watch_added_reason: str | None
    watch_last_checked_at: datetime | None
    score_summary: WalletReportScoreSummary | None
    latest_positions: tuple[WalletReportPositionRow, ...]
    recent_trades: tuple[WalletReportTradeRow, ...]
    recent_closed_positions: tuple[WalletReportClosedPositionRow, ...]
    recent_alerts: tuple[WalletReportAlertRow, ...]


def build_wallet_detail_report(
    repository: DatabaseRepository,
    *,
    wallet_address: str,
    trade_limit: int = 10,
    closed_position_limit: int = 10,
    alert_limit: int = 10,
) -> WalletDetailReport:
    """Build a single-wallet drill-down report from local persisted data."""

    wallet = repository.get_wallet(wallet_address)
    if wallet is None:
        raise ValueError(f"Wallet not found: {wallet_address}")

    if trade_limit < 1 or closed_position_limit < 1 or alert_limit < 1:
        msg = "Wallet report limits must all be at least 1"
        raise ValueError(msg)

    watchlist_entry = repository.get_watchlist_entry(wallet_address)
    feature_snapshot = repository.get_latest_feature_snapshot(wallet_address)

    latest_positions = tuple(
        _build_position_row(repository, snapshot)
        for snapshot in repository.list_latest_position_snapshots(wallet_address)
    )
    recent_trades = tuple(
        _build_trade_row(repository, trade)
        for trade in list(reversed(repository.get_trades_for_wallet(wallet_address)))[
            :trade_limit
        ]
    )
    recent_closed_positions = tuple(
        _build_closed_position_row(repository, position)
        for position in repository.list_closed_positions_for_wallet(
            wallet_address,
            limit=closed_position_limit,
        )
    )
    recent_alerts = tuple(
        _build_alert_row(repository, alert)
        for alert in repository.list_recent_alerts_for_wallet(
            wallet_address,
            limit=alert_limit,
        )
    )

    return WalletDetailReport(
        wallet_address=wallet.wallet_address,
        display_name=wallet.display_name,
        profile_slug=wallet.profile_slug,
        first_seen_at=wallet.first_seen_at,
        last_seen_at=wallet.last_seen_at,
        flag_status=wallet.flag_status,
        is_flagged=wallet.is_flagged,
        notes=wallet.notes,
        watch_status=None if watchlist_entry is None else watchlist_entry.watch_status,
        watch_priority=None if watchlist_entry is None else watchlist_entry.priority,
        watch_added_reason=None if watchlist_entry is None else watchlist_entry.added_reason,
        watch_last_checked_at=(
            None if watchlist_entry is None else watchlist_entry.last_checked_at
        ),
        score_summary=_build_score_summary(feature_snapshot),
        latest_positions=latest_positions,
        recent_trades=recent_trades,
        recent_closed_positions=recent_closed_positions,
        recent_alerts=recent_alerts,
    )


def _build_score_summary(
    feature_snapshot: WalletFeatureSnapshot | None,
) -> WalletReportScoreSummary | None:
    if feature_snapshot is None:
        return None
    explanation_payload = _load_explanation_payload(feature_snapshot.explanations_json)
    sample_size = _normalize_mapping(explanation_payload.get("sample_size"))
    return WalletReportScoreSummary(
        as_of_time=feature_snapshot.as_of_time,
        adjusted_score=feature_snapshot.adjusted_score,
        composite_score=feature_snapshot.composite_score,
        confidence_score=feature_snapshot.confidence_score,
        resolved_markets_count=feature_snapshot.resolved_markets_count,
        trades_count=feature_snapshot.trades_count,
        recent_trades_count_90d=_normalize_int(sample_size.get("recent_trades_count_90d")),
        top_reasons=tuple(_normalize_string_list(explanation_payload.get("top_reasons"))[:3]),
        threshold_reason_keys=tuple(
            _normalize_string_list(explanation_payload.get("threshold_reason_keys"))
        ),
        raw_features=_normalize_mapping(explanation_payload.get("raw_features")),
        normalized_features=_normalize_mapping(
            explanation_payload.get("normalized_features")
        ),
    )


def _build_position_row(
    repository: DatabaseRepository,
    snapshot: PositionSnapshot,
) -> WalletReportPositionRow:
    market = repository.get_market(snapshot.market_id)
    return WalletReportPositionRow(
        market_id=snapshot.market_id,
        market_question=snapshot.market_id if market is None else market.question,
        outcome=snapshot.outcome,
        quantity=snapshot.quantity,
        avg_entry_price=snapshot.avg_entry_price,
        current_value=snapshot.current_value,
        unrealized_pnl=snapshot.unrealized_pnl,
        realized_pnl=snapshot.realized_pnl,
        snapshot_time=snapshot.snapshot_time,
        status=snapshot.status,
    )


def _build_trade_row(repository: DatabaseRepository, trade: Trade) -> WalletReportTradeRow:
    market = repository.get_market(trade.market_id)
    return WalletReportTradeRow(
        trade_time=trade.trade_time,
        market_id=trade.market_id,
        market_question=trade.market_id if market is None else market.question,
        outcome=trade.outcome,
        side=trade.side,
        price=trade.price,
        size=trade.size,
        notional=trade.notional,
    )


def _build_closed_position_row(
    repository: DatabaseRepository,
    closed_position: ClosedPosition,
) -> WalletReportClosedPositionRow:
    market = repository.get_market(closed_position.market_id)
    return WalletReportClosedPositionRow(
        closed_at=closed_position.closed_at,
        market_id=closed_position.market_id,
        market_question=closed_position.market_id if market is None else market.question,
        outcome=closed_position.outcome,
        quantity=closed_position.quantity,
        realized_pnl=closed_position.realized_pnl,
        roi=closed_position.roi,
    )


def _build_alert_row(repository: DatabaseRepository, alert: Alert) -> WalletReportAlertRow:
    market_question: str | None = None
    if alert.market_id is not None:
        market = repository.get_market(alert.market_id)
        market_question = None if market is None else market.question
    return WalletReportAlertRow(
        detected_at=alert.detected_at,
        alert_type=alert.alert_type,
        severity=alert.severity,
        market_id=alert.market_id,
        market_question=market_question,
        summary=alert.summary,
    )


def _load_explanation_payload(explanations_json: str) -> dict[str, object]:
    try:
        payload = json.loads(explanations_json)
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _normalize_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _normalize_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items()}


def _normalize_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0
