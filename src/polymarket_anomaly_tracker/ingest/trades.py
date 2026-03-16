"""Trade enrichment helpers."""

from __future__ import annotations

from datetime import UTC, datetime

from polymarket_anomaly_tracker.clients.dto import UserTradeDto
from polymarket_anomaly_tracker.db.enums import TradeSource
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository


def persist_wallet_trades(
    *,
    repository: DatabaseRepository,
    wallet_address: str,
    trades: list[UserTradeDto],
) -> int:
    """Persist normalized public trades for one wallet."""

    for trade in trades:
        repository.upsert_trade(
            trade_id=_derive_trade_id(wallet_address=wallet_address, trade=trade),
            wallet_address=wallet_address,
            market_id=trade.condition_id,
            event_id=None,
            outcome=trade.outcome or "UNKNOWN",
            side=trade.side.lower(),
            price=trade.price,
            size=trade.size,
            notional=trade.price * trade.size,
            trade_time=datetime.fromtimestamp(trade.timestamp, UTC),
            source=TradeSource.REST.value,
            raw_json=trade.model_dump_json(by_alias=True, exclude_none=True),
        )

    return len(trades)


def _derive_trade_id(*, wallet_address: str, trade: UserTradeDto) -> str:
    if trade.transaction_hash:
        return trade.transaction_hash
    return (
        f"{wallet_address}:{trade.condition_id}:{trade.timestamp}:"
        f"{trade.side}:{trade.price}:{trade.size}:{trade.outcome or 'unknown'}"
    )
