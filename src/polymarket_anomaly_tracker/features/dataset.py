"""Dataset assembly for wallet-level feature computation."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
from sqlalchemy import inspect, select
from sqlalchemy.orm import Session

from polymarket_anomaly_tracker.db.models import (
    ClosedPosition,
    Market,
    MarketPriceSnapshot,
    Trade,
    Wallet,
)
from polymarket_anomaly_tracker.db.session import create_session_factory
from polymarket_anomaly_tracker.features.base import (
    CLOSED_POSITION_DATASET_COLUMNS,
    MARKET_DATASET_COLUMNS,
    PRICE_SNAPSHOT_DATASET_COLUMNS,
    TRADE_DATASET_COLUMNS,
    WALLET_DATASET_COLUMNS,
    WalletAnalysisDataset,
    empty_frame,
)


def build_wallet_analysis_dataset(
    session: Session,
    *,
    wallet_addresses: Sequence[str] | None = None,
) -> WalletAnalysisDataset:
    """Assemble reproducible wallet, trade, market, and price-history frames."""

    normalized_wallet_addresses = _normalize_wallet_addresses(wallet_addresses)
    if wallet_addresses is not None and not normalized_wallet_addresses:
        return WalletAnalysisDataset(
            wallets=empty_frame(WALLET_DATASET_COLUMNS),
            trades=empty_frame(TRADE_DATASET_COLUMNS),
            closed_positions=empty_frame(CLOSED_POSITION_DATASET_COLUMNS),
            markets=empty_frame(MARKET_DATASET_COLUMNS),
            price_snapshots=empty_frame(PRICE_SNAPSHOT_DATASET_COLUMNS),
        )

    wallet_stmt = (
        select(
            Wallet.wallet_address,
            Wallet.display_name,
            Wallet.first_seen_at,
            Wallet.last_seen_at,
        )
        .order_by(Wallet.wallet_address)
    )
    trade_stmt = (
        select(
            Trade.wallet_address,
            Trade.trade_id,
            Trade.market_id,
            Trade.outcome,
            Trade.side,
            Trade.price,
            Trade.size,
            Trade.notional,
            Trade.trade_time,
        )
        .order_by(Trade.wallet_address, Trade.trade_time, Trade.trade_id)
    )
    closed_position_stmt = (
        select(
            ClosedPosition.wallet_address,
            ClosedPosition.market_id,
            ClosedPosition.outcome,
            ClosedPosition.quantity,
            ClosedPosition.realized_pnl,
            ClosedPosition.roi,
            ClosedPosition.closed_at,
        )
        .order_by(ClosedPosition.wallet_address, ClosedPosition.closed_at, ClosedPosition.market_id)
    )
    if normalized_wallet_addresses is not None:
        wallet_stmt = wallet_stmt.where(Wallet.wallet_address.in_(normalized_wallet_addresses))
        trade_stmt = trade_stmt.where(Trade.wallet_address.in_(normalized_wallet_addresses))
        closed_position_stmt = closed_position_stmt.where(
            ClosedPosition.wallet_address.in_(normalized_wallet_addresses)
        )

    wallet_frame = _normalize_wallet_frame(
        pd.DataFrame(session.execute(wallet_stmt).mappings().all(), columns=WALLET_DATASET_COLUMNS)
    )
    trade_frame = _normalize_trade_frame(
        pd.DataFrame(session.execute(trade_stmt).mappings().all(), columns=TRADE_DATASET_COLUMNS)
    )
    closed_position_frame = _normalize_closed_position_frame(
        pd.DataFrame(
            session.execute(closed_position_stmt).mappings().all(),
            columns=CLOSED_POSITION_DATASET_COLUMNS,
        )
    )
    relevant_market_ids = _collect_relevant_market_ids(
        trade_frame=trade_frame,
        closed_position_frame=closed_position_frame,
        wallet_addresses=normalized_wallet_addresses,
    )
    market_stmt = select(Market.market_id, Market.category).order_by(Market.market_id)
    if relevant_market_ids is not None:
        market_stmt = market_stmt.where(Market.market_id.in_(sorted(relevant_market_ids)))
    market_frame = _normalize_market_frame(
        pd.DataFrame(session.execute(market_stmt).mappings().all(), columns=MARKET_DATASET_COLUMNS)
    )
    market_price_snapshot_frame = _load_market_price_snapshot_frame(
        session=session,
        relevant_market_ids=relevant_market_ids,
    )
    return WalletAnalysisDataset(
        wallets=wallet_frame,
        trades=trade_frame,
        closed_positions=closed_position_frame,
        markets=market_frame,
        price_snapshots=market_price_snapshot_frame,
    )


def load_wallet_analysis_dataset(
    database_url: str,
    *,
    wallet_addresses: Sequence[str] | None = None,
) -> WalletAnalysisDataset:
    """Load a wallet-analysis dataset directly from the configured database."""

    session_factory = create_session_factory(database_url)
    session = session_factory()
    try:
        return build_wallet_analysis_dataset(session, wallet_addresses=wallet_addresses)
    finally:
        session.close()


def _normalize_wallet_addresses(wallet_addresses: Sequence[str] | None) -> tuple[str, ...] | None:
    if wallet_addresses is None:
        return None
    return tuple(sorted({wallet_address for wallet_address in wallet_addresses}))


def _collect_relevant_market_ids(
    *,
    trade_frame: pd.DataFrame,
    closed_position_frame: pd.DataFrame,
    wallet_addresses: tuple[str, ...] | None,
) -> set[str] | None:
    if wallet_addresses is None:
        return None

    relevant_market_ids = {
        str(market_id)
        for market_id in trade_frame.get("market_id", pd.Series(dtype="object")).dropna().tolist()
    }
    relevant_market_ids.update(
        str(market_id)
        for market_id in closed_position_frame.get(
            "market_id",
            pd.Series(dtype="object"),
        )
        .dropna()
        .tolist()
    )
    return relevant_market_ids


def _load_market_price_snapshot_frame(
    *,
    session: Session,
    relevant_market_ids: set[str] | None,
) -> pd.DataFrame:
    if not _has_market_price_snapshot_table(session):
        return empty_frame(PRICE_SNAPSHOT_DATASET_COLUMNS)

    market_price_snapshot_stmt = (
        select(
            MarketPriceSnapshot.market_id,
            MarketPriceSnapshot.snapshot_time,
            MarketPriceSnapshot.best_bid,
            MarketPriceSnapshot.best_ask,
            MarketPriceSnapshot.mid_price,
            MarketPriceSnapshot.last_price,
            MarketPriceSnapshot.volume,
            MarketPriceSnapshot.liquidity,
            MarketPriceSnapshot.source,
        )
        .order_by(MarketPriceSnapshot.market_id, MarketPriceSnapshot.snapshot_time)
    )
    if relevant_market_ids is not None:
        market_price_snapshot_stmt = market_price_snapshot_stmt.where(
            MarketPriceSnapshot.market_id.in_(sorted(relevant_market_ids))
        )

    return _normalize_market_price_snapshot_frame(
        pd.DataFrame(
            session.execute(market_price_snapshot_stmt).mappings().all(),
            columns=PRICE_SNAPSHOT_DATASET_COLUMNS,
        )
    )


def _has_market_price_snapshot_table(session: Session) -> bool:
    inspector = inspect(session.get_bind())
    return inspector.has_table("market_price_snapshots")


def _normalize_wallet_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return empty_frame(WALLET_DATASET_COLUMNS)

    normalized_frame = frame.copy()
    normalized_frame["first_seen_at"] = pd.to_datetime(
        normalized_frame["first_seen_at"],
        utc=True,
    )
    normalized_frame["last_seen_at"] = pd.to_datetime(
        normalized_frame["last_seen_at"],
        utc=True,
    )
    return normalized_frame


def _normalize_trade_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return empty_frame(TRADE_DATASET_COLUMNS)

    normalized_frame = frame.copy()
    normalized_frame["price"] = pd.to_numeric(normalized_frame["price"], errors="coerce")
    normalized_frame["size"] = pd.to_numeric(normalized_frame["size"], errors="coerce")
    normalized_frame["notional"] = pd.to_numeric(normalized_frame["notional"], errors="coerce")
    normalized_frame["trade_time"] = pd.to_datetime(normalized_frame["trade_time"], utc=True)
    return normalized_frame


def _normalize_closed_position_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return empty_frame(CLOSED_POSITION_DATASET_COLUMNS)

    normalized_frame = frame.copy()
    normalized_frame["quantity"] = pd.to_numeric(normalized_frame["quantity"], errors="coerce")
    normalized_frame["realized_pnl"] = pd.to_numeric(
        normalized_frame["realized_pnl"],
        errors="coerce",
    )
    normalized_frame["roi"] = pd.to_numeric(normalized_frame["roi"], errors="coerce")
    normalized_frame["closed_at"] = pd.to_datetime(normalized_frame["closed_at"], utc=True)
    return normalized_frame


def _normalize_market_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return empty_frame(MARKET_DATASET_COLUMNS)

    normalized_frame = frame.copy()
    normalized_frame["category"] = normalized_frame["category"].astype("object")
    return normalized_frame


def _normalize_market_price_snapshot_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return empty_frame(PRICE_SNAPSHOT_DATASET_COLUMNS)

    normalized_frame = frame.copy()
    normalized_frame["snapshot_time"] = pd.to_datetime(
        normalized_frame["snapshot_time"],
        utc=True,
    )
    for column_name in (
        "best_bid",
        "best_ask",
        "mid_price",
        "last_price",
        "volume",
        "liquidity",
    ):
        normalized_frame[column_name] = pd.to_numeric(
            normalized_frame[column_name],
            errors="coerce",
        )
    normalized_frame["source"] = normalized_frame["source"].astype("object")
    return normalized_frame
