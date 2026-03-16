"""Wallet enrichment workflow for profiles, trades, and positions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from polymarket_anomaly_tracker.clients.polymarket_rest import PolymarketRESTClient
from polymarket_anomaly_tracker.db.enums import WalletFlagStatus
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.ingest.markets import build_market_references, sync_market_metadata
from polymarket_anomaly_tracker.ingest.positions import (
    persist_closed_positions,
    persist_current_positions,
)
from polymarket_anomaly_tracker.ingest.trades import persist_wallet_trades


@dataclass(frozen=True)
class WalletEnrichmentResult:
    """Counts describing data persisted for one enriched wallet."""

    wallet_address: str
    profiles_written: int
    trades_written: int
    current_positions_written: int
    closed_positions_written: int
    markets_written: int
    events_written: int

    @property
    def records_written(self) -> int:
        """Return the aggregate write count for ingestion-run bookkeeping."""

        return (
            self.profiles_written
            + self.trades_written
            + self.current_positions_written
            + self.closed_positions_written
            + self.markets_written
            + self.events_written
        )


def enrich_wallet(
    *,
    repository: DatabaseRepository,
    client: PolymarketRESTClient,
    wallet_address: str,
    observed_at: datetime,
) -> WalletEnrichmentResult:
    """Fetch and persist profile, trade, position, and market data for one wallet."""

    profile = client.get_profile(wallet_address)
    trades = client.get_user_trades(wallet_address)
    current_positions = client.get_current_positions(wallet_address)
    closed_positions = client.get_closed_positions(wallet_address)

    market_references = build_market_references(
        trades=trades,
        current_positions=current_positions,
        closed_positions=closed_positions,
    )
    market_sync_result = sync_market_metadata(
        repository=repository,
        client=client,
        market_references=market_references,
    )
    profiles_written = _persist_wallet_profile(
        repository=repository,
        wallet_address=wallet_address,
        observed_at=observed_at,
        display_name=profile.name or profile.pseudonym or profile.x_username,
    )
    trades_written = persist_wallet_trades(
        repository=repository,
        wallet_address=wallet_address,
        trades=trades,
    )
    current_positions_written = persist_current_positions(
        repository=repository,
        wallet_address=wallet_address,
        current_positions=current_positions,
        snapshot_time=observed_at,
    )
    closed_positions_written = persist_closed_positions(
        repository=repository,
        wallet_address=wallet_address,
        closed_positions=closed_positions,
    )

    return WalletEnrichmentResult(
        wallet_address=wallet_address,
        profiles_written=profiles_written,
        trades_written=trades_written,
        current_positions_written=current_positions_written,
        closed_positions_written=closed_positions_written,
        markets_written=market_sync_result.markets_written,
        events_written=market_sync_result.events_written,
    )


def _persist_wallet_profile(
    *,
    repository: DatabaseRepository,
    wallet_address: str,
    observed_at: datetime,
    display_name: str | None,
) -> int:
    existing_wallet = repository.get_wallet(wallet_address)
    repository.upsert_wallet(
        wallet_address=wallet_address,
        first_seen_at=existing_wallet.first_seen_at if existing_wallet is not None else observed_at,
        last_seen_at=observed_at,
        display_name=display_name or (existing_wallet.display_name if existing_wallet else None),
        profile_slug=existing_wallet.profile_slug if existing_wallet is not None else None,
        is_flagged=existing_wallet.is_flagged if existing_wallet is not None else False,
        flag_status=(
            existing_wallet.flag_status
            if existing_wallet is not None
            else WalletFlagStatus.UNFLAGGED.value
        ),
        notes=existing_wallet.notes if existing_wallet is not None else None,
    )
    return 1
