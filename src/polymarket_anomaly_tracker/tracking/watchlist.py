"""Watchlist synchronization helpers for flagged wallets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from polymarket_anomaly_tracker.db.enums import WalletFlagStatus, WatchStatus
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository

if TYPE_CHECKING:
    from polymarket_anomaly_tracker.scoring.flagger import WalletClassification


@dataclass(frozen=True)
class WatchlistSyncResult:
    """Summary of watchlist changes applied during a refresh."""

    created: int
    updated: int
    removed: int
    active_count: int


def sync_watchlist(
    repository: DatabaseRepository,
    classifications: list[WalletClassification],
    *,
    synced_at: datetime,
) -> WatchlistSyncResult:
    """Synchronize active watchlist rows from the latest flagging decisions."""

    existing_active_entries = {
        entry.wallet_address: entry for entry in repository.list_active_watchlist_entries()
    }
    flagged_classifications = [
        classification
        for classification in classifications
        if classification.new_flag_status == WalletFlagStatus.FLAGGED.value
    ]
    flagged_wallets = {
        classification.wallet_address: classification for classification in flagged_classifications
    }

    created = 0
    updated = 0
    removed = 0
    for classification in flagged_classifications:
        existing_entry = repository.get_watchlist_entry(classification.wallet_address)
        repository.upsert_watchlist_entry(
            wallet_address=classification.wallet_address,
            added_reason=classification.watchlist_reason,
            added_at=synced_at if existing_entry is None else existing_entry.added_at,
            watch_status=WatchStatus.ACTIVE.value,
            last_checked_at=None if existing_entry is None else existing_entry.last_checked_at,
            priority=_compute_watch_priority(classification.adjusted_score),
            notes=_build_watchlist_notes(classification),
        )
        if existing_entry is None:
            created += 1
        else:
            updated += 1

    for wallet_address, existing_entry in existing_active_entries.items():
        if wallet_address in flagged_wallets:
            continue
        repository.upsert_watchlist_entry(
            wallet_address=wallet_address,
            added_reason=existing_entry.added_reason,
            added_at=existing_entry.added_at,
            watch_status=WatchStatus.REMOVED.value,
            last_checked_at=synced_at,
            priority=existing_entry.priority,
            notes="Removed after latest flag refresh no longer met flagged criteria.",
        )
        removed += 1

    return WatchlistSyncResult(
        created=created,
        updated=updated,
        removed=removed,
        active_count=len(flagged_classifications),
    )


def _compute_watch_priority(adjusted_score: float | None) -> int:
    if adjusted_score is None:
        return 100
    return max(1, 100 - int(round(adjusted_score * 100)))


def _build_watchlist_notes(classification: WalletClassification) -> str:
    reason_list = ", ".join(classification.top_reasons[:3]) or "No explanation reasons available."
    return (
        f"Adjusted score {classification.adjusted_score:.3f}; "
        f"rank {classification.rank}/{classification.total_wallets}; "
        f"threshold reasons {classification.threshold_reason_count}; "
        f"top reasons: {reason_list}"
    )
