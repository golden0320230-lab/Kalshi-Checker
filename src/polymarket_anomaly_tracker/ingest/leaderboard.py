"""Leaderboard seeding workflow for wallet discovery."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.orm import Session, sessionmaker

from polymarket_anomaly_tracker.clients.dto import TraderLeaderboardEntryDto
from polymarket_anomaly_tracker.clients.polymarket_rest import (
    PolymarketRESTClient,
    make_client,
)
from polymarket_anomaly_tracker.db.enums import (
    IngestionRunStatus,
    IngestionRunType,
    WalletFlagStatus,
)
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope


@dataclass(frozen=True)
class LeaderboardSeedResult:
    """Operational summary of a leaderboard seed run."""

    window: str
    requested_limit: int
    records_written: int
    new_wallets: int
    existing_wallets: int
    started_at: datetime
    finished_at: datetime


@dataclass(frozen=True)
class _WalletSeedCounts:
    """Inserted versus updated wallet counts for a leaderboard run."""

    new_wallets: int
    existing_wallets: int


class LeaderboardSeedError(RuntimeError):
    """Raised when leaderboard seeding fails after recording run metadata."""


def seed_leaderboard_wallets(
    *,
    database_url: str,
    window: str,
    limit: int,
    client: PolymarketRESTClient | None = None,
    started_at: datetime | None = None,
) -> LeaderboardSeedResult:
    """Fetch leaderboard wallets and persist them idempotently."""

    if limit < 1:
        msg = "Leaderboard seed limit must be at least 1"
        raise ValueError(msg)

    normalized_window = window.lower()
    run_started_at = _normalize_datetime(started_at)
    session_factory = create_session_factory(database_url)
    run_metadata = _build_run_metadata(
        window=normalized_window,
        requested_limit=limit,
        fetched_entries=0,
        new_wallets=0,
        existing_wallets=0,
    )
    _write_ingestion_run(
        session_factory=session_factory,
        started_at=run_started_at,
        finished_at=None,
        status=IngestionRunStatus.RUNNING.value,
        records_written=0,
        error_message=None,
        metadata_json=run_metadata,
    )

    owned_client = client is None
    active_client = client or make_client()
    try:
        leaderboard_entries = active_client.get_trader_leaderboard(
            window=normalized_window,
            limit=limit,
        )
        wallet_counts = _persist_leaderboard_wallets(
            session_factory=session_factory,
            leaderboard_entries=leaderboard_entries,
            observed_at=run_started_at,
        )
        finished_at = datetime.now(UTC)
        run_metadata = _build_run_metadata(
            window=normalized_window,
            requested_limit=limit,
            fetched_entries=len(leaderboard_entries),
            new_wallets=wallet_counts.new_wallets,
            existing_wallets=wallet_counts.existing_wallets,
        )
        _write_ingestion_run(
            session_factory=session_factory,
            started_at=run_started_at,
            finished_at=finished_at,
            status=IngestionRunStatus.SUCCEEDED.value,
            records_written=len(leaderboard_entries),
            error_message=None,
            metadata_json=run_metadata,
        )
        return LeaderboardSeedResult(
            window=normalized_window,
            requested_limit=limit,
            records_written=len(leaderboard_entries),
            new_wallets=wallet_counts.new_wallets,
            existing_wallets=wallet_counts.existing_wallets,
            started_at=run_started_at,
            finished_at=finished_at,
        )
    except Exception as error:
        finished_at = datetime.now(UTC)
        _write_ingestion_run(
            session_factory=session_factory,
            started_at=run_started_at,
            finished_at=finished_at,
            status=IngestionRunStatus.FAILED.value,
            records_written=0,
            error_message=str(error),
            metadata_json=run_metadata,
        )
        raise LeaderboardSeedError(
            f"Failed to seed leaderboard window={normalized_window} limit={limit}: {error}"
        ) from error
    finally:
        if owned_client:
            active_client.close()


def _persist_leaderboard_wallets(
    *,
    session_factory: sessionmaker[Session],
    leaderboard_entries: list[TraderLeaderboardEntryDto],
    observed_at: datetime,
) -> _WalletSeedCounts:
    new_wallets = 0
    existing_wallets = 0

    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        for entry in leaderboard_entries:
            existing_wallet = repository.get_wallet(entry.proxy_wallet)
            if existing_wallet is None:
                new_wallets += 1
            else:
                existing_wallets += 1

            repository.upsert_wallet(
                wallet_address=entry.proxy_wallet,
                first_seen_at=_resolve_first_seen_at(
                    observed_at=observed_at,
                    existing_first_seen_at=(
                        existing_wallet.first_seen_at if existing_wallet is not None else None
                    ),
                ),
                last_seen_at=observed_at,
                display_name=_resolve_display_name(
                    entry,
                    existing_display_name=(
                        existing_wallet.display_name if existing_wallet is not None else None
                    ),
                ),
                profile_slug=existing_wallet.profile_slug if existing_wallet is not None else None,
                is_flagged=existing_wallet.is_flagged if existing_wallet is not None else False,
                flag_status=(
                    existing_wallet.flag_status
                    if existing_wallet is not None
                    else WalletFlagStatus.UNFLAGGED.value
                ),
                notes=existing_wallet.notes if existing_wallet is not None else None,
            )

    return _WalletSeedCounts(
        new_wallets=new_wallets,
        existing_wallets=existing_wallets,
    )


def _write_ingestion_run(
    *,
    session_factory: sessionmaker[Session],
    started_at: datetime,
    finished_at: datetime | None,
    status: str,
    records_written: int,
    error_message: str | None,
    metadata_json: str,
) -> None:
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        repository.upsert_ingestion_run(
            run_type=IngestionRunType.LEADERBOARD.value,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            records_written=records_written,
            error_message=error_message,
            metadata_json=metadata_json,
        )


def _build_run_metadata(
    *,
    window: str,
    requested_limit: int,
    fetched_entries: int,
    new_wallets: int,
    existing_wallets: int,
) -> str:
    return json.dumps(
        {
            "existing_wallets": existing_wallets,
            "fetched_entries": fetched_entries,
            "new_wallets": new_wallets,
            "requested_limit": requested_limit,
            "window": window,
        },
        sort_keys=True,
    )


def _normalize_datetime(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _resolve_first_seen_at(
    *,
    observed_at: datetime,
    existing_first_seen_at: datetime | None,
) -> datetime:
    if existing_first_seen_at is not None:
        return existing_first_seen_at
    return observed_at


def _resolve_display_name(
    entry: TraderLeaderboardEntryDto,
    *,
    existing_display_name: str | None,
) -> str | None:
    if entry.user_name:
        return entry.user_name
    if existing_display_name:
        return existing_display_name
    return entry.x_username
