"""Batch orchestration for wallet enrichment runs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.orm import Session, sessionmaker

from polymarket_anomaly_tracker.clients.polymarket_rest import PolymarketRESTClient, make_client
from polymarket_anomaly_tracker.db.enums import IngestionRunStatus, IngestionRunType
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope
from polymarket_anomaly_tracker.ingest.wallets import WalletEnrichmentResult, enrich_wallet


@dataclass(frozen=True)
class WalletEnrichmentFailure:
    """One wallet-level enrichment failure captured during a batch run."""

    wallet_address: str
    error_message: str


@dataclass(frozen=True)
class EnrichmentBatchResult:
    """Operational summary of an enrichment batch."""

    wallets_requested: int
    wallets_succeeded: int
    wallets_failed: int
    profiles_written: int
    trades_written: int
    current_positions_written: int
    closed_positions_written: int
    markets_written: int
    events_written: int
    failed_wallets: tuple[WalletEnrichmentFailure, ...]
    started_at: datetime
    finished_at: datetime

    @property
    def records_written(self) -> int:
        """Return the aggregate write count across the batch."""

        return (
            self.profiles_written
            + self.trades_written
            + self.current_positions_written
            + self.closed_positions_written
            + self.markets_written
            + self.events_written
        )


class WalletEnrichmentBatchError(RuntimeError):
    """Raised when a batch enrichment run cannot complete successfully."""


def enrich_seeded_wallets(
    *,
    database_url: str,
    wallet_batch_size: int,
    client: PolymarketRESTClient | None = None,
    started_at: datetime | None = None,
) -> EnrichmentBatchResult:
    """Enrich a deterministic batch of seeded wallets from the local database."""

    if wallet_batch_size < 1:
        msg = "Wallet batch size must be at least 1"
        raise ValueError(msg)

    session_factory = create_session_factory(database_url)
    run_started_at = _normalize_datetime(started_at)
    wallet_addresses = _list_wallet_addresses(
        session_factory=session_factory,
        wallet_batch_size=wallet_batch_size,
    )
    running_metadata = _build_run_metadata(
        wallet_addresses=wallet_addresses,
        result=None,
    )
    _write_ingestion_run(
        session_factory=session_factory,
        started_at=run_started_at,
        finished_at=None,
        status=IngestionRunStatus.RUNNING.value,
        records_written=0,
        error_message=None,
        metadata_json=running_metadata,
    )

    owned_client = client is None
    active_client = client or make_client()
    try:
        result = _enrich_wallet_batch(
            session_factory=session_factory,
            wallet_addresses=wallet_addresses,
            client=active_client,
            observed_at=run_started_at,
        )
        status = IngestionRunStatus.SUCCEEDED.value
        error_message = None
        if result.wallets_requested > 0 and result.wallets_succeeded == 0:
            status = IngestionRunStatus.FAILED.value
            error_message = "All wallet enrichments failed"
        elif result.wallets_failed > 0:
            error_message = f"{result.wallets_failed} wallet(s) failed"

        _write_ingestion_run(
            session_factory=session_factory,
            started_at=run_started_at,
            finished_at=result.finished_at,
            status=status,
            records_written=result.records_written,
            error_message=error_message,
            metadata_json=_build_run_metadata(wallet_addresses=wallet_addresses, result=result),
        )
        if status == IngestionRunStatus.FAILED.value:
            raise WalletEnrichmentBatchError(error_message or "Wallet enrichment failed")
        return result
    except Exception as error:
        if isinstance(error, WalletEnrichmentBatchError):
            raise
        finished_at = datetime.now(UTC)
        _write_ingestion_run(
            session_factory=session_factory,
            started_at=run_started_at,
            finished_at=finished_at,
            status=IngestionRunStatus.FAILED.value,
            records_written=0,
            error_message=str(error),
            metadata_json=running_metadata,
        )
        raise WalletEnrichmentBatchError(f"Wallet enrichment batch failed: {error}") from error
    finally:
        if owned_client:
            active_client.close()


def _enrich_wallet_batch(
    *,
    session_factory: sessionmaker[Session],
    wallet_addresses: list[str],
    client: PolymarketRESTClient,
    observed_at: datetime,
) -> EnrichmentBatchResult:
    wallet_results: list[WalletEnrichmentResult] = []
    failed_wallets: list[WalletEnrichmentFailure] = []

    for wallet_address in wallet_addresses:
        try:
            with session_scope(session_factory) as session:
                repository = DatabaseRepository(session)
                wallet_results.append(
                    enrich_wallet(
                        repository=repository,
                        client=client,
                        wallet_address=wallet_address,
                        observed_at=observed_at,
                    )
                )
        except Exception as error:
            failed_wallets.append(
                WalletEnrichmentFailure(
                    wallet_address=wallet_address,
                    error_message=str(error),
                )
            )

    finished_at = datetime.now(UTC)
    return EnrichmentBatchResult(
        wallets_requested=len(wallet_addresses),
        wallets_succeeded=len(wallet_results),
        wallets_failed=len(failed_wallets),
        profiles_written=sum(result.profiles_written for result in wallet_results),
        trades_written=sum(result.trades_written for result in wallet_results),
        current_positions_written=sum(
            result.current_positions_written for result in wallet_results
        ),
        closed_positions_written=sum(
            result.closed_positions_written for result in wallet_results
        ),
        markets_written=sum(result.markets_written for result in wallet_results),
        events_written=sum(result.events_written for result in wallet_results),
        failed_wallets=tuple(failed_wallets),
        started_at=observed_at,
        finished_at=finished_at,
    )


def _list_wallet_addresses(
    *,
    session_factory: sessionmaker[Session],
    wallet_batch_size: int,
) -> list[str]:
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        wallets = repository.list_wallets(limit=wallet_batch_size)
        return [wallet.wallet_address for wallet in wallets]


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
            run_type=IngestionRunType.WALLET_ENRICHMENT.value,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            records_written=records_written,
            error_message=error_message,
            metadata_json=metadata_json,
        )


def _build_run_metadata(
    *,
    wallet_addresses: list[str],
    result: EnrichmentBatchResult | None,
) -> str:
    payload: dict[str, object] = {
        "wallet_addresses": wallet_addresses,
        "wallets_requested": len(wallet_addresses),
    }
    if result is not None:
        payload.update(
            {
                "closed_positions_written": result.closed_positions_written,
                "current_positions_written": result.current_positions_written,
                "events_written": result.events_written,
                "failed_wallets": [
                    {
                        "wallet_address": failure.wallet_address,
                        "error_message": failure.error_message,
                    }
                    for failure in result.failed_wallets
                ],
                "markets_written": result.markets_written,
                "profiles_written": result.profiles_written,
                "trades_written": result.trades_written,
                "wallets_failed": result.wallets_failed,
                "wallets_succeeded": result.wallets_succeeded,
            }
        )
    return json.dumps(payload, sort_keys=True)


def _normalize_datetime(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
