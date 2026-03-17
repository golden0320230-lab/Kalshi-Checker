"""Market price snapshot ingestion for timing-aware feature computation."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from polymarket_anomaly_tracker.clients.dto import MarketDto
from polymarket_anomaly_tracker.clients.polymarket_rest import PolymarketRESTClient, make_client
from polymarket_anomaly_tracker.db.enums import IngestionRunStatus, IngestionRunType
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from sqlalchemy.orm import Session, sessionmaker


MARKET_PRICE_SNAPSHOT_SOURCE = "gamma_rest"


@dataclass(frozen=True)
class MarketPriceIngestionResult:
    """Operational summary for one or more market price snapshot cycles."""

    market_ids: tuple[str, ...]
    cycles_completed: int
    markets_requested: int
    markets_snapshotted: int
    snapshots_written: int
    started_at: datetime
    finished_at: datetime


class MarketPriceIngestionError(RuntimeError):
    """Raised when market price snapshot ingestion cannot complete."""


def ingest_market_price_snapshots(
    *,
    database_url: str,
    market_ids: Sequence[str],
    interval_seconds: float = 0.0,
    max_cycles: int = 1,
    client: PolymarketRESTClient | None = None,
    started_at: datetime | None = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] | None = None,
) -> MarketPriceIngestionResult:
    """Fetch and persist market price snapshots for known markets."""

    normalized_market_ids = _normalize_market_ids(market_ids)
    if not normalized_market_ids:
        msg = "At least one market ID is required for market price ingestion"
        raise ValueError(msg)
    if interval_seconds < 0:
        msg = "Market price polling interval must be non-negative"
        raise ValueError(msg)
    if max_cycles < 1:
        msg = "Market price polling max cycles must be at least 1"
        raise ValueError(msg)

    session_factory = create_session_factory(database_url)
    run_started_at = _normalize_datetime(started_at or _now(clock))
    running_metadata = _build_run_metadata(
        market_ids=normalized_market_ids,
        interval_seconds=interval_seconds,
        max_cycles=max_cycles,
        cycles=(),
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
        cycle_results: list[_CycleResult] = []
        for cycle_number in range(1, max_cycles + 1):
            snapshot_time = (
                run_started_at
                if cycle_number == 1
                else _normalize_datetime(_now(clock))
            )
            cycle_results.append(
                _snapshot_market_ids(
                    session_factory=session_factory,
                    client=active_client,
                    market_ids=normalized_market_ids,
                    snapshot_time=snapshot_time,
                    cycle_number=cycle_number,
                )
            )
            if cycle_number < max_cycles:
                sleep(interval_seconds)

        finished_at = _normalize_datetime(_now(clock))
        result = MarketPriceIngestionResult(
            market_ids=normalized_market_ids,
            cycles_completed=len(cycle_results),
            markets_requested=len(normalized_market_ids),
            markets_snapshotted=sum(cycle.markets_snapshotted for cycle in cycle_results),
            snapshots_written=sum(cycle.snapshots_written for cycle in cycle_results),
            started_at=run_started_at,
            finished_at=finished_at,
        )
        _write_ingestion_run(
            session_factory=session_factory,
            started_at=run_started_at,
            finished_at=finished_at,
            status=IngestionRunStatus.SUCCEEDED.value,
            records_written=result.snapshots_written,
            error_message=None,
            metadata_json=_build_run_metadata(
                market_ids=normalized_market_ids,
                interval_seconds=interval_seconds,
                max_cycles=max_cycles,
                cycles=tuple(cycle_results),
            ),
        )
        return result
    except Exception as error:
        finished_at = _normalize_datetime(_now(clock))
        _write_ingestion_run(
            session_factory=session_factory,
            started_at=run_started_at,
            finished_at=finished_at,
            status=IngestionRunStatus.FAILED.value,
            records_written=0,
            error_message=str(error),
            metadata_json=running_metadata,
        )
        msg = f"Market price snapshot ingestion failed: {error}"
        raise MarketPriceIngestionError(msg) from error
    finally:
        if owned_client:
            active_client.close()


def resolve_market_ids(
    *,
    database_url: str,
    market_ids: Sequence[str] | None,
    market_file: Path | None,
    markets_from_db: bool,
    max_markets: int,
) -> tuple[str, ...]:
    """Resolve market IDs from explicit input, file input, and the local database."""

    resolved_market_ids: list[str] = []
    if market_ids is not None:
        resolved_market_ids.extend(market_ids)
    if market_file is not None:
        resolved_market_ids.extend(_read_market_file(market_file))
    if markets_from_db:
        session_factory = create_session_factory(database_url)
        with session_scope(session_factory) as session:
            repository = DatabaseRepository(session)
            resolved_market_ids.extend(repository.list_market_ids(limit=max_markets))

    normalized_market_ids = _normalize_market_ids(resolved_market_ids)
    if len(normalized_market_ids) > max_markets:
        return normalized_market_ids[:max_markets]
    return normalized_market_ids


@dataclass(frozen=True)
class _CycleResult:
    cycle_number: int
    snapshot_time: datetime
    markets_snapshotted: int
    snapshots_written: int


def _snapshot_market_ids(
    *,
    session_factory: sessionmaker[Session],
    client: PolymarketRESTClient,
    market_ids: tuple[str, ...],
    snapshot_time: datetime,
    cycle_number: int,
) -> _CycleResult:
    market_payloads = client.get_markets_by_ids(market_ids)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        for market_payload in market_payloads:
            market_id = market_payload.condition_id or market_payload.id
            repository.upsert_market(
                market_id=market_id,
                question=market_payload.question,
                status=_derive_market_status(market_payload),
                slug=market_payload.slug,
                category=_normalize_category(market_payload.category),
                close_time=market_payload.end_date,
                liquidity=market_payload.liquidity,
                volume=market_payload.volume,
                raw_json=market_payload.model_dump_json(by_alias=True, exclude_none=True),
            )
            repository.upsert_market_price_snapshot(
                market_id=market_id,
                snapshot_time=snapshot_time,
                source=MARKET_PRICE_SNAPSHOT_SOURCE,
                best_bid=market_payload.best_bid,
                best_ask=market_payload.best_ask,
                mid_price=_compute_mid_price(market_payload),
                last_price=market_payload.last_price,
                volume=market_payload.volume,
                liquidity=market_payload.liquidity,
                raw_json=market_payload.model_dump_json(by_alias=True, exclude_none=True),
            )

    return _CycleResult(
        cycle_number=cycle_number,
        snapshot_time=snapshot_time,
        markets_snapshotted=len(market_payloads),
        snapshots_written=len(market_payloads),
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
            run_type=IngestionRunType.MARKET_PRICES.value,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            records_written=records_written,
            error_message=error_message,
            metadata_json=metadata_json,
        )


def _build_run_metadata(
    *,
    market_ids: tuple[str, ...],
    interval_seconds: float,
    max_cycles: int,
    cycles: tuple[_CycleResult, ...],
) -> str:
    payload: dict[str, object] = {
        "market_ids": list(market_ids),
        "markets_requested": len(market_ids),
        "interval_seconds": interval_seconds,
        "max_cycles": max_cycles,
    }
    if cycles:
        payload["cycles"] = [
            {
                "cycle_number": cycle.cycle_number,
                "snapshot_time": cycle.snapshot_time.isoformat(),
                "markets_snapshotted": cycle.markets_snapshotted,
                "snapshots_written": cycle.snapshots_written,
            }
            for cycle in cycles
        ]
    return json.dumps(payload, sort_keys=True)


def _read_market_file(market_file: Path) -> list[str]:
    return [
        line.strip()
        for line in market_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _normalize_market_ids(market_ids: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted({market_id.strip() for market_id in market_ids if market_id.strip()}))


def _derive_market_status(market_payload: MarketDto) -> str:
    if market_payload.archived:
        return "archived"
    if market_payload.closed:
        return "closed"
    if market_payload.active:
        return "active"
    return "unknown"


def _normalize_category(category: str | None) -> str | None:
    if category is None:
        return None
    return category.lower()


def _compute_mid_price(market_payload: MarketDto) -> float | None:
    if market_payload.best_bid is not None and market_payload.best_ask is not None:
        return (market_payload.best_bid + market_payload.best_ask) / 2.0
    return market_payload.last_price


def _now(clock: Callable[[], datetime] | None) -> datetime:
    if clock is None:
        return datetime.now(UTC)
    return clock()


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
