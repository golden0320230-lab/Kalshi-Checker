"""Finite-cycle watch-mode orchestration for flagged wallets."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.orm import Session, sessionmaker

from polymarket_anomaly_tracker.clients.polymarket_rest import (
    PolymarketRESTClient,
    make_client,
)
from polymarket_anomaly_tracker.db.enums import IngestionRunStatus, IngestionRunType
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope
from polymarket_anomaly_tracker.tracking.alerts import persist_position_change_alerts
from polymarket_anomaly_tracker.tracking.diffing import (
    WatchAlertThresholds,
    diff_position_state_maps,
)
from polymarket_anomaly_tracker.tracking.snapshots import (
    PositionKey,
    PositionState,
    capture_current_position_snapshot,
    load_position_state_map,
)


class WatchMonitorError(RuntimeError):
    """Raised when a watch-monitor run cannot complete successfully."""


@dataclass(frozen=True)
class WatchWalletFailure:
    """One wallet-level watch-monitor failure."""

    wallet_address: str
    error_message: str
    cycle_number: int


@dataclass(frozen=True)
class WalletWatchResult:
    """Operational summary for one watched wallet in one cycle."""

    wallet_address: str
    positions_written: int
    alerts_written: int
    markets_written: int
    events_written: int
    opened_alerts: int
    increased_alerts: int
    decreased_alerts: int
    closed_alerts: int

    @property
    def records_written(self) -> int:
        """Return the total DB writes attributed to this wallet cycle."""

        return (
            self.positions_written
            + self.alerts_written
            + self.markets_written
            + self.events_written
        )


@dataclass(frozen=True)
class WatchCycleResult:
    """Summary of one watch cycle across all active wallets."""

    cycle_number: int
    observed_at: datetime
    wallet_checks_requested: int
    wallet_checks_succeeded: int
    wallet_checks_failed: int
    positions_written: int
    alerts_written: int
    markets_written: int
    events_written: int
    opened_alerts: int
    increased_alerts: int
    decreased_alerts: int
    closed_alerts: int
    failures: tuple[WatchWalletFailure, ...]

    @property
    def records_written(self) -> int:
        """Return the aggregate write count for the cycle."""

        return (
            self.positions_written
            + self.alerts_written
            + self.markets_written
            + self.events_written
        )


@dataclass(frozen=True)
class WatchRunResult:
    """Summary of a complete finite watch-monitor run."""

    cycles_completed: int
    wallet_checks_requested: int
    wallet_checks_succeeded: int
    wallet_checks_failed: int
    positions_written: int
    alerts_written: int
    markets_written: int
    events_written: int
    opened_alerts: int
    increased_alerts: int
    decreased_alerts: int
    closed_alerts: int
    failures: tuple[WatchWalletFailure, ...]
    started_at: datetime
    finished_at: datetime

    @property
    def records_written(self) -> int:
        """Return the aggregate write count for the run."""

        return (
            self.positions_written
            + self.alerts_written
            + self.markets_written
            + self.events_written
        )


def run_watch_monitor(
    *,
    database_url: str,
    interval_seconds: float,
    max_cycles: int,
    client: PolymarketRESTClient | None = None,
    thresholds: WatchAlertThresholds | None = None,
    sleep: Callable[[float], None] = time.sleep,
    started_at: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
) -> WatchRunResult:
    """Run finite watch cycles for all active watchlist entries."""

    if interval_seconds < 0:
        msg = "Watch interval seconds must be non-negative"
        raise ValueError(msg)
    if max_cycles < 1:
        msg = "Max cycles must be at least 1"
        raise ValueError(msg)

    session_factory = create_session_factory(database_url)
    run_started_at = _normalize_datetime(started_at or _now(clock))
    running_metadata = _build_run_metadata(
        interval_seconds=interval_seconds,
        max_cycles=max_cycles,
        cycle_results=(),
    )
    _write_watch_run(
        session_factory=session_factory,
        started_at=run_started_at,
        finished_at=None,
        status=IngestionRunStatus.RUNNING.value,
        records_written=0,
        error_message=None,
        metadata_json=running_metadata,
    )

    active_thresholds = thresholds or WatchAlertThresholds()
    cycle_results: list[WatchCycleResult] = []
    owned_client = client is None
    active_client = client or make_client()
    try:
        for cycle_number in range(1, max_cycles + 1):
            observed_at = run_started_at if cycle_number == 1 else _normalize_datetime(_now(clock))
            cycle_results.append(
                _run_watch_cycle(
                    session_factory=session_factory,
                    client=active_client,
                    cycle_number=cycle_number,
                    observed_at=observed_at,
                    thresholds=active_thresholds,
                )
            )
            if cycle_number < max_cycles:
                sleep(interval_seconds)

        finished_at = _normalize_datetime(_now(clock))
        result = _build_run_result(
            cycle_results=cycle_results,
            started_at=run_started_at,
            finished_at=finished_at,
        )
        status = IngestionRunStatus.SUCCEEDED.value
        error_message = None
        if result.wallet_checks_requested > 0 and result.wallet_checks_succeeded == 0:
            status = IngestionRunStatus.FAILED.value
            error_message = "All watched wallet checks failed"
        elif result.wallet_checks_failed > 0:
            error_message = f"{result.wallet_checks_failed} wallet check(s) failed"

        _write_watch_run(
            session_factory=session_factory,
            started_at=run_started_at,
            finished_at=finished_at,
            status=status,
            records_written=result.records_written,
            error_message=error_message,
            metadata_json=_build_run_metadata(
                interval_seconds=interval_seconds,
                max_cycles=max_cycles,
                cycle_results=tuple(cycle_results),
            ),
        )
        if status == IngestionRunStatus.FAILED.value:
            raise WatchMonitorError(error_message or "Watch monitor failed")
        return result
    except Exception as error:
        if isinstance(error, WatchMonitorError):
            raise
        finished_at = _normalize_datetime(_now(clock))
        _write_watch_run(
            session_factory=session_factory,
            started_at=run_started_at,
            finished_at=finished_at,
            status=IngestionRunStatus.FAILED.value,
            records_written=0,
            error_message=str(error),
            metadata_json=running_metadata,
        )
        raise WatchMonitorError(f"Watch monitor failed: {error}") from error
    finally:
        if owned_client:
            active_client.close()


def _run_watch_cycle(
    *,
    session_factory: sessionmaker[Session],
    client: PolymarketRESTClient,
    cycle_number: int,
    observed_at: datetime,
    thresholds: WatchAlertThresholds,
) -> WatchCycleResult:
    watch_targets = _list_watch_targets(session_factory)
    wallet_results: list[WalletWatchResult] = []
    failures: list[WatchWalletFailure] = []

    for watch_target in watch_targets:
        try:
            with session_scope(session_factory) as session:
                repository = DatabaseRepository(session)
                wallet_results.append(
                    _watch_wallet(
                        repository=repository,
                        client=client,
                        wallet_address=watch_target.wallet_address,
                        previous_snapshot_time=watch_target.last_checked_at,
                        observed_at=observed_at,
                        thresholds=thresholds,
                    )
                )
        except Exception as error:
            failures.append(
                WatchWalletFailure(
                    wallet_address=watch_target.wallet_address,
                    error_message=str(error),
                    cycle_number=cycle_number,
                )
            )

    return WatchCycleResult(
        cycle_number=cycle_number,
        observed_at=observed_at,
        wallet_checks_requested=len(watch_targets),
        wallet_checks_succeeded=len(wallet_results),
        wallet_checks_failed=len(failures),
        positions_written=sum(result.positions_written for result in wallet_results),
        alerts_written=sum(result.alerts_written for result in wallet_results),
        markets_written=sum(result.markets_written for result in wallet_results),
        events_written=sum(result.events_written for result in wallet_results),
        opened_alerts=sum(result.opened_alerts for result in wallet_results),
        increased_alerts=sum(result.increased_alerts for result in wallet_results),
        decreased_alerts=sum(result.decreased_alerts for result in wallet_results),
        closed_alerts=sum(result.closed_alerts for result in wallet_results),
        failures=tuple(failures),
    )


def _watch_wallet(
    *,
    repository: DatabaseRepository,
    client: PolymarketRESTClient,
    wallet_address: str,
    previous_snapshot_time: datetime | None,
    observed_at: datetime,
    thresholds: WatchAlertThresholds,
) -> WalletWatchResult:
    previous_states = _load_previous_states(
        repository=repository,
        wallet_address=wallet_address,
        previous_snapshot_time=previous_snapshot_time,
    )
    capture_result = capture_current_position_snapshot(
        repository,
        client,
        wallet_address=wallet_address,
        snapshot_time=observed_at,
    )
    current_states = {state.key: state for state in capture_result.position_states}
    position_changes = diff_position_state_maps(
        wallet_address=wallet_address,
        previous_states=previous_states,
        current_states=current_states,
        detected_at=observed_at,
        thresholds=thresholds,
    )
    alert_result = persist_position_change_alerts(repository, position_changes)
    repository.update_watchlist_last_checked_at(wallet_address, checked_at=observed_at)
    return WalletWatchResult(
        wallet_address=wallet_address,
        positions_written=capture_result.positions_written,
        alerts_written=alert_result.alerts_written,
        markets_written=capture_result.markets_written,
        events_written=capture_result.events_written,
        opened_alerts=alert_result.opened_alerts,
        increased_alerts=alert_result.increased_alerts,
        decreased_alerts=alert_result.decreased_alerts,
        closed_alerts=alert_result.closed_alerts,
    )


def _load_previous_states(
    *,
    repository: DatabaseRepository,
    wallet_address: str,
    previous_snapshot_time: datetime | None,
) -> dict[PositionKey, PositionState]:
    if previous_snapshot_time is not None:
        return load_position_state_map(
            repository,
            wallet_address=wallet_address,
            snapshot_time=_normalize_datetime(previous_snapshot_time),
        )
    return load_position_state_map(repository, wallet_address=wallet_address)


@dataclass(frozen=True)
class _WatchTarget:
    wallet_address: str
    last_checked_at: datetime | None


def _list_watch_targets(session_factory: sessionmaker[Session]) -> tuple[_WatchTarget, ...]:
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        return tuple(
            _WatchTarget(
                wallet_address=entry.wallet_address,
                last_checked_at=(
                    None
                    if entry.last_checked_at is None
                    else _normalize_datetime(entry.last_checked_at)
                ),
            )
            for entry in repository.list_active_watchlist_entries()
        )


def _write_watch_run(
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
            run_type=IngestionRunType.WATCH.value,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            records_written=records_written,
            error_message=error_message,
            metadata_json=metadata_json,
        )


def _build_run_result(
    *,
    cycle_results: list[WatchCycleResult],
    started_at: datetime,
    finished_at: datetime,
) -> WatchRunResult:
    return WatchRunResult(
        cycles_completed=len(cycle_results),
        wallet_checks_requested=sum(result.wallet_checks_requested for result in cycle_results),
        wallet_checks_succeeded=sum(result.wallet_checks_succeeded for result in cycle_results),
        wallet_checks_failed=sum(result.wallet_checks_failed for result in cycle_results),
        positions_written=sum(result.positions_written for result in cycle_results),
        alerts_written=sum(result.alerts_written for result in cycle_results),
        markets_written=sum(result.markets_written for result in cycle_results),
        events_written=sum(result.events_written for result in cycle_results),
        opened_alerts=sum(result.opened_alerts for result in cycle_results),
        increased_alerts=sum(result.increased_alerts for result in cycle_results),
        decreased_alerts=sum(result.decreased_alerts for result in cycle_results),
        closed_alerts=sum(result.closed_alerts for result in cycle_results),
        failures=tuple(
            failure
            for cycle_result in cycle_results
            for failure in cycle_result.failures
        ),
        started_at=started_at,
        finished_at=finished_at,
    )


def _build_run_metadata(
    *,
    interval_seconds: float,
    max_cycles: int,
    cycle_results: tuple[WatchCycleResult, ...],
) -> str:
    payload: dict[str, object] = {
        "interval_seconds": interval_seconds,
        "max_cycles": max_cycles,
    }
    if cycle_results:
        payload["cycles"] = [
            {
                "alerts_written": cycle_result.alerts_written,
                "closed_alerts": cycle_result.closed_alerts,
                "cycle_number": cycle_result.cycle_number,
                "decreased_alerts": cycle_result.decreased_alerts,
                "events_written": cycle_result.events_written,
                "failures": [
                    {
                        "cycle_number": failure.cycle_number,
                        "error_message": failure.error_message,
                        "wallet_address": failure.wallet_address,
                    }
                    for failure in cycle_result.failures
                ],
                "increased_alerts": cycle_result.increased_alerts,
                "markets_written": cycle_result.markets_written,
                "observed_at": cycle_result.observed_at.isoformat(),
                "opened_alerts": cycle_result.opened_alerts,
                "positions_written": cycle_result.positions_written,
                "wallet_checks_failed": cycle_result.wallet_checks_failed,
                "wallet_checks_requested": cycle_result.wallet_checks_requested,
                "wallet_checks_succeeded": cycle_result.wallet_checks_succeeded,
            }
            for cycle_result in cycle_results
        ]
    return json.dumps(payload, sort_keys=True)


def _now(clock: Callable[[], datetime] | None) -> datetime:
    if clock is None:
        return datetime.now(UTC)
    return clock()


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
