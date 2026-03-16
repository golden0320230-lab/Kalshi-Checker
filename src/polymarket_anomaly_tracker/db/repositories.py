"""Repository helpers for idempotent persistence and common read models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from sqlalchemy import and_, desc, func, select
from sqlalchemy.orm import Session

from polymarket_anomaly_tracker.db.enums import WatchStatus
from polymarket_anomaly_tracker.db.models import (
    Alert,
    ClosedPosition,
    Event,
    IngestionRun,
    Market,
    PositionSnapshot,
    Trade,
    Wallet,
    WalletFeatureSnapshot,
    WatchlistEntry,
)


@dataclass(frozen=True)
class WalletScoreRow:
    """A lightweight wallet-scoring row for reporting and ranking."""

    wallet_address: str
    display_name: str | None
    flag_status: str
    is_flagged: bool
    as_of_time: datetime
    adjusted_score: float | None
    composite_score: float | None
    confidence_score: float | None
    resolved_markets_count: int
    trades_count: int


@dataclass(frozen=True)
class WalletFeatureSnapshotRow:
    """Run-scoped wallet snapshot row used by flagging and watchlist sync."""

    wallet_address: str
    display_name: str | None
    flag_status: str
    is_flagged: bool
    as_of_time: datetime
    adjusted_score: float | None
    composite_score: float | None
    confidence_score: float | None
    resolved_markets_count: int
    trades_count: int
    explanations_json: str


class DatabaseRepository:
    """Explicit repository for deterministic DB access."""

    def __init__(self, session: Session):
        self.session = session

    def upsert_wallet(
        self,
        *,
        wallet_address: str,
        first_seen_at: datetime,
        last_seen_at: datetime,
        display_name: str | None = None,
        profile_slug: str | None = None,
        is_flagged: bool = False,
        flag_status: str = "unflagged",
        notes: str | None = None,
    ) -> Wallet:
        """Insert or update a wallet by its public address."""

        first_seen_at = _normalize_required_datetime(first_seen_at)
        last_seen_at = _normalize_required_datetime(last_seen_at)
        wallet = self.get_wallet(wallet_address)
        if wallet is None:
            wallet = Wallet(
                wallet_address=wallet_address,
                first_seen_at=first_seen_at,
                last_seen_at=last_seen_at,
                display_name=display_name,
                profile_slug=profile_slug,
                is_flagged=is_flagged,
                flag_status=flag_status,
                notes=notes,
            )
            self.session.add(wallet)
        else:
            wallet.first_seen_at = min(wallet.first_seen_at, first_seen_at)
            wallet.last_seen_at = max(wallet.last_seen_at, last_seen_at)
            wallet.display_name = display_name
            wallet.profile_slug = profile_slug
            wallet.is_flagged = is_flagged
            wallet.flag_status = flag_status
            wallet.notes = notes

        self.session.flush()
        return wallet

    def upsert_event(
        self,
        *,
        event_id: str,
        title: str,
        status: str,
        category: str | None = None,
        slug: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        raw_json: str = "{}",
    ) -> Event:
        """Insert or update an event by its upstream ID."""

        start_time = _normalize_optional_datetime(start_time)
        end_time = _normalize_optional_datetime(end_time)
        event = cast(
            Event | None,
            self.session.scalar(select(Event).where(Event.event_id == event_id)),
        )
        if event is None:
            event = Event(
                event_id=event_id,
                title=title,
                category=category,
                slug=slug,
                start_time=start_time,
                end_time=end_time,
                status=status,
                raw_json=raw_json,
            )
            self.session.add(event)
        else:
            event.title = title
            event.category = category
            event.slug = slug
            event.start_time = start_time
            event.end_time = end_time
            event.status = status
            event.raw_json = raw_json

        self.session.flush()
        return event

    def upsert_market(
        self,
        *,
        market_id: str,
        question: str,
        status: str,
        event_id: str | None = None,
        slug: str | None = None,
        category: str | None = None,
        subcategory: str | None = None,
        resolution_outcome: str | None = None,
        resolution_time: datetime | None = None,
        close_time: datetime | None = None,
        liquidity: float | None = None,
        volume: float | None = None,
        raw_json: str = "{}",
    ) -> Market:
        """Insert or update a market by its upstream ID."""

        resolution_time = _normalize_optional_datetime(resolution_time)
        close_time = _normalize_optional_datetime(close_time)
        market = cast(
            Market | None,
            self.session.scalar(select(Market).where(Market.market_id == market_id)),
        )
        if market is None:
            market = Market(
                market_id=market_id,
                event_id=event_id,
                slug=slug,
                question=question,
                category=category,
                subcategory=subcategory,
                status=status,
                resolution_outcome=resolution_outcome,
                resolution_time=resolution_time,
                close_time=close_time,
                liquidity=liquidity,
                volume=volume,
                raw_json=raw_json,
            )
            self.session.add(market)
        else:
            market.event_id = event_id
            market.slug = slug
            market.question = question
            market.category = category
            market.subcategory = subcategory
            market.status = status
            market.resolution_outcome = resolution_outcome
            market.resolution_time = resolution_time
            market.close_time = close_time
            market.liquidity = liquidity
            market.volume = volume
            market.raw_json = raw_json

        self.session.flush()
        return market

    def upsert_trade(
        self,
        *,
        trade_id: str,
        wallet_address: str,
        market_id: str,
        outcome: str,
        side: str,
        price: float,
        size: float,
        notional: float,
        trade_time: datetime,
        event_id: str | None = None,
        source: str = "rest",
        raw_json: str = "{}",
    ) -> Trade:
        """Insert or update a normalized trade row."""

        trade_time = _normalize_required_datetime(trade_time)
        trade = cast(
            Trade | None,
            self.session.scalar(select(Trade).where(Trade.trade_id == trade_id)),
        )
        if trade is None:
            trade = Trade(
                trade_id=trade_id,
                wallet_address=wallet_address,
                market_id=market_id,
                event_id=event_id,
                outcome=outcome,
                side=side,
                price=price,
                size=size,
                notional=notional,
                trade_time=trade_time,
                source=source,
                raw_json=raw_json,
            )
            self.session.add(trade)
        else:
            trade.wallet_address = wallet_address
            trade.market_id = market_id
            trade.event_id = event_id
            trade.outcome = outcome
            trade.side = side
            trade.price = price
            trade.size = size
            trade.notional = notional
            trade.trade_time = trade_time
            trade.source = source
            trade.raw_json = raw_json

        self.session.flush()
        return trade

    def upsert_position_snapshot(
        self,
        *,
        wallet_address: str,
        snapshot_time: datetime,
        market_id: str,
        outcome: str,
        quantity: float,
        event_id: str | None = None,
        avg_entry_price: float | None = None,
        current_value: float | None = None,
        unrealized_pnl: float | None = None,
        realized_pnl: float | None = None,
        status: str | None = None,
        raw_json: str = "{}",
    ) -> PositionSnapshot:
        """Insert or update a point-in-time position snapshot."""

        snapshot_time = _normalize_required_datetime(snapshot_time)
        position_snapshot = cast(
            PositionSnapshot | None,
            self.session.scalar(
                select(PositionSnapshot).where(
                    PositionSnapshot.wallet_address == wallet_address,
                    PositionSnapshot.snapshot_time == snapshot_time,
                    PositionSnapshot.market_id == market_id,
                    PositionSnapshot.outcome == outcome,
                )
            ),
        )
        if position_snapshot is None:
            position_snapshot = PositionSnapshot(
                wallet_address=wallet_address,
                snapshot_time=snapshot_time,
                market_id=market_id,
                event_id=event_id,
                outcome=outcome,
                quantity=quantity,
                avg_entry_price=avg_entry_price,
                current_value=current_value,
                unrealized_pnl=unrealized_pnl,
                realized_pnl=realized_pnl,
                status=status,
                raw_json=raw_json,
            )
            self.session.add(position_snapshot)
        else:
            position_snapshot.event_id = event_id
            position_snapshot.quantity = quantity
            position_snapshot.avg_entry_price = avg_entry_price
            position_snapshot.current_value = current_value
            position_snapshot.unrealized_pnl = unrealized_pnl
            position_snapshot.realized_pnl = realized_pnl
            position_snapshot.status = status
            position_snapshot.raw_json = raw_json

        self.session.flush()
        return position_snapshot

    def upsert_closed_position(
        self,
        *,
        wallet_address: str,
        market_id: str,
        outcome: str,
        event_id: str | None = None,
        entry_price_avg: float | None = None,
        exit_price_avg: float | None = None,
        quantity: float | None = None,
        realized_pnl: float | None = None,
        roi: float | None = None,
        opened_at: datetime | None = None,
        closed_at: datetime | None = None,
        resolution_outcome: str | None = None,
        raw_json: str = "{}",
    ) -> ClosedPosition:
        """Insert or update a normalized closed position row."""

        opened_at = _normalize_optional_datetime(opened_at)
        closed_at = _normalize_optional_datetime(closed_at)
        closed_position = cast(
            ClosedPosition | None,
            self.session.scalar(
                select(ClosedPosition).where(
                    ClosedPosition.wallet_address == wallet_address,
                    ClosedPosition.market_id == market_id,
                    ClosedPosition.outcome == outcome,
                    ClosedPosition.opened_at == opened_at,
                    ClosedPosition.closed_at == closed_at,
                )
            ),
        )
        if closed_position is None:
            closed_position = ClosedPosition(
                wallet_address=wallet_address,
                market_id=market_id,
                event_id=event_id,
                outcome=outcome,
                entry_price_avg=entry_price_avg,
                exit_price_avg=exit_price_avg,
                quantity=quantity,
                realized_pnl=realized_pnl,
                roi=roi,
                opened_at=opened_at,
                closed_at=closed_at,
                resolution_outcome=resolution_outcome,
                raw_json=raw_json,
            )
            self.session.add(closed_position)
        else:
            closed_position.event_id = event_id
            closed_position.entry_price_avg = entry_price_avg
            closed_position.exit_price_avg = exit_price_avg
            closed_position.quantity = quantity
            closed_position.realized_pnl = realized_pnl
            closed_position.roi = roi
            closed_position.resolution_outcome = resolution_outcome
            closed_position.raw_json = raw_json

        self.session.flush()
        return closed_position

    def upsert_wallet_feature_snapshot(
        self,
        *,
        wallet_address: str,
        as_of_time: datetime,
        resolved_markets_count: int,
        trades_count: int,
        win_rate: float | None = None,
        avg_roi: float | None = None,
        median_roi: float | None = None,
        realized_pnl_total: float | None = None,
        early_entry_edge: float | None = None,
        specialization_score: float | None = None,
        conviction_score: float | None = None,
        consistency_score: float | None = None,
        timing_score: float | None = None,
        composite_score: float | None = None,
        confidence_score: float | None = None,
        adjusted_score: float | None = None,
        explanations_json: str = "{}",
    ) -> WalletFeatureSnapshot:
        """Insert or update a persisted wallet feature snapshot."""

        as_of_time = _normalize_required_datetime(as_of_time)
        feature_snapshot = cast(
            WalletFeatureSnapshot | None,
            self.session.scalar(
                select(WalletFeatureSnapshot).where(
                    WalletFeatureSnapshot.wallet_address == wallet_address,
                    WalletFeatureSnapshot.as_of_time == as_of_time,
                )
            ),
        )
        if feature_snapshot is None:
            feature_snapshot = WalletFeatureSnapshot(
                wallet_address=wallet_address,
                as_of_time=as_of_time,
                resolved_markets_count=resolved_markets_count,
                trades_count=trades_count,
                win_rate=win_rate,
                avg_roi=avg_roi,
                median_roi=median_roi,
                realized_pnl_total=realized_pnl_total,
                early_entry_edge=early_entry_edge,
                specialization_score=specialization_score,
                conviction_score=conviction_score,
                consistency_score=consistency_score,
                timing_score=timing_score,
                composite_score=composite_score,
                confidence_score=confidence_score,
                adjusted_score=adjusted_score,
                explanations_json=explanations_json,
            )
            self.session.add(feature_snapshot)
        else:
            feature_snapshot.resolved_markets_count = resolved_markets_count
            feature_snapshot.trades_count = trades_count
            feature_snapshot.win_rate = win_rate
            feature_snapshot.avg_roi = avg_roi
            feature_snapshot.median_roi = median_roi
            feature_snapshot.realized_pnl_total = realized_pnl_total
            feature_snapshot.early_entry_edge = early_entry_edge
            feature_snapshot.specialization_score = specialization_score
            feature_snapshot.conviction_score = conviction_score
            feature_snapshot.consistency_score = consistency_score
            feature_snapshot.timing_score = timing_score
            feature_snapshot.composite_score = composite_score
            feature_snapshot.confidence_score = confidence_score
            feature_snapshot.adjusted_score = adjusted_score
            feature_snapshot.explanations_json = explanations_json

        self.session.flush()
        return feature_snapshot

    def upsert_watchlist_entry(
        self,
        *,
        wallet_address: str,
        added_reason: str,
        added_at: datetime,
        watch_status: str = "active",
        last_checked_at: datetime | None = None,
        priority: int = 100,
        notes: str | None = None,
    ) -> WatchlistEntry:
        """Insert or update a watchlist entry by wallet address."""

        added_at = _normalize_required_datetime(added_at)
        last_checked_at = _normalize_optional_datetime(last_checked_at)
        watchlist_entry = cast(
            WatchlistEntry | None,
            self.session.scalar(
                select(WatchlistEntry).where(WatchlistEntry.wallet_address == wallet_address)
            ),
        )
        if watchlist_entry is None:
            watchlist_entry = WatchlistEntry(
                wallet_address=wallet_address,
                watch_status=watch_status,
                added_reason=added_reason,
                added_at=added_at,
                last_checked_at=last_checked_at,
                priority=priority,
                notes=notes,
            )
            self.session.add(watchlist_entry)
        else:
            watchlist_entry.watch_status = watch_status
            watchlist_entry.added_reason = added_reason
            watchlist_entry.added_at = added_at
            watchlist_entry.last_checked_at = last_checked_at
            watchlist_entry.priority = priority
            watchlist_entry.notes = notes

        self.session.flush()
        return watchlist_entry

    def upsert_alert(
        self,
        *,
        wallet_address: str,
        alert_type: str,
        severity: str,
        summary: str,
        detected_at: datetime,
        market_id: str | None = None,
        event_id: str | None = None,
        details_json: str = "{}",
        is_read: bool = False,
    ) -> Alert:
        """Insert or update a persisted alert by its natural event key."""

        detected_at = _normalize_required_datetime(detected_at)
        alert = cast(
            Alert | None,
            self.session.scalar(
                select(Alert).where(
                    Alert.wallet_address == wallet_address,
                    Alert.alert_type == alert_type,
                    Alert.market_id == market_id,
                    Alert.event_id == event_id,
                    Alert.detected_at == detected_at,
                    Alert.summary == summary,
                )
            ),
        )
        if alert is None:
            alert = Alert(
                wallet_address=wallet_address,
                alert_type=alert_type,
                severity=severity,
                market_id=market_id,
                event_id=event_id,
                summary=summary,
                details_json=details_json,
                detected_at=detected_at,
                is_read=is_read,
            )
            self.session.add(alert)
        else:
            alert.severity = severity
            alert.details_json = details_json
            alert.is_read = is_read

        self.session.flush()
        return alert

    def upsert_ingestion_run(
        self,
        *,
        run_type: str,
        started_at: datetime,
        status: str,
        finished_at: datetime | None = None,
        records_written: int = 0,
        error_message: str | None = None,
        metadata_json: str = "{}",
    ) -> IngestionRun:
        """Insert or update an ingestion run by run type and start time."""

        started_at = _normalize_required_datetime(started_at)
        finished_at = _normalize_optional_datetime(finished_at)
        ingestion_run = cast(
            IngestionRun | None,
            self.session.scalar(
                select(IngestionRun).where(
                    IngestionRun.run_type == run_type,
                    IngestionRun.started_at == started_at,
                )
            ),
        )
        if ingestion_run is None:
            ingestion_run = IngestionRun(
                run_type=run_type,
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                records_written=records_written,
                error_message=error_message,
                metadata_json=metadata_json,
            )
            self.session.add(ingestion_run)
        else:
            ingestion_run.finished_at = finished_at
            ingestion_run.status = status
            ingestion_run.records_written = records_written
            ingestion_run.error_message = error_message
            ingestion_run.metadata_json = metadata_json

        self.session.flush()
        return ingestion_run

    def get_wallet(self, wallet_address: str) -> Wallet | None:
        """Return a wallet by address, if present."""

        return cast(
            Wallet | None,
            self.session.scalar(select(Wallet).where(Wallet.wallet_address == wallet_address)),
        )

    def update_wallet_flag_state(
        self,
        wallet_address: str,
        *,
        flag_status: str,
        is_flagged: bool,
    ) -> Wallet:
        """Update wallet classification state without clobbering profile fields."""

        wallet = self.get_wallet(wallet_address)
        if wallet is None:
            msg = f"Wallet not found for flag update: {wallet_address}"
            raise ValueError(msg)

        wallet.flag_status = flag_status
        wallet.is_flagged = is_flagged
        self.session.flush()
        return wallet

    def list_wallets(self, *, limit: int | None = None) -> list[Wallet]:
        """Return wallets in deterministic order for enrichment batches."""

        stmt = select(Wallet).order_by(Wallet.first_seen_at, Wallet.wallet_address)
        if limit is not None:
            stmt = stmt.limit(limit)
        return list(self.session.scalars(stmt))

    def get_event(self, event_id: str) -> Event | None:
        """Return an event by upstream ID, if present."""

        return cast(
            Event | None,
            self.session.scalar(select(Event).where(Event.event_id == event_id)),
        )

    def get_market(self, market_id: str) -> Market | None:
        """Return a market by upstream key, if present."""

        return cast(
            Market | None,
            self.session.scalar(select(Market).where(Market.market_id == market_id)),
        )

    def get_latest_position_snapshot_time(self, wallet_address: str) -> datetime | None:
        """Return the latest position snapshot timestamp for one wallet."""

        latest_snapshot_time = cast(
            datetime | None,
            self.session.scalar(
                select(func.max(PositionSnapshot.snapshot_time)).where(
                    PositionSnapshot.wallet_address == wallet_address
                )
            ),
        )
        if latest_snapshot_time is None:
            return None
        return _restore_utc_datetime(latest_snapshot_time)

    def list_position_snapshots(
        self,
        *,
        wallet_address: str,
        snapshot_time: datetime,
    ) -> list[PositionSnapshot]:
        """Return one wallet's position rows for an exact snapshot time."""

        normalized_snapshot_time = _normalize_required_datetime(snapshot_time)
        stmt = (
            select(PositionSnapshot)
            .where(
                PositionSnapshot.wallet_address == wallet_address,
                PositionSnapshot.snapshot_time == normalized_snapshot_time,
            )
            .order_by(PositionSnapshot.market_id, PositionSnapshot.outcome)
        )
        return list(self.session.scalars(stmt))

    def list_latest_position_snapshots(self, wallet_address: str) -> list[PositionSnapshot]:
        """Return the most recent position snapshot rows for one wallet."""

        latest_snapshot_time = self.get_latest_position_snapshot_time(wallet_address)
        if latest_snapshot_time is None:
            return []
        return self.list_position_snapshots(
            wallet_address=wallet_address,
            snapshot_time=latest_snapshot_time,
        )

    def list_flagged_wallets(self) -> list[Wallet]:
        """Return flagged wallets ordered for deterministic display."""

        stmt = (
            select(Wallet)
            .where(Wallet.is_flagged.is_(True))
            .order_by(desc(Wallet.last_seen_at), Wallet.wallet_address)
        )
        return list(self.session.scalars(stmt))

    def get_latest_feature_snapshot(
        self,
        wallet_address: str,
    ) -> WalletFeatureSnapshot | None:
        """Return the latest feature snapshot for a wallet."""

        stmt = (
            select(WalletFeatureSnapshot)
            .where(WalletFeatureSnapshot.wallet_address == wallet_address)
            .order_by(desc(WalletFeatureSnapshot.as_of_time))
            .limit(1)
        )
        return cast(WalletFeatureSnapshot | None, self.session.scalar(stmt))

    def get_latest_feature_snapshot_time(self) -> datetime | None:
        """Return the latest scoring snapshot timestamp across all wallets."""

        latest_as_of_time = cast(
            datetime | None,
            self.session.scalar(select(func.max(WalletFeatureSnapshot.as_of_time))),
        )
        if latest_as_of_time is None:
            return None
        return _restore_utc_datetime(latest_as_of_time)

    def list_wallet_feature_snapshot_rows(
        self,
        *,
        as_of_time: datetime,
    ) -> list[WalletFeatureSnapshotRow]:
        """Return scored wallet snapshot rows for a single scoring run."""

        normalized_as_of_time = _normalize_required_datetime(as_of_time)
        stmt = (
            select(
                Wallet.wallet_address,
                Wallet.display_name,
                Wallet.flag_status,
                Wallet.is_flagged,
                WalletFeatureSnapshot.as_of_time,
                WalletFeatureSnapshot.adjusted_score,
                WalletFeatureSnapshot.composite_score,
                WalletFeatureSnapshot.confidence_score,
                WalletFeatureSnapshot.resolved_markets_count,
                WalletFeatureSnapshot.trades_count,
                WalletFeatureSnapshot.explanations_json,
            )
            .join(
                WalletFeatureSnapshot,
                Wallet.wallet_address == WalletFeatureSnapshot.wallet_address,
            )
            .where(WalletFeatureSnapshot.as_of_time == normalized_as_of_time)
            .order_by(
                desc(WalletFeatureSnapshot.adjusted_score),
                desc(WalletFeatureSnapshot.composite_score),
                desc(WalletFeatureSnapshot.confidence_score),
                Wallet.wallet_address,
            )
        )
        rows = self.session.execute(stmt).all()
        return [
            WalletFeatureSnapshotRow(
                wallet_address=row.wallet_address,
                display_name=row.display_name,
                flag_status=row.flag_status,
                is_flagged=row.is_flagged,
                as_of_time=_restore_utc_datetime(row.as_of_time),
                adjusted_score=row.adjusted_score,
                composite_score=row.composite_score,
                confidence_score=row.confidence_score,
                resolved_markets_count=row.resolved_markets_count,
                trades_count=row.trades_count,
                explanations_json=row.explanations_json,
            )
            for row in rows
        ]

    def list_wallet_scores(
        self,
        *,
        min_composite_score: float | None = None,
        limit: int | None = None,
    ) -> list[WalletScoreRow]:
        """Return one latest scoring row per wallet for reporting and ranking."""

        latest_snapshot_subquery = (
            select(
                WalletFeatureSnapshot.wallet_address.label("wallet_address"),
                func.max(WalletFeatureSnapshot.as_of_time).label("latest_as_of_time"),
            )
            .group_by(WalletFeatureSnapshot.wallet_address)
            .subquery()
        )

        stmt = (
            select(
                Wallet.wallet_address,
                Wallet.display_name,
                Wallet.flag_status,
                Wallet.is_flagged,
                WalletFeatureSnapshot.as_of_time,
                WalletFeatureSnapshot.adjusted_score,
                WalletFeatureSnapshot.composite_score,
                WalletFeatureSnapshot.confidence_score,
                WalletFeatureSnapshot.resolved_markets_count,
                WalletFeatureSnapshot.trades_count,
            )
            .join(
                latest_snapshot_subquery,
                Wallet.wallet_address == latest_snapshot_subquery.c.wallet_address,
            )
            .join(
                WalletFeatureSnapshot,
                and_(
                    WalletFeatureSnapshot.wallet_address
                    == latest_snapshot_subquery.c.wallet_address,
                    WalletFeatureSnapshot.as_of_time
                    == latest_snapshot_subquery.c.latest_as_of_time,
                ),
            )
            .order_by(
                desc(WalletFeatureSnapshot.adjusted_score),
                desc(WalletFeatureSnapshot.composite_score),
                desc(WalletFeatureSnapshot.confidence_score),
                Wallet.wallet_address,
            )
        )

        if min_composite_score is not None:
            stmt = stmt.where(WalletFeatureSnapshot.composite_score >= min_composite_score)
        if limit is not None:
            stmt = stmt.limit(limit)

        rows = self.session.execute(stmt).all()
        return [
            WalletScoreRow(
                wallet_address=row.wallet_address,
                display_name=row.display_name,
                flag_status=row.flag_status,
                is_flagged=row.is_flagged,
                as_of_time=_restore_utc_datetime(row.as_of_time),
                adjusted_score=row.adjusted_score,
                composite_score=row.composite_score,
                confidence_score=row.confidence_score,
                resolved_markets_count=row.resolved_markets_count,
                trades_count=row.trades_count,
            )
            for row in rows
        ]

    def get_trades_for_wallet(self, wallet_address: str) -> list[Trade]:
        """Return a wallet's trades ordered chronologically."""

        stmt = (
            select(Trade)
            .where(Trade.wallet_address == wallet_address)
            .order_by(Trade.trade_time, Trade.trade_id)
        )
        return list(self.session.scalars(stmt))

    def list_closed_positions_for_wallet(
        self,
        wallet_address: str,
        *,
        limit: int | None = None,
    ) -> list[ClosedPosition]:
        """Return a wallet's closed positions in most-recent-first order."""

        stmt = (
            select(ClosedPosition)
            .where(ClosedPosition.wallet_address == wallet_address)
            .order_by(
                desc(ClosedPosition.closed_at),
                ClosedPosition.market_id,
                ClosedPosition.outcome,
            )
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return list(self.session.scalars(stmt))

    def list_active_watchlist_entries(self) -> list[WatchlistEntry]:
        """Return active watchlist entries ordered by priority."""

        stmt = (
            select(WatchlistEntry)
            .where(WatchlistEntry.watch_status == WatchStatus.ACTIVE.value)
            .order_by(WatchlistEntry.priority, WatchlistEntry.wallet_address)
        )
        return list(self.session.scalars(stmt))

    def get_watchlist_entry(self, wallet_address: str) -> WatchlistEntry | None:
        """Return a watchlist entry by wallet address, if present."""

        return cast(
            WatchlistEntry | None,
            self.session.scalar(
                select(WatchlistEntry).where(WatchlistEntry.wallet_address == wallet_address)
            ),
        )

    def update_watchlist_last_checked_at(
        self,
        wallet_address: str,
        *,
        checked_at: datetime,
    ) -> WatchlistEntry:
        """Update the last checked timestamp for an existing watchlist entry."""

        watchlist_entry = self.get_watchlist_entry(wallet_address)
        if watchlist_entry is None:
            msg = f"Watchlist entry not found for wallet: {wallet_address}"
            raise ValueError(msg)

        watchlist_entry.last_checked_at = _normalize_required_datetime(checked_at)
        self.session.flush()
        return watchlist_entry

    def list_recent_alerts(
        self,
        *,
        limit: int = 50,
        unread_only: bool = False,
    ) -> list[Alert]:
        """Return recent alerts for local reporting."""

        stmt = select(Alert).order_by(desc(Alert.detected_at), desc(Alert.id))
        if unread_only:
            stmt = stmt.where(Alert.is_read.is_(False))

        return list(self.session.scalars(stmt.limit(limit)))

    def list_recent_alerts_for_wallet(
        self,
        wallet_address: str,
        *,
        limit: int = 20,
        unread_only: bool = False,
    ) -> list[Alert]:
        """Return recent alerts scoped to one wallet."""

        stmt = (
            select(Alert)
            .where(Alert.wallet_address == wallet_address)
            .order_by(desc(Alert.detected_at), desc(Alert.id))
        )
        if unread_only:
            stmt = stmt.where(Alert.is_read.is_(False))
        return list(self.session.scalars(stmt.limit(limit)))


def _normalize_required_datetime(value: datetime) -> datetime:
    """Normalize required datetimes for SQLite storage and comparisons."""

    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _normalize_optional_datetime(value: datetime | None) -> datetime | None:
    """Normalize datetimes for SQLite storage and comparisons."""

    if value is None:
        return None
    return _normalize_required_datetime(value)


def _restore_utc_datetime(value: datetime) -> datetime:
    """Reattach UTC tzinfo for values read back from SQLite."""

    if value.tzinfo is not None:
        return value.astimezone(UTC)
    return value.replace(tzinfo=UTC)
