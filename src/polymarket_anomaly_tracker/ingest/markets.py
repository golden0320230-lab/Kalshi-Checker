"""Market and event enrichment helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from polymarket_anomaly_tracker.clients.dto import (
    ClosedPositionDto,
    CurrentPositionDto,
    MarketDto,
    UserTradeDto,
)
from polymarket_anomaly_tracker.clients.exceptions import PolymarketClientError
from polymarket_anomaly_tracker.clients.polymarket_rest import PolymarketRESTClient
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository


@dataclass(frozen=True)
class MarketReference:
    """Minimal market context extracted from wallet-level payloads."""

    market_id: str
    question: str
    slug: str | None
    event_slug: str | None


@dataclass(frozen=True)
class MarketSyncResult:
    """Counts describing the market metadata written for one wallet."""

    markets_written: int
    events_written: int


def build_market_references(
    *,
    trades: list[UserTradeDto],
    current_positions: list[CurrentPositionDto],
    closed_positions: list[ClosedPositionDto],
) -> dict[str, MarketReference]:
    """Collect market references from trade and position payloads."""

    market_references: dict[str, MarketReference] = {}
    for trade in trades:
        market_references.setdefault(
            trade.condition_id,
            MarketReference(
                market_id=trade.condition_id,
                question=trade.title or trade.condition_id,
                slug=trade.slug,
                event_slug=trade.event_slug,
            ),
        )

    for position in current_positions:
        market_references.setdefault(
            position.condition_id,
            MarketReference(
                market_id=position.condition_id,
                question=position.title or position.condition_id,
                slug=position.slug,
                event_slug=position.event_slug,
            ),
        )

    for closed_position in closed_positions:
        market_references.setdefault(
            closed_position.condition_id,
            MarketReference(
                market_id=closed_position.condition_id,
                question=closed_position.title or closed_position.condition_id,
                slug=closed_position.slug,
                event_slug=closed_position.event_slug,
            ),
        )

    return market_references


def sync_market_metadata(
    *,
    repository: DatabaseRepository,
    client: PolymarketRESTClient,
    market_references: dict[str, MarketReference],
) -> MarketSyncResult:
    """Upsert linked market and event metadata, falling back to local stubs."""

    if not market_references:
        return MarketSyncResult(markets_written=0, events_written=0)

    market_payloads = _fetch_markets_with_fallback(
        client=client,
        market_ids=tuple(sorted(market_references)),
    )

    events_written = 0
    stored_market_ids: set[str] = set()
    for market_payload in market_payloads:
        market_key = resolve_market_key(
            condition_id=market_payload.condition_id,
            market_id=market_payload.id,
        )
        stored_market_ids.add(market_key)
        event_id = None

        for event_payload in market_payload.events or ():
            parsed_event_id = _extract_event_id(event_payload)
            if parsed_event_id is None:
                continue
            events_written += 1
            repository.upsert_event(
                event_id=parsed_event_id,
                title=_extract_string(event_payload, "title") or market_payload.question,
                status=_extract_event_status(event_payload, market_payload),
                category=_normalize_category(_extract_string(event_payload, "category")),
                slug=_extract_string(event_payload, "slug"),
                start_time=_parse_datetime(_extract_string(event_payload, "startDate")),
                end_time=_parse_datetime(_extract_string(event_payload, "endDate")),
                raw_json=json.dumps(event_payload, sort_keys=True),
            )
            event_id = parsed_event_id
            break

        repository.upsert_market(
            market_id=market_key,
            event_id=event_id,
            question=market_payload.question,
            slug=market_payload.slug,
            category=_normalize_category(market_payload.category),
            status=_derive_market_status(market_payload),
            resolution_time=market_payload.end_date if market_payload.closed else None,
            close_time=market_payload.end_date,
            liquidity=market_payload.liquidity,
            volume=market_payload.volume,
            raw_json=market_payload.model_dump_json(by_alias=True, exclude_none=True),
        )

    for market_id, reference in market_references.items():
        if market_id in stored_market_ids:
            continue
        repository.upsert_market(
            market_id=market_id,
            question=reference.question,
            slug=reference.slug,
            status="unknown",
            raw_json=json.dumps(
                {
                    "event_slug": reference.event_slug,
                    "market_id": reference.market_id,
                    "question": reference.question,
                    "slug": reference.slug,
                    "source": "stub",
                },
                sort_keys=True,
            ),
        )

    return MarketSyncResult(
        markets_written=len(market_references),
        events_written=events_written,
    )


def resolve_market_key(*, condition_id: str | None, market_id: str) -> str:
    """Resolve a storage key that matches trade and position payloads."""

    if condition_id:
        return condition_id
    return market_id


def _fetch_markets_with_fallback(
    *,
    client: PolymarketRESTClient,
    market_ids: tuple[str, ...],
) -> list[MarketDto]:
    try:
        return client.get_markets_by_ids(market_ids)
    except PolymarketClientError:
        market_payloads: list[MarketDto] = []
        for market_id in market_ids:
            try:
                market_payloads.append(client.get_market(market_id))
            except PolymarketClientError:
                continue
        return market_payloads


def _derive_market_status(market_payload: MarketDto) -> str:
    if market_payload.archived:
        return "archived"
    if market_payload.closed:
        return "closed"
    if market_payload.active:
        return "active"
    return "unknown"


def _extract_event_id(event_payload: dict[str, object]) -> str | None:
    event_id = _extract_string(event_payload, "id")
    if event_id:
        return event_id
    return _extract_string(event_payload, "slug")


def _extract_event_status(event_payload: dict[str, object], market_payload: MarketDto) -> str:
    explicit_status = _extract_string(event_payload, "status")
    if explicit_status:
        return explicit_status
    return _derive_market_status(market_payload)


def _extract_string(payload: dict[str, object], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value:
        return value
    return None


def _normalize_category(category: str | None) -> str | None:
    if category is None:
        return None
    return category.lower()


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
