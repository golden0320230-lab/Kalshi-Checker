"""Tests for the Polymarket public REST client."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from polymarket_anomaly_tracker.clients.dto import MarketDto, PublicProfileDto
from polymarket_anomaly_tracker.clients.exceptions import (
    PolymarketNotFoundError,
    PolymarketPayloadError,
    PolymarketRateLimitError,
    PolymarketTransportError,
)
from polymarket_anomaly_tracker.clients.polymarket_rest import make_client

FIXTURES_DIR = Path(__file__).parent / "fixtures"
WALLET_ADDRESS = "0x1111111111111111111111111111111111111111"


def load_fixture(name: str) -> object:
    """Load a JSON fixture into Python objects."""

    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def test_public_methods_return_validated_dtos() -> None:
    fixtures = {
        "/v1/leaderboard": load_fixture("leaderboard.json"),
        "/public-profile": load_fixture("profile.json"),
        "/positions": load_fixture("current_positions.json"),
        "/closed-positions": load_fixture("closed_positions.json"),
        "/activity": load_fixture("activity.json"),
        "/trades": load_fixture("trades.json"),
        "/markets/12345": load_fixture("market.json"),
        "/markets": load_fixture("markets.json"),
    }

    def handler(request: httpx.Request) -> httpx.Response:
        payload = fixtures.get(request.url.path)
        if payload is None:
            return httpx.Response(404, request=request, json={"detail": "not found"})
        return httpx.Response(200, request=request, json=payload)

    client = make_client(transport=httpx.MockTransport(handler))
    try:
        leaderboard = client.get_trader_leaderboard(window="day", limit=10)
        profile = client.get_profile(WALLET_ADDRESS)
        current_positions = client.get_current_positions(WALLET_ADDRESS)
        closed_positions = client.get_closed_positions(WALLET_ADDRESS)
        activity = client.get_user_activity(WALLET_ADDRESS)
        trades = client.get_user_trades(WALLET_ADDRESS)
        market = client.get_market("12345")
        markets = client.get_markets_by_ids(["12345", "67890"])

        assert leaderboard[0].proxy_wallet == "0x1111111111111111111111111111111111111111"
        assert isinstance(profile, PublicProfileDto)
        assert profile.name == "Alpha"
        assert current_positions[0].current_value == 72.0
        assert closed_positions[0].realized_pnl == 12.0
        assert activity[0].type == "TRADE"
        assert trades[0].transaction_hash == "0xaaa"
        assert isinstance(market, MarketDto)
        assert market.id == "12345"
        assert [item.id for item in markets] == ["12345", "67890"]
    finally:
        client.close()


def test_client_retries_on_rate_limit_then_succeeds() -> None:
    attempts = {"count": 0}
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        if attempts["count"] < 3:
            return httpx.Response(
                429,
                request=request,
                headers={"Retry-After": "0"},
                json={"detail": "too many requests"},
            )
        return httpx.Response(200, request=request, json=load_fixture("leaderboard.json"))

    client = make_client(
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
    )
    try:
        leaderboard = client.get_trader_leaderboard(window="day", limit=5)

        assert attempts["count"] == 3
        assert len(sleeps) == 2
        assert leaderboard[0].rank == "1"
    finally:
        client.close()


def test_client_raises_typed_not_found_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, request=request, json={"detail": "not found"})

    client = make_client(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(PolymarketNotFoundError):
            client.get_profile(WALLET_ADDRESS)
    finally:
        client.close()


def test_client_raises_payload_error_on_malformed_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, json={"unexpected": "shape"})

    client = make_client(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(PolymarketPayloadError):
            client.get_user_trades(WALLET_ADDRESS)
    finally:
        client.close()


def test_client_raises_transport_error_after_retries() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("network timeout", request=request)

    client = make_client(
        transport=httpx.MockTransport(handler),
        max_retries=1,
        sleep=lambda _: None,
    )
    try:
        with pytest.raises(PolymarketTransportError):
            client.get_market("12345")
    finally:
        client.close()


def test_client_raises_rate_limit_error_after_exhausting_retries() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            request=request,
            headers={"Retry-After": "0"},
            json={"detail": "too many requests"},
        )

    client = make_client(
        transport=httpx.MockTransport(handler),
        max_retries=1,
        sleep=lambda _: None,
    )
    try:
        with pytest.raises(PolymarketRateLimitError):
            client.get_trader_leaderboard(window="week", limit=5)
    finally:
        client.close()
