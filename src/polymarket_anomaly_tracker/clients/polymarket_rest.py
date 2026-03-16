"""Public Polymarket REST client with retry and DTO normalization."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any, TypeAlias, TypeVar

import httpx
from pydantic import BaseModel, ValidationError

from polymarket_anomaly_tracker.clients.dto import (
    ClosedPositionDto,
    CurrentPositionDto,
    MarketDto,
    PublicProfileDto,
    TraderLeaderboardEntryDto,
    UserActivityDto,
    UserTradeDto,
)
from polymarket_anomaly_tracker.clients.exceptions import (
    PolymarketHTTPError,
    PolymarketNotFoundError,
    PolymarketPayloadError,
    PolymarketRateLimitError,
    PolymarketTransportError,
)

DATA_API_BASE_URL = "https://data-api.polymarket.com"
GAMMA_API_BASE_URL = "https://gamma-api.polymarket.com"
RETRY_STATUS_CODES = {408, 429, 500, 502, 503, 504}
VALID_LEADERBOARD_WINDOWS = {"day", "week", "month", "all"}

TDto = TypeVar("TDto", bound=BaseModel)
QueryScalar: TypeAlias = str | int | float | bool | None
QueryValue: TypeAlias = QueryScalar | Sequence[QueryScalar]
QueryParams: TypeAlias = Mapping[str, QueryValue]


class PolymarketRESTClient:
    """Typed client for Polymarket's public Gamma and Data APIs."""

    def __init__(
        self,
        *,
        timeout: float = 10.0,
        max_retries: int = 2,
        retry_backoff_seconds: float = 0.5,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.sleep = sleep
        self._client = httpx.Client(
            timeout=timeout,
            transport=transport,
            headers={"Accept": "application/json"},
        )

    def close(self) -> None:
        """Close the underlying HTTP client."""

        self._client.close()

    def get_trader_leaderboard(
        self,
        *,
        window: str,
        limit: int = 50,
    ) -> list[TraderLeaderboardEntryDto]:
        """Return leaderboard entries for a documented time window."""

        normalized_window = window.lower()
        if normalized_window not in VALID_LEADERBOARD_WINDOWS:
            msg = f"Unsupported leaderboard window: {window}"
            raise ValueError(msg)

        payload = self._request_json(
            url=f"{DATA_API_BASE_URL}/v1/leaderboard",
            params={"timePeriod": normalized_window.upper(), "limit": limit},
        )
        return self._validate_dto_list(
            payload,
            TraderLeaderboardEntryDto,
            endpoint="get_trader_leaderboard",
        )

    def get_profile(self, wallet_address: str) -> PublicProfileDto:
        """Return the public profile for a wallet address."""

        payload = self._request_json(
            url=f"{GAMMA_API_BASE_URL}/public-profile",
            params={"address": wallet_address},
        )
        return self._validate_dto(
            payload,
            PublicProfileDto,
            endpoint="get_profile",
        )

    def get_current_positions(self, wallet_address: str) -> list[CurrentPositionDto]:
        """Return current positions for a wallet address."""

        payload = self._request_json(
            url=f"{DATA_API_BASE_URL}/positions",
            params={"user": wallet_address},
        )
        return self._validate_dto_list(
            payload,
            CurrentPositionDto,
            endpoint="get_current_positions",
        )

    def get_closed_positions(self, wallet_address: str) -> list[ClosedPositionDto]:
        """Return closed positions for a wallet address."""

        payload = self._request_json(
            url=f"{DATA_API_BASE_URL}/closed-positions",
            params={"user": wallet_address},
        )
        return self._validate_dto_list(
            payload,
            ClosedPositionDto,
            endpoint="get_closed_positions",
        )

    def get_user_activity(self, wallet_address: str) -> list[UserActivityDto]:
        """Return public user activity for a wallet address."""

        payload = self._request_json(
            url=f"{DATA_API_BASE_URL}/activity",
            params={"user": wallet_address},
        )
        return self._validate_dto_list(
            payload,
            UserActivityDto,
            endpoint="get_user_activity",
        )

    def get_user_trades(self, wallet_address: str) -> list[UserTradeDto]:
        """Return public trades for a wallet address."""

        payload = self._request_json(
            url=f"{DATA_API_BASE_URL}/trades",
            params={"user": wallet_address},
        )
        return self._validate_dto_list(
            payload,
            UserTradeDto,
            endpoint="get_user_trades",
        )

    def get_market(self, market_id: str) -> MarketDto:
        """Return a single market by ID from the Gamma API."""

        payload = self._request_json(url=f"{GAMMA_API_BASE_URL}/markets/{market_id}")
        return self._validate_dto(
            payload,
            MarketDto,
            endpoint="get_market",
        )

    def get_markets_by_ids(self, market_ids: Sequence[str]) -> list[MarketDto]:
        """Return multiple markets by ID using the list endpoint."""

        if not market_ids:
            return []

        payload = self._request_json(
            url=f"{GAMMA_API_BASE_URL}/markets",
            params={"id": list(market_ids)},
        )
        return self._validate_dto_list(
            payload,
            MarketDto,
            endpoint="get_markets_by_ids",
        )

    def _request_json(
        self,
        *,
        url: str,
        params: QueryParams | None = None,
    ) -> Any:
        last_transport_error: PolymarketTransportError | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.get(url, params=params)
            except httpx.TransportError as error:
                last_transport_error = PolymarketTransportError(url=url, message=str(error))
                if attempt == self.max_retries:
                    raise last_transport_error from error
                self.sleep(self._retry_delay(attempt))
                continue

            if response.status_code == 404:
                raise PolymarketNotFoundError(
                    status_code=response.status_code,
                    url=str(response.request.url),
                    message=response.text,
                )

            if response.status_code in RETRY_STATUS_CODES:
                if attempt == self.max_retries:
                    if response.status_code == 429:
                        raise PolymarketRateLimitError(
                            status_code=response.status_code,
                            url=str(response.request.url),
                            message=response.text,
                        )
                    raise PolymarketHTTPError(
                        status_code=response.status_code,
                        url=str(response.request.url),
                        message=response.text,
                    )
                self.sleep(self._retry_delay(attempt, response))
                continue

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as error:
                raise PolymarketHTTPError(
                    status_code=response.status_code,
                    url=str(response.request.url),
                    message=response.text,
                ) from error

            try:
                return response.json()
            except ValueError as error:
                raise PolymarketPayloadError(
                    url=str(response.request.url),
                    message="Response was not valid JSON",
                ) from error

        if last_transport_error is not None:
            raise last_transport_error

        raise PolymarketTransportError(url=url, message="Request failed without a response")

    def _validate_dto(
        self,
        payload: Any,
        dto_type: type[TDto],
        *,
        endpoint: str,
    ) -> TDto:
        try:
            return dto_type.model_validate(payload)
        except ValidationError as error:
            raise PolymarketPayloadError(
                url=endpoint,
                message=str(error),
            ) from error

    def _validate_dto_list(
        self,
        payload: Any,
        dto_type: type[TDto],
        *,
        endpoint: str,
    ) -> list[TDto]:
        if not isinstance(payload, list):
            raise PolymarketPayloadError(
                url=endpoint,
                message="Expected a list payload",
            )

        dtos: list[TDto] = []
        for item in payload:
            dtos.append(self._validate_dto(item, dto_type, endpoint=endpoint))

        return dtos

    def _retry_delay(
        self,
        attempt: int,
        response: httpx.Response | None = None,
    ) -> float:
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after is not None:
                try:
                    return float(retry_after)
                except ValueError:
                    pass

        return self.retry_backoff_seconds * float(2**attempt)


def make_client(
    *,
    timeout: float = 10.0,
    max_retries: int = 2,
    retry_backoff_seconds: float = 0.5,
    transport: httpx.BaseTransport | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> PolymarketRESTClient:
    """Create a Polymarket public REST client."""

    return PolymarketRESTClient(
        timeout=timeout,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        transport=transport,
        sleep=sleep,
    )
