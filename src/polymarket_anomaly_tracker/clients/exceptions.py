"""Typed exceptions for Polymarket public REST client failures."""

from __future__ import annotations


class PolymarketClientError(Exception):
    """Base exception for Polymarket client failures."""


class PolymarketHTTPError(PolymarketClientError):
    """Raised when the API returns an HTTP error response."""

    def __init__(self, *, status_code: int, url: str, message: str):
        self.status_code = status_code
        self.url = url
        self.message = message
        super().__init__(f"HTTP {status_code} for {url}: {message}")


class PolymarketNotFoundError(PolymarketHTTPError):
    """Raised when a requested public resource is not found."""


class PolymarketRateLimitError(PolymarketHTTPError):
    """Raised when the API keeps returning 429 after retries."""


class PolymarketTransportError(PolymarketClientError):
    """Raised when the request cannot be completed over the network."""

    def __init__(self, *, url: str, message: str):
        self.url = url
        self.message = message
        super().__init__(f"Transport error for {url}: {message}")


class PolymarketPayloadError(PolymarketClientError):
    """Raised when the API payload is malformed or incompatible."""

    def __init__(self, *, url: str, message: str):
        self.url = url
        self.message = message
        super().__init__(f"Payload error for {url}: {message}")

