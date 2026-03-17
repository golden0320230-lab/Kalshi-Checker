"""Validated DTOs for Polymarket public REST responses."""

from __future__ import annotations

from datetime import datetime

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class PolymarketBaseDto(BaseModel):
    """Base DTO with permissive parsing for API shape drift."""

    model_config = ConfigDict(
        extra="ignore",
        populate_by_name=True,
    )


class TraderLeaderboardEntryDto(PolymarketBaseDto):
    """Trader leaderboard entry returned by the Data API."""

    rank: str
    proxy_wallet: str = Field(alias="proxyWallet")
    user_name: str | None = Field(default=None, alias="userName")
    vol: float | None = None
    pnl: float | None = None
    profile_image: str | None = Field(default=None, alias="profileImage")
    x_username: str | None = Field(default=None, alias="xUsername")


class PublicProfileUserDto(PolymarketBaseDto):
    """Linked user info for a public profile."""

    id: str | None = None
    creator: bool | None = None
    mod: bool | None = None


class PublicProfileDto(PolymarketBaseDto):
    """Public wallet profile from the Gamma API."""

    created_at: datetime | None = Field(default=None, alias="createdAt")
    proxy_wallet: str | None = Field(default=None, alias="proxyWallet")
    profile_image: str | None = Field(default=None, alias="profileImage")
    display_username_public: bool | None = Field(
        default=None,
        alias="displayUsernamePublic",
    )
    bio: str | None = None
    pseudonym: str | None = None
    name: str | None = None
    users: tuple[PublicProfileUserDto, ...] | None = None
    x_username: str | None = Field(default=None, alias="xUsername")
    verified_badge: bool | None = Field(default=None, alias="verifiedBadge")


class CurrentPositionDto(PolymarketBaseDto):
    """Current open position row from the Data API."""

    proxy_wallet: str = Field(alias="proxyWallet")
    asset: str
    condition_id: str = Field(alias="conditionId")
    size: float
    avg_price: float | None = Field(default=None, alias="avgPrice")
    initial_value: float | None = Field(default=None, alias="initialValue")
    current_value: float | None = Field(default=None, alias="currentValue")
    cash_pnl: float | None = Field(default=None, alias="cashPnl")
    percent_pnl: float | None = Field(default=None, alias="percentPnl")
    total_bought: float | None = Field(default=None, alias="totalBought")
    realized_pnl: float | None = Field(default=None, alias="realizedPnl")
    percent_realized_pnl: float | None = Field(
        default=None,
        alias="percentRealizedPnl",
    )
    cur_price: float | None = Field(default=None, alias="curPrice")
    redeemable: bool | None = None
    mergeable: bool | None = None
    title: str | None = None
    slug: str | None = None
    icon: str | None = None
    event_slug: str | None = Field(default=None, alias="eventSlug")
    outcome: str | None = None
    outcome_index: int | None = Field(default=None, alias="outcomeIndex")


class ClosedPositionDto(PolymarketBaseDto):
    """Closed position row from the Data API."""

    proxy_wallet: str = Field(alias="proxyWallet")
    asset: str
    condition_id: str = Field(alias="conditionId")
    avg_price: float | None = Field(default=None, alias="avgPrice")
    total_bought: float | None = Field(default=None, alias="totalBought")
    realized_pnl: float | None = Field(default=None, alias="realizedPnl")
    cur_price: float | None = Field(default=None, alias="curPrice")
    timestamp: int
    title: str | None = None
    slug: str | None = None
    icon: str | None = None
    event_slug: str | None = Field(default=None, alias="eventSlug")
    outcome: str | None = None
    outcome_index: int | None = Field(default=None, alias="outcomeIndex")
    opposite_outcome: str | None = Field(default=None, alias="oppositeOutcome")
    opposite_asset: str | None = Field(default=None, alias="oppositeAsset")
    end_date: datetime | None = Field(default=None, alias="endDate")


class UserActivityDto(PolymarketBaseDto):
    """User activity row from the Data API."""

    proxy_wallet: str = Field(alias="proxyWallet")
    timestamp: int
    condition_id: str = Field(alias="conditionId")
    type: str
    size: float | None = None
    usdc_size: float | None = Field(default=None, alias="usdcSize")
    transaction_hash: str | None = Field(default=None, alias="transactionHash")
    price: float | None = None
    asset: str | None = None
    side: str | None = None
    outcome_index: int | None = Field(default=None, alias="outcomeIndex")
    title: str | None = None
    slug: str | None = None
    icon: str | None = None
    event_slug: str | None = Field(default=None, alias="eventSlug")
    outcome: str | None = None
    name: str | None = None
    pseudonym: str | None = None
    bio: str | None = None
    profile_image: str | None = Field(default=None, alias="profileImage")
    profile_image_optimized: str | None = Field(
        default=None,
        alias="profileImageOptimized",
    )


class UserTradeDto(PolymarketBaseDto):
    """User trade row from the Data API."""

    proxy_wallet: str = Field(alias="proxyWallet")
    side: str
    asset: str
    condition_id: str = Field(alias="conditionId")
    size: float
    price: float
    timestamp: int
    title: str | None = None
    slug: str | None = None
    icon: str | None = None
    event_slug: str | None = Field(default=None, alias="eventSlug")
    outcome: str | None = None
    outcome_index: int | None = Field(default=None, alias="outcomeIndex")
    name: str | None = None
    pseudonym: str | None = None
    bio: str | None = None
    profile_image: str | None = Field(default=None, alias="profileImage")
    profile_image_optimized: str | None = Field(
        default=None,
        alias="profileImageOptimized",
    )
    transaction_hash: str | None = Field(default=None, alias="transactionHash")


class MarketDto(PolymarketBaseDto):
    """Market DTO from the Gamma API."""

    id: str
    question: str
    condition_id: str | None = Field(default=None, alias="conditionId")
    slug: str | None = None
    category: str | None = None
    end_date: datetime | None = Field(default=None, alias="endDate")
    start_date: datetime | None = Field(default=None, alias="startDate")
    liquidity: float | None = None
    volume: float | None = None
    best_bid: float | None = Field(default=None, alias="bestBid")
    best_ask: float | None = Field(default=None, alias="bestAsk")
    last_price: float | None = Field(
        default=None,
        validation_alias=AliasChoices("lastPrice", "lastTradePrice"),
    )
    active: bool | None = None
    closed: bool | None = None
    archived: bool | None = None
    image: str | None = None
    icon: str | None = None
    market_type: str | None = Field(default=None, alias="marketType")
    description: str | None = None
    events: tuple[dict[str, object], ...] | None = None
