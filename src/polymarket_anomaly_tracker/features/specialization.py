"""Category specialization feature computation."""

from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

from polymarket_anomaly_tracker.features.base import WalletAnalysisDataset, build_wallet_index_frame

SPECIALIZATION_FEATURE_COLUMNS = (
    "wallet_address",
    "display_name",
    "specialization_score",
    "specialization_category",
)


@dataclass(frozen=True)
class WalletSpecializationFeatures:
    """Category specialization score and the strongest category, if any."""

    wallet_address: str
    display_name: str | None
    specialization_score: float | None
    specialization_category: str | None


@dataclass(frozen=True)
class _CategoryAggregate:
    wins: int
    markets: int


def compute_specialization_feature_frame(
    dataset: WalletAnalysisDataset,
    *,
    min_category_markets: int = 3,
    min_peer_markets: int = 1,
) -> pd.DataFrame:
    """Compute per-wallet category edge versus a peer baseline excluding self."""

    wallet_index = build_wallet_index_frame(dataset)
    if wallet_index.empty:
        return pd.DataFrame(columns=list(SPECIALIZATION_FEATURE_COLUMNS))

    category_lookup = {
        str(row["market_id"]): _normalize_optional_string(row.get("category"))
        for row in dataset.markets.to_dict(orient="records")
    }
    wallet_category_market_rows = _build_wallet_category_market_rows(dataset, category_lookup)
    category_aggregate_totals = _build_category_aggregate_totals(wallet_category_market_rows)

    specialization_results = {
        wallet_address: _compute_wallet_specialization(
            wallet_category_market_rows=wallet_category_market_rows,
            category_aggregate_totals=category_aggregate_totals,
            wallet_address=wallet_address,
            min_category_markets=min_category_markets,
            min_peer_markets=min_peer_markets,
        )
        for wallet_address in wallet_index["wallet_address"].tolist()
    }

    feature_frame = wallet_index.copy()
    feature_frame["specialization_score"] = [
        specialization_results[wallet_address][0]
        for wallet_address in feature_frame["wallet_address"].tolist()
    ]
    feature_frame["specialization_category"] = [
        specialization_results[wallet_address][1]
        for wallet_address in feature_frame["wallet_address"].tolist()
    ]
    return feature_frame.loc[:, list(SPECIALIZATION_FEATURE_COLUMNS)]


def compute_specialization_features(
    dataset: WalletAnalysisDataset,
    *,
    min_category_markets: int = 3,
    min_peer_markets: int = 1,
) -> list[WalletSpecializationFeatures]:
    """Compute specialization scores as typed records."""

    feature_frame = compute_specialization_feature_frame(
        dataset,
        min_category_markets=min_category_markets,
        min_peer_markets=min_peer_markets,
    )
    return [
        WalletSpecializationFeatures(
            wallet_address=str(row["wallet_address"]),
            display_name=_normalize_optional_string(row["display_name"]),
            specialization_score=_normalize_optional_float(row["specialization_score"]),
            specialization_category=_normalize_optional_string(row["specialization_category"]),
        )
        for row in feature_frame.to_dict(orient="records")
    ]


def _build_wallet_category_market_rows(
    dataset: WalletAnalysisDataset,
    category_lookup: dict[str, str | None],
) -> list[dict[str, object]]:
    market_results: dict[tuple[str, str], float] = {}
    for row in dataset.closed_positions.to_dict(orient="records"):
        market_key = (str(row["wallet_address"]), str(row["market_id"]))
        market_results[market_key] = market_results.get(market_key, 0.0) + (
            _normalize_optional_float(row["realized_pnl"]) or 0.0
        )

    rows: list[dict[str, object]] = []
    for (wallet_address, market_id), realized_pnl in market_results.items():
        category = category_lookup.get(market_id)
        if category is None:
            continue
        rows.append(
            {
                "wallet_address": wallet_address,
                "market_id": market_id,
                "category": category,
                "won_market": realized_pnl > 0,
            }
        )
    return rows


def _build_category_aggregate_totals(
    wallet_category_market_rows: list[dict[str, object]],
) -> dict[str, _CategoryAggregate]:
    category_totals: dict[str, _CategoryAggregate] = {}
    for row in wallet_category_market_rows:
        category = str(row["category"])
        existing_totals = category_totals.get(category, _CategoryAggregate(wins=0, markets=0))
        category_totals[category] = _CategoryAggregate(
            wins=existing_totals.wins + int(bool(row["won_market"])),
            markets=existing_totals.markets + 1,
        )
    return category_totals


def _compute_wallet_specialization(
    *,
    wallet_category_market_rows: list[dict[str, object]],
    category_aggregate_totals: dict[str, _CategoryAggregate],
    wallet_address: str,
    min_category_markets: int,
    min_peer_markets: int,
) -> tuple[float | None, str | None]:
    wallet_category_totals: dict[str, _CategoryAggregate] = {}
    for row in wallet_category_market_rows:
        if row["wallet_address"] != wallet_address:
            continue
        category = str(row["category"])
        existing_totals = wallet_category_totals.get(
            category,
            _CategoryAggregate(wins=0, markets=0),
        )
        wallet_category_totals[category] = _CategoryAggregate(
            wins=existing_totals.wins + int(bool(row["won_market"])),
            markets=existing_totals.markets + 1,
        )

    best_category: str | None = None
    best_edge: float | None = None
    for category, wallet_totals in wallet_category_totals.items():
        if wallet_totals.markets < min_category_markets:
            continue

        category_totals = category_aggregate_totals.get(category)
        if category_totals is None:
            continue

        peer_wins = category_totals.wins - wallet_totals.wins
        peer_markets = category_totals.markets - wallet_totals.markets
        if peer_markets < min_peer_markets:
            continue

        category_win_rate = wallet_totals.wins / wallet_totals.markets
        peer_win_rate = peer_wins / peer_markets
        category_edge = category_win_rate - peer_win_rate
        if best_edge is None or category_edge > best_edge:
            best_edge = category_edge
            best_category = category

    return best_edge, best_category


def _normalize_optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (float, int)):
        return float(value)
    return None


def _normalize_optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        return value
    return None
