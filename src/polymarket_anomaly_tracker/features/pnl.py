"""Core PnL-oriented wallet feature computation."""

from __future__ import annotations

import math
from typing import cast

import pandas as pd

from polymarket_anomaly_tracker.features.base import (
    CORE_PNL_FEATURE_COLUMNS,
    WalletAnalysisDataset,
    WalletCorePnLFeatures,
    empty_frame,
)


def compute_core_pnl_feature_frame(dataset: WalletAnalysisDataset) -> pd.DataFrame:
    """Compute core wallet-level PnL features from the assembled dataset."""

    wallet_index = _build_wallet_index_frame(dataset)
    if wallet_index.empty:
        return empty_frame(CORE_PNL_FEATURE_COLUMNS)

    feature_frame = wallet_index.copy()
    trade_counts = _build_trade_counts(dataset)
    resolved_market_summary = _build_resolved_market_summary(dataset)
    roi_summary = _build_roi_summary(dataset)
    realized_pnl_summary = _build_realized_pnl_summary(dataset)

    for summary_frame in (
        trade_counts,
        resolved_market_summary,
        roi_summary,
        realized_pnl_summary,
    ):
        if not summary_frame.empty:
            feature_frame = feature_frame.merge(summary_frame, on="wallet_address", how="left")

    feature_frame["trades_count"] = feature_frame["trades_count"].fillna(0).astype("int64")
    feature_frame["resolved_markets_count"] = (
        feature_frame["resolved_markets_count"].fillna(0).astype("int64")
    )
    feature_frame = feature_frame.loc[:, list(CORE_PNL_FEATURE_COLUMNS)]
    return feature_frame.sort_values("wallet_address").reset_index(drop=True)


def compute_core_pnl_features(dataset: WalletAnalysisDataset) -> list[WalletCorePnLFeatures]:
    """Compute core PnL features as strongly typed records."""

    feature_frame = compute_core_pnl_feature_frame(dataset)
    feature_records: list[WalletCorePnLFeatures] = []
    for row in feature_frame.to_dict(orient="records"):
        wallet_address = cast(str, row["wallet_address"])
        display_name = cast(str | None, row["display_name"])
        resolved_markets_count = int(cast(int | float, row["resolved_markets_count"]))
        trades_count = int(cast(int | float, row["trades_count"]))
        feature_records.append(
            WalletCorePnLFeatures(
                wallet_address=wallet_address,
                display_name=_normalize_optional_string(display_name),
                resolved_markets_count=resolved_markets_count,
                trades_count=trades_count,
                win_rate=_normalize_optional_float(row["win_rate"]),
                avg_roi=_normalize_optional_float(row["avg_roi"]),
                median_roi=_normalize_optional_float(row["median_roi"]),
                realized_pnl_total=_normalize_optional_float(row["realized_pnl_total"]),
            )
        )
    return feature_records


def _build_wallet_index_frame(dataset: WalletAnalysisDataset) -> pd.DataFrame:
    if dataset.wallets.empty and dataset.trades.empty and dataset.closed_positions.empty:
        return empty_frame(("wallet_address", "display_name"))

    wallet_index = empty_frame(("wallet_address", "display_name"))
    if not dataset.wallets.empty:
        wallet_index = dataset.wallets.loc[:, ["wallet_address", "display_name"]].copy()

    fallback_addresses = sorted(
        {
            *dataset.trades.get("wallet_address", pd.Series(dtype="object")).dropna().tolist(),
            *dataset.closed_positions.get("wallet_address", pd.Series(dtype="object"))
            .dropna()
            .tolist(),
        }
    )
    if wallet_index.empty:
        wallet_index = pd.DataFrame(
            {
                "wallet_address": fallback_addresses,
                "display_name": [None] * len(fallback_addresses),
            }
        )
    else:
        existing_addresses = set(wallet_index["wallet_address"].tolist())
        missing_addresses = [
            wallet_address
            for wallet_address in fallback_addresses
            if wallet_address not in existing_addresses
        ]
        if missing_addresses:
            wallet_index = pd.concat(
                [
                    wallet_index,
                    pd.DataFrame(
                        {
                            "wallet_address": missing_addresses,
                            "display_name": [None] * len(missing_addresses),
                        }
                    ),
                ],
                ignore_index=True,
            )

    return wallet_index.sort_values("wallet_address").reset_index(drop=True)


def _build_trade_counts(dataset: WalletAnalysisDataset) -> pd.DataFrame:
    if dataset.trades.empty:
        return empty_frame(("wallet_address", "trades_count"))

    return (
        dataset.trades.groupby("wallet_address")
        .size()
        .rename("trades_count")
        .reset_index()
    )


def _build_resolved_market_summary(dataset: WalletAnalysisDataset) -> pd.DataFrame:
    if dataset.closed_positions.empty:
        return empty_frame(("wallet_address", "resolved_markets_count", "win_rate"))

    market_pnl_series = dataset.closed_positions.groupby(["wallet_address", "market_id"])[
        "realized_pnl"
    ].sum(min_count=1)
    market_pnl = market_pnl_series.to_frame(name="market_realized_pnl").reset_index()
    market_pnl["won_market"] = market_pnl["market_realized_pnl"].gt(0)
    return market_pnl.groupby("wallet_address", as_index=False).agg(
        resolved_markets_count=("market_id", "nunique"),
        win_rate=("won_market", "mean"),
    )


def _build_roi_summary(dataset: WalletAnalysisDataset) -> pd.DataFrame:
    if dataset.closed_positions.empty:
        return empty_frame(("wallet_address", "avg_roi", "median_roi"))

    roi_frame = dataset.closed_positions.dropna(subset=["roi"])
    if roi_frame.empty:
        return empty_frame(("wallet_address", "avg_roi", "median_roi"))

    return roi_frame.groupby("wallet_address", as_index=False).agg(
        avg_roi=("roi", "mean"),
        median_roi=("roi", "median"),
    )


def _build_realized_pnl_summary(dataset: WalletAnalysisDataset) -> pd.DataFrame:
    if dataset.closed_positions.empty:
        return empty_frame(("wallet_address", "realized_pnl_total"))

    realized_pnl_series = dataset.closed_positions.groupby("wallet_address")["realized_pnl"].sum(
        min_count=1
    )
    return realized_pnl_series.to_frame(name="realized_pnl_total").reset_index()


def _normalize_optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return float(cast(float | int, value))


def _normalize_optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return cast(str, value)
