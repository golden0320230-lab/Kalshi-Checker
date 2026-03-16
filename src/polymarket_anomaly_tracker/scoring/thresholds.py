"""Deterministic thresholds for candidate and flagged wallet classification."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class ClassificationThresholds:
    """Thresholds used to classify wallets from the latest scoring run."""

    score_eligible_min_resolved_markets: int = 5
    score_eligible_min_trades: int = 10
    flag_eligible_min_resolved_markets: int = 8
    flag_eligible_min_trades: int = 20
    flag_eligible_min_confidence_score: float = 0.50
    candidate_adjusted_score: float = 0.70
    candidate_top_fraction: float = 0.10
    flagged_adjusted_score: float = 0.80
    flagged_top_fraction: float = 0.05
    flagged_min_reason_count: int = 2


def is_score_eligible(
    *,
    resolved_markets_count: int,
    trades_count: int,
    thresholds: ClassificationThresholds,
) -> bool:
    """Return whether a wallet is eligible for candidate scoring."""

    return (
        resolved_markets_count >= thresholds.score_eligible_min_resolved_markets
        and trades_count >= thresholds.score_eligible_min_trades
    )


def is_flag_eligible(
    *,
    resolved_markets_count: int,
    trades_count: int,
    confidence_score: float | None,
    thresholds: ClassificationThresholds,
) -> bool:
    """Return whether a wallet is eligible for flagged promotion."""

    return (
        resolved_markets_count >= thresholds.flag_eligible_min_resolved_markets
        and trades_count >= thresholds.flag_eligible_min_trades
        and confidence_score is not None
        and confidence_score >= thresholds.flag_eligible_min_confidence_score
    )


def top_fraction_rank_cutoff(total_wallets: int, top_fraction: float) -> int:
    """Return the inclusive rank cutoff for a top-fraction rule."""

    if total_wallets <= 0:
        return 0
    return max(1, math.ceil(total_wallets * top_fraction))


def is_within_top_fraction(*, rank: int, total_wallets: int, top_fraction: float) -> bool:
    """Return whether the rank falls inside the requested top fraction."""

    return rank <= top_fraction_rank_cutoff(total_wallets, top_fraction)
