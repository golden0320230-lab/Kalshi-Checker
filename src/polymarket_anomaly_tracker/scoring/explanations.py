"""Explanation payload generation for anomaly scores."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import cast

import pandas as pd

RAW_FEATURE_KEYS = (
    "win_rate",
    "avg_roi",
    "median_roi",
    "realized_pnl_total",
    "early_entry_edge",
    "timing_score",
    "specialization_score",
    "specialization_category",
    "conviction_score",
    "consistency_score",
)

NORMALIZED_FEATURE_KEYS = (
    "normalized_early_entry_edge",
    "normalized_timing_score",
    "normalized_win_rate",
    "normalized_avg_roi",
    "normalized_realized_pnl_percentile",
    "normalized_specialization_score",
    "normalized_conviction_score",
    "normalized_consistency_score",
)

REASON_RAW_FIELD_MAP = {
    "normalized_early_entry_edge": "early_entry_edge",
    "normalized_timing_score": "timing_score",
    "normalized_win_rate": "win_rate",
    "normalized_avg_roi": "avg_roi",
    "normalized_realized_pnl_percentile": "realized_pnl_total",
    "normalized_specialization_score": "specialization_score",
    "normalized_conviction_score": "conviction_score",
    "normalized_consistency_score": "consistency_score",
}


def build_explanation_payloads(score_frame: pd.DataFrame) -> dict[str, dict[str, object]]:
    """Build deterministic explanation payloads for a scored wallet frame."""

    return {
        _normalize_required_string(row["wallet_address"]): build_explanation_payload(row)
        for row in cast(list[dict[str, object]], score_frame.to_dict(orient="records"))
    }


def build_explanation_payload(
    row: Mapping[str, object],
    *,
    reason_threshold: float = 0.75,
    max_reasons: int = 3,
) -> dict[str, object]:
    """Build a machine-readable explanation payload for one scored wallet."""

    reason_details = _build_reason_details(row, reason_threshold=reason_threshold)
    top_reasons = [detail["message"] for detail in reason_details[:max_reasons]]
    threshold_reason_keys = [
        detail["key"] for detail in reason_details if cast(bool, detail["meets_reason_threshold"])
    ]

    return {
        "wallet_address": _normalize_required_string(row["wallet_address"]),
        "display_name": _normalize_optional_string(row.get("display_name")),
        "top_reasons": top_reasons,
        "threshold_reason_keys": threshold_reason_keys,
        "reason_details": reason_details,
        "metrics": {
            "adjusted_score": _normalize_optional_float(row.get("adjusted_score")),
            "composite_score": _normalize_optional_float(row.get("composite_score")),
            "confidence_score": _normalize_optional_float(row.get("confidence_score")),
        },
        "sample_size": {
            "resolved_markets_count": _normalize_required_int(row.get("resolved_markets_count")),
            "trades_count": _normalize_required_int(row.get("trades_count")),
            "recent_trades_count_90d": _normalize_required_int(row.get("recent_trades_count_90d")),
        },
        "eligibility": {
            "score_eligible": _normalize_bool(row.get("score_eligible")),
            "flag_eligible": _normalize_bool(row.get("flag_eligible")),
        },
        "raw_features": {
            key: _serialize_feature_value(row.get(key))
            for key in RAW_FEATURE_KEYS
        },
        "normalized_features": {
            key: _serialize_feature_value(row.get(key))
            for key in NORMALIZED_FEATURE_KEYS
        },
    }


def _build_reason_details(
    row: Mapping[str, object],
    *,
    reason_threshold: float,
) -> list[dict[str, object]]:
    reason_details: list[dict[str, object]] = []
    for normalized_key in NORMALIZED_FEATURE_KEYS:
        normalized_value = _normalize_optional_float(row.get(normalized_key))
        if normalized_value is None:
            continue

        raw_key = REASON_RAW_FIELD_MAP[normalized_key]
        reason_details.append(
            {
                "key": normalized_key,
                "message": _build_reason_message(normalized_key, row),
                "normalized_value": normalized_value,
                "raw_value": _serialize_feature_value(row.get(raw_key)),
                "meets_reason_threshold": normalized_value >= reason_threshold,
            }
        )

    reason_details.sort(
        key=lambda detail: (
            _normalize_optional_float(detail["normalized_value"]) or 0.0,
            _normalize_required_string(detail["key"]),
        ),
        reverse=True,
    )
    return reason_details


def _build_reason_message(normalized_key: str, row: Mapping[str, object]) -> str:
    if normalized_key == "normalized_early_entry_edge":
        return "Repeated favorable early entries before price movement"
    if normalized_key == "normalized_timing_score":
        return "Entries stayed favorable relative to final market resolution"
    if normalized_key == "normalized_win_rate":
        return "Resolved-market win rate ranked near the top of this run"
    if normalized_key == "normalized_avg_roi":
        return "Average ROI ranked near the top of this run"
    if normalized_key == "normalized_realized_pnl_percentile":
        return "Realized PnL ranked near the top of this run"
    if normalized_key == "normalized_specialization_score":
        category = _normalize_optional_string(row.get("specialization_category"))
        if category is not None:
            return f"Unusually strong results in {category} markets"
        return "Results were unusually strong in one market category"
    if normalized_key == "normalized_conviction_score":
        return "Larger sizing tended to align with profitable outcomes"
    return "Positive performance persisted across scoring periods"


def _serialize_feature_value(value: object) -> object:
    normalized_float = _normalize_optional_float(value)
    if normalized_float is not None:
        return normalized_float
    normalized_string = _normalize_optional_string(value)
    if normalized_string is not None:
        return normalized_string
    return None


def _normalize_optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (float, int)):
        return float(value)
    return None


def _normalize_required_int(value: object) -> int:
    normalized_value = _normalize_optional_float(value)
    if normalized_value is None:
        return 0
    return int(normalized_value)


def _normalize_optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        return value
    return None


def _normalize_required_string(value: object) -> str:
    if isinstance(value, str):
        return value
    return str(value)


def _normalize_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return False
