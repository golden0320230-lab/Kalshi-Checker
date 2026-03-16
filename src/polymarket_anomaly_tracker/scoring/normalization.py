"""Normalization helpers for deterministic wallet scoring."""

from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from collections.abc import Mapping

import pandas as pd


def clamp_float(value: float, *, lower: float = 0.0, upper: float = 1.0) -> float:
    """Clamp a float to an inclusive range."""

    return max(lower, min(upper, value))


def percentile_normalize_series(
    series: pd.Series,
    *,
    reference_mask: pd.Series | None = None,
) -> pd.Series:
    """Normalize a numeric series into `[0, 1]` percentiles.

    When the reference distribution is constant, non-null values map to `0.5`
    so the score stays neutral instead of artificially ranking ties at an edge.
    If a reference mask is provided but selects no usable values, the full
    non-null series is used as the fallback reference distribution.
    """

    numeric_series = pd.to_numeric(series, errors="coerce")
    normalized = pd.Series([math.nan] * len(numeric_series), index=numeric_series.index)
    reference_series = _select_reference_series(
        numeric_series,
        reference_mask=reference_mask,
    )
    reference_values = sorted(float(value) for value in reference_series.dropna().tolist())
    if not reference_values:
        return normalized

    for index, value in numeric_series.items():
        if pd.isna(value):
            continue
        normalized.at[index] = _percentile_from_sorted_values(float(value), reference_values)

    return normalized


def add_percentile_normalized_columns(
    frame: pd.DataFrame,
    *,
    column_mapping: Mapping[str, str],
    reference_mask: pd.Series | None = None,
) -> pd.DataFrame:
    """Return a copy of the frame with percentile-normalized columns added."""

    normalized_frame = frame.copy()
    for source_column, normalized_column in column_mapping.items():
        normalized_frame[normalized_column] = percentile_normalize_series(
            normalized_frame[source_column],
            reference_mask=reference_mask,
        )
    return normalized_frame


def _select_reference_series(
    series: pd.Series,
    *,
    reference_mask: pd.Series | None,
) -> pd.Series:
    if reference_mask is None:
        return series

    aligned_mask = reference_mask.reindex(series.index, fill_value=False).astype(bool)
    reference_series = series.loc[aligned_mask]
    if reference_series.dropna().empty:
        return series
    return reference_series


def _percentile_from_sorted_values(value: float, sorted_values: list[float]) -> float:
    if len(sorted_values) == 1:
        return 0.5

    left_index = bisect_left(sorted_values, value)
    right_index = bisect_right(sorted_values, value)
    equal_count = right_index - left_index
    if equal_count > 0:
        percentile = (left_index + 0.5 * (equal_count - 1)) / (len(sorted_values) - 1)
        return clamp_float(percentile)

    if value <= sorted_values[0]:
        return 0.0
    if value >= sorted_values[-1]:
        return 1.0

    percentile = left_index / (len(sorted_values) - 1)
    return clamp_float(percentile)
