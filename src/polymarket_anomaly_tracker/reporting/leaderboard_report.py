"""Ranked wallet reporting helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

from polymarket_anomaly_tracker.db.repositories import DatabaseRepository


@dataclass(frozen=True)
class TopWalletReportRow:
    """One wallet row in the ranked top-wallets report."""

    rank: int
    wallet_address: str
    display_name: str | None
    flag_status: str
    is_flagged: bool
    adjusted_score: float | None
    composite_score: float | None
    confidence_score: float | None
    resolved_markets_count: int
    trades_count: int
    recent_trades_count_90d: int
    top_reasons: tuple[str, ...]


@dataclass(frozen=True)
class TopWalletsReport:
    """A full ranked leaderboard report for the latest scoring run."""

    as_of_time: datetime
    rows: tuple[TopWalletReportRow, ...]
    total_wallets: int


def build_top_wallets_report(
    repository: DatabaseRepository,
    *,
    limit: int = 20,
    min_adjusted_score: float | None = None,
) -> TopWalletsReport:
    """Build the latest ranked wallet report from persisted feature snapshots."""

    if limit < 1:
        msg = "Top-wallets report limit must be at least 1"
        raise ValueError(msg)

    latest_as_of_time = repository.get_latest_feature_snapshot_time()
    if latest_as_of_time is None:
        raise RuntimeError("No wallet feature snapshots found. Run scoring first.")

    snapshot_rows = repository.list_wallet_feature_snapshot_rows(as_of_time=latest_as_of_time)
    report_rows: list[TopWalletReportRow] = []
    for snapshot_row in snapshot_rows:
        adjusted_score = snapshot_row.adjusted_score
        if min_adjusted_score is not None and (
            adjusted_score is None or adjusted_score < min_adjusted_score
        ):
            continue

        explanation_payload = _load_explanation_payload(snapshot_row.explanations_json)
        sample_size = _normalize_mapping(explanation_payload.get("sample_size"))
        top_reasons = _normalize_string_list(explanation_payload.get("top_reasons"))
        report_rows.append(
            TopWalletReportRow(
                rank=len(report_rows) + 1,
                wallet_address=snapshot_row.wallet_address,
                display_name=snapshot_row.display_name,
                flag_status=snapshot_row.flag_status,
                is_flagged=snapshot_row.is_flagged,
                adjusted_score=snapshot_row.adjusted_score,
                composite_score=snapshot_row.composite_score,
                confidence_score=snapshot_row.confidence_score,
                resolved_markets_count=snapshot_row.resolved_markets_count,
                trades_count=snapshot_row.trades_count,
                recent_trades_count_90d=_normalize_int(
                    sample_size.get("recent_trades_count_90d")
                ),
                top_reasons=tuple(top_reasons[:3]),
            )
        )
        if len(report_rows) >= limit:
            break

    return TopWalletsReport(
        as_of_time=latest_as_of_time,
        rows=tuple(report_rows),
        total_wallets=len(snapshot_rows),
    )


def _load_explanation_payload(explanations_json: str) -> dict[str, object]:
    try:
        payload = json.loads(explanations_json)
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _normalize_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _normalize_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _normalize_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items()}
