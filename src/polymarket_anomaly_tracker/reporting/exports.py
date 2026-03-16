"""CSV and JSON export helpers for local wallet reports."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Literal

from polymarket_anomaly_tracker.reporting.leaderboard_report import TopWalletsReport
from polymarket_anomaly_tracker.reporting.wallet_report import WalletDetailReport

ExportFormat = Literal["csv", "json"]


def export_top_wallets_report(
    report: TopWalletsReport,
    *,
    output_path: Path,
    export_format: ExportFormat,
) -> Path:
    """Write the ranked top-wallets report to CSV or JSON."""

    _ensure_output_parent(output_path)
    if export_format == "json":
        output_path.write_text(
            json.dumps(_build_top_wallets_payload(report), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return output_path

    rows = [
        {
            "rank": row.rank,
            "wallet_address": row.wallet_address,
            "display_name": row.display_name or "",
            "flag_status": row.flag_status,
            "is_flagged": row.is_flagged,
            "adjusted_score": _serialize_float(row.adjusted_score),
            "composite_score": _serialize_float(row.composite_score),
            "confidence_score": _serialize_float(row.confidence_score),
            "resolved_markets_count": row.resolved_markets_count,
            "trades_count": row.trades_count,
            "recent_trades_count_90d": row.recent_trades_count_90d,
            "top_reasons": " | ".join(row.top_reasons),
        }
        for row in report.rows
    ]
    _write_csv(output_path, rows)
    return output_path


def export_wallet_detail_report(
    report: WalletDetailReport,
    *,
    output_path: Path,
    export_format: ExportFormat,
) -> Path:
    """Write a single-wallet drill-down report to CSV or JSON."""

    _ensure_output_parent(output_path)
    payload = _build_wallet_detail_payload(report)
    if export_format == "json":
        output_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return output_path

    flattened_row = {
        "wallet_address": payload["wallet_address"],
        "display_name": payload["display_name"] or "",
        "profile_slug": payload["profile_slug"] or "",
        "flag_status": payload["flag_status"],
        "is_flagged": payload["is_flagged"],
        "first_seen_at": payload["first_seen_at"],
        "last_seen_at": payload["last_seen_at"],
        "watch_status": payload["watch_status"] or "",
        "watch_priority": payload["watch_priority"] or "",
        "watch_added_reason": payload["watch_added_reason"] or "",
        "watch_last_checked_at": payload["watch_last_checked_at"] or "",
        "score_summary_json": json.dumps(payload["score_summary"], sort_keys=True),
        "latest_positions_json": json.dumps(payload["latest_positions"], sort_keys=True),
        "recent_trades_json": json.dumps(payload["recent_trades"], sort_keys=True),
        "recent_closed_positions_json": json.dumps(
            payload["recent_closed_positions"],
            sort_keys=True,
        ),
        "recent_alerts_json": json.dumps(payload["recent_alerts"], sort_keys=True),
    }
    _write_csv(output_path, [flattened_row])
    return output_path


def _build_top_wallets_payload(report: TopWalletsReport) -> dict[str, object]:
    return {
        "as_of_time": report.as_of_time.isoformat(),
        "total_wallets": report.total_wallets,
        "rows": [
            {
                "rank": row.rank,
                "wallet_address": row.wallet_address,
                "display_name": row.display_name,
                "flag_status": row.flag_status,
                "is_flagged": row.is_flagged,
                "adjusted_score": row.adjusted_score,
                "composite_score": row.composite_score,
                "confidence_score": row.confidence_score,
                "resolved_markets_count": row.resolved_markets_count,
                "trades_count": row.trades_count,
                "recent_trades_count_90d": row.recent_trades_count_90d,
                "top_reasons": list(row.top_reasons),
            }
            for row in report.rows
        ],
    }


def _build_wallet_detail_payload(report: WalletDetailReport) -> dict[str, object]:
    return {
        "wallet_address": report.wallet_address,
        "display_name": report.display_name,
        "profile_slug": report.profile_slug,
        "first_seen_at": report.first_seen_at.isoformat(),
        "last_seen_at": report.last_seen_at.isoformat(),
        "flag_status": report.flag_status,
        "is_flagged": report.is_flagged,
        "notes": report.notes,
        "watch_status": report.watch_status,
        "watch_priority": report.watch_priority,
        "watch_added_reason": report.watch_added_reason,
        "watch_last_checked_at": (
            None
            if report.watch_last_checked_at is None
            else report.watch_last_checked_at.isoformat()
        ),
        "score_summary": _build_score_summary_payload(report),
        "latest_positions": [
            {
                "market_id": row.market_id,
                "market_question": row.market_question,
                "outcome": row.outcome,
                "quantity": row.quantity,
                "avg_entry_price": row.avg_entry_price,
                "current_value": row.current_value,
                "unrealized_pnl": row.unrealized_pnl,
                "realized_pnl": row.realized_pnl,
                "snapshot_time": row.snapshot_time.isoformat(),
                "status": row.status,
            }
            for row in report.latest_positions
        ],
        "recent_trades": [
            {
                "trade_time": row.trade_time.isoformat(),
                "market_id": row.market_id,
                "market_question": row.market_question,
                "outcome": row.outcome,
                "side": row.side,
                "price": row.price,
                "size": row.size,
                "notional": row.notional,
            }
            for row in report.recent_trades
        ],
        "recent_closed_positions": [
            {
                "closed_at": None if row.closed_at is None else row.closed_at.isoformat(),
                "market_id": row.market_id,
                "market_question": row.market_question,
                "outcome": row.outcome,
                "quantity": row.quantity,
                "realized_pnl": row.realized_pnl,
                "roi": row.roi,
            }
            for row in report.recent_closed_positions
        ],
        "recent_alerts": [
            {
                "detected_at": row.detected_at.isoformat(),
                "alert_type": row.alert_type,
                "severity": row.severity,
                "market_id": row.market_id,
                "market_question": row.market_question,
                "summary": row.summary,
            }
            for row in report.recent_alerts
        ],
    }


def _build_score_summary_payload(report: WalletDetailReport) -> dict[str, object] | None:
    if report.score_summary is None:
        return None
    return {
        "as_of_time": report.score_summary.as_of_time.isoformat(),
        "adjusted_score": report.score_summary.adjusted_score,
        "composite_score": report.score_summary.composite_score,
        "confidence_score": report.score_summary.confidence_score,
        "resolved_markets_count": report.score_summary.resolved_markets_count,
        "trades_count": report.score_summary.trades_count,
        "recent_trades_count_90d": report.score_summary.recent_trades_count_90d,
        "top_reasons": list(report.score_summary.top_reasons),
        "threshold_reason_keys": list(report.score_summary.threshold_reason_keys),
        "raw_features": report.score_summary.raw_features,
        "normalized_features": report.score_summary.normalized_features,
    }


def _ensure_output_parent(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)


def _write_csv(output_path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    with output_path.open("w", encoding="utf-8", newline="") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _serialize_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.6f}"
