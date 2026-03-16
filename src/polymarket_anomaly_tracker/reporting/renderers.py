"""Rich renderers for local reports."""

from __future__ import annotations

from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from polymarket_anomaly_tracker.reporting.leaderboard_report import TopWalletsReport
from polymarket_anomaly_tracker.reporting.wallet_report import WalletDetailReport


def render_top_wallets_report(report: TopWalletsReport) -> Table | Group:
    """Render the ranked wallet report as a Rich table."""

    table = Table(
        title=f"Top Wallets as of {report.as_of_time.isoformat()}",
        expand=True,
        show_lines=False,
    )
    table.add_column("Rank", justify="right")
    table.add_column("Wallet", no_wrap=True, min_width=12)
    table.add_column("Flag")
    table.add_column("Adj", justify="right")
    table.add_column("Conf", justify="right")
    table.add_column("Markets", justify="right")
    table.add_column("Trades", justify="right")
    table.add_column("Recent90d", justify="right")
    table.add_column("Top Reasons", overflow="fold")

    for row in report.rows:
        wallet_label = row.display_name or row.wallet_address
        table.add_row(
            str(row.rank),
            wallet_label,
            row.flag_status,
            _format_optional_float(row.adjusted_score),
            _format_optional_float(row.confidence_score),
            str(row.resolved_markets_count),
            str(row.trades_count),
            str(row.recent_trades_count_90d),
            _format_top_reasons_summary(row.top_reasons),
        )

    if not report.rows:
        return table

    reason_summary = Text(
        "\n".join(
            (
                f"{row.rank}. {(row.display_name or row.wallet_address)}: {row.top_reasons[0]}"
                if row.top_reasons
                else f"{row.rank}. {(row.display_name or row.wallet_address)}: No reasons available"
            )
            for row in report.rows
        )
    )
    return Group(table, Panel(reason_summary, title="Primary Reasons"))


def render_wallet_detail_report(report: WalletDetailReport) -> Group:
    """Render a single-wallet drill-down using Rich tables and panels."""

    summary_table = Table(show_header=False, box=None)
    summary_table.add_column("Field", style="bold")
    summary_table.add_column("Value")
    summary_table.add_row("Wallet", report.wallet_address)
    summary_table.add_row("Display", report.display_name or "-")
    summary_table.add_row("Flag status", report.flag_status)
    summary_table.add_row("Flagged", "yes" if report.is_flagged else "no")
    summary_table.add_row("First seen", report.first_seen_at.isoformat())
    summary_table.add_row("Last seen", report.last_seen_at.isoformat())
    summary_table.add_row("Profile slug", report.profile_slug or "-")
    summary_table.add_row("Watch status", report.watch_status or "-")
    summary_table.add_row(
        "Watch checked",
        "-" if report.watch_last_checked_at is None else report.watch_last_checked_at.isoformat(),
    )

    score_table = Table(show_header=False, box=None)
    score_table.add_column("Field", style="bold")
    score_table.add_column("Value")
    if report.score_summary is None:
        score_table.add_row("Score", "No scoring snapshot available")
    else:
        score_table.add_row("As of", report.score_summary.as_of_time.isoformat())
        score_table.add_row("Adjusted", _format_optional_float(report.score_summary.adjusted_score))
        score_table.add_row(
            "Composite",
            _format_optional_float(report.score_summary.composite_score),
        )
        score_table.add_row(
            "Confidence",
            _format_optional_float(report.score_summary.confidence_score),
        )
        score_table.add_row(
            "Resolved markets",
            str(report.score_summary.resolved_markets_count),
        )
        score_table.add_row("Trades", str(report.score_summary.trades_count))
        score_table.add_row(
            "Recent trades 90d",
            str(report.score_summary.recent_trades_count_90d),
        )
        score_table.add_row(
            "Top reasons",
            " | ".join(report.score_summary.top_reasons)
            if report.score_summary.top_reasons
            else "No reasons available",
        )

    positions_table = Table(title="Latest Positions")
    positions_table.add_column("Market")
    positions_table.add_column("Outcome")
    positions_table.add_column("Qty", justify="right")
    positions_table.add_column("Value", justify="right")
    positions_table.add_column("Unrealized", justify="right")
    if report.latest_positions:
        for position_row in report.latest_positions:
            positions_table.add_row(
                position_row.market_question,
                position_row.outcome,
                _format_optional_float(position_row.quantity),
                _format_optional_float(position_row.current_value),
                _format_optional_float(position_row.unrealized_pnl),
            )
    else:
        positions_table.add_row("No open positions", "-", "-", "-", "-")

    trades_table = Table(title="Recent Trades")
    trades_table.add_column("Time")
    trades_table.add_column("Market")
    trades_table.add_column("Side")
    trades_table.add_column("Outcome")
    trades_table.add_column("Notional", justify="right")
    if report.recent_trades:
        for trade_row in report.recent_trades:
            trades_table.add_row(
                trade_row.trade_time.isoformat(),
                trade_row.market_question,
                trade_row.side,
                trade_row.outcome,
                _format_optional_float(trade_row.notional),
            )
    else:
        trades_table.add_row("No trades", "-", "-", "-", "-")

    closed_positions_table = Table(title="Recent Closed Positions")
    closed_positions_table.add_column("Closed At")
    closed_positions_table.add_column("Market")
    closed_positions_table.add_column("Outcome")
    closed_positions_table.add_column("PnL", justify="right")
    closed_positions_table.add_column("ROI", justify="right")
    if report.recent_closed_positions:
        for closed_position_row in report.recent_closed_positions:
            closed_positions_table.add_row(
                "-"
                if closed_position_row.closed_at is None
                else closed_position_row.closed_at.isoformat(),
                closed_position_row.market_question,
                closed_position_row.outcome,
                _format_optional_float(closed_position_row.realized_pnl),
                _format_optional_float(closed_position_row.roi),
            )
    else:
        closed_positions_table.add_row("No closed positions", "-", "-", "-", "-")

    alerts_table = Table(title="Recent Alerts")
    alerts_table.add_column("Time")
    alerts_table.add_column("Type")
    alerts_table.add_column("Severity")
    alerts_table.add_column("Summary")
    if report.recent_alerts:
        for alert_row in report.recent_alerts:
            alerts_table.add_row(
                alert_row.detected_at.isoformat(),
                alert_row.alert_type,
                alert_row.severity,
                alert_row.summary,
            )
    else:
        alerts_table.add_row("No alerts", "-", "-", "-")

    notes_text = Text(report.notes or report.watch_added_reason or "No notes available")
    return Group(
        Panel(summary_table, title="Wallet Summary"),
        Panel(score_table, title="Score Context"),
        Panel(notes_text, title="Notes"),
        positions_table,
        trades_table,
        closed_positions_table,
        alerts_table,
    )


def _format_optional_float(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.3f}"


def _format_top_reasons_summary(top_reasons: tuple[str, ...]) -> str:
    if not top_reasons:
        return "No reasons available"
    if len(top_reasons) == 1:
        return top_reasons[0]
    return f"{top_reasons[0]} (+{len(top_reasons) - 1} more)"
