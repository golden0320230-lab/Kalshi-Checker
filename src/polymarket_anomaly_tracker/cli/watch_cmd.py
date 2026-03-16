"""CLI commands for finite watch-mode monitoring and alert emission."""

from __future__ import annotations

import typer
from rich.console import Console

from polymarket_anomaly_tracker.config import get_settings
from polymarket_anomaly_tracker.tracking.monitor import WatchMonitorError, run_watch_monitor

console = Console()
watch_app = typer.Typer(help="Monitor flagged wallets and emit local alerts.")


@watch_app.command("run")
def watch_run_command(
    interval_seconds: float = typer.Option(
        60.0,
        "--interval-seconds",
        min=0.0,
        help="Seconds to sleep between finite watch cycles.",
    ),
    max_cycles: int = typer.Option(
        1,
        "--max-cycles",
        min=1,
        help="Number of watch cycles to run before exiting.",
    ),
) -> None:
    """Run finite watch cycles for all active watchlist entries."""

    settings = get_settings()
    try:
        result = run_watch_monitor(
            database_url=settings.database_url,
            interval_seconds=interval_seconds,
            max_cycles=max_cycles,
        )
    except (ValueError, WatchMonitorError) as error:
        console.print(f"Watch monitor failed: {error}")
        raise typer.Exit(code=1) from error

    console.print(
        "Ran watch monitor. "
        f"Cycles: {result.cycles_completed}. "
        f"Wallet checks: {result.wallet_checks_requested}. "
        f"Succeeded: {result.wallet_checks_succeeded}. "
        f"Failed: {result.wallet_checks_failed}. "
        f"Alerts: {result.alerts_written}. "
        f"Opened: {result.opened_alerts}. "
        f"Increased: {result.increased_alerts}. "
        f"Decreased: {result.decreased_alerts}. "
        f"Closed: {result.closed_alerts}."
    )
