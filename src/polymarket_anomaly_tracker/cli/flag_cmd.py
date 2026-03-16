"""CLI commands for candidate/flagged wallet refresh workflows."""

from __future__ import annotations

import typer
from rich.console import Console

from polymarket_anomaly_tracker.config import get_settings
from polymarket_anomaly_tracker.scoring.flagger import FlagRefreshError, refresh_flag_statuses

console = Console()
flag_app = typer.Typer(help="Refresh candidate and flagged wallet classifications.")


@flag_app.command("refresh")
def flag_refresh_command() -> None:
    """Classify wallets from the latest scoring run and synchronize the watchlist."""

    settings = get_settings()
    try:
        result = refresh_flag_statuses(settings.database_url)
    except FlagRefreshError as error:
        console.print(f"Flag refresh failed: {error}")
        raise typer.Exit(code=1) from error

    console.print(
        "Refreshed wallet flags. "
        f"As of: {result.as_of_time.isoformat()}. "
        f"Evaluated: {result.wallets_evaluated}. "
        f"Flagged: {result.flagged_wallets}. "
        f"Candidates: {result.candidate_wallets}. "
        f"Unflagged: {result.unflagged_wallets}. "
        f"Watchlist created: {result.watchlist_created}. "
        f"Watchlist updated: {result.watchlist_updated}. "
        f"Watchlist removed: {result.watchlist_removed}."
    )
