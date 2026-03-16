"""CLI commands for public-data ingestion workflows."""

from __future__ import annotations

import typer
from rich.console import Console

from polymarket_anomaly_tracker.config import get_settings
from polymarket_anomaly_tracker.ingest.leaderboard import (
    LeaderboardSeedError,
    seed_leaderboard_wallets,
)

console = Console()
ingest_app = typer.Typer(help="Run public-data ingestion workflows.")


@ingest_app.command("seed")
def ingest_seed_command(
    leaderboard_window: str = typer.Option(
        "all",
        "--leaderboard-window",
        help="Leaderboard window to seed: day, week, month, or all.",
    ),
    top_wallets: int = typer.Option(
        100,
        "--top-wallets",
        min=1,
        help="Maximum number of leaderboard wallets to seed.",
    ),
) -> None:
    """Seed the wallet universe from the public leaderboard."""

    settings = get_settings()
    try:
        result = seed_leaderboard_wallets(
            database_url=settings.database_url,
            window=leaderboard_window,
            limit=top_wallets,
        )
    except (LeaderboardSeedError, ValueError) as error:
        console.print(f"Leaderboard seed failed: {error}")
        raise typer.Exit(code=1) from error

    console.print(
        "Seeded leaderboard wallets. "
        f"Window: {result.window}. "
        f"Fetched: {result.records_written}. "
        f"New wallets: {result.new_wallets}. "
        f"Existing wallets updated: {result.existing_wallets}."
    )
