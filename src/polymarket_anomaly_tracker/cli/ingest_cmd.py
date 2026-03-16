"""CLI commands for public-data ingestion workflows."""

from __future__ import annotations

import typer
from rich.console import Console

from polymarket_anomaly_tracker.config import get_settings
from polymarket_anomaly_tracker.ingest.leaderboard import (
    LeaderboardSeedError,
    seed_leaderboard_wallets,
)
from polymarket_anomaly_tracker.ingest.orchestrator import (
    WalletEnrichmentBatchError,
    enrich_seeded_wallets,
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


@ingest_app.command("enrich")
def ingest_enrich_command(
    wallet_batch_size: int = typer.Option(
        25,
        "--wallet-batch-size",
        min=1,
        help="Maximum number of seeded wallets to enrich in this batch.",
    ),
) -> None:
    """Enrich seeded wallets with profiles, trades, positions, and markets."""

    settings = get_settings()
    try:
        result = enrich_seeded_wallets(
            database_url=settings.database_url,
            wallet_batch_size=wallet_batch_size,
        )
    except (ValueError, WalletEnrichmentBatchError) as error:
        console.print(f"Wallet enrichment failed: {error}")
        raise typer.Exit(code=1) from error

    console.print(
        "Enriched seeded wallets. "
        f"Requested: {result.wallets_requested}. "
        f"Succeeded: {result.wallets_succeeded}. "
        f"Failed: {result.wallets_failed}. "
        f"Trades: {result.trades_written}. "
        f"Current positions: {result.current_positions_written}. "
        f"Closed positions: {result.closed_positions_written}. "
        f"Markets: {result.markets_written}. "
        f"Events: {result.events_written}."
    )
