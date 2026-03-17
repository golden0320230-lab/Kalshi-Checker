"""CLI commands for public-data ingestion workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from polymarket_anomaly_tracker.config import get_settings
from polymarket_anomaly_tracker.ingest.leaderboard import (
    LeaderboardSeedError,
    seed_leaderboard_wallets,
)
from polymarket_anomaly_tracker.ingest.market_prices import (
    MarketPriceIngestionError,
    ingest_market_price_snapshots,
    resolve_market_ids,
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


@ingest_app.command("market-prices")
def ingest_market_prices_command(
    market_id: Annotated[
        list[str] | None,
        typer.Option(
            "--market-id",
            help="One or more explicit market IDs to snapshot.",
        ),
    ] = None,
    market_file: Annotated[
        Path | None,
        typer.Option(
            "--market-file",
            help="Optional file containing one market ID per line.",
        ),
    ] = None,
    markets_from_db: bool = typer.Option(
        True,
        "--markets-from-db/--no-markets-from-db",
        help="Include known market IDs from the local database.",
    ),
    max_markets: int = typer.Option(
        100,
        "--max-markets",
        min=1,
        help="Maximum number of market IDs to snapshot in one run.",
    ),
    interval_seconds: float = typer.Option(
        0.0,
        "--interval-seconds",
        min=0.0,
        help="Seconds to sleep between price snapshot cycles.",
    ),
    max_cycles: int = typer.Option(
        1,
        "--max-cycles",
        min=1,
        help="Number of snapshot cycles to run before exiting.",
    ),
) -> None:
    """Snapshot current market quote data for known markets."""

    settings = get_settings()
    try:
        market_ids = resolve_market_ids(
            database_url=settings.database_url,
            market_ids=market_id,
            market_file=market_file,
            markets_from_db=markets_from_db,
            max_markets=max_markets,
        )
        result = ingest_market_price_snapshots(
            database_url=settings.database_url,
            market_ids=market_ids,
            interval_seconds=interval_seconds,
            max_cycles=max_cycles,
        )
    except (MarketPriceIngestionError, OSError, ValueError) as error:
        console.print(f"Market price snapshot ingestion failed: {error}")
        raise typer.Exit(code=1) from error

    console.print(
        "Snapshotted market prices. "
        f"Markets requested: {result.markets_requested}. "
        f"Markets snapshotted: {result.markets_snapshotted}. "
        f"Snapshots written: {result.snapshots_written}. "
        f"Cycles: {result.cycles_completed}."
    )
