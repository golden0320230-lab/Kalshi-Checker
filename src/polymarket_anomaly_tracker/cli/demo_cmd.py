"""CLI commands for the deterministic end-to-end fixture demo flow."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from polymarket_anomaly_tracker.demo.workflow import (
    DEFAULT_DEMO_DATABASE_URL,
    DEFAULT_DEMO_OUTPUT_DIR,
    run_fixture_demo,
)

console = Console()
demo_app = typer.Typer(help="Run the deterministic offline fixture demo workflow.")


@demo_app.command("run")
def demo_run_command(
    database_url: Annotated[
        str,
        typer.Option(
            "--database-url",
            help="SQLite database URL for the demo run.",
        ),
    ] = DEFAULT_DEMO_DATABASE_URL,
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            help="Directory where demo reports will be exported.",
        ),
    ] = DEFAULT_DEMO_OUTPUT_DIR,
    reset_db: Annotated[
        bool,
        typer.Option(
            "--reset-db/--no-reset-db",
            help="Reset the SQLite demo database before running the pipeline.",
        ),
    ] = True,
) -> None:
    """Run the full offline demo pipeline and export local reports."""

    result = run_fixture_demo(
        database_url=database_url,
        output_dir=output_dir,
        reset_database=reset_db,
    )
    console.print(
        "Completed offline fixture demo. "
        f"Database: {result.database_url}. "
        f"Seeded: {result.seeded_wallets}. "
        f"Enriched: {result.enriched_wallets}. "
        f"Scored: {result.scored_wallets}. "
        f"Flagged: {result.flagged_wallets}. "
        f"Candidates: {result.candidate_wallets}. "
        f"Alerts: {result.alerts_written}. "
        f"Top wallet: {result.top_wallet_address}. "
        f"Top adjusted score: {result.top_adjusted_score:.3f}. "
        f"Exports: {result.export_paths.top_wallets_json}, "
        f"{result.export_paths.top_wallets_csv}, "
        f"{result.export_paths.wallet_json}, "
        f"{result.export_paths.wallet_csv}."
    )
