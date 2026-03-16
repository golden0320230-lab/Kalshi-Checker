"""CLI entry point for the Polymarket anomaly tracker."""

from __future__ import annotations

import logging

import typer
from rich.console import Console

from polymarket_anomaly_tracker.cli.init_cmd import init_db_command
from polymarket_anomaly_tracker.config import get_settings
from polymarket_anomaly_tracker.logging_config import configure_logging

app = typer.Typer(
    add_completion=False,
    help=(
        "Analyze public Polymarket data locally to identify anomalous wallets, "
        "rank candidates, and track flagged traders over time."
    ),
)
console = Console()
logger = logging.getLogger(__name__)
app.command("init-db")(init_db_command)


@app.callback(invoke_without_command=True)
def main(context: typer.Context) -> None:
    """Run the root CLI."""
    settings = get_settings()
    configure_logging(settings.log_level)
    logger.debug("CLI initialized for environment %s", settings.env)

    if context.invoked_subcommand is None:
        console.print(
            "Polymarket anomaly tracker bootstrap is installed. "
            f"Environment: {settings.env}. Database: {settings.database_url}"
        )


if __name__ == "__main__":
    app()
