"""CLI entry point for the Polymarket anomaly tracker."""

from __future__ import annotations

import logging

import typer
from rich.console import Console

from polymarket_anomaly_tracker.cli.demo_cmd import demo_app
from polymarket_anomaly_tracker.cli.flag_cmd import flag_app
from polymarket_anomaly_tracker.cli.ingest_cmd import ingest_app
from polymarket_anomaly_tracker.cli.init_cmd import init_db_command
from polymarket_anomaly_tracker.cli.report_cmd import report_app
from polymarket_anomaly_tracker.cli.score_cmd import score_app
from polymarket_anomaly_tracker.cli.watch_cmd import watch_app
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
app.add_typer(demo_app, name="demo")
app.add_typer(flag_app, name="flag")
app.add_typer(ingest_app, name="ingest")
app.add_typer(report_app, name="report")
app.add_typer(score_app, name="score")
app.add_typer(watch_app, name="watch")


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
