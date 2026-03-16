"""CLI entry point for the Polymarket anomaly tracker."""

from __future__ import annotations

import typer
from rich.console import Console

app = typer.Typer(
    add_completion=False,
    help=(
        "Analyze public Polymarket data locally to identify anomalous wallets, "
        "rank candidates, and track flagged traders over time."
    ),
)
console = Console()


@app.callback(invoke_without_command=True)
def main(context: typer.Context) -> None:
    """Run the root CLI."""
    if context.invoked_subcommand is None:
        console.print(
            "Polymarket anomaly tracker bootstrap is installed. "
            "Configuration, ingest, scoring, and tracking commands will be added in later issues."
        )


if __name__ == "__main__":
    app()

