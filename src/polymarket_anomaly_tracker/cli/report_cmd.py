"""CLI commands for local reporting and export workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal, cast

import typer
from rich.console import Console

from polymarket_anomaly_tracker.config import get_settings
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import get_session_factory, session_scope
from polymarket_anomaly_tracker.reporting.exports import (
    export_top_wallets_report,
    export_wallet_detail_report,
)
from polymarket_anomaly_tracker.reporting.leaderboard_report import build_top_wallets_report
from polymarket_anomaly_tracker.reporting.renderers import (
    render_top_wallets_report,
    render_wallet_detail_report,
)
from polymarket_anomaly_tracker.reporting.wallet_report import build_wallet_detail_report

console = Console()
report_app = typer.Typer(help="Render local wallet reports and export outputs.")


@report_app.command("top-wallets")
def report_top_wallets_command(
    limit: int = typer.Option(
        20,
        "--limit",
        min=1,
        help="Maximum number of ranked wallets to render.",
    ),
    min_adjusted_score: float | None = typer.Option(
        None,
        "--min-adjusted-score",
        help="Optional lower bound for adjusted score filtering.",
    ),
) -> None:
    """Render the latest ranked wallet table."""

    settings = get_settings()
    try:
        with session_scope(get_session_factory(settings)) as session:
            repository = DatabaseRepository(session)
            report = build_top_wallets_report(
                repository,
                limit=limit,
                min_adjusted_score=min_adjusted_score,
            )
    except (RuntimeError, ValueError) as error:
        console.print(f"Top-wallets report failed: {error}")
        raise typer.Exit(code=1) from error

    console.print(render_top_wallets_report(report))


@report_app.command("wallet")
def report_wallet_command(
    wallet_address: str = typer.Argument(..., help="Wallet address to inspect."),
    trade_limit: int = typer.Option(
        10,
        "--trade-limit",
        min=1,
        help="Maximum number of recent trades to render.",
    ),
    closed_position_limit: int = typer.Option(
        10,
        "--closed-position-limit",
        min=1,
        help="Maximum number of recent closed positions to render.",
    ),
    alert_limit: int = typer.Option(
        10,
        "--alert-limit",
        min=1,
        help="Maximum number of recent alerts to render.",
    ),
) -> None:
    """Render a detailed drill-down for one wallet."""

    settings = get_settings()
    try:
        with session_scope(get_session_factory(settings)) as session:
            repository = DatabaseRepository(session)
            report = build_wallet_detail_report(
                repository,
                wallet_address=wallet_address,
                trade_limit=trade_limit,
                closed_position_limit=closed_position_limit,
                alert_limit=alert_limit,
            )
    except (RuntimeError, ValueError) as error:
        console.print(f"Wallet report failed: {error}")
        raise typer.Exit(code=1) from error

    console.print(render_wallet_detail_report(report))


@report_app.command("export")
def report_export_command(
    report_name: str = typer.Option(
        "top-wallets",
        "--report",
        help="Which report to export: top-wallets or wallet.",
    ),
    output_format: str = typer.Option(
        "json",
        "--format",
        help="Output format: json or csv.",
    ),
    output_path: Annotated[
        Path,
        typer.Option("--output", help="Destination file path for the exported report."),
    ] = Path(""),
    wallet_address: str | None = typer.Option(
        None,
        "--wallet-address",
        help="Wallet address to export when --report wallet is selected.",
    ),
    limit: int = typer.Option(
        20,
        "--limit",
        min=1,
        help="Maximum number of top-wallet rows to export.",
    ),
    trade_limit: int = typer.Option(
        10,
        "--trade-limit",
        min=1,
        help="Maximum number of recent trades to export for a wallet report.",
    ),
    closed_position_limit: int = typer.Option(
        10,
        "--closed-position-limit",
        min=1,
        help="Maximum number of recent closed positions to export for a wallet report.",
    ),
    alert_limit: int = typer.Option(
        10,
        "--alert-limit",
        min=1,
        help="Maximum number of recent alerts to export for a wallet report.",
    ),
) -> None:
    """Export a ranked or wallet-level report to a local file."""

    normalized_report = report_name.strip().lower()
    normalized_format = output_format.strip().lower()
    if output_path == Path(""):
        console.print("Report export failed: --output is required.")
        raise typer.Exit(code=1)
    if normalized_report not in {"top-wallets", "wallet"}:
        console.print("Report export failed: --report must be 'top-wallets' or 'wallet'.")
        raise typer.Exit(code=1)
    if normalized_format not in {"json", "csv"}:
        console.print("Report export failed: --format must be 'json' or 'csv'.")
        raise typer.Exit(code=1)

    settings = get_settings()
    try:
        with session_scope(get_session_factory(settings)) as session:
            repository = DatabaseRepository(session)
            if normalized_report == "top-wallets":
                leaderboard_report = build_top_wallets_report(repository, limit=limit)
                export_top_wallets_report(
                    leaderboard_report,
                    output_path=output_path,
                    export_format=cast(Literal["csv", "json"], normalized_format),
                )
            else:
                if wallet_address is None:
                    raise ValueError("--wallet-address is required for --report wallet")
                wallet_report = build_wallet_detail_report(
                    repository,
                    wallet_address=wallet_address,
                    trade_limit=trade_limit,
                    closed_position_limit=closed_position_limit,
                    alert_limit=alert_limit,
                )
                export_wallet_detail_report(
                    wallet_report,
                    output_path=output_path,
                    export_format=cast(Literal["csv", "json"], normalized_format),
                )
    except (RuntimeError, ValueError) as error:
        console.print(f"Report export failed: {error}")
        raise typer.Exit(code=1) from error

    console.print(
        f"Exported {normalized_report} report to {output_path} as {normalized_format}."
    )
