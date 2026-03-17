"""CLI commands for anomaly score computation and persistence."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, cast

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from polymarket_anomaly_tracker.config import (
    SUPPORTED_WEIGHT_PROFILES,
    get_settings,
    get_weight_profiles,
)
from polymarket_anomaly_tracker.db.session import get_session_factory, session_scope
from polymarket_anomaly_tracker.scoring.anomaly_score import score_and_persist_wallets
from polymarket_anomaly_tracker.scoring.backtest import (
    export_backtest_summary,
    run_walk_forward_backtest,
)

DEFAULT_BACKTEST_OUTPUT_DIR = Path("data/backtests")
console = Console()
score_app = typer.Typer(help="Compute and persist wallet anomaly scores.")


@score_app.command("compute")
def score_compute_command() -> None:
    """Compute anomaly scores for all wallets with available history."""

    settings = get_settings()
    session_factory = get_session_factory(settings)
    with session_scope(session_factory) as session:
        score_frame = score_and_persist_wallets(
            session,
            as_of_time=datetime.now(UTC),
            composite_weights=settings.scoring.composite_weights.as_mapping(),
        )

    if score_frame.empty:
        console.print("Computed wallet scores. Wallets scored: 0.")
        return

    top_row = score_frame.iloc[0].to_dict()
    console.print(
        "Computed wallet scores. "
        f"Wallets scored: {len(score_frame)}. "
        f"Score-eligible: {int(score_frame['score_eligible'].sum())}. "
        f"Flag-eligible: {int(score_frame['flag_eligible'].sum())}. "
        f"Top wallet: {top_row['wallet_address']}. "
        f"Top adjusted score: {float(top_row['adjusted_score']):.3f}."
    )


@score_app.command("backtest")
def score_backtest_command(
    train_days: int = typer.Option(
        90,
        "--train-days",
        min=1,
        help="Training-history window in days before the cutoff.",
    ),
    test_days: int = typer.Option(
        30,
        "--test-days",
        min=1,
        help="Future evaluation window in days after the cutoff.",
    ),
    top_n: int = typer.Option(
        25,
        "--top-n",
        min=1,
        help="Number of highest-ranked wallets to evaluate in the top-N slice.",
    ),
    profiles: str = typer.Option(
        "configured,equal,timing-light",
        "--profiles",
        help=(
            "Comma-separated weight profiles to evaluate. "
            "Supported profiles: configured, equal, timing-light."
        ),
    ),
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            help="Directory where JSON and CSV backtest summaries will be written.",
        ),
    ] = DEFAULT_BACKTEST_OUTPUT_DIR,
) -> None:
    """Run a walk-forward score backtest across one or more weight profiles."""

    normalized_profiles = _parse_profile_names(profiles)
    if not normalized_profiles:
        console.print("Score backtest failed: at least one valid profile is required.")
        raise typer.Exit(code=1)

    settings = get_settings()
    all_weight_profiles = get_weight_profiles(settings.scoring.composite_weights)
    selected_profiles = {
        profile_name: all_weight_profiles[profile_name]
        for profile_name in normalized_profiles
    }
    session_factory = get_session_factory(settings)
    with session_scope(session_factory) as session:
        result = run_walk_forward_backtest(
            session,
            train_days=train_days,
            test_days=test_days,
            top_n=top_n,
            weight_profiles=selected_profiles,
        )

    export_paths = export_backtest_summary(result, output_dir=output_dir)
    _render_backtest_summary(result.summary_frame)
    console.print(
        "Backtest complete. "
        f"Profiles: {', '.join(normalized_profiles)}. "
        f"Exports: {export_paths.json_path}, {export_paths.csv_path}."
    )


def _parse_profile_names(profiles: str) -> tuple[str, ...]:
    normalized_profiles = tuple(
        dict.fromkeys(
            profile_name.strip()
            for profile_name in profiles.split(",")
            if profile_name.strip()
        )
    )
    unsupported_profiles = [
        profile_name
        for profile_name in normalized_profiles
        if profile_name not in SUPPORTED_WEIGHT_PROFILES
    ]
    if unsupported_profiles:
        unsupported = ", ".join(sorted(unsupported_profiles))
        supported = ", ".join(SUPPORTED_WEIGHT_PROFILES)
        console.print(
            f"Score backtest failed: unsupported profiles [{unsupported}]. "
            f"Supported profiles: {supported}."
        )
        raise typer.Exit(code=1)
    return normalized_profiles


def _render_backtest_summary(summary_frame: pd.DataFrame) -> None:
    if summary_frame.empty:
        console.print("Backtest summary is empty.")
        return

    table = Table(title="Score Walk-Forward Backtest")
    table.add_column("Profile")
    table.add_column("Ranked", justify="right")
    table.add_column("Evaluated", justify="right")
    table.add_column("Top-N ROI", justify="right")
    table.add_column("Top-N PnL %ile", justify="right")
    table.add_column("Top Decile Hit", justify="right")
    table.add_column("Baseline Hit", justify="right")
    table.add_column("Spearman", justify="right")

    for row in summary_frame.to_dict(orient="records"):
        table.add_row(
            cast(str, row["profile_name"]),
            str(int(cast(int, row["ranked_wallets"]))),
            str(int(cast(int, row["evaluated_wallets"]))),
            _format_optional_metric(row["average_future_roi_top_n"]),
            _format_optional_metric(row["average_future_pnl_percentile_top_n"]),
            _format_optional_metric(row["top_decile_hit_rate"]),
            _format_optional_metric(row["baseline_hit_rate"]),
            _format_optional_metric(row["spearman_future_pnl_correlation"]),
        )

    console.print(table)


def _format_optional_metric(value: object) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    return f"{float(value):.3f}"
