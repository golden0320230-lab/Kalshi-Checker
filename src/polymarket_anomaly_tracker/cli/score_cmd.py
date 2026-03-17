"""CLI commands for anomaly score computation and persistence."""

from __future__ import annotations

from datetime import UTC, datetime

import typer
from rich.console import Console

from polymarket_anomaly_tracker.config import get_settings
from polymarket_anomaly_tracker.db.session import get_session_factory, session_scope
from polymarket_anomaly_tracker.scoring.anomaly_score import score_and_persist_wallets

console = Console()
score_app = typer.Typer(help="Compute and persist wallet anomaly scores.")


@score_app.command("compute")
def score_compute_command() -> None:
    """Compute anomaly scores for all wallets with available history."""

    settings = get_settings()
    session_factory = get_session_factory(settings)
    with session_scope(session_factory) as session:
        score_frame = score_and_persist_wallets(session, as_of_time=datetime.now(UTC))

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
