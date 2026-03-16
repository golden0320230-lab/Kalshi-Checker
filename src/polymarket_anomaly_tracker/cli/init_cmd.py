"""CLI command for database initialization."""

from __future__ import annotations

from rich.console import Console

from polymarket_anomaly_tracker.config import get_settings
from polymarket_anomaly_tracker.db.init_db import init_database

console = Console()


def init_db_command() -> None:
    """Initialize or migrate the local SQLite database."""

    settings = get_settings()
    tables = init_database(settings.database_url)
    console.print(
        "Initialized database at "
        f"{settings.database_url} with {len(tables)} tables."
    )

