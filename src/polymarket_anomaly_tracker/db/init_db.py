"""Database initialization helpers backed by Alembic migrations."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect

from polymarket_anomaly_tracker.db.session import create_db_engine

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ALEMBIC_INI_PATH = PROJECT_ROOT / "alembic.ini"
MIGRATIONS_PATH = PROJECT_ROOT / "migrations"


def build_alembic_config(database_url: str) -> Config:
    """Build a configured Alembic runtime config for a target database."""

    config = Config(str(ALEMBIC_INI_PATH))
    config.set_main_option("script_location", str(MIGRATIONS_PATH))
    config.set_main_option("sqlalchemy.url", database_url)
    config.attributes["configure_logger"] = False
    return config


def init_database(database_url: str) -> tuple[str, ...]:
    """Create or migrate the target database to the latest revision."""

    config = build_alembic_config(database_url)
    command.upgrade(config, "head")
    return get_table_names(database_url)


def get_table_names(database_url: str) -> tuple[str, ...]:
    """Return the sorted table names for an initialized database."""

    engine = create_db_engine(database_url)
    try:
        inspector = inspect(engine)
        table_names = {
            table_name
            for table_name in inspector.get_table_names()
            if table_name != "alembic_version"
        }
        return tuple(sorted(table_names))
    finally:
        engine.dispose()
