"""Engine and session helpers for the local SQLite database."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from polymarket_anomaly_tracker.config import Settings, get_settings


def create_db_engine(database_url: str, *, echo: bool = False) -> Engine:
    """Create a SQLAlchemy engine for the given database URL."""

    return create_engine(
        database_url,
        echo=echo,
        future=True,
    )


def create_session_factory(
    database_url: str,
    *,
    echo: bool = False,
) -> sessionmaker[Session]:
    """Create a session factory bound to the given database URL."""

    engine = create_db_engine(database_url, echo=echo)
    return sessionmaker(bind=engine, expire_on_commit=False)


def get_session_factory(settings: Settings | None = None) -> sessionmaker[Session]:
    """Create a session factory using the active application settings."""

    active_settings = settings or get_settings()
    return create_session_factory(active_settings.database_url)


@contextmanager
def session_scope(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    """Provide a transactional session scope."""

    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

