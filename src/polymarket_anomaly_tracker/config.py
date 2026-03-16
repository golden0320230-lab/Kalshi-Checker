"""Application settings for the Polymarket anomaly tracker."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, cast

from pydantic import BaseModel, Field, NonNegativeInt, PositiveFloat, PositiveInt, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

DEFAULT_ENV_FILE = Path(".env")
DEFAULT_SETTINGS_FILE = Path("config/settings.yaml")


class LeaderboardSettings(BaseModel):
    """Configuration for leaderboard ingestion and filtering."""

    window_days: PositiveInt = 30
    market_categories: tuple[str, ...] = ("politics", "macro", "crypto")


class WatchlistSettings(BaseModel):
    """Configuration for candidate and flagged wallet monitoring."""

    candidate_score_threshold: PositiveFloat = 70.0
    flagged_score_threshold: PositiveFloat = 85.0
    persistence_days: PositiveInt = 14


class ScoringSettings(BaseModel):
    """Configuration for anomaly scoring thresholds."""

    candidate_threshold: PositiveFloat = 70.0
    flagged_threshold: PositiveFloat = 85.0
    min_trades: NonNegativeInt = 10


class AlertsSettings(BaseModel):
    """Configuration for local alert emission thresholds."""

    min_position_change_usd: PositiveFloat = 1000.0
    min_trade_notional_usd: PositiveFloat = 500.0
    cooccurrence_window_minutes: PositiveInt = 30


class RuntimeYamlSettingsSource(YamlConfigSettingsSource):
    """Load YAML settings from the configured runtime path, if present."""

    def __init__(self, settings_cls: type[BaseSettings]):
        yaml_path = os.getenv("PMAT_SETTINGS_FILE")
        if yaml_path:
            super().__init__(settings_cls, yaml_file=Path(yaml_path))
            return

        super().__init__(settings_cls)


class Settings(BaseSettings):
    """Validated application settings."""

    env: Literal["development", "test", "production"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    database_url: str = "sqlite:///data/polymarket_anomaly_tracker.db"
    leaderboard: LeaderboardSettings = Field(default_factory=LeaderboardSettings)
    watchlist: WatchlistSettings = Field(default_factory=WatchlistSettings)
    scoring: ScoringSettings = Field(default_factory=ScoringSettings)
    alerts: AlertsSettings = Field(default_factory=AlertsSettings)

    model_config = SettingsConfigDict(
        env_prefix="PMAT_",
        env_file=DEFAULT_ENV_FILE,
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
        nested_model_default_partial_update=True,
        yaml_file=DEFAULT_SETTINGS_FILE,
        yaml_file_encoding="utf-8",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Load init values first, then env, dotenv, YAML, and finally secrets."""

        return (
            init_settings,
            env_settings,
            dotenv_settings,
            RuntimeYamlSettingsSource(settings_cls),
            file_secret_settings,
        )

    @property
    def database_path(self) -> Path | None:
        """Return the SQLite database file path when the URL points to a file."""

        try:
            url = make_url(self.database_url)
        except ArgumentError as error:
            raise ValueError(f"Invalid database URL: {self.database_url}") from error

        if not url.drivername.startswith("sqlite"):
            return None

        database_name = url.database
        if database_name is None or database_name in {"", ":memory:"}:
            return None

        return Path(database_name)

    @model_validator(mode="after")
    def validate_database_parent(self) -> Settings:
        """Ensure the SQLite database parent directory is ready for local use."""

        database_path = self.database_path
        if database_path is None:
            return self

        database_path.parent.mkdir(parents=True, exist_ok=True)
        if not database_path.parent.is_dir():
            msg = f"Database parent path is not a directory: {database_path.parent}"
            raise ValueError(msg)

        return self


def load_settings(
    *,
    env_file: Path | None = DEFAULT_ENV_FILE,
    settings_file: Path | None = DEFAULT_SETTINGS_FILE,
) -> Settings:
    """Load settings with optional overrides for tests or local tooling."""

    previous_settings_file = os.environ.get("PMAT_SETTINGS_FILE")

    try:
        if settings_file is None:
            os.environ.pop("PMAT_SETTINGS_FILE", None)
        else:
            os.environ["PMAT_SETTINGS_FILE"] = str(settings_file)

        settings_factory = cast(Any, Settings)
        return cast(Settings, settings_factory(_env_file=env_file))
    finally:
        if previous_settings_file is None:
            os.environ.pop("PMAT_SETTINGS_FILE", None)
        else:
            os.environ["PMAT_SETTINGS_FILE"] = previous_settings_file


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached application settings."""

    return load_settings()


def clear_settings_cache() -> None:
    """Clear the cached application settings."""

    get_settings.cache_clear()
