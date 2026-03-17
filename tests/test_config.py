"""Configuration and logging tests."""

from __future__ import annotations

import io
import logging
import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from polymarket_anomaly_tracker.config import (
    DEFAULT_COMPOSITE_SCORE_WEIGHTS,
    DEFAULT_SETTINGS_FILE,
    Settings,
    clear_settings_cache,
    load_settings,
)
from polymarket_anomaly_tracker.logging_config import (
    HANDLER_NAME,
    LOGGER_NAME,
    configure_logging,
    reset_logging,
)


@pytest.fixture(autouse=True)
def reset_runtime_state(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in tuple(os.environ):
        if key.startswith("PMAT_"):
            monkeypatch.delenv(key, raising=False)
    clear_settings_cache()
    reset_logging()


def test_load_settings_from_dotenv(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "PMAT_ENV=test",
                "PMAT_LOG_LEVEL=DEBUG",
                "PMAT_DATABASE_URL=sqlite:///tmp/test-settings.db",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(env_file=env_file, settings_file=None)

    assert settings.env == "test"
    assert settings.log_level == "DEBUG"
    assert settings.database_url == "sqlite:///tmp/test-settings.db"


def test_load_settings_from_yaml(tmp_path: Path) -> None:
    yaml_file = tmp_path / "settings.yaml"
    yaml_file.write_text(
        "\n".join(
            [
                "env: production",
                "log_level: WARNING",
                "database_url: sqlite:///tmp/pmat.yaml.db",
                "scoring:",
                "  flagged_threshold: 91.5",
                "  composite_weights:",
                "    normalized_value_at_entry_score: 0.07",
                "    normalized_timing_drift_score: 0.11",
                "    normalized_timing_positive_capture_score: 0.06",
                "    normalized_win_rate: 0.18",
                "    normalized_avg_roi: 0.14",
                "    normalized_realized_pnl_percentile: 0.12",
                "    normalized_specialization_score: 0.12",
                "    normalized_conviction_score: 0.10",
                "    normalized_consistency_score: 0.10",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(env_file=None, settings_file=yaml_file)

    assert settings.env == "production"
    assert settings.log_level == "WARNING"
    assert settings.scoring.flagged_threshold == 91.5
    assert settings.scoring.composite_weights.normalized_value_at_entry_score == pytest.approx(
        0.07
    )


def test_env_overrides_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    yaml_file = tmp_path / "settings.yaml"
    yaml_file.write_text(
        "\n".join(
            [
                "env: development",
                "log_level: INFO",
                "scoring:",
                "  flagged_threshold: 80.0",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PMAT_LOG_LEVEL", "ERROR")
    monkeypatch.setenv("PMAT_SCORING__FLAGGED_THRESHOLD", "95")

    settings = load_settings(env_file=None, settings_file=yaml_file)

    assert settings.log_level == "ERROR"
    assert settings.scoring.flagged_threshold == 95.0


def test_sqlite_database_parent_is_created(tmp_path: Path) -> None:
    database_file = tmp_path / "nested" / "pmat.db"

    settings = Settings(database_url=f"sqlite:///{database_file}", _env_file=None)

    assert settings.database_path == database_file
    assert database_file.parent.is_dir()


def test_get_settings_uses_default_yaml_location(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    yaml_file = tmp_path / "settings.yaml"
    yaml_file.write_text("env: test\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    settings_dir = tmp_path / DEFAULT_SETTINGS_FILE.parent
    settings_dir.mkdir(parents=True, exist_ok=True)
    yaml_file.replace(settings_dir / DEFAULT_SETTINGS_FILE.name)

    settings = load_settings(env_file=None)

    assert settings.env == "test"


def test_load_settings_respects_runtime_settings_file_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_yaml = tmp_path / "runtime-settings.yaml"
    runtime_yaml.write_text(
        "\n".join(
            [
                "env: production",
                "scoring:",
                "  composite_weights:",
                "    normalized_value_at_entry_score: 0.04",
                "    normalized_timing_drift_score: 0.05",
                "    normalized_timing_positive_capture_score: 0.03",
                "    normalized_win_rate: 0.23",
                "    normalized_avg_roi: 0.16",
                "    normalized_realized_pnl_percentile: 0.16",
                "    normalized_specialization_score: 0.13",
                "    normalized_conviction_score: 0.11",
                "    normalized_consistency_score: 0.09",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PMAT_SETTINGS_FILE", str(runtime_yaml))

    settings = load_settings(env_file=None)

    assert settings.env == "production"
    assert settings.scoring.composite_weights.normalized_win_rate == pytest.approx(0.23)


def test_invalid_composite_weight_sum_fails_validation() -> None:
    invalid_weights = dict(DEFAULT_COMPOSITE_SCORE_WEIGHTS)
    invalid_weights["normalized_consistency_score"] = 0.25

    with pytest.raises(ValidationError, match="sum to 1.0"):
        Settings(
            _env_file=None,
            scoring={"composite_weights": invalid_weights},
        )


def test_invalid_composite_weight_keys_fail_validation() -> None:
    invalid_weights = dict(DEFAULT_COMPOSITE_SCORE_WEIGHTS)
    invalid_weights["normalized_bonus_score"] = 0.01

    with pytest.raises(ValidationError, match="normalized_bonus_score"):
        Settings(
            _env_file=None,
            scoring={"composite_weights": invalid_weights},
        )


def test_configure_logging_is_idempotent() -> None:
    logger = configure_logging("INFO")
    same_logger = configure_logging("DEBUG")

    handlers = [handler for handler in logger.handlers if handler.name == HANDLER_NAME]

    assert logger is same_logger
    assert len(handlers) == 1
    assert logger.level == logging.DEBUG


def test_configure_logging_emits_structured_output() -> None:
    stream = io.StringIO()
    logger = configure_logging("INFO", stream=stream)
    child_logger = logging.getLogger(f"{LOGGER_NAME}.tests")
    child_logger.info("configuration loaded")

    output = stream.getvalue().strip()

    assert logger.name == LOGGER_NAME
    assert "timestamp=" in output
    assert "level=INFO" in output
    assert f"module={LOGGER_NAME}.tests" in output
    assert 'message="configuration loaded"' in output
