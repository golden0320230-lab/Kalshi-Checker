"""Smoke tests for the CLI bootstrap."""

from typer.testing import CliRunner

from polymarket_anomaly_tracker.main import app

runner = CliRunner()


def test_root_command_displays_bootstrap_message() -> None:
    result = runner.invoke(app)

    assert result.exit_code == 0
    assert "bootstrap is installed" in result.stdout
    assert "Environment: development" in result.stdout


def test_help_lists_project_scope() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "Analyze public Polymarket data locally" in result.stdout
    assert "track flagged traders over time" in result.stdout
    assert "demo" in result.stdout
    assert "flag" in result.stdout
    assert "ingest" in result.stdout
    assert "init-db" in result.stdout
    assert "report" in result.stdout
    assert "score" in result.stdout
    assert "watch" in result.stdout


def test_score_help_lists_backtest_command() -> None:
    result = runner.invoke(app, ["score", "--help"])

    assert result.exit_code == 0
    assert "backtest" in result.stdout
    assert "compute" in result.stdout
