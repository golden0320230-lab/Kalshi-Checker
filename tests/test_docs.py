"""Tests for README and project documentation coverage."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_readme_documents_local_only_scope_and_workflow() -> None:
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")

    assert "local-only" in readme
    assert "no trade execution" in readme.lower()
    assert "place bets" in readme
    assert "automate trading" in readme
    assert "uv run pmat init-db" in readme
    assert "uv run pmat ingest seed" in readme
    assert "uv run pmat flag refresh" in readme
    assert "uv run pmat watch run" in readme
    assert "uv run pmat report top-wallets" in readme


def test_docs_files_exist_and_cover_required_topics() -> None:
    docs = {
        "architecture": PROJECT_ROOT / "docs" / "architecture.md",
        "best_usage": PROJECT_ROOT / "docs" / "best-usage.md",
        "scoring": PROJECT_ROOT / "docs" / "scoring.md",
        "schema": PROJECT_ROOT / "docs" / "schema.md",
        "cli": PROJECT_ROOT / "docs" / "cli.md",
        "threat_model": PROJECT_ROOT / "docs" / "threat-model.md",
    }

    for path in docs.values():
        assert path.exists()
        assert path.read_text(encoding="utf-8").strip()

    architecture = docs["architecture"].read_text(encoding="utf-8")
    best_usage = docs["best_usage"].read_text(encoding="utf-8")
    scoring = docs["scoring"].read_text(encoding="utf-8")
    schema = docs["schema"].read_text(encoding="utf-8")
    cli = docs["cli"].read_text(encoding="utf-8")
    threat_model = docs["threat_model"].read_text(encoding="utf-8")

    assert "local-only" in architecture
    assert "score backtest" in best_usage
    assert "ingest market-prices" in best_usage
    assert "Example Cron Block" in best_usage
    assert "every 10 minutes" in best_usage
    assert "launchd" in best_usage
    assert "adjusted_score" in scoring
    assert "wallet_feature_snapshots" in schema
    assert "report export" in cli
    assert "deanonymization" in threat_model
