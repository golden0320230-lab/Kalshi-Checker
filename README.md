# Polymarket Anomaly Tracker

This repository keeps the GitHub name `Kalshi-Checker`, but the project being built here is a local-only Python tool for analyzing public Polymarket data and surfacing anomalous or high-signal wallets for personal research.

## Scope

- Public-data analysis only
- Local SQLite storage
- CLI-first workflow
- No trade execution
- No wallet signing or account linking
- No deanonymization features

## Status

Issue 01 bootstrap is in place. The repository currently provides:

- `uv`-managed Python packaging
- a minimal `typer` CLI exposed as `pmat`
- `ruff`, `mypy`, `pytest`, and `pre-commit` configuration
- a smoke test that validates the CLI entry point

The ingestion, database, scoring, watchlist, and alerting workflows will be added in later issues.

## Quickstart

```bash
uv sync
uv run pmat --help
uv run pytest
```

