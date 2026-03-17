# Polymarket Anomaly Tracker

This repository keeps the GitHub name `Kalshi-Checker`, but the software implemented here is a local-only Python tool for analyzing public Polymarket data, scoring unusual wallets, flagging high-signal candidates, tracking flagged wallets over time, and exporting local reports.

The tool is for personal research and signal discovery only.

It does not:
- place bets
- sign wallets
- connect user accounts
- automate trading
- attempt deanonymization

## What It Does

Current `main` supports:

- initializing a local SQLite database with Alembic migrations
- loading public Polymarket data for leaderboard wallets
- enriching wallets with profiles, trades, current positions, closed positions, markets, and events
- computing explainable anomaly features
- scoring wallets and persisting score snapshots with `pmat score compute`
- promoting wallets to `candidate` and `flagged`
- synchronizing a local watchlist for flagged wallets
- running finite watch cycles and creating local alerts for material position changes
- rendering ranked and wallet-level reports
- exporting reports to CSV and JSON
- running a deterministic offline fixture demo with `pmat demo run`

## Local-Only Boundary

This project is intentionally scoped as a local analysis tool:

- all storage is local SQLite
- all commands operate on public data
- no exchange credentials are required
- no trade execution exists in the codebase
- no order placement path exists
- no signing or wallet-management code exists

If you need the precise misuse boundaries, see [docs/threat-model.md](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/docs/threat-model.md).

## Requirements

- Python 3.11+
- `uv`
- SQLite available locally

## Install

```bash
uv sync
uv run pmat --help
```

Optional developer checks:

```bash
uv run ruff check .
uv run mypy src
uv run pytest
```

## Configuration

The app reads configuration from:

1. environment variables
2. `.env`
3. `config/settings.yaml`

Example files:

- [.env.example](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/.env.example)
- [config/settings.example.yaml](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/config/settings.example.yaml)

Minimal local setup:

```bash
cp .env.example .env
cp config/settings.example.yaml config/settings.yaml
```

Default DB:

```text
sqlite:///data/polymarket_anomaly_tracker.db
```

## Quickstart

Initialize the local database:

```bash
uv run pmat init-db
```

Seed public leaderboard wallets:

```bash
uv run pmat ingest seed --leaderboard-window all --top-wallets 25
```

Enrich seeded wallets:

```bash
uv run pmat ingest enrich --wallet-batch-size 25
```

Persist scoring snapshots:

```bash
uv run pmat score compute
```

Refresh wallet flags and the watchlist:

```bash
uv run pmat flag refresh
```

Run one watch cycle:

```bash
uv run pmat watch run --max-cycles 1 --interval-seconds 0
```

Render reports:

```bash
uv run pmat report top-wallets --limit 10
uv run pmat report wallet 0xYOUR_WALLET_ADDRESS
```

Export reports:

```bash
uv run pmat report export --report top-wallets --format csv --output data/reports/top_wallets.csv
uv run pmat report export --report wallet --format json --output data/reports/wallet.json --wallet-address 0xYOUR_WALLET_ADDRESS
```

Run the full offline demo flow:

```bash
uv run pmat demo run
```

## Current Workflow

The current recommended order is:

1. `init-db`
2. `ingest seed`
3. `ingest enrich`
4. `score compute`
5. `flag refresh`
6. `watch run`
7. `report ...`

## Repository Layout

```text
Kalshi-Checker/
├── config/
├── docs/
├── migrations/
├── src/polymarket_anomaly_tracker/
│   ├── cli/
│   ├── clients/
│   ├── db/
│   ├── features/
│   ├── ingest/
│   ├── reporting/
│   ├── scoring/
│   └── tracking/
└── tests/
```

## Docs

- [docs/architecture.md](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/docs/architecture.md)
- [docs/scoring.md](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/docs/scoring.md)
- [docs/schema.md](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/docs/schema.md)
- [docs/cli.md](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/docs/cli.md)
- [docs/threat-model.md](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/docs/threat-model.md)

## Quality Gates

```bash
uv run ruff check .
uv run mypy src
uv run pytest
```

## Status

`main` is through Issue 15. The initial v1 CLI, offline demo flow, docs, and tests are all in place.
