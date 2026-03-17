# Architecture

## Purpose

The system is a local-only pipeline that ingests public Polymarket data, normalizes it into SQLite, computes explainable anomaly features, ranks wallets, tracks flagged wallets, and emits local reports and alerts.

## Top-Level Flow

1. Public REST data is fetched through `clients/polymarket_rest.py`.
2. Ingestion services normalize and persist data through `db/repositories.py`.
3. Feature modules assemble a pandas-backed analysis dataset.
4. Scoring modules normalize features, compute composite and adjusted scores, and persist explanation payloads.
5. Flagging promotes wallets into `candidate` and `flagged`, then synchronizes the watchlist.
6. Watch mode captures fresh position snapshots for flagged wallets and emits alerts on material changes.
7. Reporting modules render ranked and wallet-level outputs or export them to disk.

## Package Responsibilities

### `cli/`

Thin Typer command handlers only.

- `init-db`
- `ingest seed`
- `ingest enrich`
- `flag refresh`
- `watch run`
- `report top-wallets`
- `report wallet`
- `report export`

Business logic does not live here.

### `clients/`

Public API access and DTO validation.

- typed REST client
- retry and transport error handling
- payload validation through Pydantic DTOs

### `db/`

Persistence layer.

- SQLAlchemy models
- Alembic migrations
- session helpers
- explicit repository methods for reads and idempotent upserts

### `ingest/`

Public-data ingestion workflows.

- leaderboard seeding
- wallet enrichment
- trade, position, and market/event persistence helpers

### `features/`

Analytics-oriented dataset and feature computation.

- reproducible wallet analysis dataset
- core PnL features
- timing, specialization, conviction, and consistency features

### `scoring/`

Ranking and promotion logic.

- percentile normalization
- confidence, composite, and adjusted scores
- explanation payload generation
- candidate and flagged classification thresholds

### `tracking/`

Flagged-wallet monitoring.

- watchlist synchronization
- snapshot capture
- snapshot diffing
- alert persistence
- finite-cycle watch monitor orchestration

### `reporting/`

Local output layer.

- ranked wallet report
- wallet drill-down report
- Rich renderers
- CSV/JSON export helpers

## Persistence Model

SQLite is the only supported store right now.

Operationally important tables:

- `wallets`
- `events`
- `markets`
- `trades`
- `positions_snapshots`
- `closed_positions`
- `wallet_feature_snapshots`
- `watchlist`
- `alerts`
- `ingestion_runs`

See [schema.md](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/docs/schema.md) for field-level detail.

## Design Constraints

- local-only execution
- deterministic, testable service functions
- no trading code paths
- no private wallet access
- no raw SQL unless clearly justified
- explainability preferred over opaque heuristics

## Operational Shape

The current workflow is batch-first:

1. initialize DB
2. seed wallets
3. enrich wallets
4. score wallets
5. refresh flags
6. run watch cycles
7. inspect reports or exports

There is no daemon process, background worker, or remote service dependency today.
