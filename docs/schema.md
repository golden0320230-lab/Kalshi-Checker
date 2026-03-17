# Schema

## Overview

The database is SQLite with Alembic migrations. The schema is designed around public wallet analysis and local operational tracking.

Migration entry points:

- [alembic.ini](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/alembic.ini)
- [migrations/env.py](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/migrations/env.py)

## Domain Tables

### `wallets`

One row per public wallet.

Important fields:

- `wallet_address`
- `first_seen_at`
- `last_seen_at`
- `display_name`
- `profile_slug`
- `is_flagged`
- `flag_status`
- `notes`

### `events`

Optional event-level metadata used to group markets.

Important fields:

- `event_id`
- `title`
- `category`
- `slug`
- `start_time`
- `end_time`
- `status`

### `markets`

Market-level metadata linked to optional events.

Important fields:

- `market_id`
- `event_id`
- `question`
- `slug`
- `category`
- `status`
- `resolution_outcome`
- `resolution_time`
- `close_time`
- `liquidity`
- `volume`

### `trades`

Normalized public trades.

Important fields:

- `trade_id`
- `wallet_address`
- `market_id`
- `event_id`
- `outcome`
- `side`
- `price`
- `size`
- `notional`
- `trade_time`
- `source`

### `positions_snapshots`

Point-in-time current-position snapshots for wallets.

Important fields:

- `wallet_address`
- `snapshot_time`
- `market_id`
- `event_id`
- `outcome`
- `quantity`
- `avg_entry_price`
- `current_value`
- `unrealized_pnl`
- `realized_pnl`
- `status`

### `closed_positions`

Normalized resolved or exited wallet positions.

Important fields:

- `wallet_address`
- `market_id`
- `event_id`
- `outcome`
- `entry_price_avg`
- `exit_price_avg`
- `quantity`
- `realized_pnl`
- `roi`
- `opened_at`
- `closed_at`
- `resolution_outcome`

### `wallet_feature_snapshots`

Persisted scoring outputs.

Important fields:

- `wallet_address`
- `as_of_time`
- `resolved_markets_count`
- `trades_count`
- `win_rate`
- `avg_roi`
- `median_roi`
- `realized_pnl_total`
- `early_entry_edge`
- `specialization_score`
- `conviction_score`
- `consistency_score`
- `timing_score`
- `composite_score`
- `confidence_score`
- `adjusted_score`
- `explanations_json`

### `watchlist`

Locally tracked wallets that need tighter monitoring.

Important fields:

- `wallet_address`
- `watch_status`
- `added_reason`
- `added_at`
- `last_checked_at`
- `priority`
- `notes`

### `alerts`

Local alerts generated from watch-mode change detection.

Important fields:

- `wallet_address`
- `alert_type`
- `severity`
- `market_id`
- `event_id`
- `summary`
- `details_json`
- `detected_at`
- `is_read`

### `ingestion_runs`

Operational metadata for ingest, scoring, and watch workflows.

Important fields:

- `run_type`
- `started_at`
- `finished_at`
- `status`
- `records_written`
- `error_message`
- `metadata_json`

## Table Roles by Stage

Seed and enrich:

- `wallets`
- `events`
- `markets`
- `trades`
- `positions_snapshots`
- `closed_positions`
- `ingestion_runs`

Score and classify:

- `wallet_feature_snapshots`
- `wallets`
- `watchlist`

Watch and alert:

- `positions_snapshots`
- `watchlist`
- `alerts`
- `ingestion_runs`

Report and export:

- `wallet_feature_snapshots`
- `wallets`
- `trades`
- `closed_positions`
- `positions_snapshots`
- `alerts`
- `watchlist`

## Migration Notes

At the time of writing, the project has two schema revisions:

- initial schema creation
- `wallet_feature_snapshots.adjusted_score` addition for Issue 10 scoring

If scoring fails with a schema mismatch, rerun:

```bash
uv run pmat init-db
```
