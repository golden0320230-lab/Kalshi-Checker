# CLI

## Entry Point

The CLI entry point is:

```bash
uv run pmat --help
```

Root command file:

- [src/polymarket_anomaly_tracker/main.py](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/src/polymarket_anomaly_tracker/main.py)

## Commands

### `init-db`

Create or migrate the local SQLite database.

```bash
uv run pmat init-db
```

### `ingest seed`

Seed wallets from the public Polymarket leaderboard.

```bash
uv run pmat ingest seed --leaderboard-window all --top-wallets 25
```

Options:

- `--leaderboard-window`: `day`, `week`, `month`, or `all`
- `--top-wallets`: max number of wallets to seed

### `ingest enrich`

Enrich seeded wallets with profiles, trades, positions, closed positions, markets, and events.

```bash
uv run pmat ingest enrich --wallet-batch-size 25
```

### `ingest market-prices`

Snapshot current market quote data for known markets.

```bash
uv run pmat ingest market-prices --markets-from-db --max-markets 100
```

Options:

- `--market-id`: one or more explicit market IDs
- `--market-file`: file containing one market ID per line
- `--markets-from-db`: include known local markets
- `--max-markets`: cap the requested market set
- `--interval-seconds`: polling sleep between cycles
- `--max-cycles`: number of polling cycles before exit

### `score compute`

Compute and persist anomaly scores for all wallets with local history.

```bash
uv run pmat score compute
```

### `demo run`

Run the full deterministic offline demo flow on fixture-backed data.

```bash
uv run pmat demo run
```

### `flag refresh`

Promote wallets to `candidate` or `flagged` using the latest scoring run and synchronize the watchlist.

```bash
uv run pmat flag refresh
```

### `watch run`

Run finite watch cycles against active watchlist entries and persist local alerts.

```bash
uv run pmat watch run --max-cycles 1 --interval-seconds 0
```

Options:

- `--max-cycles`: number of finite cycles before exit
- `--interval-seconds`: sleep interval between cycles

### `report top-wallets`

Render the latest ranked top-wallet table.

```bash
uv run pmat report top-wallets --limit 10
```

Options:

- `--limit`: maximum rows to render
- `--min-adjusted-score`: optional score filter

### `report wallet`

Render a single-wallet drill-down.

```bash
uv run pmat report wallet 0xYOUR_WALLET_ADDRESS
```

Options:

- `--trade-limit`
- `--closed-position-limit`
- `--alert-limit`

### `report export`

Export either a ranked report or a wallet drill-down to disk.

Top-wallets CSV:

```bash
uv run pmat report export \
  --report top-wallets \
  --format csv \
  --output data/reports/top_wallets.csv \
  --limit 10
```

Wallet JSON:

```bash
uv run pmat report export \
  --report wallet \
  --format json \
  --output data/reports/wallet.json \
  --wallet-address 0xYOUR_WALLET_ADDRESS
```

## Recommended End-to-End Order

```bash
uv run pmat init-db
uv run pmat ingest seed --leaderboard-window all --top-wallets 25
uv run pmat ingest enrich --wallet-batch-size 25
uv run pmat score compute
uv run pmat flag refresh
uv run pmat watch run --max-cycles 1 --interval-seconds 0
uv run pmat report top-wallets --limit 10
```

## Failure Notes

- If `report wallet` fails, make sure you pass the wallet address without angle brackets.
- If scoring fails with a missing `adjusted_score` column, rerun `uv run pmat init-db`.
- If watch mode produces zero alerts, that is valid when no material position changes occurred between snapshots.
- `demo run` defaults to `sqlite:///data/demo_fixture.db` and resets that SQLite file unless you pass `--no-reset-db`.
