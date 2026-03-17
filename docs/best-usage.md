# Best Usage

## Goal

Use the tracker as a local research workflow for finding unusual wallets with repeatable behavior, not as a one-shot market scanner.

The strongest usage pattern is:

1. build a broad local wallet universe
2. collect enough market-price history for timing features
3. validate score weights with walk-forward backtests
4. compute scores and review the top wallets
5. watch the strongest flagged wallets over time

## Recommended Workflow

From the repo root:

```bash
export PMAT_DATABASE_URL=sqlite:///data/live.db

uv run pmat init-db
uv run pmat ingest seed --leaderboard-window month --top-wallets 100
uv run pmat ingest enrich --wallet-batch-size 100
uv run pmat ingest market-prices --markets-from-db --max-markets 100
uv run pmat score backtest --train-days 90 --test-days 30 --top-n 25
uv run pmat score compute
uv run pmat flag refresh
uv run pmat report top-wallets --limit 25
uv run pmat watch run --max-cycles 1 --interval-seconds 0
```

## Scheduled Operation

If you want this tool to stay useful over time, schedule it as a collection pipeline plus a ranking pipeline.

The key idea is:

- collect `market_price_snapshots` frequently
- enrich wallets often enough to keep trades and positions fresh
- score and refresh flags after enrichment
- run backtests rarely, because they are for validation rather than alerting

### Before You Schedule Anything

1. make sure the database is initialized once with `uv run pmat init-db`
2. use an absolute repo path in every scheduled command
3. run commands from the repo root so relative paths like `data/live.db` work consistently
4. stagger jobs by a few minutes so `enrich`, `score`, `flag`, and `watch` do not collide
5. if this runs on a Mac that sleeps, prefer `launchd` over plain cron

### Recommended Default Cadence

| Command | Timing | Why |
| --- | --- | --- |
| `uv run pmat ingest seed --leaderboard-window month --top-wallets 100` | every 6 hours | the wallet universe changes slowly relative to pricing and watch activity |
| `uv run pmat ingest market-prices --markets-from-db --max-markets 100 --max-cycles 1` | every 10 minutes | true timing features depend on quote history, so this is the highest-value recurring collector |
| `uv run pmat ingest enrich --wallet-batch-size 100` | every 30 minutes | trades, positions, and market metadata should refresh regularly but do not need minute-level polling |
| `uv run pmat score compute` | every 30 minutes, after `enrich` | new scores should reflect the latest enriched wallet state |
| `uv run pmat flag refresh` | every 30 minutes, right after `score compute` | candidates and flagged wallets should track the newest score snapshot |
| `uv run pmat watch run --max-cycles 1 --interval-seconds 0` | every 10 minutes | flagged-wallet monitoring is the alerting loop and should be more frequent than rescoring |
| `uv run pmat score backtest --train-days 90 --test-days 30 --top-n 25` | daily, overnight | backtesting validates weight choices and does not need intraday frequency |
| `uv run pmat report export --report top-wallets --format csv --output data/reports/top_wallets.csv --limit 25` | daily, after backtest | export a stable artifact after the scoring and validation cycle |

### Suggested Clock Times

This is a good balanced schedule in Eastern Time:

- `00:05, 06:05, 12:05, 18:05` seed the wallet universe
- `every 10 minutes` ingest market prices
- `:07 and :37 each hour` enrich wallets
- `:12 and :42 each hour` compute scores
- `:14 and :44 each hour` refresh flags
- `:09, :19, :29, :39, :49, :59` run watch mode
- `03:30 daily` run the backtest
- `03:40 daily` export reports

The offsets matter. `watch run` should not start on the same minute as `enrich` or `score compute`, and `flag refresh` should always come after scoring.

### Example Cron Block

This example assumes the repo lives at `/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker` and that you want the database at `data/live.db`.

```cron
SHELL=/bin/bash
PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin

5 */6 * * * cd /Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker && export PMAT_DATABASE_URL=sqlite:///data/live.db && uv run pmat ingest seed --leaderboard-window month --top-wallets 100 >> logs/seed.log 2>&1
*/10 * * * * cd /Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker && export PMAT_DATABASE_URL=sqlite:///data/live.db && uv run pmat ingest market-prices --markets-from-db --max-markets 100 --max-cycles 1 >> logs/market-prices.log 2>&1
7,37 * * * * cd /Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker && export PMAT_DATABASE_URL=sqlite:///data/live.db && uv run pmat ingest enrich --wallet-batch-size 100 >> logs/enrich.log 2>&1
12,42 * * * * cd /Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker && export PMAT_DATABASE_URL=sqlite:///data/live.db && uv run pmat score compute >> logs/score.log 2>&1
14,44 * * * * cd /Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker && export PMAT_DATABASE_URL=sqlite:///data/live.db && uv run pmat flag refresh >> logs/flag.log 2>&1
9,19,29,39,49,59 * * * * cd /Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker && export PMAT_DATABASE_URL=sqlite:///data/live.db && uv run pmat watch run --max-cycles 1 --interval-seconds 0 >> logs/watch.log 2>&1
30 3 * * * cd /Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker && export PMAT_DATABASE_URL=sqlite:///data/live.db && uv run pmat score backtest --train-days 90 --test-days 30 --top-n 25 >> logs/backtest.log 2>&1
40 3 * * * cd /Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker && export PMAT_DATABASE_URL=sqlite:///data/live.db && uv run pmat report export --report top-wallets --format csv --output data/reports/top_wallets.csv --limit 25 >> logs/report.log 2>&1
```

### How To Adjust The Plan

- if this is a laptop or low-noise setup, move `market-prices` and `watch run` to every 15 minutes and move `enrich` and `score compute` to hourly
- if this is a dedicated always-on machine, move `market-prices` and `watch run` to every 5 minutes and `enrich` to every 15 minutes
- if you are actively changing score weights, keep the nightly backtest; if the weights are stable, daily is enough
- if alerts matter more than ranking freshness, prioritize `market-prices` and `watch run` over more frequent rescoring

### Operational Warnings

- do not schedule `score backtest` as part of the intraday hot path
- do not rely on a single `score compute` run from a thin dataset
- do not run overlapping jobs against the same local DB if your scheduler can start a new run before the last one finishes
- do not treat watch alerts or flagged wallets as auto-trade instructions

## What Matters Most

- `adjusted_score` is the practical ranking output.
- `confidence_score` tells you how much sample support the score has.
- timing features are much more useful when you have accumulated `market_price_snapshots` over time.
- `score backtest` is the right way to compare weight profiles before trusting a custom weight map.

## Best Operating Pattern

### 1. Build price history continuously

True timing metrics depend on forward snapshots after trades. If you do not collect enough quote history, timing-related fields will be sparse and the scorer will fall back toward neutral timing contributions.

### 2. Rank wallets globally first

Find the strongest wallets across the full local universe before narrowing to one market. This reduces the chance that you overfit to a single event or one lucky outcome.

### 3. Use backtesting before changing weights

If you change `scoring.composite_weights`, run:

```bash
uv run pmat score backtest --train-days 90 --test-days 30 --top-n 25
```

Compare:

- `configured`
- `equal`
- `timing-light`

If the configured profile does not beat the simple baselines, do not trust the custom map yet.

### 4. Treat flagged wallets as watch targets, not auto-signals

A flagged wallet is a candidate for deeper review. It is not a trading instruction. Use:

```bash
uv run pmat report wallet 0xYOUR_WALLET_ADDRESS
```

to inspect:

- top reasons
- sample size
- recent trades
- recent closed positions
- watchlist state
- alerts

## What To Trust More

- high `adjusted_score` with strong `confidence_score`
- wallets with enough resolved markets and trades
- strong timing/value signals backed by real snapshot coverage
- wallets that remain strong under `score backtest`
- flagged wallets that keep appearing in later refresh cycles

## What To Trust Less

- thin-sample wallets
- wallets with sparse timing coverage
- one-off PnL spikes without support from conviction, consistency, or specialization
- custom weight maps that only look good intuitively

## Anti-Patterns

- running `score compute` once on a nearly empty local dataset and over-interpreting the ranks
- changing weights without running `score backtest`
- relying on one market in isolation instead of starting from global wallet quality
- treating this tool as a trading bot or execution system

This project remains local-only, public-data-only, and intentionally excludes trade execution, account linking, signing, and deanonymization.
