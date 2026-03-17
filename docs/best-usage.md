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
