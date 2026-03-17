# Scoring

## Goal

The scoring system tries to surface wallets with unusual or high-signal historical behavior using explainable, persisted features rather than opaque model output.

## Input Dataset

Scoring uses the wallet analysis dataset assembled from:

- `wallets`
- `trades`
- `closed_positions`

Additional context such as current positions and alerts is used elsewhere, but the core scoring path is based on historical trade and resolved-position behavior.

## Core Features

The current core features are:

- `resolved_markets_count`
- `trades_count`
- `win_rate`
- `avg_roi`
- `median_roi`
- `realized_pnl_total`

These come from [features/pnl.py](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/src/polymarket_anomaly_tracker/features/pnl.py).

## Advanced Features

The current advanced features are:

- `early_entry_edge`
- `timing_score`
- `specialization_score`
- `conviction_score`
- `consistency_score`

These come from:

- [features/timing.py](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/src/polymarket_anomaly_tracker/features/timing.py)
- [features/specialization.py](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/src/polymarket_anomaly_tracker/features/specialization.py)
- [features/conviction.py](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/src/polymarket_anomaly_tracker/features/conviction.py)
- [features/consistency.py](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/src/polymarket_anomaly_tracker/features/consistency.py)

## Normalization

Raw features are converted to percentile-like normalized columns before they are combined.

Current normalized columns:

- `normalized_early_entry_edge`
- `normalized_timing_score`
- `normalized_win_rate`
- `normalized_avg_roi`
- `normalized_realized_pnl_percentile`
- `normalized_specialization_score`
- `normalized_conviction_score`
- `normalized_consistency_score`

If a normalized input is missing, the scoring code uses a neutral contribution rather than forcing it to zero.

## Composite and Adjusted Scores

The system currently computes:

- `confidence_score`
- `composite_score`
- `adjusted_score`

`composite_score` blends the normalized feature columns with fixed weights.

`confidence_score` reflects sample size and recency support.

`adjusted_score` is the persisted ranking score used for later candidate and flagged classification.

## Explanation Payloads

Every persisted feature snapshot includes `explanations_json`, which carries:

- `top_reasons`
- `threshold_reason_keys`
- `reason_details`
- score metrics
- sample-size context
- raw feature values
- normalized feature values

This is what powers both the flagging rules and the reporting layer.

## Candidate and Flagged Rules

Current thresholds in [scoring/thresholds.py](/Users/aliel-asmar/Desktop/CopyTrader/Kalshi-Checker/src/polymarket_anomaly_tracker/scoring/thresholds.py):

- score eligibility:
  - at least `5` resolved markets
  - at least `10` trades
- flag eligibility:
  - at least `8` resolved markets
  - at least `20` trades
  - `confidence_score >= 0.50`
- candidate promotion:
  - `adjusted_score >= 0.70`
  - within the top `10%`
- flagged promotion:
  - `adjusted_score >= 0.80`
  - within the top `5%`
  - at least `2` threshold reasons

Tie handling is deterministic because the ranked snapshot rows are sorted by adjusted score, composite score, confidence score, and wallet address.

## Persistence

Scoring output is persisted into `wallet_feature_snapshots`.

Important persisted fields:

- raw feature values
- `composite_score`
- `confidence_score`
- `adjusted_score`
- `explanations_json`

## Current Limitation

Scoring is currently available through Python service code, not a dedicated CLI command.

The current documented scoring entry point is:

```bash
uv run python - <<'PY'
from datetime import UTC, datetime
from polymarket_anomaly_tracker.config import get_settings
from polymarket_anomaly_tracker.db.session import get_session_factory, session_scope
from polymarket_anomaly_tracker.scoring.anomaly_score import score_and_persist_wallets

settings = get_settings()
session_factory = get_session_factory(settings)

with session_scope(session_factory) as session:
    score_and_persist_wallets(session, as_of_time=datetime.now(UTC))
PY
```
