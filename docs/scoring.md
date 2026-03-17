# Scoring

## Goal

The scoring system tries to surface wallets with unusual or high-signal historical behavior using explainable, persisted features rather than opaque model output.

## Input Dataset

Scoring uses the wallet analysis dataset assembled from:

- `wallets`
- `trades`
- `closed_positions`
- `market_price_snapshots`

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

- `value_at_entry_score`
- `timing_drift_score`
- `timing_positive_capture_score`
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

- `normalized_value_at_entry_score`
- `normalized_timing_drift_score`
- `normalized_timing_positive_capture_score`
- `normalized_win_rate`
- `normalized_avg_roi`
- `normalized_realized_pnl_percentile`
- `normalized_specialization_score`
- `normalized_conviction_score`
- `normalized_consistency_score`

If a normalized input is missing, the scoring code uses a neutral contribution rather than forcing it to zero.

## Timing vs Value At Entry

The timing/value layer is now split into two different concepts:

- `value_at_entry_score`
  - a resolved-outcome proxy
  - answers: was the wallet's entry price favorable relative to the eventual market outcome
- `timing_drift_score`
  - a true timing metric built from forward market snapshots
  - answers: did price move favorably after the trade
- `timing_positive_capture_score`
  - a true timing metric built from forward market snapshots
  - answers: how much positive post-entry drift the wallet captured without counting adverse drift as positive

True timing uses the first available snapshot at or after `+1h`, `+6h`, and `+24h` from each trade. Trade-level drift is measured on the traded contract side:

- buys are favorable when contract price rises
- sells are favorable when contract price falls
- `NO` trades are evaluated against `1 - YES_price`

If a wallet does not have enough trades with forward snapshot matches, both timing-drift metrics return `None`. The scoring layer then treats those missing timing values neutrally instead of assuming a bad score.

## Composite and Adjusted Scores

The system currently computes:

- `confidence_score`
- `composite_score`
- `adjusted_score`

`composite_score` blends the normalized feature columns with fixed weights.

The timing/value block is intentionally lighter than before while it is still being validated:

- `normalized_value_at_entry_score`: `0.08`
- `normalized_timing_drift_score`: `0.10`
- `normalized_timing_positive_capture_score`: `0.06`

That keeps combined timing/value weight at `0.24` instead of the earlier `0.40` proxy-heavy allocation.

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
- `value_at_entry_score`
- `timing_drift_score`
- `timing_positive_capture_score`
- `composite_score`
- `confidence_score`
- `adjusted_score`
- `explanations_json`
