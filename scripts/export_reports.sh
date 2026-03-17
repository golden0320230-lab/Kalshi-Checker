#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DB_URL="${PMAT_DATABASE_URL:-sqlite:///data/demo_fixture.db}"
OUTPUT_DIR="${PMAT_EXPORT_DIR:-data/demo_reports}"

export PMAT_DATABASE_URL="$DB_URL"

TOP_WALLET="$(uv run python - <<'PY'
from polymarket_anomaly_tracker.config import clear_settings_cache
from polymarket_anomaly_tracker.db.session import get_session_factory, session_scope
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository

clear_settings_cache()
with session_scope(get_session_factory()) as session:
    repository = DatabaseRepository(session)
    latest_as_of_time = repository.get_latest_feature_snapshot_time()
    if latest_as_of_time is None:
        raise SystemExit("No wallet feature snapshots found. Run scoring or the demo first.")
    rows = repository.list_wallet_feature_snapshot_rows(as_of_time=latest_as_of_time)
    if not rows:
        raise SystemExit("No scored wallets found. Run scoring or the demo first.")
    print(rows[0].wallet_address)
PY
)"

uv run pmat report export \
  --report top-wallets \
  --format json \
  --output "$OUTPUT_DIR/top_wallets.json" \
  --limit 10

uv run pmat report export \
  --report top-wallets \
  --format csv \
  --output "$OUTPUT_DIR/top_wallets.csv" \
  --limit 10

uv run pmat report export \
  --report wallet \
  --format json \
  --output "$OUTPUT_DIR/top_wallet.json" \
  --wallet-address "$TOP_WALLET"

uv run pmat report export \
  --report wallet \
  --format csv \
  --output "$OUTPUT_DIR/top_wallet.csv" \
  --wallet-address "$TOP_WALLET"

printf 'Exported reports for %s to %s\n' "$TOP_WALLET" "$OUTPUT_DIR"
