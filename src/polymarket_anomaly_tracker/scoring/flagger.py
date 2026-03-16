"""Candidate and flagged wallet classification from persisted score snapshots."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

from polymarket_anomaly_tracker.db.enums import WalletFlagStatus
from polymarket_anomaly_tracker.db.repositories import (
    DatabaseRepository,
    WalletFeatureSnapshotRow,
)
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope
from polymarket_anomaly_tracker.scoring.thresholds import (
    ClassificationThresholds,
    is_flag_eligible,
    is_score_eligible,
    is_within_top_fraction,
)
from polymarket_anomaly_tracker.tracking.watchlist import sync_watchlist


class FlagRefreshError(RuntimeError):
    """Raised when flag refresh cannot evaluate the latest scoring run."""


@dataclass(frozen=True)
class WalletClassification:
    """Classification output for one wallet in the current scoring run."""

    wallet_address: str
    display_name: str | None
    as_of_time: datetime
    rank: int
    total_wallets: int
    adjusted_score: float | None
    composite_score: float | None
    confidence_score: float | None
    score_eligible: bool
    flag_eligible: bool
    previous_flag_status: str
    new_flag_status: str
    threshold_reason_count: int
    top_reasons: tuple[str, ...]
    watchlist_reason: str


@dataclass(frozen=True)
class FlagRefreshResult:
    """Summary of a full candidate/flagged refresh run."""

    as_of_time: datetime
    wallets_evaluated: int
    candidate_wallets: int
    flagged_wallets: int
    unflagged_wallets: int
    watchlist_created: int
    watchlist_updated: int
    watchlist_removed: int
    classifications: tuple[WalletClassification, ...]


def refresh_flag_statuses(
    database_url: str,
    *,
    thresholds: ClassificationThresholds | None = None,
) -> FlagRefreshResult:
    """Refresh wallet candidate/flagged state from the latest scoring run."""

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        return refresh_flag_statuses_in_repository(
            repository,
            thresholds=thresholds or ClassificationThresholds(),
        )


def refresh_flag_statuses_in_repository(
    repository: DatabaseRepository,
    *,
    thresholds: ClassificationThresholds,
) -> FlagRefreshResult:
    """Refresh wallet flags using the latest persisted scoring snapshots."""

    latest_as_of_time = repository.get_latest_feature_snapshot_time()
    if latest_as_of_time is None:
        raise FlagRefreshError("No wallet feature snapshots found. Run scoring first.")

    snapshot_rows = repository.list_wallet_feature_snapshot_rows(as_of_time=latest_as_of_time)
    if not snapshot_rows:
        raise FlagRefreshError("Latest scoring run did not contain any wallet snapshots.")

    classifications = classify_wallet_feature_snapshots(
        snapshot_rows,
        thresholds=thresholds,
    )
    for classification in classifications:
        repository.update_wallet_flag_state(
            classification.wallet_address,
            flag_status=classification.new_flag_status,
            is_flagged=classification.new_flag_status == WalletFlagStatus.FLAGGED.value,
        )

    watchlist_result = sync_watchlist(
        repository,
        list(classifications),
        synced_at=latest_as_of_time,
    )
    candidate_wallets = sum(
        1
        for classification in classifications
        if classification.new_flag_status == WalletFlagStatus.CANDIDATE.value
    )
    flagged_wallets = sum(
        1
        for classification in classifications
        if classification.new_flag_status == WalletFlagStatus.FLAGGED.value
    )

    return FlagRefreshResult(
        as_of_time=latest_as_of_time,
        wallets_evaluated=len(classifications),
        candidate_wallets=candidate_wallets,
        flagged_wallets=flagged_wallets,
        unflagged_wallets=len(classifications) - candidate_wallets - flagged_wallets,
        watchlist_created=watchlist_result.created,
        watchlist_updated=watchlist_result.updated,
        watchlist_removed=watchlist_result.removed,
        classifications=tuple(classifications),
    )


def classify_wallet_feature_snapshots(
    snapshot_rows: list[WalletFeatureSnapshotRow],
    *,
    thresholds: ClassificationThresholds,
) -> list[WalletClassification]:
    """Classify a single scoring run deterministically."""

    total_wallets = len(snapshot_rows)
    classifications: list[WalletClassification] = []
    for rank, snapshot_row in enumerate(snapshot_rows, start=1):
        explanation_payload = _load_explanation_payload(snapshot_row.explanations_json)
        threshold_reason_count = len(
            _normalize_string_list(explanation_payload.get("threshold_reason_keys"))
        )
        top_reasons = tuple(_normalize_string_list(explanation_payload.get("top_reasons")))
        score_eligible = is_score_eligible(
            resolved_markets_count=snapshot_row.resolved_markets_count,
            trades_count=snapshot_row.trades_count,
            thresholds=thresholds,
        )
        flag_eligible = is_flag_eligible(
            resolved_markets_count=snapshot_row.resolved_markets_count,
            trades_count=snapshot_row.trades_count,
            confidence_score=snapshot_row.confidence_score,
            thresholds=thresholds,
        )
        adjusted_score = snapshot_row.adjusted_score or 0.0
        is_flagged = (
            flag_eligible
            and adjusted_score >= thresholds.flagged_adjusted_score
            and is_within_top_fraction(
                rank=rank,
                total_wallets=total_wallets,
                top_fraction=thresholds.flagged_top_fraction,
            )
            and threshold_reason_count >= thresholds.flagged_min_reason_count
        )
        is_candidate = (
            not is_flagged
            and score_eligible
            and adjusted_score >= thresholds.candidate_adjusted_score
            and is_within_top_fraction(
                rank=rank,
                total_wallets=total_wallets,
                top_fraction=thresholds.candidate_top_fraction,
            )
        )
        new_flag_status = (
            WalletFlagStatus.FLAGGED.value
            if is_flagged
            else WalletFlagStatus.CANDIDATE.value
            if is_candidate
            else WalletFlagStatus.UNFLAGGED.value
        )
        classifications.append(
            WalletClassification(
                wallet_address=snapshot_row.wallet_address,
                display_name=snapshot_row.display_name,
                as_of_time=snapshot_row.as_of_time,
                rank=rank,
                total_wallets=total_wallets,
                adjusted_score=snapshot_row.adjusted_score,
                composite_score=snapshot_row.composite_score,
                confidence_score=snapshot_row.confidence_score,
                score_eligible=score_eligible,
                flag_eligible=flag_eligible,
                previous_flag_status=snapshot_row.flag_status,
                new_flag_status=new_flag_status,
                threshold_reason_count=threshold_reason_count,
                top_reasons=top_reasons,
                watchlist_reason=_build_watchlist_reason(
                    adjusted_score=snapshot_row.adjusted_score,
                    rank=rank,
                    total_wallets=total_wallets,
                    top_reasons=top_reasons,
                ),
            )
        )
    return classifications


def _build_watchlist_reason(
    *,
    adjusted_score: float | None,
    rank: int,
    total_wallets: int,
    top_reasons: tuple[str, ...],
) -> str:
    normalized_score = 0.0 if adjusted_score is None else adjusted_score
    if top_reasons:
        return (
            f"Flagged at adjusted score {normalized_score:.3f} "
            f"(rank {rank}/{total_wallets}). {top_reasons[0]}"
        )
    return (
        f"Flagged at adjusted score {normalized_score:.3f} "
        f"(rank {rank}/{total_wallets})."
    )


def _load_explanation_payload(explanations_json: str) -> dict[str, object]:
    try:
        payload = json.loads(explanations_json)
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _normalize_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]
