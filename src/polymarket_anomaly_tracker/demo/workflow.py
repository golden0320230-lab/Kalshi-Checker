"""Deterministic offline demo flow backed by generated fixture data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy.engine.url import make_url

from polymarket_anomaly_tracker.clients.polymarket_rest import PolymarketRESTClient, make_client
from polymarket_anomaly_tracker.db.init_db import init_database
from polymarket_anomaly_tracker.db.repositories import DatabaseRepository
from polymarket_anomaly_tracker.db.session import create_session_factory, session_scope
from polymarket_anomaly_tracker.ingest.leaderboard import seed_leaderboard_wallets
from polymarket_anomaly_tracker.ingest.orchestrator import enrich_seeded_wallets
from polymarket_anomaly_tracker.reporting.exports import (
    export_top_wallets_report,
    export_wallet_detail_report,
)
from polymarket_anomaly_tracker.reporting.leaderboard_report import build_top_wallets_report
from polymarket_anomaly_tracker.reporting.wallet_report import build_wallet_detail_report
from polymarket_anomaly_tracker.scoring.anomaly_score import score_and_persist_wallets
from polymarket_anomaly_tracker.scoring.flagger import refresh_flag_statuses
from polymarket_anomaly_tracker.tracking.monitor import run_watch_monitor

DEFAULT_DEMO_DATABASE_URL = "sqlite:///data/demo_fixture.db"
DEFAULT_DEMO_OUTPUT_DIR = Path("data/demo_reports")
DEMO_SEED_TIME = datetime(2026, 5, 1, 9, 0, tzinfo=UTC)
DEMO_ENRICH_TIME = datetime(2026, 5, 1, 10, 0, tzinfo=UTC)
DEMO_SCORE_TIME = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
DEMO_WATCH_TIME = datetime(2026, 5, 1, 13, 0, tzinfo=UTC)
_FIELD_CATEGORIES = ("Politics", "Crypto", "Macro", "Sports")


@dataclass(frozen=True)
class DemoExportPaths:
    """Report file paths emitted by the demo run."""

    top_wallets_json: Path
    top_wallets_csv: Path
    wallet_json: Path
    wallet_csv: Path


@dataclass(frozen=True)
class DemoRunResult:
    """Operational summary for a full offline demo run."""

    database_url: str
    export_paths: DemoExportPaths
    seeded_wallets: int
    enriched_wallets: int
    scored_wallets: int
    flagged_wallets: int
    candidate_wallets: int
    alerts_written: int
    top_wallet_address: str
    top_adjusted_score: float


@dataclass(frozen=True)
class _WalletPlan:
    wallet_address: str
    display_name: str
    rank: int
    resolved_markets: int
    trades_count: int
    wins_count: int
    primary_category: str
    is_recent: bool
    profit_scale: float
    price_edge: float
    trade_size_scale: float


class DemoFixtureApi:
    """Generated offline Polymarket fixture API for deterministic demos."""

    def __init__(self) -> None:
        self._score_time = DEMO_SCORE_TIME
        self._wallet_plans = self._build_wallet_plans()
        self.top_wallet_address = self._wallet_plans[0].wallet_address
        self._leaderboard = [
            {
                "rank": str(plan.rank),
                "proxyWallet": plan.wallet_address,
                "userName": plan.display_name,
                "pnl": round(1000.0 - (plan.rank * 11.5), 2),
                "profileImage": None,
                "vol": round(5000.0 - (plan.rank * 75.0), 2),
                "xUsername": f"demo_{plan.rank:02d}",
            }
            for plan in self._wallet_plans
        ]
        self._profiles = {
            plan.wallet_address: {
                "createdAt": (self._score_time - timedelta(days=365)).isoformat(),
                "displayUsernamePublic": True,
                "name": plan.display_name,
                "proxyWallet": plan.wallet_address,
                "pseudonym": plan.display_name,
                "verifiedBadge": plan.rank <= 2,
                "xUsername": f"demo_{plan.rank:02d}",
            }
            for plan in self._wallet_plans
        }
        self._trades: dict[str, list[dict[str, Any]]] = {}
        self._closed_positions: dict[str, list[dict[str, Any]]] = {}
        self._markets: dict[str, dict[str, Any]] = {}
        self._current_positions_sequences: dict[str, tuple[list[dict[str, Any]], ...]] = {}
        self._current_position_calls: dict[str, int] = {}
        self._build_wallet_payloads()

    def build_client(self) -> PolymarketRESTClient:
        """Return a typed REST client backed by the generated fixture transport."""

        return make_client(
            transport=httpx.MockTransport(self._handler),
            max_retries=0,
            sleep=lambda _: None,
        )

    def _handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/leaderboard":
            return httpx.Response(200, request=request, json=self._leaderboard)
        if path == "/public-profile":
            wallet_address = request.url.params["address"]
            return httpx.Response(200, request=request, json=self._profiles[wallet_address])
        if path == "/trades":
            wallet_address = request.url.params["user"]
            return httpx.Response(200, request=request, json=self._trades[wallet_address])
        if path == "/positions":
            wallet_address = request.url.params["user"]
            sequence = self._current_positions_sequences[wallet_address]
            call_count = self._current_position_calls.get(wallet_address, 0)
            self._current_position_calls[wallet_address] = call_count + 1
            return httpx.Response(
                200,
                request=request,
                json=sequence[min(call_count, len(sequence) - 1)],
            )
        if path == "/closed-positions":
            wallet_address = request.url.params["user"]
            return httpx.Response(
                200,
                request=request,
                json=self._closed_positions[wallet_address],
            )
        if path == "/markets":
            market_ids = list(request.url.params.get_list("id"))
            payload = [
                self._markets[market_id]
                for market_id in market_ids
                if market_id in self._markets
            ]
            return httpx.Response(200, request=request, json=payload)
        if path.startswith("/markets/"):
            market_id = path.rsplit("/", maxsplit=1)[-1]
            market_payload = self._markets.get(market_id)
            if market_payload is None:
                return httpx.Response(404, request=request, json={"detail": "not found"})
            return httpx.Response(200, request=request, json=market_payload)
        return httpx.Response(404, request=request, json={"detail": "not found"})

    def _build_wallet_plans(self) -> tuple[_WalletPlan, ...]:
        wallet_plans = [
            _WalletPlan(
                wallet_address=_wallet_address(1),
                display_name="Demo Flagged Wallet",
                rank=1,
                resolved_markets=20,
                trades_count=50,
                wins_count=17,
                primary_category="Politics",
                is_recent=True,
                profit_scale=1.35,
                price_edge=0.13,
                trade_size_scale=1.45,
            ),
            _WalletPlan(
                wallet_address=_wallet_address(2),
                display_name="Demo Candidate Wallet",
                rank=2,
                resolved_markets=18,
                trades_count=40,
                wins_count=14,
                primary_category="Crypto",
                is_recent=True,
                profit_scale=1.05,
                price_edge=0.08,
                trade_size_scale=1.10,
            ),
        ]
        for index in range(3, 21):
            resolved_markets = 7 + (index % 4)
            wallet_plans.append(
                _WalletPlan(
                    wallet_address=_wallet_address(index),
                    display_name=f"Demo Wallet {index:02d}",
                    rank=index,
                    resolved_markets=resolved_markets,
                    trades_count=16 + (index % 6),
                    wins_count=max(2, resolved_markets // 2),
                    primary_category=_FIELD_CATEGORIES[(index - 1) % len(_FIELD_CATEGORIES)],
                    is_recent=index % 3 == 0,
                    profit_scale=0.45 - (index * 0.01),
                    price_edge=0.01 + ((index % 3) * 0.01),
                    trade_size_scale=0.55 - (index * 0.01),
                )
            )
        return tuple(wallet_plans)

    def _build_wallet_payloads(self) -> None:
        for plan in self._wallet_plans:
            closed_positions = self._build_closed_positions(plan)
            trades = self._build_trades(plan, closed_positions=closed_positions)
            self._closed_positions[plan.wallet_address] = closed_positions
            self._trades[plan.wallet_address] = trades
            self._current_positions_sequences[plan.wallet_address] = (
                self._build_current_position_sequences(plan)
            )

    def _build_closed_positions(self, plan: _WalletPlan) -> list[dict[str, Any]]:
        closed_positions: list[dict[str, Any]] = []
        base_offset_days = 75 if plan.is_recent else 175
        for market_index in range(1, plan.resolved_markets + 1):
            market_id = f"{plan.wallet_address}-resolved-{market_index:02d}"
            category = (
                plan.primary_category
                if market_index <= max(3, int(plan.resolved_markets * 0.65))
                else _FIELD_CATEGORIES[(market_index - 1) % len(_FIELD_CATEGORIES)]
            )
            did_win = market_index <= plan.wins_count
            total_bought = round(90.0 + (market_index * 6.0), 2)
            avg_price = round(0.21 + ((market_index % 4) * 0.04), 3) if did_win else round(
                0.62 + ((market_index % 3) * 0.06),
                3,
            )
            roi = (
                round(0.18 + (plan.profit_scale * 0.09) + ((market_index % 3) * 0.02), 3)
                if did_win
                else round(-0.06 - ((market_index % 2) * 0.05), 3)
            )
            realized_pnl = round(total_bought * roi, 2)
            closed_at = self._score_time - timedelta(days=base_offset_days - (market_index * 4))
            timestamp = int(closed_at.timestamp())
            outcome = "YES" if did_win else "NO"
            question = f"{plan.display_name} market {market_index:02d}"
            slug = f"{plan.display_name.lower().replace(' ', '-')}-market-{market_index:02d}"
            self._markets[market_id] = _build_market_payload(
                market_id=market_id,
                question=question,
                slug=slug,
                category=category,
                event_title=f"{category} Event {market_index:02d}",
                event_slug=f"{category.lower()}-event-{market_index:02d}",
                active=False,
                closed=True,
                end_date=closed_at,
            )
            closed_positions.append(
                {
                    "asset": f"{market_id}-asset",
                    "avgPrice": avg_price,
                    "conditionId": market_id,
                    "curPrice": 1.0 if did_win else 0.0,
                    "eventSlug": f"{category.lower()}-event-{market_index:02d}",
                    "outcome": outcome,
                    "outcomeIndex": 0 if outcome == "YES" else 1,
                    "proxyWallet": plan.wallet_address,
                    "realizedPnl": realized_pnl,
                    "slug": slug,
                    "timestamp": timestamp,
                    "title": question,
                    "totalBought": total_bought,
                }
            )
        return closed_positions

    def _build_trades(
        self,
        plan: _WalletPlan,
        *,
        closed_positions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        trade_rows: list[dict[str, Any]] = []
        trade_counts = _trade_counts_by_market(
            total_markets=len(closed_positions),
            total_trades=plan.trades_count,
        )
        for market_index, position in enumerate(closed_positions, start=1):
            total_trades_for_market = trade_counts[market_index - 1]
            realized_pnl = float(position["realizedPnl"])
            base_notional = 80.0 + (market_index * 5.0 * plan.trade_size_scale)
            if realized_pnl > 0:
                base_notional += abs(realized_pnl) * 2.0
            trade_time_anchor = datetime.fromtimestamp(int(position["timestamp"]), UTC) - (
                timedelta(days=8)
            )
            if not plan.is_recent:
                trade_time_anchor -= timedelta(days=120)
            for trade_index in range(total_trades_for_market):
                trade_time = trade_time_anchor + timedelta(days=trade_index * 2)
                price = float(position["avgPrice"])
                if realized_pnl > 0:
                    price = max(
                        0.05,
                        round(price - plan.price_edge + (trade_index * 0.015), 3),
                    )
                else:
                    price = min(
                        0.95,
                        round(
                            price
                            + (plan.price_edge / 2.0)
                            + (trade_index * 0.01),
                            3,
                        ),
                    )
                notional = round(base_notional + (trade_index * 18.0), 2)
                size = round(notional / price, 3)
                trade_rows.append(
                    {
                        "asset": str(position["asset"]),
                        "conditionId": str(position["conditionId"]),
                        "eventSlug": position["eventSlug"],
                        "name": plan.display_name,
                        "outcome": str(position["outcome"]),
                        "outcomeIndex": int(position["outcomeIndex"]),
                        "price": price,
                        "profileImage": None,
                        "proxyWallet": plan.wallet_address,
                        "pseudonym": plan.display_name,
                        "side": "BUY",
                        "size": size,
                        "slug": position["slug"],
                        "timestamp": int(trade_time.timestamp()),
                        "title": position["title"],
                        "transactionHash": (
                            f"0x{plan.rank:02x}{market_index:02x}{trade_index:02x}"
                        ),
                    }
                )
        trade_rows.sort(key=lambda row: int(row["timestamp"]))
        return trade_rows

    def _build_current_position_sequences(
        self,
        plan: _WalletPlan,
    ) -> tuple[list[dict[str, Any]], ...]:
        if plan.rank != 1:
            return ([],)

        baseline_market_id = f"{plan.wallet_address}-active-01"
        opened_market_id = f"{plan.wallet_address}-active-02"
        self._markets[baseline_market_id] = _build_market_payload(
            market_id=baseline_market_id,
            question="Will the demo flagged wallet increase conviction?",
            slug="demo-flagged-wallet-conviction",
            category="Politics",
            event_title="Demo Watch Cycle One",
            event_slug="demo-watch-cycle-one",
            active=True,
            closed=False,
            end_date=self._score_time + timedelta(days=14),
        )
        self._markets[opened_market_id] = _build_market_payload(
            market_id=opened_market_id,
            question="Will the demo flagged wallet open a new position?",
            slug="demo-flagged-wallet-opened",
            category="Politics",
            event_title="Demo Watch Cycle Two",
            event_slug="demo-watch-cycle-two",
            active=True,
            closed=False,
            end_date=self._score_time + timedelta(days=21),
        )
        baseline_position = _build_current_position_payload(
            wallet_address=plan.wallet_address,
            market_id=baseline_market_id,
            question="Will the demo flagged wallet increase conviction?",
            slug="demo-flagged-wallet-conviction",
            event_slug="demo-watch-cycle-one",
            outcome="YES",
            size=100.0,
            avg_price=0.62,
            current_value=68.0,
            cash_pnl=6.0,
        )
        increased_position = _build_current_position_payload(
            wallet_address=plan.wallet_address,
            market_id=baseline_market_id,
            question="Will the demo flagged wallet increase conviction?",
            slug="demo-flagged-wallet-conviction",
            event_slug="demo-watch-cycle-one",
            outcome="YES",
            size=165.0,
            avg_price=0.62,
            current_value=122.0,
            cash_pnl=19.0,
        )
        opened_position = _build_current_position_payload(
            wallet_address=plan.wallet_address,
            market_id=opened_market_id,
            question="Will the demo flagged wallet open a new position?",
            slug="demo-flagged-wallet-opened",
            event_slug="demo-watch-cycle-two",
            outcome="NO",
            size=70.0,
            avg_price=0.48,
            current_value=39.0,
            cash_pnl=5.4,
        )
        return ([baseline_position], [increased_position, opened_position])


def run_fixture_demo(
    *,
    database_url: str = DEFAULT_DEMO_DATABASE_URL,
    output_dir: Path = DEFAULT_DEMO_OUTPUT_DIR,
    reset_database: bool = True,
) -> DemoRunResult:
    """Run the full deterministic offline demo flow and export reports."""

    if reset_database:
        _reset_sqlite_database(database_url)

    init_database(database_url)
    fixture_api = DemoFixtureApi()
    client = fixture_api.build_client()
    try:
        seed_result = seed_leaderboard_wallets(
            database_url=database_url,
            window="all",
            limit=len(fixture_api._leaderboard),
            client=client,
            started_at=DEMO_SEED_TIME,
        )
        enrich_result = enrich_seeded_wallets(
            database_url=database_url,
            wallet_batch_size=len(fixture_api._leaderboard),
            client=client,
            started_at=DEMO_ENRICH_TIME,
        )
        session_factory = create_session_factory(database_url)
        with session_scope(session_factory) as session:
            score_frame = score_and_persist_wallets(session, as_of_time=DEMO_SCORE_TIME)
            top_wallet_address = str(score_frame.iloc[0]["wallet_address"])
            top_adjusted_score = float(score_frame.iloc[0]["adjusted_score"])
        flag_result = refresh_flag_statuses(database_url)
        watch_result = run_watch_monitor(
            database_url=database_url,
            interval_seconds=0.0,
            max_cycles=1,
            client=client,
            started_at=DEMO_WATCH_TIME,
            clock=lambda: DEMO_WATCH_TIME,
        )
        export_paths = export_demo_reports(
            database_url=database_url,
            output_dir=output_dir,
            wallet_address=top_wallet_address,
        )
    finally:
        client.close()

    return DemoRunResult(
        database_url=database_url,
        export_paths=export_paths,
        seeded_wallets=seed_result.records_written,
        enriched_wallets=enrich_result.wallets_succeeded,
        scored_wallets=len(score_frame),
        flagged_wallets=flag_result.flagged_wallets,
        candidate_wallets=flag_result.candidate_wallets,
        alerts_written=watch_result.alerts_written,
        top_wallet_address=top_wallet_address,
        top_adjusted_score=top_adjusted_score,
    )


def export_demo_reports(
    *,
    database_url: str,
    output_dir: Path,
    wallet_address: str,
) -> DemoExportPaths:
    """Export the standard demo reports for the top-ranked wallet."""

    session_factory = create_session_factory(database_url)
    with session_scope(session_factory) as session:
        repository = DatabaseRepository(session)
        top_wallets_report = build_top_wallets_report(repository, limit=10)
        wallet_report = build_wallet_detail_report(repository, wallet_address=wallet_address)

    output_dir.mkdir(parents=True, exist_ok=True)
    top_wallets_json = output_dir / "top_wallets.json"
    top_wallets_csv = output_dir / "top_wallets.csv"
    wallet_json = output_dir / "top_wallet.json"
    wallet_csv = output_dir / "top_wallet.csv"
    export_top_wallets_report(
        top_wallets_report,
        output_path=top_wallets_json,
        export_format="json",
    )
    export_top_wallets_report(
        top_wallets_report,
        output_path=top_wallets_csv,
        export_format="csv",
    )
    export_wallet_detail_report(
        wallet_report,
        output_path=wallet_json,
        export_format="json",
    )
    export_wallet_detail_report(
        wallet_report,
        output_path=wallet_csv,
        export_format="csv",
    )
    return DemoExportPaths(
        top_wallets_json=top_wallets_json,
        top_wallets_csv=top_wallets_csv,
        wallet_json=wallet_json,
        wallet_csv=wallet_csv,
    )


def _reset_sqlite_database(database_url: str) -> None:
    parsed_url = make_url(database_url)
    if parsed_url.drivername != "sqlite":
        msg = "Demo reset only supports SQLite database URLs."
        raise ValueError(msg)
    database_path = parsed_url.database
    if database_path in (None, "", ":memory:"):
        return
    assert database_path is not None
    db_file = Path(database_path)
    for suffix in ("", "-shm", "-wal"):
        candidate = Path(f"{db_file}{suffix}")
        if candidate.exists():
            candidate.unlink()


def _wallet_address(index: int) -> str:
    return f"0x{index:040x}"


def _trade_counts_by_market(*, total_markets: int, total_trades: int) -> list[int]:
    base_trade_count, remainder = divmod(total_trades, total_markets)
    return [
        base_trade_count + (1 if index < remainder else 0)
        for index in range(total_markets)
    ]


def _build_market_payload(
    *,
    market_id: str,
    question: str,
    slug: str,
    category: str,
    event_title: str,
    event_slug: str,
    active: bool,
    closed: bool,
    end_date: datetime,
) -> dict[str, Any]:
    return {
        "active": active,
        "archived": False,
        "category": category,
        "closed": closed,
        "conditionId": market_id,
        "description": question,
        "endDate": end_date.isoformat(),
        "events": [
            {
                "category": category,
                "endDate": end_date.isoformat(),
                "id": f"event-{market_id}",
                "slug": event_slug,
                "startDate": (end_date - timedelta(days=14)).isoformat(),
                "status": "closed" if closed else "active",
                "title": event_title,
            }
        ],
        "id": market_id,
        "liquidity": 12000.0,
        "marketType": "binary",
        "question": question,
        "slug": slug,
        "startDate": (end_date - timedelta(days=21)).isoformat(),
        "volume": 8000.0,
    }


def _build_current_position_payload(
    *,
    wallet_address: str,
    market_id: str,
    question: str,
    slug: str,
    event_slug: str,
    outcome: str,
    size: float,
    avg_price: float,
    current_value: float,
    cash_pnl: float,
) -> dict[str, Any]:
    return {
        "asset": f"{market_id}-asset",
        "avgPrice": avg_price,
        "cashPnl": cash_pnl,
        "conditionId": market_id,
        "currentValue": current_value,
        "curPrice": round(current_value / size, 3),
        "eventSlug": event_slug,
        "initialValue": round(size * avg_price, 3),
        "outcome": outcome,
        "outcomeIndex": 0 if outcome == "YES" else 1,
        "percentPnl": round(cash_pnl / max(size * avg_price, 1.0), 4),
        "percentRealizedPnl": 0.0,
        "proxyWallet": wallet_address,
        "realizedPnl": 0.0,
        "redeemable": False,
        "size": size,
        "slug": slug,
        "title": question,
        "totalBought": round(size * avg_price, 3),
    }
