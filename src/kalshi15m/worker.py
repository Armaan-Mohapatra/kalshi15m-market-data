from __future__ import annotations

import asyncio
import json
import logging
import logging.handlers
import os
import shutil
import threading
import time
import uuid
import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import websockets

from .api import PaginationLimitExceeded, PublicKalshiAPI
from .auth import ws_auth_headers
from .book import OrderBook
from .classify import executable_quotes, parse_time
from .config import Settings, WS_URL, settings
from .discovery import (
    candidate_series_tickers,
    discover,
    refresh_known_market_rows,
    refresh_upcoming_series_markets,
    series_catalog_changes,
    utc_now,
)
from .storage import Storage
from .paper import (
    crossmarket_performance_summary,
    has_final_official_settlement,
    resolve_fee_terms,
    settle_binary_paper_fill,
    simulate_taker_ioc,
)
from .strategy import PROFILE_ID, evaluate_market, load_frozen_strategy
from .crossmarket import (
    PROFILE_ID as CROSSMARKET_PROFILE_ID,
    crossmarket_eligibility,
    load_crossmarket_strategy,
    validate_market_reference_policy,
)

LOG = logging.getLogger("kalshi15m")
STATUS_FILE_REPLACE_RETRIES = 5
STATUS_FILE_REPLACE_BASE_DELAY_SECONDS = 0.025


def configure_logging(cfg: Settings) -> None:
    cfg.ensure_dirs()
    handler = logging.handlers.RotatingFileHandler(
        cfg.log_dir / "worker.log",
        maxBytes=cfg.log_file_max_bytes,
        backupCount=cfg.log_backup_count,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)sZ %(levelname)s %(message)s"))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)


class RuntimeStatus:
    def __init__(self, cfg: Settings) -> None:
        self.cfg = cfg
        self.lock = threading.RLock()
        self.status_file_write_interval_seconds = 1.0
        self.last_status_file_write_monotonic: float | None = None
        self.started_utc = utc_now()
        self.data: dict[str, Any] = {
            "service": "kalshi-15m-native",
            "service_state": "STARTING",
            "mode": "PAPER_ONLY",
            "runtime_home": str(cfg.home.resolve()),
            "data_dir": str(cfg.data_dir.resolve()),
            "paper_ledger_path": str(cfg.ledger_path.resolve()),
            "parquet_dir": str(cfg.parquet_dir.resolve()),
            "pid": os.getpid(),
            "started_utc": self.started_utc,
            "heartbeat_utc": self.started_utc,
            "host": cfg.status_host,
            "port": cfg.status_port,
            "rest_state": "STARTING",
            "websocket_state": "BLOCKED_NO_CREDENTIALS",
            "strategy_state": "BLOCKED_SOURCE_INCOMPLETE",
            "strategy_evaluators_active": 0,
            "strategy_candidate_markets_compatible": 0,
            "strategy_candidate_markets_out_of_scope": 0,
            "strategy_in_scope_evaluators_active": 0,
            "strategy_all_profiles_evaluators_active": 0,
            "strategy_decisions_persisted": 0,
            "paper_entry_attempts": 0,
            "paper_fill_records": 0,
            "paper_settlement_records": 0,
            "paper_provisional_outcome_count": 0,
            "paper_open_positions": 0,
            "paper_realized_net_pnl_dollars": "0",
            "paper_net_pnl_state": "NO_SETTLED_FILLS",
            "exchange_write_calls": 0,
            "orders_submitted_to_exchange": 0,
            "sequence_gap_count": 0,
            "orderbook_sequence_state": "UNKNOWN_NOT_CONNECTED",
            "orderbook_sequence_connected": False,
            "orderbook_sequence_last_session_state": "UNKNOWN_NOT_CONNECTED",
            "orderbook_sequence_subscription_count": 0,
            "orderbook_sequence_verified_subscription_count": 0,
            "orderbook_sequence_snapshot_count": 0,
            "orderbook_sequence_delta_count": 0,
            "orderbook_sequence_last_verified_utc": None,
            "orderbook_sequence_last_error": None,
            "orderbook_sequence_by_subscription": {},
            "orderbook_quote_last_update_utc_by_ticker": {},
            "orderbook_quote_age_seconds_by_ticker": {},
            "orderbook_quote_freshness_by_ticker": {},
            "orderbook_quote_freshness_threshold_seconds": 5.0,
            "reconnect_count": 0,
            "last_by_channel": {},
            "feed_ages_seconds": {},
            "last_rest_poll_by_channel": {},
            "rest_poll_ages_seconds": {},
            "rest_poll_health": {},
            "strategy_orderbook_source_state": "UNINITIALIZED",
            "strategy_websocket_books_used": 0,
            "strategy_rest_fallback_markets_used": 0,
            "strategy_rest_fallback_requests": 0,
            "crossmarket_orderbook_source_state": "UNINITIALIZED",
            "crossmarket_websocket_books_used": 0,
            "crossmarket_rest_fallback_markets_used": 0,
            "crossmarket_rest_fallback_requests": 0,
            "rest_poll_stale_thresholds_seconds": {
                "rest_orderbook": cfg.book_poll_interval_seconds * 2,
                "rest_public_trades": cfg.trades_poll_interval_seconds * 2,
                "rest_strategy_orderbook": max(
                    5.0,
                    cfg.strategy_poll_interval_seconds * 2,
                ),
                "rest_crossmarket_orderbook": max(
                    5.0,
                    cfg.strategy_poll_interval_seconds * 2,
                ),
            },
            "series_count": 0,
            "series_catalog_state": "UNKNOWN",
            "series_catalog_last_checked_utc": None,
            "series_catalog_api_rows": 0,
            "series_catalog_candidate_count": 0,
            "series_catalog_added_tickers": [],
            "series_catalog_removed_tickers": [],
            "series_catalog_refresh_seconds": cfg.series_catalog_refresh_seconds,
            "active_market_rows": 0,
            "market_tickers": [],
            "inventory_last_reconciled_utc": None,
            "inventory_reconcile_interval_seconds": cfg.reconcile_interval_seconds,
            "inventory_reconcile_duration_seconds": None,
            "inventory_reconcile_request_count": 0,
            "inventory_reconcile_attempt_count": 0,
            "inventory_reconcile_error_count": 0,
            "disk_free_bytes": None,
            "disk_state": "CHECKING",
            "raw_capture_state": "ENABLED",
            "queue_depth": 0,
            "events_inserted": 0,
            "events_duplicate": 0,
            "events_dropped": 0,
            "sqlite_bytes": 0,
            "parquet_bytes": 0,
            "parquet_flush_count": 0,
            "parquet_last_flush_utc": None,
            "parquet_last_flush_rows": 0,
            "parquet_last_flush_reason": None,
            "parquet_flush_trigger_queue_depth": None,
            "parquet_dropped_rows": 0,
            "storage_configured_ledger_path": str(cfg.ledger_path.resolve()),
            "storage_open_database_path": None,
            "storage_database_path_matches_config": None,
            "storage_path_file_identity_state": "UNKNOWN",
            "storage_paper_ledger_record_count": 0,
            "storage_latest_paper_record_utc": None,
            "storage_latest_market_event_received_utc": None,
            "trades_observed": 0,
            "lifecycle_events_observed": 0,
            "settlements_observed": 0,
            "actual_rollovers_observed": 0,
            "rollover_detection_sample_count": 0,
            "rollover_preopen_seconds": cfg.rollover_preopen_seconds,
            "rollover_preflight_lead_seconds": cfg.rollover_preflight_lead_seconds,
            "rollover_priority_batch_size": cfg.rollover_priority_batch_size,
            "rollover_trade_quiet_before_seconds": cfg.rollover_trade_quiet_before_seconds,
            "rollover_trade_quiet_after_seconds": cfg.rollover_trade_quiet_after_seconds,
            "public_trade_rollover_deferrals": 0,
            "rollover_priority_reconcile_state": "IDLE",
            "rollover_priority_reconcile_ticker_count": 0,
            "rollover_priority_reconcile_request_count": 0,
            "rollover_priority_reconcile_retry_count": 0,
            "paper_settlement_poll_interval_seconds": cfg.paper_settlement_poll_interval_seconds,
            "crossmarket_strategy_state": "STARTING",
            "crossmarket_evaluators_active": 0,
            "crossmarket_eligible_market_tickers": [],
            "crossmarket_excluded_market_tickers": [],
            "crossmarket_blocked_market_reasons": {},
            "crossmarket_excluded_market_reasons": {},
            "crossmarket_decisions_persisted": 0,
            "crossmarket_paper_entry_attempts": 0,
            "crossmarket_paper_fill_records": 0,
            "crossmarket_paper_settlement_records": 0,
            "crossmarket_paper_provisional_outcome_count": 0,
            "crossmarket_paper_open_positions": 0,
            "crossmarket_paper_realized_net_pnl_dollars": "0",
            "crossmarket_paper_net_pnl_state": "NO_SETTLED_FILLS",
            "crossmarket_performance_by_asset_class": {},
            "crossmarket_performance_by_market": {},
            "error_count": 0,
            "last_error": None,
            "storage_write_state": "NOT_OPEN",
            "storage_write_retry_count": 0,
            "storage_pending_batch_events": 0,
            "storage_last_write_error": None,
            "source_policy": "PUBLIC_REST_GET_ONLY; WSS_READ_MARKET_DATA_ONLY",
            "missing_underlying_sources": True,
        }

    def update(self, **values: Any) -> None:
        with self.lock:
            self.data.update(values)
            self.data["heartbeat_utc"] = utc_now()
            self._write_locked(force="service_state" in values)

    def mark_channel(self, channel: str, exchange_ts: Any = None) -> None:
        with self.lock:
            received = utc_now()
            self.data.setdefault("last_by_channel", {})[channel] = {
                "received_utc": received,
                "exchange_ts": exchange_ts,
            }
            self.data.setdefault("feed_ages_seconds", {})[channel] = 0.0
            self.data["heartbeat_utc"] = received
            self._write_locked()

    def mark_rest_poll(
        self,
        channel: str,
        *,
        started_utc: str,
        request_count: int,
        successful_request_count: int,
        requested_market_count: int,
        markets_with_data: int,
        rows_received: int,
        error_count: int,
    ) -> None:
        """Track successful REST polling separately from observed market data."""
        completed = utc_now()
        # A successful trade poll may legitimately return zero trades. A book
        # snapshot poll cannot claim full coverage when requested tickers
        # are absent from the response, even if HTTP returned 200.
        missing_market_count = max(0, requested_market_count - markets_with_data)
        poll_status = (
            "NO_MARKETS"
            if requested_market_count == 0
            else "DEGRADED"
            if error_count or (
                channel == "rest_orderbook" and missing_market_count > 0
            )
            else "READY"
        )
        record = {
            "started_utc": started_utc,
            "completed_utc": completed,
            "request_count": request_count,
            "successful_request_count": successful_request_count,
            "requested_market_count": requested_market_count,
            "markets_with_data": markets_with_data,
            "missing_market_count": missing_market_count,
            "rows_received": rows_received,
            "error_count": error_count,
            "status": poll_status,
        }
        with self.lock:
            self.data.setdefault("last_rest_poll_by_channel", {})[channel] = record
            self.data.setdefault("rest_poll_ages_seconds", {})[channel] = 0.0
            self.data.setdefault("rest_poll_health", {})[channel] = poll_status
            self.data["heartbeat_utc"] = completed
            self._write_locked()

    def mark_rest_poll_not_due(
        self, channel: str, state: str, *, profile_health_field: str | None = None
    ) -> None:
        """Distinguish intentionally idle evaluators from a stale active poll."""
        with self.lock:
            self.data.setdefault("rest_poll_health", {})[channel] = state
            if profile_health_field:
                self.data[profile_health_field] = state
            self.data["heartbeat_utc"] = utc_now()
            self._write_locked()

    def refresh_ages(self) -> None:
        now = datetime.now(timezone.utc)
        with self.lock:
            channels = self.data.get("last_by_channel", {})
            self.data["feed_ages_seconds"] = {
                channel: round(
                    max(0.0, (now - datetime.fromisoformat(item["received_utc"])).total_seconds()),
                    3,
                )
                for channel, item in channels.items()
            }
            polls = self.data.get("last_rest_poll_by_channel", {})
            self.data["rest_poll_ages_seconds"] = {
                channel: round(
                    max(
                        0.0,
                        (now - datetime.fromisoformat(item["completed_utc"])).total_seconds(),
                    ),
                    3,
                )
                for channel, item in polls.items()
            }
            quote_updates = self.data.get("orderbook_quote_last_update_utc_by_ticker", {})
            quote_ages = {
                ticker: round(
                    max(0.0, (now - datetime.fromisoformat(received)).total_seconds()),
                    3,
                )
                for ticker, received in quote_updates.items()
            }
            self.data["orderbook_quote_age_seconds_by_ticker"] = quote_ages
            quote_states = self.data.setdefault("orderbook_quote_freshness_by_ticker", {})
            threshold = float(self.data.get("orderbook_quote_freshness_threshold_seconds", 5.0))
            for ticker, age in quote_ages.items():
                if quote_states.get(ticker) == "FRESH" and age > threshold:
                    quote_states[ticker] = "STALE"
            health = self.data.setdefault("rest_poll_health", {})
            thresholds = self.data.get("rest_poll_stale_thresholds_seconds", {})
            for channel, age in self.data["rest_poll_ages_seconds"].items():
                threshold = thresholds.get(channel)
                if (
                    threshold is not None
                    and age > threshold
                    and health.get(channel) in {"READY", "DEGRADED"}
                ):
                    health[channel] = "STALE"
                    profile_health_field = {
                        "rest_strategy_orderbook": "strategy_rest_poll_health",
                        "rest_crossmarket_orderbook": "crossmarket_rest_poll_health",
                    }.get(channel)
                    if profile_health_field and self.data.get(profile_health_field) == "READY":
                        self.data[profile_health_field] = "STALE"
            self.data["heartbeat_utc"] = now.isoformat()
            self._write_locked()

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return json.loads(json.dumps(self.data))

    def _write_locked(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if (
            not force
            and self.last_status_file_write_monotonic is not None
            and now - self.last_status_file_write_monotonic
            < self.status_file_write_interval_seconds
        ):
            return
        self.last_status_file_write_monotonic = now
        self.cfg.runtime_dir.mkdir(parents=True, exist_ok=True)
        path = self.cfg.runtime_dir / "status.json"
        temporary = path.with_name(
            f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        payload = json.dumps(self.data, indent=2)
        for attempt in range(STATUS_FILE_REPLACE_RETRIES):
            try:
                temporary.write_text(payload, encoding="utf-8")
                temporary.replace(path)
                self.last_status_file_write_monotonic = time.monotonic()
                return
            except PermissionError:
                if attempt + 1 == STATUS_FILE_REPLACE_RETRIES:
                    LOG.warning(
                        "status.json stayed locked after %d write/replace attempts; "
                        "keeping the in-memory status current",
                        STATUS_FILE_REPLACE_RETRIES,
                        exc_info=True,
                    )
                    return
                time.sleep(STATUS_FILE_REPLACE_BASE_DELAY_SECONDS * (2**attempt))


def status_server(status: RuntimeStatus, cfg: Settings) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path not in {"/healthz", "/api/v1/status"}:
                self.send_error(404)
                return
            body = json.dumps(status.snapshot(), separators=(",", ":")).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:  # noqa: N802
            self.send_error(405, "read-only status service")

        def do_PUT(self) -> None:  # noqa: N802
            self.send_error(405, "read-only status service")

        def do_DELETE(self) -> None:  # noqa: N802
            self.send_error(405, "read-only status service")

        def log_message(self, fmt: str, *args: Any) -> None:
            LOG.info("status_http " + fmt, *args)

    server = ThreadingHTTPServer((cfg.status_host, cfg.status_port), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, name="status-http", daemon=True).start()
    return server


class Collector:
    def __init__(self, cfg: Settings = settings) -> None:
        self.cfg = cfg
        self.run_id = uuid.uuid4().hex
        self.websocket_session_id: str | None = None
        self.first_seen_tickers: set[str] = set()
        self.first_rest_books: set[str] = set()
        self.first_executable_quotes: set[str] = set()
        self.first_trades: set[str] = set()
        self.trade_poll_since_by_ticker: dict[str, int] = {}
        self.api = PublicKalshiAPI()
        self.status = RuntimeStatus(cfg)
        self.storage = Storage(cfg)
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=cfg.max_queue_events)
        self.writer_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="journal-writer")
        self.stopping = asyncio.Event()
        self.refresh_requested = asyncio.Event()
        self.subscription_updates: asyncio.Queue[set[str]] = asyncio.Queue(maxsize=8)
        self.subscription_sids: dict[str, set[int]] = {}
        self.subscription_channel_by_sid: dict[int, str] = {}
        self.subscription_membership: dict[int, set[str]] = {}
        self.pending_subscriptions: dict[int, tuple[str, set[str]]] = {}
        self.pending_subscription_futures: dict[int, asyncio.Future[None]] = {}
        self.pending_updates: dict[int, tuple[int, str, set[str]]] = {}
        self.pending_update_futures: dict[int, asyncio.Future[None]] = {}
        self.initial_subscription_commands: set[int] = set()
        self.ws_subscriptions_ready = asyncio.Event()
        self.ws_next_command_id = 2
        self.series: list[dict[str, Any]] = []
        self.markets: dict[str, dict[str, Any]] = {}
        self.inventory_snapshot, inventory_cache_age = self._load_recent_inventory()
        self.last_full_inventory_monotonic = (
            time.monotonic() - inventory_cache_age if self.inventory_snapshot is not None else 0.0
        )
        self.last_series_catalog_monotonic = 0.0
        if self.inventory_snapshot is not None:
            self.status.update(
                inventory_cache_state="WARMED_FROM_PERSISTED_DISCOVERY",
                inventory_cache_age_seconds=round(inventory_cache_age, 3),
            )
        self.open_tickers: set[str] = set()
        self.active_tickers: set[str] = set()
        self.subscribed_tickers: set[str] = set()
        self.books: dict[str, OrderBook] = {}
        self.last_seq: dict[int, int] = {}
        self.orderbook_sequence_by_sid: dict[int, dict[str, Any]] = {}
        self.orderbook_sequence_last_verified_utc: str | None = None
        self.book_received_monotonic: dict[str, float] = {}
        self.rest_orderbook_cache: dict[str, tuple[dict[str, Any], datetime, float]] = {}
        self.last_rest_fallback_request_monotonic: dict[str, float] = {}
        self.previous_current_open_windows: set[datetime] = set()
        self.rollover_preflighted_groups: set[str] = set()
        self.pending_rollover_check: tuple[str, datetime] | None = None
        self.pending_rollover_tickers: set[str] = set()
        self.rollover_postopen_reconcile_pending = False
        self.rollover_priority_tickers: set[str] = set()
        self.rollover_priority_retry_count = 0
        self.rollover_series_scan_cursor = 0
        self.strategy_config: dict[str, Any] | None = None
        self.strategy_config_hash: str | None = None
        self.strategy_profile_id: str | None = None
        self.paper_attempted_tickers: set[str] = set()
        self.paper_positions: dict[str, dict[str, Any]] = {}
        self.paper_settled_tickers: set[str] = set()
        self.paper_provisional_outcome_tickers: set[str] = set()
        self.paper_scope_decision_windows: dict[str, str] = {}
        self.paper_decision_count = 0
        self.paper_attempt_count = 0
        self.paper_fill_count = 0
        self.paper_settlement_count = 0
        self.paper_realized_net_pnl = Decimal("0")
        self.paper_unresolved_fee_settlements = 0
        self.last_settlement_poll_monotonic: dict[str, float] = {}
        self.crossmarket_strategy_config: dict[str, Any] | None = None
        self.crossmarket_strategy_config_hash: str | None = None
        self.crossmarket_profile_id: str | None = None
        self.crossmarket_attempted_tickers: set[str] = set()
        self.crossmarket_positions: dict[str, dict[str, Any]] = {}
        self.crossmarket_settled_tickers: set[str] = set()
        self.crossmarket_provisional_outcome_tickers: set[str] = set()
        self.crossmarket_scope_decision_windows: dict[str, str] = {}
        self.crossmarket_decision_count = 0
        self.crossmarket_attempt_count = 0
        self.crossmarket_fill_count = 0
        self.crossmarket_settlement_count = 0
        self.crossmarket_realized_net_pnl = Decimal("0")
        self.crossmarket_unresolved_fee_settlements = 0
        self.crossmarket_performance_records: list[dict[str, Any]] = []
        self.crossmarket_series_categories: dict[str, str] = {}
        self.crossmarket_performance_summary: dict[str, Any] = {
            "by_asset_class": {},
            "by_market": {},
        }
        self.crossmarket_performance_dirty = True
        self.queue_dropped = 0
        self.status_http: ThreadingHTTPServer | None = None
        self.stop_file = cfg.runtime_dir / "stop.request"
        try:
            self.strategy_config, self.strategy_config_hash = load_frozen_strategy(
                Path(__file__).resolve().parents[2]
            )
            self.strategy_profile_id = f"{PROFILE_ID}:{self.strategy_config['revision']}"
            self.status.update(
                strategy_state="ACTIVE_WAITING_FOR_MARKET",
                strategy_config_revision=self.strategy_config["revision"],
                strategy_config_sha256=self.strategy_config_hash,
                strategy_market_scope=self.strategy_config["strategy"]["market_series"],
            )
        except Exception as exc:
            self.strategy_config = None
            self.strategy_config_hash = None
            self.strategy_profile_id = None
            self.status.update(
                strategy_state="BLOCKED_FROZEN_CONFIG_INVALID",
                strategy_block_reason=f"{type(exc).__name__}: {exc}",
            )
        try:
            (
                self.crossmarket_strategy_config,
                self.crossmarket_strategy_config_hash,
            ) = load_crossmarket_strategy(Path(__file__).resolve().parents[2])
            self.crossmarket_profile_id = CROSSMARKET_PROFILE_ID
            self.status.update(
                crossmarket_strategy_revision=self.crossmarket_strategy_config["revision"],
                crossmarket_strategy_config_sha256=self.crossmarket_strategy_config_hash,
                crossmarket_parent_btc_config_sha256=self.strategy_config_hash,
                crossmarket_strategy_authority_state=(
                    "VALIDATED_PAPER_ONLY"
                    if validate_market_reference_policy(self.crossmarket_strategy_config)
                    else "BLOCKED_REFERENCE_POLICY"
                ),
            )
        except Exception as exc:
            self.crossmarket_strategy_config = None
            self.crossmarket_strategy_config_hash = None
            self.crossmarket_profile_id = None
            self.status.update(
                crossmarket_strategy_state="BLOCKED_CROSSMARKET_CONFIG_INVALID",
                crossmarket_strategy_block_reason=f"{type(exc).__name__}: {exc}",
            )

    def _load_recent_inventory(self) -> tuple[dict[str, Any] | None, float]:
        """Warm startup from this project's recent, previously verified REST discovery."""
        reports = self.cfg.home / "reports"
        try:
            candidates = sorted(
                reports.glob("series_inventory_*.json"),
                key=lambda path: path.stat().st_mtime,
                reverse=True,
            )
        except OSError:
            return None, 0.0
        now = datetime.now(timezone.utc)
        for path in candidates[:5]:
            try:
                snapshot = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(snapshot, dict):
                    continue
                snapshot_time = parse_time(snapshot.get("snapshot_utc"))
                if (
                    not isinstance(snapshot.get("series"), list)
                    or not isinstance(snapshot.get("markets"), list)
                    or not snapshot.get("series")
                    or snapshot_time is None
                ):
                    continue
                age = max(0.0, (now - snapshot_time).total_seconds())
                if age > self.cfg.discovery_refresh_seconds:
                    continue
                LOG.info("warming inventory from local discovery snapshot age_seconds=%.1f", age)
                return snapshot, age
            except (OSError, json.JSONDecodeError, TypeError, ValueError, AttributeError):
                continue
        return None, 0.0

    def _next_inventory_reconcile_delay(self) -> float:
        """Schedule one inventory pass just before, then just after, a known open."""
        now = datetime.now(timezone.utc)
        if self.pending_rollover_check is not None:
            _, check_at = self.pending_rollover_check
            return max(1.0, (check_at - now).total_seconds())
        if self.rollover_priority_tickers:
            # Retry only the prepared opening group while the exchange finishes
            # activating it. The shared API client still enforces its 1 req/sec cap.
            return 5.0

        next_open: datetime | None = None
        next_key: str | None = None
        for market in self.markets.values():
            if not market.get("is_15m_confirmed"):
                continue
            if str(market.get("market_status", "")).lower() not in {
                "initialized", "unopened", "active", "open"
            }:
                continue
            opened = parse_time(market.get("open_time"))
            closed = parse_time(market.get("close_time"))
            if opened is None or opened <= now or (closed is not None and closed <= now):
                continue
            key = opened.isoformat()
            if key in self.rollover_preflighted_groups:
                continue
            if next_open is None or opened < next_open:
                next_open, next_key = opened, key

        if next_open is None or next_key is None:
            return float(self.cfg.reconcile_interval_seconds)

        seconds_until_open = (next_open - now).total_seconds()
        preflight_lead_seconds = float(self.cfg.rollover_preflight_lead_seconds)
        if seconds_until_open > preflight_lead_seconds:
            return max(
                1.0,
                min(
                    float(self.cfg.reconcile_interval_seconds),
                    seconds_until_open - preflight_lead_seconds,
                ),
            )

        self.rollover_preflighted_groups.add(next_key)
        # The upcoming group was already discovered and is being book-polled
        # in the wider pre-open window. Reconcile its active state at the
        # scheduled boundary; retries remain rate-limited if the exchange has
        # not flipped the market state yet.
        self.pending_rollover_check = (next_key, next_open)
        self.pending_rollover_tickers = self._rollover_group_tickers(next_open)
        self.rollover_priority_retry_count = 0
        self.status.update(
            rollover_preflight_group_open_utc=next_key,
            rollover_preflight_started_utc=utc_now(),
            rollover_preflighted_group_count=len(self.rollover_preflighted_groups),
            rollover_priority_reconcile_ticker_count=len(self.pending_rollover_tickers),
            rollover_priority_reconcile_state=(
                "PREPARED" if self.pending_rollover_tickers else "NO_PREPARED_TICKERS"
            ),
        )
        return max(1.0, (next_open - now).total_seconds())

    def _rollover_group_tickers(self, opening: datetime) -> set[str]:
        """Return confirmed contracts prepared for one scheduled opening."""
        allowed = {"initialized", "unopened", "active", "open"}
        return {
            ticker
            for ticker, market in self.markets.items()
            if market.get("is_15m_confirmed")
            and parse_time(market.get("open_time")) == opening
            and str(market.get("market_status", "")).lower() in allowed
        }

    def _skip_upcoming_scan_for_postopen_reconcile(self) -> bool:
        """Use one scheduled inventory pass to publish a prewarmed opening quickly."""
        if not self.rollover_postopen_reconcile_pending:
            return False
        self.rollover_postopen_reconcile_pending = False
        return True

    def _public_trade_poll_rollover_deferral(
        self, now: datetime | None = None
    ) -> tuple[float, datetime] | None:
        """Yield the shared REST request budget to a market opening reconcile."""
        now = now or datetime.now(timezone.utc)
        quiet_before = self.cfg.rollover_trade_quiet_before_seconds
        quiet_after = self.cfg.rollover_trade_quiet_after_seconds
        imminent: list[tuple[float, datetime]] = []
        for market in self.markets.values():
            if not market.get("is_15m_confirmed"):
                continue
            opened = parse_time(market.get("open_time"))
            closed = parse_time(market.get("close_time"))
            if opened is None or (closed is not None and closed <= now):
                continue
            delta = (opened - now).total_seconds()
            if -quiet_after <= delta <= quiet_before:
                imminent.append((delta, opened))
        if not imminent:
            return None
        future = [item for item in imminent if item[0] >= 0]
        target_open = min(future, key=lambda item: item[0])[1] if future else max(
            imminent, key=lambda item: item[0]
        )[1]
        delay = max(1.0, (target_open + timedelta(seconds=quiet_after) - now).total_seconds())
        return delay, target_open

    def _observe_rollover_transition(
        self, current: set[str], now: datetime
    ) -> tuple[int, set[str], datetime | None, float | None]:
        """Detect a newly active 15-minute window from its exchange open time."""
        current_open_times = {
            ticker: opened
            for ticker in current
            if (opened := parse_time(self.markets[ticker].get("open_time"))) is not None
        }
        current_windows = set(current_open_times.values())
        new_windows = current_windows - self.previous_current_open_windows
        transition_tickers = {
            ticker for ticker, opened in current_open_times.items() if opened in new_windows
        }
        preflight_open = parse_time(
            self.status.data.get("rollover_preflight_group_open_utc")
        )
        preflight_matches = preflight_open is not None and preflight_open in new_windows
        scheduled_open: datetime | None = None
        latency_seconds: float | None = None
        rollover_count = 0
        if (
            (self.previous_current_open_windows or preflight_matches)
            and new_windows
            and transition_tickers
        ):
            scheduled_open = min(new_windows)
            event_ids = {
                str(self.markets[ticker].get("event_ticker"))
                for ticker in transition_tickers
                if self.markets[ticker].get("event_ticker")
            }
            rollover_count = len(event_ids) or len(transition_tickers)
            latency_seconds = max(0.0, (now - scheduled_open).total_seconds())
        # Keep the last known open window across a temporarily empty inventory.
        if current_windows:
            self.previous_current_open_windows = current_windows
        return rollover_count, transition_tickers, scheduled_open, latency_seconds

    async def emit(
        self,
        channel: str,
        kind: str,
        payload: dict[str, Any],
        market_ticker: str | None = None,
        exchange_ts: Any = None,
        dedupe_key: str | None = None,
    ) -> None:
        event = {
            "channel": channel,
            "kind": kind,
            "market_ticker": market_ticker,
            "exchange_ts": str(exchange_ts) if exchange_ts is not None else None,
            "received_utc": utc_now(),
            "dedupe_key": dedupe_key,
            "payload": payload,
        }
        # Backpressure the async producer instead of dropping a journal event.
        await self.queue.put(event)

    async def writer_loop(self) -> None:
        loop = asyncio.get_running_loop()

        async def flush_aged_buffer(reason: str) -> int:
            dropped_before = self.storage.parquet_dropped_rows
            try:
                flushed_rows = await loop.run_in_executor(
                    self.writer_pool,
                    self.storage.flush_parquet_if_due,
                    reason,
                )
            except Exception as exc:
                self.status.update(
                    error_count=self.status.data.get("error_count", 0) + 1,
                    last_error=f"Parquet age flush failed: {type(exc).__name__}",
                )
                LOG.exception("Parquet age flush failed")
                return 0
            if not flushed_rows:
                if self.storage.parquet_dropped_rows != dropped_before:
                    storage_status = await loop.run_in_executor(
                        self.writer_pool, self.storage.connection_status
                    )
                    self.status.update(**storage_status)
                return 0
            storage_status = await loop.run_in_executor(
                self.writer_pool, self.storage.connection_status
            )
            self.status.update(
                queue_depth=self.queue.qsize(),
                parquet_flush_trigger_queue_depth=self.queue.qsize(),
                **storage_status,
            )
            return flushed_rows

        while not self.stopping.is_set() or not self.queue.empty():
            batch: list[dict[str, Any]] = []
            try:
                batch.append(await asyncio.wait_for(self.queue.get(), timeout=1.0))
            except TimeoutError:
                await flush_aged_buffer("IDLE_BUFFER_AGE")
                continue
            while len(batch) < 500:
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            retry_count = 0
            while True:
                try:
                    await loop.run_in_executor(
                        self.writer_pool, self.storage.append_batch_resilient, batch
                    )
                    break
                except Exception as exc:
                    retry_count += 1
                    delay = min(0.25 * (2 ** min(retry_count - 1, 7)), 30.0)
                    try:
                        self.status.update(
                            error_count=self.status.data.get("error_count", 0) + 1,
                            last_error=f"journal write pending retry: {type(exc).__name__}",
                            storage_write_state="DEGRADED_RETRYING",
                            storage_pending_batch_events=len(batch),
                            storage_write_retry_count=(
                                int(self.status.data.get("storage_write_retry_count", 0)) + 1
                            ),
                            storage_last_write_error=type(exc).__name__,
                        )
                    except Exception:
                        LOG.exception("could not persist journal retry status")
                    if retry_count == 1 or retry_count % 10 == 0:
                        LOG.error(
                            "journal write failed; retaining %d events and retrying in %.2fs",
                            len(batch),
                            delay,
                            exc_info=True,
                        )
                    await asyncio.sleep(delay)
            for _ in batch:
                self.queue.task_done()
            try:
                self.status.update(
                    storage_write_state="HEALTHY",
                    storage_pending_batch_events=0,
                    storage_last_write_error=None,
                    storage_last_write_retry_count=retry_count,
                )
            except Exception:
                LOG.exception("could not persist journal write status")
            await flush_aged_buffer("BUSY_BUFFER_AGE")
            storage_status = await loop.run_in_executor(
                self.writer_pool, self.storage.connection_status
            )
            self.status.update(
                queue_depth=self.queue.qsize(),
                events_inserted=self.storage.events_inserted,
                events_duplicate=self.storage.events_duplicate,
                events_key_conflict=self.storage.events_key_conflict,
                events_quarantined=self.storage.events_quarantined,
                raw_capture_state="ENABLED" if self.storage.raw_capture_enabled else "THROTTLED_BUDGET",
                **storage_status,
            )

    async def heartbeat_loop(self) -> None:
        while not self.stopping.is_set():
            self.status.refresh_ages()
            try:
                await asyncio.wait_for(self.stopping.wait(), timeout=5)
            except TimeoutError:
                pass

    async def refresh_inventory(self) -> None:
        began = time.monotonic()
        requests_before = self.api.requests
        attempts_before = self.api.attempts
        errors_before = self.api.errors
        upcoming_rows_discovered = 0
        postopen_series_scan_skipped = False
        priority_tickers: set[str] = set()
        priority_request_count = 0
        priority_reconcile_state = "IDLE"
        catalog_status: dict[str, Any] = {}
        series_rows_override: list[dict[str, Any]] | None = None
        try:
            postopen_check_due = self._skip_upcoming_scan_for_postopen_reconcile()
            priority_tickers = set(self.rollover_priority_tickers)
            postopen_series_scan_skipped = postopen_check_due or bool(priority_tickers)
            full_refresh = (
                self.inventory_snapshot is None
                or time.monotonic() - self.last_full_inventory_monotonic
                >= self.cfg.discovery_refresh_seconds
            )
            # A due full discovery can wait one pass so the prepared opening
            # group reaches the transition check using one narrow batch first.
            if priority_tickers and self.inventory_snapshot is not None:
                full_refresh = False
            snapshot = self.inventory_snapshot
            if (
                not full_refresh
                and not priority_tickers
                and snapshot is not None
                and time.monotonic() - self.last_series_catalog_monotonic
                >= self.cfg.series_catalog_refresh_seconds
            ):
                # The catalog endpoint is a single small request. Keep this
                # membership probe separate from broad paginated discovery.
                self.last_series_catalog_monotonic = time.monotonic()
                try:
                    fresh_series = await self.api.pages("/series", "series", max_pages=1)
                    added, removed = series_catalog_changes(snapshot, fresh_series)
                    checked_utc = utc_now()
                    catalog_status = {
                        "series_catalog_state": "CHANGED" if added or removed else "READY",
                        "series_catalog_last_checked_utc": checked_utc,
                        "series_catalog_api_rows": len(fresh_series),
                        "series_catalog_candidate_count": len(
                            candidate_series_tickers(fresh_series)
                        ),
                        "series_catalog_added_tickers": added,
                        "series_catalog_removed_tickers": removed,
                        "series_catalog_error": None,
                    }
                    if added or removed:
                        full_refresh = True
                        series_rows_override = fresh_series
                    else:
                        snapshot["series_api_rows"] = len(fresh_series)
                        snapshot["series_catalog_candidate_tickers"] = sorted(
                            candidate_series_tickers(fresh_series)
                        )
                        snapshot["series_catalog_fingerprint_checked_utc"] = checked_utc
                except PaginationLimitExceeded as exc:
                    catalog_status = {
                        "series_catalog_state": "INCOMPLETE_PAGINATION",
                        "series_catalog_error": f"pagination cursor after {exc.pages} page(s)",
                    }
                except Exception as exc:
                    catalog_status = {
                        "series_catalog_state": "PROBE_ERROR",
                        "series_catalog_error": type(exc).__name__,
                    }
            if full_refresh:
                snapshot = await discover(
                    self.api,
                    self.cfg.home / "reports",
                    series_rows_override=series_rows_override,
                )
                self.inventory_snapshot = snapshot
                self.last_full_inventory_monotonic = time.monotonic()
                self.last_series_catalog_monotonic = self.last_full_inventory_monotonic
                catalog_status = {
                    "series_catalog_state": "READY",
                    "series_catalog_last_checked_utc": snapshot.get(
                        "series_catalog_fingerprint_checked_utc", snapshot.get("snapshot_utc")
                    ),
                    "series_catalog_api_rows": snapshot.get("series_api_rows", 0),
                    "series_catalog_candidate_count": len(
                        snapshot.get("series_catalog_candidate_tickers", [])
                    ),
                    "series_catalog_added_tickers": catalog_status.get(
                        "series_catalog_added_tickers", []
                    ),
                    "series_catalog_removed_tickers": catalog_status.get(
                        "series_catalog_removed_tickers", []
                    ),
                    "series_catalog_error": None,
                }
            else:
                assert snapshot is not None
                if priority_tickers:
                    requests_before_priority = self.api.requests
                    await refresh_known_market_rows(
                        self.api,
                        snapshot,
                        near_open_seconds=self.cfg.rollover_preopen_seconds,
                        ticker_override=priority_tickers,
                        ticker_batch_size=self.cfg.rollover_priority_batch_size,
                    )
                    priority_request_count = self.api.requests - requests_before_priority
                    markets_by_ticker = {
                        str(market.get("ticker")): market
                        for market in snapshot.get("markets", [])
                        if market.get("ticker")
                    }
                    returned_tickers = set(
                        snapshot.get("market_api_reconciliation_returned_tickers", [])
                    )
                    group_rows = [markets_by_ticker.get(ticker) for ticker in priority_tickers]
                    all_active = all(
                        row is not None
                        and str(row.get("market_status", "")).lower() in {"active", "open"}
                        for row in group_rows
                    )
                    complete = priority_tickers <= returned_tickers
                    if complete and all_active:
                        priority_reconcile_state = "READY"
                        self.rollover_priority_tickers.clear()
                        self.rollover_priority_retry_count = 0
                    else:
                        self.rollover_priority_retry_count += 1
                        if self.rollover_priority_retry_count >= 3:
                            priority_reconcile_state = (
                                "NOT_ACTIVE_AFTER_RETRIES" if complete else "INCOMPLETE_AFTER_RETRIES"
                            )
                            self.rollover_priority_tickers.clear()
                            self.rollover_priority_retry_count = 0
                        else:
                            priority_reconcile_state = (
                                "WAITING_FOR_EXCHANGE_OPEN" if complete else "INCOMPLETE_RETRY_PENDING"
                            )
                else:
                    await refresh_known_market_rows(
                        self.api,
                        snapshot,
                        near_open_seconds=self.cfg.rollover_preopen_seconds,
                    )
                if not postopen_series_scan_skipped:
                    (
                        self.rollover_series_scan_cursor,
                        upcoming_rows_discovered,
                    ) = await refresh_upcoming_series_markets(
                        self.api,
                        snapshot,
                        near_open_seconds=self.cfg.rollover_preopen_seconds,
                        scan_cursor=self.rollover_series_scan_cursor,
                        scan_batch_size=self.cfg.rollover_series_scan_batch_size,
                    )
            self.series = snapshot["series"]
            series_categories = {
                str(row.get("series_ticker") or row.get("ticker")): str(
                    row.get("category") or "UNKNOWN"
                )
                for row in self.series
                if row.get("series_ticker") or row.get("ticker")
            }
            if series_categories != self.crossmarket_series_categories:
                self.crossmarket_series_categories = series_categories
                self.crossmarket_performance_dirty = True
            self.markets = {
                str(market["ticker"]): market
                for market in snapshot["markets"]
                if market.get("ticker")
            }
            now = datetime.now(timezone.utc)
            current: set[str] = set()
            opening_soon: set[str] = set()
            for ticker, market in self.markets.items():
                opens = parse_time(market.get("open_time"))
                closes = parse_time(market.get("close_time"))
                if market["is_15m_confirmed"]:
                    status = str(market["market_status"]).lower()
                    if (opens is None or opens <= now) and (closes is None or closes > now):
                        if status in {"active", "open"}:
                            current.add(ticker)
                    if status in {"active", "open", "initialized", "unopened"} and (
                        opens
                        and -120 <= (opens - now).total_seconds() <= self.cfg.rollover_preopen_seconds
                        and (closes is None or closes > now)
                    ):
                        opening_soon.add(ticker)
            desired = current | opening_soon
            self.active_tickers = current
            retired_tickers = self.open_tickers - desired
            self.open_tickers = desired
            for ticker in retired_tickers:
                self.books.pop(ticker, None)
                self.book_received_monotonic.pop(ticker, None)
            if desired != self.subscribed_tickers:
                while self.subscription_updates.full():
                    try:
                        self.subscription_updates.get_nowait()
                        self.subscription_updates.task_done()
                    except asyncio.QueueEmpty:
                        break
                self.subscription_updates.put_nowait(set(desired))
            series_category_counts: dict[str, int] = {}
            for series in snapshot["series"]:
                category = str(series.get("category") or "UNKNOWN")
                series_category_counts[category] = series_category_counts.get(category, 0) + 1
            active_compatibility_counts: dict[str, int] = {}
            for market in snapshot["markets"]:
                if market.get("ticker") in current:
                    compatibility = str(market.get("paper_compatibility") or "UNKNOWN")
                    active_compatibility_counts[compatibility] = (
                        active_compatibility_counts.get(compatibility, 0) + 1
                    )
            snapshot["active_15m_market_rows"] = len(current)
            (
                rollover_count,
                rollover_tickers,
                scheduled_open,
                rollover_latency_seconds,
            ) = self._observe_rollover_transition(current, now)
            rollover_open_utc = scheduled_open.isoformat() if scheduled_open else None
            self.status.update(
                service_state="RUNNING",
                rest_state="READY" if snapshot["api_errors"] == 0 else "DEGRADED",
                series_count=snapshot["fifteen_min_series"],
                series_category_counts=series_category_counts,
                series_api_rows=snapshot["series_api_rows"],
                series_catalog_refresh_seconds=self.cfg.series_catalog_refresh_seconds,
                active_market_rows=len(current),
                active_market_compatibility_counts=active_compatibility_counts,
                market_tickers=sorted(current),
                open_or_upcoming_count=len(desired),
                rollover_preopen_seconds=self.cfg.rollover_preopen_seconds,
                rollover_preflight_lead_seconds=self.cfg.rollover_preflight_lead_seconds,
                rollover_priority_batch_size=self.cfg.rollover_priority_batch_size,
                rollover_prepared_market_tickers=sorted(opening_soon),
                rollover_series_scan_batch_size=self.cfg.rollover_series_scan_batch_size,
                rollover_series_scan_cursor=self.rollover_series_scan_cursor,
                rollover_series_scan=snapshot.get("rollover_series_scan"),
                rollover_postopen_series_scan_skipped=postopen_series_scan_skipped,
                rollover_priority_reconcile_state=(
                    priority_reconcile_state if priority_tickers else
                    ("PREPARED" if self.rollover_priority_tickers else "IDLE")
                ),
                rollover_priority_reconcile_ticker_count=(
                    len(priority_tickers) or len(self.rollover_priority_tickers)
                ),
                rollover_priority_reconcile_request_count=priority_request_count,
                rollover_priority_reconcile_retry_count=self.rollover_priority_retry_count,
                rollover_upcoming_market_rows_discovered=(
                    self.status.data.get("rollover_upcoming_market_rows_discovered", 0)
                    + upcoming_rows_discovered
                ),
                inventory_snapshot_utc=snapshot["snapshot_utc"],
                inventory_last_reconciled_utc=utc_now(),
                inventory_reconcile_interval_seconds=self.cfg.reconcile_interval_seconds,
                inventory_reconcile_duration_seconds=round(time.monotonic() - began, 3),
                inventory_reconcile_request_count=self.api.requests - requests_before,
                inventory_reconcile_attempt_count=self.api.attempts - attempts_before,
                inventory_reconcile_error_count=self.api.errors - errors_before,
                inventory_refresh_mode=(
                    "ROLLOVER_PRIORITY_RECONCILIATION" if priority_tickers
                    else "FULL_DISCOVERY" if full_refresh
                    else "CACHED_SCHEDULE_RECONCILIATION"
                ),
                **catalog_status,
                discovery_coverage_state=snapshot.get("discovery_coverage_state", "UNKNOWN"),
                discovery_coverage_error_count=len(snapshot.get("discovery_coverage_errors", [])),
                event_pagination_truncated=snapshot.get("event_pagination_truncated", []),
                mve_history_truncated_series=snapshot.get("mve_latest_page_truncated_series", []),
                rest_settlements_observed=snapshot.get("rest_settlements_observed", 0),
                discovery_duration_seconds=round(time.monotonic() - began, 3),
                market_api_requests=self.api.requests,
                market_api_attempts=self.api.attempts,
                market_api_rate_limit_responses=self.api.rate_limit_responses,
                market_api_rate_limit_backoff_count=self.api.rate_limit_backoff_count,
                market_api_rate_limit_backoff_last_seconds=self.api.rate_limit_backoff_seconds,
                market_api_rate_limit_backoff_scheduled_seconds=(
                    self.api.rate_limit_backoff_scheduled_seconds
                ),
                market_api_last_rate_limit_path=self.api.last_rate_limit_path,
                market_api_last_rate_limit_utc=self.api.last_rate_limit_utc,
                market_api_errors=self.api.errors,
                market_api_last_error=self.api.last_error,
                actual_rollovers_observed=self.status.data.get("actual_rollovers_observed", 0) + rollover_count,
                rollover_detection_sample_count=(
                    self.status.data.get("rollover_detection_sample_count", 0)
                    + (1 if rollover_latency_seconds is not None else 0)
                ),
                **(
                    {
                        "last_rollover_detected_utc": utc_now(),
                        "last_rollover_scheduled_open_utc": rollover_open_utc,
                        "last_rollover_detection_latency_seconds": round(rollover_latency_seconds, 3),
                        "last_rollover_new_market_tickers": sorted(rollover_tickers),
                    }
                    if rollover_latency_seconds is not None
                    else {}
                ),
            )
            if rollover_latency_seconds is not None:
                await self.emit(
                    "rollover",
                    "market_transition_detected",
                    {
                        "detected_utc": utc_now(),
                        "scheduled_open_utc": rollover_open_utc,
                        "detection_latency_seconds": round(rollover_latency_seconds, 3),
                        "new_market_tickers": sorted(rollover_tickers),
                        "preopen_seconds": self.cfg.rollover_preopen_seconds,
                        "priority_reconcile_state": self.status.data.get(
                            "rollover_priority_reconcile_state"
                        ),
                        "priority_reconcile_ticker_count": self.status.data.get(
                            "rollover_priority_reconcile_ticker_count"
                        ),
                        "priority_reconcile_request_count": self.status.data.get(
                            "rollover_priority_reconcile_request_count"
                        ),
                        "exchange_write_calls": 0,
                    },
                    dedupe_key=f"rollover-detected|{rollover_open_utc}",
                )
            market_rows_to_emit = (
                snapshot["markets"]
                if full_refresh
                else [
                    self.markets[ticker]
                    for ticker in snapshot.get("market_api_reconciliation_tickers", [])
                    if ticker in self.markets
                ]
            )
            for market in market_rows_to_emit:
                ticker = str(market.get("ticker", ""))
                if ticker and ticker not in self.first_seen_tickers:
                    self.first_seen_tickers.add(ticker)
                    await self.emit(
                        "rest_reconciliation",
                        "market_first_seen",
                        {
                            "ticker": ticker,
                            "scheduled_open_utc": market.get("open_time"),
                            "first_seen_utc": utc_now(),
                            "run_id": self.run_id,
                            "first_seen_is_api_response_time": False,
                            "limitation": "observed after inventory pass; may lag first HTTP response",
                        },
                        ticker,
                        dedupe_key=f"market-first-seen|{ticker}",
                    )
                await self.emit(
                    "rest_reconciliation",
                    "market_metadata",
                    market,
                    ticker,
                    market.get("updated_time"),
                    "market-meta|" + ticker + "|" + hashlib.sha256(
                        json.dumps(
                            market.get("market_api_raw") or market,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=False,
                            default=str,
                        ).encode("utf-8")
                    ).hexdigest(),
                )
            await self.emit(
                "rest_reconciliation",
                "inventory_snapshot",
                {
                    "snapshot_utc": snapshot["snapshot_utc"],
                    "series_api_rows": snapshot["series_api_rows"],
                    "fifteen_min_series": snapshot["fifteen_min_series"],
                    "active_15m_market_rows": snapshot["active_15m_market_rows"],
                    "market_event_rows": snapshot["market_event_rows"],
                },
                dedupe_key=f"inventory-observed|{self.run_id}|{utc_now()}",
            )
            if self.paper_positions:
                await self.settle_paper_positions()
            if self.crossmarket_positions:
                await self.settle_paper_positions(crossmarket=True)
            LOG.info(
                "%s inventory series=%d active_markets=%d api_requests=%d",
                "full" if full_refresh else "scheduled-reconcile",
                snapshot["fifteen_min_series"],
                snapshot["active_15m_market_rows"],
                self.api.requests,
            )
        except Exception as exc:
            self.status.update(
                rest_state="DEGRADED",
                error_count=self.status.data.get("error_count", 0) + 1,
                last_error=f"discovery failed: {type(exc).__name__}: {exc}",
            )
            LOG.exception("inventory discovery failed")

    async def restore_paper_state(self) -> None:
        if not self.strategy_profile_id:
            return
        loop = asyncio.get_running_loop()
        records = await loop.run_in_executor(
            self.writer_pool, self.storage.paper_records, self.strategy_profile_id
        )
        self.paper_scope_decision_windows.clear()
        scope_prefix = f"scope-decision|{self.strategy_profile_id}|"
        for record in records:
            key = str(record.get("idempotency_key") or "")
            ticker = str(record.get("payload", {}).get("market_ticker") or "")
            if record.get("record_type") == "DECISION" and key.startswith(scope_prefix) and ticker:
                self.paper_scope_decision_windows[ticker] = key
        attempt_tickers: set[str] = set()
        completed_tickers: set[str] = set()
        self.paper_decision_count = 0
        self.paper_attempt_count = 0
        self.paper_fill_count = 0
        self.paper_settlement_count = 0
        self.paper_realized_net_pnl = Decimal("0")
        self.paper_unresolved_fee_settlements = 0
        self.paper_attempted_tickers.clear()
        self.paper_positions.clear()
        self.paper_settled_tickers.clear()
        self.paper_provisional_outcome_tickers.clear()
        classified_legacy_settlements = {
            str(record["payload"].get("source_record_key"))
            for record in records
            if record["record_type"] == "PAPER_SETTLEMENT_RECLASSIFICATION"
        }
        for record in records:
            kind = record["record_type"]
            payload = record["payload"]
            ticker = str(payload.get("market_ticker") or "")
            if kind == "DECISION":
                self.paper_decision_count += 1
            elif kind == "ENTRY_ATTEMPT":
                if ticker:
                    attempt_tickers.add(ticker)
                    self.paper_attempted_tickers.add(ticker)
                self.paper_attempt_count += 1
            elif kind in {"PAPER_FILL", "PAPER_ORDER_RESULT"}:
                if ticker:
                    completed_tickers.add(ticker)
            if kind == "PAPER_FILL" and ticker:
                self.paper_fill_count += 1
                filled = Decimal(str(payload.get("filled_quantity") or "0"))
                if filled > 0:
                    self.paper_positions[ticker] = payload
            elif kind == "PAPER_ORDER_RESULT":
                pass
            elif kind in {"PAPER_SETTLEMENT", "PAPER_SETTLEMENT_FINAL"} and ticker:
                if has_final_official_settlement(payload):
                    self.paper_settlement_count += 1
                    self.paper_settled_tickers.add(ticker)
                    self.paper_provisional_outcome_tickers.discard(ticker)
                    self.paper_positions.pop(ticker, None)
                    pnl = payload.get("net_pnl_dollars")
                    if pnl is None:
                        self.paper_unresolved_fee_settlements += 1
                    else:
                        self.paper_realized_net_pnl += Decimal(str(pnl))
                else:
                    self.paper_provisional_outcome_tickers.add(ticker)
                    if kind == "PAPER_SETTLEMENT":
                        source_key = str(record.get("idempotency_key") or "")
                        if source_key and source_key not in classified_legacy_settlements:
                            await self._append_paper_record(
                                f"settlement-reclassification|{self.strategy_profile_id}|{source_key}",
                                "PAPER_SETTLEMENT_RECLASSIFICATION",
                                {
                                    "market_ticker": ticker,
                                    "source_record_key": source_key,
                                    "prior_market_status": payload.get("official_market_status"),
                                    "prior_official_result": payload.get("official_market_result"),
                                    "classification": "PROVISIONAL_RESULT_NOT_FINAL_SETTLEMENT",
                                    "reason": "P&L remains unrealized until the official market status is finalized or settled",
                                    "exchange_write_calls": 0,
                                },
                            )
            elif kind == "PAPER_RESULT_PROVISIONAL" and ticker:
                self.paper_provisional_outcome_tickers.add(ticker)
            elif kind == "PAPER_SETTLEMENT_RECLASSIFICATION" and ticker:
                self.paper_provisional_outcome_tickers.add(ticker)

        # An interrupted local IOC simulation cannot be replayed from a later
        # book, so preserve the consumed one-attempt latch and close it as no fill.
        for ticker in sorted(attempt_tickers - completed_tickers):
            await self._append_paper_record(
                f"paper-order-result|{self.strategy_profile_id}|{ticker}",
                "PAPER_ORDER_RESULT",
                {
                    "market_ticker": ticker,
                    "result_state": "SIMULATION_INTERRUPTED_AFTER_DURABLE_INTENT",
                    "filled_quantity": "0",
                    "recovery_action": "NO_REPLAY_NO_RETRADE",
                    "strategy_revision": self.strategy_config["revision"] if self.strategy_config else None,
                },
            )
        self._update_paper_status()

    async def restore_crossmarket_paper_state(self) -> None:
        if not self.crossmarket_profile_id or not self.crossmarket_strategy_config:
            return
        loop = asyncio.get_running_loop()
        records = await loop.run_in_executor(
            self.writer_pool, self.storage.paper_records, self.crossmarket_profile_id
        )
        self.crossmarket_scope_decision_windows.clear()
        scope_prefix = f"scope-decision|{self.crossmarket_profile_id}|"
        for record in records:
            key = str(record.get("idempotency_key") or "")
            ticker = str(record.get("payload", {}).get("market_ticker") or "")
            if record.get("record_type") == "DECISION" and key.startswith(scope_prefix) and ticker:
                self.crossmarket_scope_decision_windows[ticker] = key
        self.crossmarket_performance_records = [
            compact
            for record in records
            if (compact := self._compact_crossmarket_performance_record(record)) is not None
        ]
        self.crossmarket_performance_dirty = True
        attempt_tickers: set[str] = set()
        completed_tickers: set[str] = set()
        self.crossmarket_attempted_tickers.clear()
        self.crossmarket_positions.clear()
        self.crossmarket_settled_tickers.clear()
        self.crossmarket_decision_count = 0
        self.crossmarket_attempt_count = 0
        self.crossmarket_fill_count = 0
        self.crossmarket_settlement_count = 0
        self.crossmarket_realized_net_pnl = Decimal("0")
        self.crossmarket_unresolved_fee_settlements = 0
        self.crossmarket_provisional_outcome_tickers.clear()
        classified_legacy_settlements = {
            str(record["payload"].get("source_record_key"))
            for record in records
            if record["record_type"] == "PAPER_SETTLEMENT_RECLASSIFICATION"
        }
        for record in records:
            kind = record["record_type"]
            payload = record["payload"]
            ticker = str(payload.get("market_ticker") or "")
            if kind == "DECISION":
                self.crossmarket_decision_count += 1
            elif kind == "ENTRY_ATTEMPT":
                if ticker:
                    attempt_tickers.add(ticker)
                    self.crossmarket_attempted_tickers.add(ticker)
                self.crossmarket_attempt_count += 1
            elif kind in {"PAPER_FILL", "PAPER_ORDER_RESULT"} and ticker:
                completed_tickers.add(ticker)
            if kind == "PAPER_FILL" and ticker:
                self.crossmarket_fill_count += 1
                if Decimal(str(payload.get("filled_quantity") or "0")) > 0:
                    self.crossmarket_positions[ticker] = payload
            elif kind in {"PAPER_SETTLEMENT", "PAPER_SETTLEMENT_FINAL"} and ticker:
                if has_final_official_settlement(payload):
                    self.crossmarket_settlement_count += 1
                    self.crossmarket_settled_tickers.add(ticker)
                    self.crossmarket_provisional_outcome_tickers.discard(ticker)
                    self.crossmarket_positions.pop(ticker, None)
                    net = payload.get("net_pnl_dollars")
                    if net is None:
                        self.crossmarket_unresolved_fee_settlements += 1
                    else:
                        self.crossmarket_realized_net_pnl += Decimal(str(net))
                else:
                    self.crossmarket_provisional_outcome_tickers.add(ticker)
                    if kind == "PAPER_SETTLEMENT":
                        source_key = str(record.get("idempotency_key") or "")
                        if source_key and source_key not in classified_legacy_settlements:
                            await self._append_paper_record(
                                f"settlement-reclassification|{self.crossmarket_profile_id}|{source_key}",
                                "PAPER_SETTLEMENT_RECLASSIFICATION",
                                {
                                    "market_ticker": ticker,
                                    "source_record_key": source_key,
                                    "prior_market_status": payload.get("official_market_status"),
                                    "prior_official_result": payload.get("official_market_result"),
                                    "classification": "PROVISIONAL_RESULT_NOT_FINAL_SETTLEMENT",
                                    "reason": "P&L remains unrealized until the official market status is finalized or settled",
                                    "exchange_write_calls": 0,
                                },
                                profile_id=self.crossmarket_profile_id,
                            )
            elif kind == "PAPER_RESULT_PROVISIONAL" and ticker:
                self.crossmarket_provisional_outcome_tickers.add(ticker)
            elif kind == "PAPER_SETTLEMENT_RECLASSIFICATION" and ticker:
                self.crossmarket_provisional_outcome_tickers.add(ticker)

        for ticker in sorted(attempt_tickers - completed_tickers):
            await self._append_paper_record(
                f"paper-order-result|{self.crossmarket_profile_id}|{ticker}",
                "PAPER_ORDER_RESULT",
                {
                    "market_ticker": ticker,
                    "result_state": "SIMULATION_INTERRUPTED_AFTER_DURABLE_INTENT",
                    "filled_quantity": "0",
                    "recovery_action": "NO_REPLAY_NO_RETRADE",
                    "strategy_revision": self.crossmarket_strategy_config["revision"],
                },
                profile_id=self.crossmarket_profile_id,
            )
        self._update_crossmarket_paper_status()

    async def _append_paper_record(
        self,
        idempotency_key: str,
        record_type: str,
        payload: dict[str, Any],
        *,
        profile_id: str | None = None,
    ) -> bool:
        target_profile = profile_id or self.strategy_profile_id
        if not target_profile:
            raise RuntimeError("paper profile is unavailable")
        loop = asyncio.get_running_loop()
        retry_count = 0
        while True:
            try:
                inserted = await loop.run_in_executor(
                    self.writer_pool,
                    self.storage.append_paper_record,
                    idempotency_key,
                    target_profile,
                    record_type,
                    payload,
                )
                break
            except Exception as exc:
                retry_count += 1
                delay = min(0.25 * (2 ** min(retry_count - 1, 7)), 30.0)
                try:
                    self.status.update(
                        error_count=self.status.data.get("error_count", 0) + 1,
                        last_error=f"paper ledger write pending retry: {type(exc).__name__}",
                        storage_write_state="DEGRADED_RETRYING",
                        storage_write_retry_count=(
                            int(self.status.data.get("storage_write_retry_count", 0)) + 1
                        ),
                        storage_last_write_error=type(exc).__name__,
                    )
                except Exception:
                    LOG.exception("could not persist paper-ledger retry status")
                if retry_count == 1 or retry_count % 10 == 0:
                    LOG.error(
                        "paper-ledger record %s failed; retrying in %.2fs",
                        idempotency_key,
                        delay,
                        exc_info=True,
                    )
                await asyncio.sleep(delay)
        if inserted and target_profile == self.crossmarket_profile_id:
            compact = self._compact_crossmarket_performance_record(
                {"record_type": record_type, "payload": payload}
            )
            if compact is not None:
                self.crossmarket_performance_records.append(compact)
                self.crossmarket_performance_dirty = True
        storage_status = await loop.run_in_executor(
            self.writer_pool, self.storage.connection_status
        )
        self.status.update(
            **storage_status,
        )
        return inserted

    @staticmethod
    def _compact_crossmarket_performance_record(
        record: dict[str, Any],
    ) -> dict[str, Any] | None:
        record_type = str(record.get("record_type") or "")
        if record_type not in {
            "ENTRY_ATTEMPT",
            "PAPER_FILL",
            "PAPER_SETTLEMENT",
            "PAPER_SETTLEMENT_FINAL",
        }:
            return None
        payload = record.get("payload")
        if not isinstance(payload, dict):
            return None
        keys = {
            "market_ticker",
            "filled_quantity",
            "status",
            "official_market_status",
            "result",
            "official_market_result",
            "settlement_result",
            "side",
            "net_pnl_dollars",
        }
        return {
            "record_type": record_type,
            "payload": {key: payload[key] for key in keys if key in payload},
        }

    def _update_paper_status(self, **extra: Any) -> None:
        pnl_state = (
            "NO_SETTLED_FILLS"
            if self.paper_settlement_count == 0
            else "PARTIAL_FEE_COVERAGE"
            if self.paper_unresolved_fee_settlements
            else "CALCULATED"
        )
        self.status.update(
            strategy_decisions_persisted=self.paper_decision_count,
            paper_entry_attempts=self.paper_attempt_count,
            paper_fill_records=self.paper_fill_count,
            paper_settlement_records=self.paper_settlement_count,
            paper_provisional_outcome_count=len(self.paper_provisional_outcome_tickers),
            paper_open_positions=len(self.paper_positions),
            paper_open_position_tickers=sorted(self.paper_positions),
            paper_realized_net_pnl_dollars=str(self.paper_realized_net_pnl),
            paper_net_pnl_state=pnl_state,
            **extra,
        )

    def _update_crossmarket_paper_status(self, **extra: Any) -> None:
        pnl_state = (
            "NO_SETTLED_FILLS"
            if self.crossmarket_settlement_count == 0
            else "PARTIAL_FEE_COVERAGE"
            if self.crossmarket_unresolved_fee_settlements
            else "CALCULATED"
        )
        if self.crossmarket_performance_dirty:
            self.crossmarket_performance_summary = crossmarket_performance_summary(
                self.crossmarket_performance_records,
                self.crossmarket_series_categories,
            )
            self.crossmarket_performance_dirty = False
        self.status.update(
            crossmarket_decisions_persisted=self.crossmarket_decision_count,
            crossmarket_paper_entry_attempts=self.crossmarket_attempt_count,
            crossmarket_paper_fill_records=self.crossmarket_fill_count,
            crossmarket_paper_settlement_records=self.crossmarket_settlement_count,
            crossmarket_paper_provisional_outcome_count=(
                len(self.crossmarket_provisional_outcome_tickers)
            ),
            crossmarket_paper_open_positions=len(self.crossmarket_positions),
            crossmarket_paper_open_position_tickers=sorted(self.crossmarket_positions),
            crossmarket_paper_realized_net_pnl_dollars=str(self.crossmarket_realized_net_pnl),
            crossmarket_paper_net_pnl_state=pnl_state,
            crossmarket_performance_by_asset_class=(
                self.crossmarket_performance_summary["by_asset_class"]
            ),
            crossmarket_performance_by_market=(
                self.crossmarket_performance_summary["by_market"]
            ),
            **extra,
        )

    def _active_strategy_markets(self, now: datetime) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        compatible: list[dict[str, Any]] = []
        in_scope: list[dict[str, Any]] = []
        if not self.strategy_config:
            return compatible, in_scope
        prefix = f"{self.strategy_config['strategy']['market_series']}-"
        for market in self.markets.values():
            if (
                str(market.get("paper_compatibility")) != "BINARY_SINGLE_MARKET"
                or not market.get("is_15m_confirmed")
                or str(market.get("market_status", "")).lower() not in {"active", "open"}
            ):
                continue
            opened, closed = parse_time(market.get("open_time")), parse_time(market.get("close_time"))
            if (opened and opened > now) or (closed and closed <= now):
                continue
            compatible.append(market)
            if str(market.get("ticker", "")).startswith(prefix):
                in_scope.append(market)
        return compatible, in_scope

    def _websocket_orderbook_input(
        self, ticker: str, *, now_monotonic: float | None = None
    ) -> tuple[dict[str, Any], datetime, float] | None:
        """Return a synchronized local book and its receive time without REST access."""
        book = self.books.get(ticker)
        received_monotonic = self.book_received_monotonic.get(ticker)
        if book is None or not book.synchronized or received_monotonic is None:
            return None
        current_monotonic = time.monotonic() if now_monotonic is None else now_monotonic
        age = current_monotonic - received_monotonic
        if age < 0:
            return None
        orderbook = {
            "ticker": ticker,
            "orderbook_fp": {
                "yes_dollars_fp": [
                    [str(price), str(size)] for price, size in sorted(book.yes.items())
                ],
                "no_dollars_fp": [
                    [str(price), str(size)] for price, size in sorted(book.no.items())
                ],
            },
        }
        received_at = datetime.now(timezone.utc) - timedelta(seconds=age)
        return orderbook, received_at, age

    async def _load_entry_orderbooks(
        self,
        markets: list[dict[str, Any]],
        *,
        max_age_seconds: float,
        rest_channel: str,
        profile_health_field: str,
        status_prefix: str,
    ) -> tuple[dict[str, tuple[dict[str, Any] | None, datetime | None, str]], int, int]:
        """Prefer fresh authenticated WSS books; use a 15-second-throttled REST fallback."""
        now_monotonic = time.monotonic()
        inputs: dict[str, tuple[dict[str, Any] | None, datetime | None, str]] = {}
        fallback_tickers: list[str] = []
        websocket_count = 0
        for market in markets:
            ticker = str(market.get("ticker") or "")
            if not ticker:
                continue
            stream_input = self._websocket_orderbook_input(
                ticker, now_monotonic=now_monotonic
            )
            if stream_input is not None and stream_input[2] <= max_age_seconds:
                raw_book, received_at, _ = stream_input
                inputs[ticker] = (raw_book, received_at, "KALSHI_AUTHENTICATED_WSS")
                websocket_count += 1
                continue

            cached = self.rest_orderbook_cache.get(ticker)
            if cached is not None:
                cached_book, cached_at, cached_monotonic = cached
                age = now_monotonic - cached_monotonic
                if 0 <= age <= max_age_seconds:
                    inputs[ticker] = (
                        cached_book,
                        cached_at,
                        "KALSHI_PUBLIC_REST_FALLBACK_CACHE",
                    )
                    continue
            last_attempt = self.last_rest_fallback_request_monotonic.get(ticker, 0.0)
            if now_monotonic - last_attempt >= self.cfg.book_poll_interval_seconds:
                fallback_tickers.append(ticker)
            else:
                inputs[ticker] = (None, None, "REST_FALLBACK_RATE_LIMITED")

        request_count = 0
        successful_requests = 0
        errors = 0
        response_rows = 0
        started_utc = utc_now()
        if fallback_tickers:
            request_count = 1
            try:
                payload = await self.api.get(
                    "/markets/orderbooks", {"tickers": fallback_tickers}
                )
                successful_requests = 1
                received_at = datetime.now(timezone.utc)
                received_monotonic = time.monotonic()
                for ticker in fallback_tickers:
                    self.last_rest_fallback_request_monotonic[ticker] = received_monotonic
                for row in payload.get("orderbooks") or []:
                    ticker = str(row.get("ticker") or row.get("market_ticker") or "")
                    if not ticker or ticker not in fallback_tickers:
                        continue
                    raw_book = {"ticker": ticker, **row}
                    self.rest_orderbook_cache[ticker] = (
                        raw_book,
                        received_at,
                        received_monotonic,
                    )
                    inputs[ticker] = (
                        raw_book,
                        received_at,
                        "KALSHI_PUBLIC_REST_FALLBACK",
                    )
                    response_rows += 1
            except Exception as exc:
                errors = 1
                received_monotonic = time.monotonic()
                for ticker in fallback_tickers:
                    self.last_rest_fallback_request_monotonic[ticker] = received_monotonic
                    inputs[ticker] = (None, None, "REST_FALLBACK_ERROR")
                self.status.update(
                    error_count=self.status.data.get("error_count", 0) + 1,
                    last_error=f"{status_prefix} REST fallback failed: {type(exc).__name__}",
                )
                LOG.warning("%s REST orderbook fallback failed: %s", status_prefix, type(exc).__name__)

        rest_count = sum(
            source in {"KALSHI_PUBLIC_REST_FALLBACK", "KALSHI_PUBLIC_REST_FALLBACK_CACHE"}
            for _, _, source in inputs.values()
        )
        self.status.update(
            **{
                f"{status_prefix}_orderbook_source_state": (
                    "WEBSOCKET_PRIMARY"
                    if websocket_count and not rest_count
                    else "WEBSOCKET_WITH_REST_FALLBACK"
                    if websocket_count or rest_count
                    else "REST_FALLBACK_UNAVAILABLE"
                ),
                f"{status_prefix}_websocket_books_used": websocket_count,
                f"{status_prefix}_rest_fallback_markets_used": rest_count,
                f"{status_prefix}_rest_fallback_requests": (
                    int(self.status.data.get(f"{status_prefix}_rest_fallback_requests", 0))
                    + request_count
                ),
            }
        )
        if request_count:
            self.status.mark_rest_poll(
                rest_channel,
                started_utc=started_utc,
                request_count=request_count,
                successful_request_count=successful_requests,
                requested_market_count=len(fallback_tickers),
                markets_with_data=response_rows,
                rows_received=response_rows,
                error_count=errors,
            )
        else:
            self.status.mark_rest_poll_not_due(
                rest_channel,
                "NOT_DUE_FRESH_WEBSOCKET_BOOKS"
                if websocket_count == len(markets)
                else "FALLBACK_CACHE_OR_RATE_LIMIT",
                profile_health_field=profile_health_field,
            )
        return inputs, request_count, errors

    async def collect_strategy_inputs(self) -> None:
        if not self.strategy_config or not self.strategy_config_hash or not self.strategy_profile_id:
            return
        now = datetime.now(timezone.utc)
        compatible, targets = self._active_strategy_markets(now)
        target_tickers = {str(row.get("ticker")) for row in targets}
        latest_scope_decisions: dict[str, dict[str, Any]] = {}
        for market in compatible:
            ticker = str(market.get("ticker") or "")
            if not ticker or ticker in target_tickers:
                continue
            decision = evaluate_market(
                self.strategy_config,
                market,
                None,
                as_of=now,
                book_received_at=None,
                config_sha256=self.strategy_config_hash,
            )
            decision.update(
                {
                    "profile_id": self.strategy_profile_id,
                    "input_source": "OFFICIAL_REST_INVENTORY_SCOPE_GATE",
                    "research_orderbook_snapshot": None,
                    "exchange_write_calls": 0,
                }
            )
            market_window = str(market.get("open_time") or market.get("event_ticker") or ticker)
            scope_key = f"scope-decision|{self.strategy_profile_id}|{ticker}|{market_window}"
            if self.paper_scope_decision_windows.get(ticker) != scope_key:
                inserted = await self._append_paper_record(
                    scope_key,
                    "DECISION",
                    decision,
                )
                if inserted:
                    self.paper_decision_count += 1
                self.paper_scope_decision_windows[ticker] = scope_key
            latest_scope_decisions[ticker] = decision
        self.status.update(
            strategy_evaluators_active=len(targets),
            strategy_in_scope_evaluators_active=len(targets),
            strategy_candidate_markets_compatible=len(compatible),
            strategy_candidate_markets_out_of_scope=max(0, len(compatible) - len(targets)),
            strategy_scope_market_tickers=sorted(str(row.get("ticker")) for row in targets),
        )
        if not targets:
            self.status.mark_rest_poll_not_due(
                "rest_strategy_orderbook",
                "NOT_DUE_NO_IN_SCOPE_MARKET",
                profile_health_field="strategy_rest_poll_health",
            )
            self._update_paper_status(
                strategy_state=(
                    "ACTIVE_EVALUATING_SCOPE_EXCLUSIONS"
                    if compatible
                    else "ACTIVE_WAITING_FOR_CONFIGURED_MARKET"
                ),
                strategy_last_poll_utc=now.isoformat(),
                strategy_last_scope_decisions=latest_scope_decisions,
            )
            return

        prewarm = int(self.strategy_config.get("market_data", {}).get("prewarm_seconds_before_close", 75))
        due: list[dict[str, Any]] = []
        for market in targets:
            close = parse_time(market.get("close_time"))
            if close is None:
                due.append(market)
                continue
            seconds_left = (close - now).total_seconds()
            if 0 < seconds_left <= prewarm:
                due.append(market)
        if not due:
            self.status.mark_rest_poll_not_due(
                "rest_strategy_orderbook",
                "NOT_DUE_OUTSIDE_ENTRY_WINDOW",
                profile_health_field="strategy_rest_poll_health",
            )
            self._update_paper_status(
                strategy_state="ACTIVE_WAITING_FOR_ENTRY_WINDOW",
                strategy_last_poll_utc=now.isoformat(),
                strategy_last_decisions={},
                strategy_rest_poll_health="NOT_DUE_OUTSIDE_ENTRY_WINDOW",
                strategy_last_scope_decisions=latest_scope_decisions,
            )
            return

        max_book_age = float(self.strategy_config["market_data"]["max_orderbook_age_seconds"])
        book_inputs, _, errors = await self._load_entry_orderbooks(
            due,
            max_age_seconds=max_book_age,
            rest_channel="rest_strategy_orderbook",
            profile_health_field="strategy_rest_poll_health",
            status_prefix="strategy",
        )
        observed_at = datetime.now(timezone.utc)

        latest_decisions: dict[str, dict[str, Any]] = {}
        for market in due:
            ticker = str(market["ticker"])
            raw_book, book_received_at, input_source = book_inputs.get(
                ticker, (None, None, "ORDERBOOK_UNAVAILABLE")
            )
            decision = evaluate_market(
                self.strategy_config,
                market,
                raw_book,
                as_of=observed_at,
                book_received_at=book_received_at,
                submitted_attempt=ticker in self.paper_attempted_tickers,
                has_position=ticker in self.paper_positions,
                config_sha256=self.strategy_config_hash,
            )
            decision.update(
                {
                    "profile_id": self.strategy_profile_id,
                    "input_source": input_source,
                    "rest_request_rtt_ms": (
                        self.api.last_request_rtt_ms
                        if input_source.startswith("KALSHI_PUBLIC_REST")
                        else None
                    ),
                    "exchange_write_calls": 0,
                    "research_orderbook_snapshot": raw_book,
                }
            )
            decision_key = (
                f"decision|{self.strategy_profile_id}|{ticker}|"
                f"{observed_at.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')}"
            )
            try:
                inserted = await self._append_paper_record(decision_key, "DECISION", decision)
            except Exception as exc:
                inserted = False
                errors += 1
                self.status.update(
                    error_count=self.status.data.get("error_count", 0) + 1,
                    last_error=f"paper decision persistence failed: {type(exc).__name__}",
                )
                LOG.exception("paper decision write failed")
            if inserted:
                self.paper_decision_count += 1
            latest_decisions[ticker] = decision
            if inserted and decision.get("eligible"):
                await self._simulate_paper_entry(market, raw_book, decision)

        state = "ACTIVE_EVALUATING" if errors == 0 else "ACTIVE_DEGRADED_INPUT_OR_PERSISTENCE"
        self._update_paper_status(
            strategy_state=state,
            strategy_last_poll_utc=observed_at.isoformat(),
            strategy_last_decisions=latest_decisions,
            strategy_last_rest_request_rtt_ms=self.api.last_request_rtt_ms,
            strategy_rest_poll_health="DEGRADED" if errors else "READY",
            strategy_last_scope_decisions=latest_scope_decisions,
        )

    async def collect_crossmarket_strategy_inputs(self) -> None:
        config = self.crossmarket_strategy_config
        config_hash = self.crossmarket_strategy_config_hash
        profile_id = self.crossmarket_profile_id
        if not config or not config_hash or not profile_id:
            return
        if not validate_market_reference_policy(config):
            self._update_crossmarket_paper_status(
                crossmarket_strategy_state="BLOCKED_REFERENCE_POLICY"
            )
            return

        now = datetime.now(timezone.utc)
        live_markets: list[dict[str, Any]] = []
        classifications: dict[str, tuple[str, str]] = {}
        for market in self.markets.values():
            if str(market.get("market_status", "")).lower() not in {"active", "open"}:
                continue
            opened, closed = parse_time(market.get("open_time")), parse_time(market.get("close_time"))
            if (opened and opened > now) or (closed and closed <= now):
                continue
            ticker = str(market.get("ticker") or "")
            if not ticker:
                continue
            state, reason = crossmarket_eligibility(market)
            classifications[ticker] = (state, reason)
            live_markets.append(market)

        eligible_markets = [
            market for market in live_markets
            if classifications[str(market["ticker"])][0] == "ELIGIBLE"
        ]
        eligible_tickers = {str(market["ticker"]) for market in eligible_markets}
        latest_scope_decisions: dict[str, dict[str, Any]] = {}
        for market in live_markets:
            ticker = str(market["ticker"])
            classification, reason = classifications[ticker]
            window = str(market.get("open_time") or market.get("event_ticker") or ticker)
            decision = {
                "decision_id": f"scope:{profile_id}:{ticker}:{window}",
                "strategy_version": config["revision"],
                "config_revision": config["revision"],
                "config_sha256": config_hash,
                "parent_btc_config_sha256": self.strategy_config_hash,
                "market_ticker": ticker,
                "decision_utc": now.isoformat(),
                "eligible": False,
                "market_eligibility_state": classification,
                "rejection_reason": (
                    "NO_ENTRY_WINDOW_BOOK_EVALUATION_YET" if classification == "ELIGIBLE" else reason
                ),
                "paper_compatibility": market.get("paper_compatibility"),
                "market_type": market.get("market_type"),
                "duration_seconds": (
                    (parse_time(market.get("close_time")) - parse_time(market.get("open_time"))).total_seconds()
                    if parse_time(market.get("close_time")) and parse_time(market.get("open_time"))
                    else None
                ),
                "market_rules_primary": market.get("rules_primary"),
                "reference_data_policy": config["market_data"]["reference_data_policy"],
                "exchange_write_calls": 0,
            }
            scope_key = f"scope-decision|{profile_id}|{ticker}|{window}"
            if self.crossmarket_scope_decision_windows.get(ticker) != scope_key:
                inserted = await self._append_paper_record(
                    scope_key,
                    "DECISION",
                    decision,
                    profile_id=profile_id,
                )
                if inserted:
                    self.crossmarket_decision_count += 1
                self.crossmarket_scope_decision_windows[ticker] = scope_key
            latest_scope_decisions[ticker] = decision

        excluded_tickers = sorted(
            ticker for ticker, (state, _) in classifications.items() if state == "EXCLUDED"
        )
        blocked_tickers = sorted(
            ticker for ticker, (state, _) in classifications.items() if state == "BLOCKED"
        )
        self.status.update(
            crossmarket_strategy_state="ACTIVE_EVALUATING",
            crossmarket_evaluators_active=len(eligible_markets),
            crossmarket_eligible_market_tickers=sorted(eligible_tickers),
            crossmarket_excluded_market_tickers=excluded_tickers,
            crossmarket_blocked_market_reasons={
                ticker: reason for ticker, (state, reason) in classifications.items() if state == "BLOCKED"
            },
            crossmarket_excluded_market_reasons={
                ticker: reason for ticker, (state, reason) in classifications.items() if state == "EXCLUDED"
            },
            strategy_all_profiles_evaluators_active=(
                int(self.status.data.get("strategy_in_scope_evaluators_active", 0))
                + len(eligible_markets)
            ),
            crossmarket_last_scope_decisions=latest_scope_decisions,
        )

        prewarm = int(config.get("market_data", {}).get("prewarm_seconds_before_close", 75))
        due: list[dict[str, Any]] = []
        for market in eligible_markets:
            ticker = str(market["ticker"])
            close = parse_time(market.get("close_time"))
            if ticker in self.crossmarket_attempted_tickers or ticker in self.crossmarket_positions:
                continue
            if close is not None and 0 < (close - now).total_seconds() <= prewarm:
                due.append(market)
        if not due:
            self.status.mark_rest_poll_not_due(
                "rest_crossmarket_orderbook",
                "NOT_DUE_OUTSIDE_ENTRY_WINDOW",
                profile_health_field="crossmarket_rest_poll_health",
            )
            self._update_crossmarket_paper_status(
                crossmarket_strategy_state="ACTIVE_WAITING_FOR_ENTRY_WINDOW",
                crossmarket_last_poll_utc=now.isoformat(),
                crossmarket_last_decisions={},
                crossmarket_last_scope_decisions=latest_scope_decisions,
            )
            return

        max_book_age = float(config["market_data"]["max_orderbook_age_seconds"])
        book_inputs, _, errors = await self._load_entry_orderbooks(
            due,
            max_age_seconds=max_book_age,
            rest_channel="rest_crossmarket_orderbook",
            profile_health_field="crossmarket_rest_poll_health",
            status_prefix="crossmarket",
        )
        observed_at = datetime.now(timezone.utc)

        latest_decisions: dict[str, dict[str, Any]] = {}
        for market in due:
            ticker = str(market["ticker"])
            raw_book, book_received_at, input_source = book_inputs.get(
                ticker, (None, None, "ORDERBOOK_UNAVAILABLE")
            )
            decision = evaluate_market(
                config,
                market,
                raw_book,
                as_of=observed_at,
                book_received_at=book_received_at,
                submitted_attempt=ticker in self.crossmarket_attempted_tickers,
                has_position=ticker in self.crossmarket_positions,
                config_sha256=config_hash,
            )
            decision.update(
                {
                    "profile_id": profile_id,
                    "parent_btc_config_sha256": self.strategy_config_hash,
                    "market_eligibility_state": "ELIGIBLE",
                    "reference_data_policy": config["market_data"]["reference_data_policy"],
                    "input_source": input_source,
                    "rest_request_rtt_ms": (
                        self.api.last_request_rtt_ms
                        if input_source.startswith("KALSHI_PUBLIC_REST")
                        else None
                    ),
                    "exchange_write_calls": 0,
                    "research_orderbook_snapshot": raw_book,
                }
            )
            decision_key = (
                f"decision|{profile_id}|{ticker}|"
                f"{observed_at.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')}"
            )
            try:
                inserted = await self._append_paper_record(
                    decision_key, "DECISION", decision, profile_id=profile_id
                )
            except Exception as exc:
                inserted = False
                errors += 1
                self.status.update(
                    error_count=self.status.data.get("error_count", 0) + 1,
                    last_error=f"cross-market decision persistence failed: {type(exc).__name__}",
                )
                LOG.exception("cross-market decision write failed")
            if inserted:
                self.crossmarket_decision_count += 1
            latest_decisions[ticker] = decision
            if inserted and decision.get("eligible"):
                await self._simulate_paper_entry(market, raw_book, decision, crossmarket=True)

        self._update_crossmarket_paper_status(
            crossmarket_strategy_state=(
                "ACTIVE_EVALUATING" if errors == 0 else "ACTIVE_DEGRADED_INPUT_OR_PERSISTENCE"
            ),
            crossmarket_last_poll_utc=observed_at.isoformat(),
            crossmarket_last_decisions=latest_decisions,
            crossmarket_last_rest_request_rtt_ms=self.api.last_request_rtt_ms,
            crossmarket_rest_poll_health="DEGRADED" if errors else "READY",
            crossmarket_last_scope_decisions=latest_scope_decisions,
        )

    async def _simulate_paper_entry(
        self,
        market: dict[str, Any],
        decision_book: dict[str, Any] | None,
        decision: dict[str, Any],
        *,
        crossmarket: bool = False,
    ) -> None:
        ticker = str(market["ticker"])
        profile_id = self.crossmarket_profile_id if crossmarket else self.strategy_profile_id
        config = self.crossmarket_strategy_config if crossmarket else self.strategy_config
        attempted_tickers = self.crossmarket_attempted_tickers if crossmarket else self.paper_attempted_tickers
        positions = self.crossmarket_positions if crossmarket else self.paper_positions
        if ticker in attempted_tickers:
            return
        assert profile_id is not None
        revision = config["revision"] if config else None
        latency_proxy = self.api.last_request_rtt_ms
        attempt = {
            "market_ticker": ticker,
            "strategy_revision": revision,
            "decision_id": decision["decision_id"],
            "decision_utc": decision["decision_utc"],
            "candidate_direction": decision["candidate_direction"],
            "executable_acquisition_price": decision["executable_acquisition_price"],
            "intended_quantity": decision["intended_quantity"],
            "order_type": "TAKER_LIMIT_IOC_SIMULATION",
            "latency_proxy_ms": latency_proxy,
            "latency_proxy_source": "measured REST orderbook GET round-trip; not exchange order-submit latency",
            "decision_book": decision_book,
        }
        claimed = await self._append_paper_record(
            f"entry-attempt|{profile_id}|{ticker}",
            "ENTRY_ATTEMPT",
            attempt,
            profile_id=profile_id,
        )
        attempted_tickers.add(ticker)
        if not claimed:
            return
        if crossmarket:
            self.crossmarket_attempt_count += 1
            self._update_crossmarket_paper_status()
        else:
            self.paper_attempt_count += 1
            self._update_paper_status()

        if latency_proxy is None or latency_proxy > 5000:
            result = {
                "market_ticker": ticker,
                "result_state": "NO_FILL_LATENCY_PROXY_EXCEEDS_BOOK_FRESHNESS" if latency_proxy else "NO_FILL_LATENCY_UNAVAILABLE",
                "filled_quantity": "0",
                "simulated_latency_proxy_ms": latency_proxy,
                "exchange_write_calls": 0,
            }
            await self._append_paper_record(
                f"paper-order-result|{profile_id}|{ticker}",
                "PAPER_ORDER_RESULT",
                result,
                profile_id=profile_id,
            )
            return

        begun = time.monotonic()
        await asyncio.sleep(latency_proxy / 1000)
        try:
            arrival_payload = await self.api.get("/markets/orderbooks", {"tickers": [ticker]})
            arrival_books = arrival_payload.get("orderbooks") or []
            arrival_book = next(
                (
                    {"ticker": str(row.get("ticker") or row.get("market_ticker") or ticker), **row}
                    for row in arrival_books
                    if str(row.get("ticker") or row.get("market_ticker") or "") == ticker
                ),
                None,
            )
            observed_arrival = datetime.now(timezone.utc)
            elapsed_ms = round((time.monotonic() - begun) * 1000, 3)
            close = parse_time(market.get("close_time"))
            if close is None or close <= observed_arrival or str(market.get("market_status", "")).lower() not in {"open", "active"}:
                result = {
                    "market_ticker": ticker,
                    "result_state": "NO_FILL_MARKET_CLOSED_BEFORE_SIMULATED_ARRIVAL",
                    "filled_quantity": "0",
                    "simulated_latency_proxy_ms": latency_proxy,
                    "decision_to_arrival_observation_ms": elapsed_ms,
                    "exchange_write_calls": 0,
                }
            elif arrival_book is None:
                result = {
                    "market_ticker": ticker,
                    "result_state": "NO_FILL_BOOK_MISSING_AT_SIMULATED_ARRIVAL",
                    "filled_quantity": "0",
                    "simulated_latency_proxy_ms": latency_proxy,
                    "decision_to_arrival_observation_ms": elapsed_ms,
                    "exchange_write_calls": 0,
                }
            else:
                fee_type, fee_multiplier = resolve_fee_terms(market)
                simulation = simulate_taker_ioc(
                    arrival_book,
                    str(decision["candidate_direction"]),
                    decision["executable_acquisition_price"],
                    decision["intended_quantity"],
                    fee_type,
                    fee_multiplier,
                )
                result = {
                    **simulation,
                    "market_ticker": ticker,
                    "strategy_revision": revision,
                    "decision_id": decision["decision_id"],
                    "decision_utc": decision["decision_utc"],
                    "simulated_latency_proxy_ms": latency_proxy,
                    "decision_to_arrival_observation_ms": elapsed_ms,
                    "arrival_book_observed_utc": observed_arrival.isoformat(),
                    "arrival_orderbook_snapshot": arrival_book,
                    "settlement_policy": "HOLD_TO_SETTLEMENT",
                    "exchange_write_calls": 0,
                }
        except Exception as exc:
            LOG.warning("paper IOC depth simulation failed: %s", type(exc).__name__)
            result = {
                "market_ticker": ticker,
                "result_state": f"NO_FILL_SIMULATION_ERROR_{type(exc).__name__}",
                "filled_quantity": "0",
                "simulated_latency_proxy_ms": latency_proxy,
                "decision_to_arrival_observation_ms": round((time.monotonic() - begun) * 1000, 3),
                "exchange_write_calls": 0,
            }

        if Decimal(str(result.get("filled_quantity") or "0")) > 0:
            inserted = await self._append_paper_record(
                f"paper-fill|{profile_id}|{ticker}",
                "PAPER_FILL",
                result,
                profile_id=profile_id,
            )
            if inserted:
                if crossmarket:
                    self.crossmarket_fill_count += 1
                else:
                    self.paper_fill_count += 1
                positions[ticker] = result
        else:
            await self._append_paper_record(
                f"paper-order-result|{profile_id}|{ticker}",
                "PAPER_ORDER_RESULT",
                result,
                profile_id=profile_id,
            )
        if crossmarket:
            self._update_crossmarket_paper_status()
        else:
            self._update_paper_status()

    async def settle_paper_positions(self, *, crossmarket: bool = False) -> None:
        profile_id = self.crossmarket_profile_id if crossmarket else self.strategy_profile_id
        positions = self.crossmarket_positions if crossmarket else self.paper_positions
        if not positions or not profile_id:
            return
        profile_key = "crossmarket" if crossmarket else "btc"
        now_monotonic = time.monotonic()
        last_poll = self.last_settlement_poll_monotonic.get(profile_key, 0.0)
        if now_monotonic - last_poll < self.cfg.paper_settlement_poll_interval_seconds:
            return
        self.last_settlement_poll_monotonic[profile_key] = now_monotonic
        tickers = sorted(positions)
        market_rows: dict[str, dict[str, Any]] = {}
        for start in range(0, len(tickers), 5):
            batch = tickers[start : start + 5]
            try:
                rows = await self.api.pages(
                    "/markets",
                    "markets",
                    {"tickers": ",".join(batch), "limit": 1000},
                    max_pages=2,
                )
                for row in rows:
                    if row.get("ticker"):
                        market_rows[str(row["ticker"])] = row
            except Exception as exc:
                LOG.info("paper settlement reconciliation failed: %s", type(exc).__name__)
        for ticker, position in list(positions.items()):
            official_market = market_rows.get(ticker)
            if not official_market:
                continue
            result = str(official_market.get("result") or "").lower()
            if result not in {"yes", "no"}:
                continue
            provisional_tickers = (
                self.crossmarket_provisional_outcome_tickers
                if crossmarket
                else self.paper_provisional_outcome_tickers
            )
            if not has_final_official_settlement(official_market):
                if ticker not in provisional_tickers:
                    provisional_payload = {
                        "market_ticker": ticker,
                        "official_result_source": "Kalshi public REST GET /markets",
                        "official_market_status": official_market.get("status"),
                        "official_market_result": result,
                        "official_settlement_ts": official_market.get("settlement_ts"),
                        "result_observed_utc": utc_now(),
                        "settlement_state": "PROVISIONAL_RESULT_WAITING_FOR_FINALIZATION",
                        "strategy_revision": (
                            self.crossmarket_strategy_config["revision"]
                            if crossmarket and self.crossmarket_strategy_config
                            else self.strategy_config["revision"]
                            if self.strategy_config
                            else None
                        ),
                        "exchange_write_calls": 0,
                    }
                    try:
                        await self._append_paper_record(
                            f"provisional-result|{profile_id}|{ticker}|{result}",
                            "PAPER_RESULT_PROVISIONAL",
                            provisional_payload,
                            profile_id=profile_id,
                        )
                        provisional_tickers.add(ticker)
                    except Exception as exc:
                        LOG.info(
                            "provisional market result persistence failed: %s",
                            type(exc).__name__,
                        )
                continue
            settlement = settle_binary_paper_fill(
                str(position["side"]),
                position["average_fill_price_dollars"],
                position["filled_quantity"],
                position.get("entry_fee_dollars"),
                result,
            )
            payload = {
                **settlement,
                "market_ticker": ticker,
                "strategy_revision": position.get("strategy_revision"),
                "entry_decision_id": position.get("decision_id"),
                "official_result_source": "Kalshi public REST GET /markets",
                "official_market_status": official_market.get("status"),
                "official_market_result": result,
                "official_settlement_ts": official_market.get("settlement_ts"),
                "official_settlement_value_dollars": official_market.get("settlement_value_dollars"),
                "settlement_observed_utc": utc_now(),
                "exchange_write_calls": 0,
            }
            inserted = await self._append_paper_record(
                f"settlement-final|{profile_id}|{ticker}|{result}",
                "PAPER_SETTLEMENT_FINAL",
                payload,
                profile_id=profile_id,
            )
            if inserted:
                if crossmarket:
                    self.crossmarket_settlement_count += 1
                    self.crossmarket_settled_tickers.add(ticker)
                    self.crossmarket_provisional_outcome_tickers.discard(ticker)
                else:
                    self.paper_settlement_count += 1
                    self.paper_settled_tickers.add(ticker)
                    self.paper_provisional_outcome_tickers.discard(ticker)
                positions.pop(ticker, None)
                net = settlement.get("net_pnl_dollars")
                if net is None:
                    if crossmarket:
                        self.crossmarket_unresolved_fee_settlements += 1
                    else:
                        self.paper_unresolved_fee_settlements += 1
                else:
                    if crossmarket:
                        self.crossmarket_realized_net_pnl += Decimal(str(net))
                    else:
                        self.paper_realized_net_pnl += Decimal(str(net))
        if crossmarket:
            self._update_crossmarket_paper_status()
        else:
            self._update_paper_status()

    async def collect_rest_depth(self) -> None:
        """Capture sampled REST book states and independently record per-market coverage."""
        tickers = sorted(self.open_tickers)
        poll_started = utc_now()
        request_count = successful_request_count = rows_received = errors = 0
        markets_with_data: set[str] = set()
        for start in range(0, len(tickers), 100):
            batch = tickers[start : start + 100]
            requested_utc = utc_now()
            began = time.perf_counter()
            request_count += 1
            try:
                payload = await self.api.get("/markets/orderbooks", {"tickers": batch})
                observed_utc = utc_now()
                successful_request_count += 1
                orderbooks = payload.get("orderbooks")
                if not isinstance(orderbooks, list):
                    raise ValueError("orderbooks response does not contain an array")
                rows_received += len(orderbooks)
                returned: set[str] = set()
                valid: set[str] = set()
                for item in orderbooks:
                    if not isinstance(item, dict):
                        errors += 1
                        continue
                    ticker = str(item.get("ticker") or "")
                    if not ticker:
                        errors += 1
                        continue
                    returned.add(ticker)
                    book_payload = item.get("orderbook_fp") or item.get("orderbook")
                    validation = "INVALID"
                    quotes: dict[str, Any] = {}
                    try:
                        if not isinstance(book_payload, dict):
                            raise ValueError("missing orderbook object")
                        diagnostic_book = OrderBook()
                        diagnostic_book.snapshot({"orderbook_fp": book_payload})
                        quotes = diagnostic_book.quotes()
                        validation = "VALID_EMPTY" if not (diagnostic_book.yes or diagnostic_book.no) else "VALID_WITH_LIQUIDITY"
                        if ticker in batch:
                            valid.add(ticker)
                            markets_with_data.add(ticker)
                            self.status.mark_channel("rest_orderbook")
                            last_seen = dict(self.status.data.get("rest_book_last_observed_utc_by_ticker", {}))
                            last_seen[ticker] = observed_utc
                            self.status.update(rest_book_last_observed_utc_by_ticker=last_seen)
                    except (ValueError, TypeError, KeyError) as exc:
                        errors += 1
                        validation = "INVALID_" + type(exc).__name__
                    await self.emit(
                        "rest_orderbook",
                        "orderbook_snapshot",
                        {
                            "source": "Kalshi public REST",
                            "rest_request_started_utc": requested_utc,
                            "rest_response_observed_utc": observed_utc,
                            "validation": validation,
                            **item,
                            "executable_quotes": quotes,
                        },
                        ticker,
                        None,
                        f"rest-book|{ticker}|{requested_utc}",
                    )
                    if ticker in valid and ticker not in self.first_rest_books:
                        self.first_rest_books.add(ticker)
                        await self.emit(
                            "coverage",
                            "first_book_observed",
                            {
                                "ticker": ticker, "source": "REST_SAMPLED_NOT_WS_SYNCHRONIZED",
                                "scheduled_open_utc": self.markets.get(ticker, {}).get("open_time"),
                                "observed_utc": observed_utc, "validation": validation,
                                "run_id": self.run_id,
                            },
                            ticker,
                            dedupe_key=f"first-rest-book|{ticker}",
                        )
                    if (ticker in valid and ticker not in self.first_executable_quotes
                            and any(value is not None for value in quotes.values())):
                        self.first_executable_quotes.add(ticker)
                        await self.emit(
                            "coverage", "first_executable_quote_observed",
                            {
                                "ticker": ticker, "source": "REST_SAMPLED",
                                "scheduled_open_utc": self.markets.get(ticker, {}).get("open_time"),
                                "observed_utc": observed_utc, "quotes": quotes, "run_id": self.run_id,
                            },
                            ticker,
                            dedupe_key=f"first-executable-quote|{ticker}",
                        )
                missing = sorted(set(batch) - valid)
                await self.emit(
                    "rest_diagnostics", "rest_poll_result",
                    {
                        "channel": "rest_orderbook",
                        "run_id": self.run_id,
                        "started_utc": requested_utc,
                        "completed_utc": observed_utc,
                        "rtt_ms": round((time.perf_counter() - began) * 1000, 3),
                        "http_status": 200,
                        "requested_tickers": batch,
                        "returned_tickers": sorted(returned),
                        "valid_tickers": sorted(valid),
                        "missing_or_invalid_tickers": missing,
                        "coverage": "COMPLETE" if not missing else "INCOMPLETE",
                        "server_date_header": None,
                        "server_clock_offset_state": "NOT_MEASURED",
                    },
                    dedupe_key=f"rest-poll|{self.run_id}|{requested_utc}|rest_orderbook",
                )
            except Exception as exc:
                errors += 1
                self.status.update(
                    error_count=self.status.data.get("error_count", 0) + 1,
                    last_error=f"orderbook REST failed: {type(exc).__name__}",
                )
                await self.emit(
                    "rest_diagnostics", "rest_poll_result",
                    {
                        "channel": "rest_orderbook", "run_id": self.run_id,
                        "started_utc": requested_utc, "completed_utc": utc_now(),
                        "rtt_ms": round((time.perf_counter() - began) * 1000, 3),
                        "http_status": None, "error_type": type(exc).__name__,
                        "requested_tickers": batch, "coverage": "INCOMPLETE",
                    },
                    dedupe_key=f"rest-poll|{self.run_id}|{requested_utc}|rest_orderbook",
                )
                LOG.warning("REST orderbook reconciliation failed: %s", type(exc).__name__)
        self.status.mark_rest_poll(
            "rest_orderbook",
            started_utc=poll_started,
            request_count=request_count,
            successful_request_count=successful_request_count,
            requested_market_count=len(tickers),
            markets_with_data=len(markets_with_data),
            rows_received=rows_received,
            error_count=errors,
        )

    async def collect_recent_trades(self) -> None:
        """Page historical public trades; advance overlap watermark only on completion."""
        tickers = sorted(self.active_tickers or self.open_tickers)
        poll_started = utc_now()
        request_count = successful_request_count = rows_received = errors = 0
        markets_with_data: set[str] = set()
        for ticker in tickers:
            market_open = parse_time(self.markets.get(ticker, {}).get("open_time"))
            # A restarted worker must not assume any earlier poll completed.
            initial_since = int((market_open - timedelta(seconds=60)).timestamp()) if market_open else int(time.time()) - 3600
            since = self.trade_poll_since_by_ticker.get(ticker, initial_since)
            since = max(initial_since, since)
            requested_utc = utc_now()
            began = time.perf_counter()
            cursor: str | None = None
            seen_cursors: set[str] = set()
            pages = trades_seen = 0
            complete = False
            request_error: str | None = None
            try:
                for _ in range(20):
                    params: dict[str, Any] = {"ticker": ticker, "limit": 1000, "min_ts": since}
                    if cursor:
                        params["cursor"] = cursor
                    request_count += 1
                    payload = await self.api.get("/markets/trades", params)
                    successful_request_count += 1
                    trades = payload.get("trades")
                    if not isinstance(trades, list):
                        raise ValueError("trades response does not contain an array")
                    pages += 1
                    rows_received += len(trades)
                    trades_seen += len(trades)
                    if trades:
                        markets_with_data.add(ticker)
                    for trade in trades:
                        if not isinstance(trade, dict):
                            raise ValueError("trade response contains a non-object")
                        trade_id = str(trade.get("trade_id") or "")
                        exchange_ts = trade.get("created_time")
                        await self.emit(
                            "rest_public_trades", "public_trade",
                            {"source": "Kalshi public REST", **trade},
                            ticker,
                            exchange_ts,
                            f"trade|{trade_id}" if trade_id else None,
                        )
                        self.status.mark_channel("rest_public_trades", exchange_ts)
                        if ticker not in self.first_trades:
                            self.first_trades.add(ticker)
                            await self.emit(
                                "coverage", "first_trade_observed",
                                {
                                    "ticker": ticker, "source": "REST_PUBLIC_TRADES",
                                    "scheduled_open_utc": self.markets.get(ticker, {}).get("open_time"),
                                    "observed_utc": utc_now(), "exchange_trade_utc": exchange_ts,
                                    "first_observed_not_first_exchange_trade": True,
                                    "run_id": self.run_id,
                                },
                                ticker, exchange_ts,
                                f"first-trade-observed|{ticker}",
                            )
                    next_cursor = payload.get("cursor") or None
                    if not next_cursor:
                        complete = True
                        break
                    if next_cursor == cursor or str(next_cursor) in seen_cursors:
                        raise ValueError("trade pagination cursor repeated")
                    seen_cursors.add(str(next_cursor))
                    cursor = str(next_cursor)
                if not complete:
                    errors += 1
                    request_error = "PAGE_LIMIT_REACHED"
                    await self.emit(
                        "rest_diagnostics", "trade_poll_saturated",
                        {"ticker": ticker, "min_ts": since, "page_limit": 20, "cursor_remaining": bool(cursor)},
                        ticker, dedupe_key=f"trade-saturated|{self.run_id}|{ticker}|{requested_utc}",
                    )
                else:
                    # Overlap handles delayed exchange trade publication.
                    self.trade_poll_since_by_ticker[ticker] = max(
                        since, int(datetime.fromisoformat(requested_utc).timestamp()) - 180
                    )
            except Exception as exc:
                errors += 1
                request_error = type(exc).__name__
                self.status.update(
                    error_count=self.status.data.get("error_count", 0) + 1,
                    last_error=f"trade REST failed: {type(exc).__name__}",
                )
                LOG.warning("public trade reconciliation incomplete for %s: %s", ticker, type(exc).__name__)
            await self.emit(
                "rest_diagnostics", "rest_poll_result",
                {
                    "channel": "rest_public_trades", "run_id": self.run_id,
                    "ticker": ticker, "started_utc": requested_utc,
                    "completed_utc": utc_now(),
                    "elapsed_ms": round((time.perf_counter() - began) * 1000, 3),
                    "min_ts": since, "page_count": pages,
                    "row_count": trades_seen, "complete": complete,
                    "error": request_error,
                    "server_date_header": None, "server_clock_offset_state": "NOT_MEASURED",
                },
                ticker,
                dedupe_key=f"trade-poll|{self.run_id}|{ticker}|{requested_utc}",
            )
        self.status.mark_rest_poll(
            "rest_public_trades",
            started_utc=poll_started,
            request_count=request_count,
            successful_request_count=successful_request_count,
            requested_market_count=len(tickers),
            markets_with_data=len(markets_with_data),
            rows_received=rows_received,
            error_count=errors,
        )

    async def websocket_loop(self) -> None:
        reconnect_delay = 1.0
        while not self.stopping.is_set():
            self._invalidate_websocket_books()
            try:
                # The Kalshi signature includes a short-lived timestamp. Sign
                # each connection attempt independently so reconnects never
                # reuse credentials that were valid only for the prior socket.
                headers = ws_auth_headers()
            except Exception as exc:
                self.status.update(
                    websocket_state="BLOCKED_CREDENTIAL_CONFIGURATION",
                    last_error=f"WebSocket credential configuration invalid: {type(exc).__name__}",
                )
                return
            if headers is None:
                self.status.update(
                    websocket_state="BLOCKED_NO_CREDENTIALS",
                    last_error="Kalshi requires signed WebSocket authentication; no caller-provided key reference is configured.",
                )
                return
            try:
                self.status.update(websocket_state="CONNECTING")
                async with websockets.connect(
                    WS_URL,
                    additional_headers=headers,
                    open_timeout=15,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=2**20,
                ) as ws:
                    self.status.update(
                        websocket_state="READY",
                        reconnect_count=self.status.data.get("reconnect_count", 0) + 1,
                    )
                    reconnect_delay = 1.0
                    self.websocket_session_id = uuid.uuid4().hex
                    await self.emit(
                        "websocket",
                        "ws_session_started",
                        {"session_id": self.websocket_session_id, "run_id": self.run_id},
                        dedupe_key=f"ws-session-start|{self.websocket_session_id}",
                    )
                    self._reset_websocket_subscriptions()
                    self.orderbook_sequence_by_sid.clear()
                    self._publish_orderbook_sequence_state(connected=True)
                    receiver = asyncio.create_task(self._receive_ws(ws))
                    updater: asyncio.Task[None] | None = None
                    try:
                        await ws.send(
                            json.dumps(
                                {
                                    "id": 1,
                                    "cmd": "subscribe",
                                    "params": {"channels": ["market_lifecycle_v2"]},
                                }
                            )
                        )
                        initial_tickers = sorted(self.open_tickers)
                        await self._subscribe_markets(ws, initial_tickers, initial=True)
                        if not self.initial_subscription_commands:
                            self.ws_subscriptions_ready.set()
                        ready_wait = asyncio.create_task(self.ws_subscriptions_ready.wait())
                        done, _ = await asyncio.wait(
                            {receiver, ready_wait},
                            timeout=15,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if receiver in done:
                            ready_wait.cancel()
                            await receiver
                            raise ConnectionError("WebSocket ended before subscriptions were ready")
                        if ready_wait not in done:
                            ready_wait.cancel()
                            raise TimeoutError("market-data subscriptions were not acknowledged")
                        updater = asyncio.create_task(self._update_ws_subscriptions(ws))
                        done, _ = await asyncio.wait(
                            {receiver, updater},
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        for task in done:
                            exc = task.exception()
                            if exc:
                                raise exc
                    finally:
                        tasks = [receiver]
                        if updater:
                            tasks.append(updater)
                        for task in tasks:
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                    if not self.stopping.is_set():
                        raise ConnectionError("WebSocket receive loop ended")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.websocket_session_id:
                    await self.emit(
                        "websocket",
                        "ws_session_interrupted",
                        {"session_id": self.websocket_session_id, "reason": type(exc).__name__},
                        dedupe_key=f"ws-session-interrupted|{self.websocket_session_id}",
                    )
                self._invalidate_websocket_books()
                self.status.update(
                    websocket_state="RECONNECTING",
                    error_count=self.status.data.get("error_count", 0) + 1,
                    last_error=f"WebSocket connection ended: {type(exc).__name__}",
                )
                LOG.warning("WebSocket disconnected: %s", type(exc).__name__)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(30.0, reconnect_delay * 2)
        self.websocket_session_id = None
        self.status.update(websocket_state="STOPPED")

    def _invalidate_websocket_books(self) -> None:
        self._publish_orderbook_sequence_state(connected=False)
        self.last_seq.clear()
        self.book_received_monotonic.clear()
        for book in self.books.values():
            book.invalidate()
        quote_states = dict(self.status.data.get("orderbook_quote_freshness_by_ticker", {}))
        for ticker in self.open_tickers:
            quote_states[ticker] = "DISCONNECTED"
        self.status.update(orderbook_quote_freshness_by_ticker=quote_states)

    def _publish_orderbook_sequence_state(self, *, connected: bool) -> None:
        states = [str(item.get("state", "AWAITING_SNAPSHOT")) for item in self.orderbook_sequence_by_sid.values()]
        if not connected:
            prior_state = self.status.data.get("orderbook_sequence_state", "UNKNOWN_NOT_CONNECTED")
            if prior_state != "UNKNOWN_NOT_CONNECTED":
                self.status.data["orderbook_sequence_last_session_state"] = prior_state
            state = "UNKNOWN_NOT_CONNECTED"
        elif any(item == "INVALIDATED" for item in states):
            state = "INVALIDATED"
        elif states and all(item == "VERIFIED_CONTIGUOUS" for item in states):
            state = "VERIFIED_CONTIGUOUS"
        elif any(item == "SNAPSHOT_ONLY" for item in states):
            state = "SNAPSHOT_ONLY"
        elif states:
            state = "AWAITING_SNAPSHOT"
        else:
            state = "AWAITING_SUBSCRIPTION"
        self.status.update(
            orderbook_sequence_state=state,
            orderbook_sequence_connected=connected,
            orderbook_sequence_subscription_count=len(states),
            orderbook_sequence_verified_subscription_count=sum(
                item == "VERIFIED_CONTIGUOUS" for item in states
            ),
            orderbook_sequence_snapshot_count=sum(
                int(item.get("snapshot_count", 0)) for item in self.orderbook_sequence_by_sid.values()
            ),
            orderbook_sequence_delta_count=sum(
                int(item.get("delta_count", 0)) for item in self.orderbook_sequence_by_sid.values()
            ),
            orderbook_sequence_last_verified_utc=self.orderbook_sequence_last_verified_utc,
            orderbook_sequence_by_subscription={
                str(sid): dict(item) for sid, item in self.orderbook_sequence_by_sid.items()
            },
        )

    def _record_orderbook_sequence(self, sid: int, seq: int, *, snapshot: bool) -> None:
        entry = self.orderbook_sequence_by_sid.setdefault(
            sid,
            {
                "state": "AWAITING_SNAPSHOT",
                "last_seq": None,
                "snapshot_count": 0,
                "delta_count": 0,
                "last_message_utc": None,
            },
        )
        entry["last_seq"] = seq
        entry["last_message_utc"] = utc_now()
        if snapshot:
            entry["snapshot_count"] += 1
            entry["state"] = "SNAPSHOT_ONLY"
        else:
            entry["delta_count"] += 1
            entry["state"] = "VERIFIED_CONTIGUOUS"
            self.orderbook_sequence_last_verified_utc = entry["last_message_utc"]
        self._publish_orderbook_sequence_state(connected=True)

    def _record_orderbook_sequence_failure(self, sid: Any, reason: str) -> None:
        if isinstance(sid, int):
            entry = self.orderbook_sequence_by_sid.setdefault(
                sid,
                {
                    "state": "AWAITING_SNAPSHOT",
                    "last_seq": self.last_seq.get(sid),
                    "snapshot_count": 0,
                    "delta_count": 0,
                    "last_message_utc": None,
                },
            )
            entry["state"] = "INVALIDATED"
            entry["last_error"] = reason
            entry["last_message_utc"] = utc_now()
        self._publish_orderbook_sequence_state(connected=True)
        self.status.update(orderbook_sequence_last_error=reason)

    def _mark_orderbook_quote(self, ticker: str, state: str = "FRESH") -> None:
        updates = dict(self.status.data.get("orderbook_quote_last_update_utc_by_ticker", {}))
        states = dict(self.status.data.get("orderbook_quote_freshness_by_ticker", {}))
        updates[ticker] = utc_now()
        states[ticker] = state
        self.status.update(
            orderbook_quote_last_update_utc_by_ticker=updates,
            orderbook_quote_freshness_by_ticker=states,
            orderbook_quote_age_seconds_by_ticker={**self.status.data.get("orderbook_quote_age_seconds_by_ticker", {}), ticker: 0.0},
        )

    def _reset_websocket_subscriptions(self) -> None:
        self.subscription_sids.clear()
        self.subscription_channel_by_sid.clear()
        self.subscription_membership.clear()
        self.pending_subscriptions.clear()
        self.pending_subscription_futures.clear()
        self.pending_updates.clear()
        self.pending_update_futures.clear()
        self.initial_subscription_commands.clear()
        self.ws_subscriptions_ready.clear()
        self.ws_next_command_id = 2
        self.subscribed_tickers.clear()

    def _next_ws_id(self) -> int:
        command_id = self.ws_next_command_id
        self.ws_next_command_id += 1
        return command_id

    async def _send_market_subscription(
        self, ws: Any, channel: str, tickers: list[str], *, initial: bool
    ) -> list[asyncio.Future[None]]:
        futures: list[asyncio.Future[None]] = []
        loop = asyncio.get_running_loop()
        for start in range(0, len(tickers), 100):
            chunk = set(tickers[start : start + 100])
            command_id = self._next_ws_id()
            self.pending_subscriptions[command_id] = (channel, chunk)
            future: asyncio.Future[None] | None = None
            if not initial:
                future = loop.create_future()
                self.pending_subscription_futures[command_id] = future
            if initial:
                self.initial_subscription_commands.add(command_id)
            await ws.send(
                json.dumps(
                    {
                        "id": command_id,
                        "cmd": "subscribe",
                        "params": {"channels": [channel], "market_tickers": sorted(chunk)},
                    }
                )
            )
            if future is not None:
                futures.append(future)
        return futures

    async def _subscribe_markets(self, ws: Any, tickers: list[str], *, initial: bool = False) -> None:
        if not tickers:
            return
        futures: list[asyncio.Future[None]] = []
        for channel in ("orderbook_delta", "trade", "ticker"):
            futures.extend(await self._send_market_subscription(ws, channel, tickers, initial=initial))
        if not initial:
            await asyncio.wait_for(asyncio.gather(*futures), timeout=15)

    def _recompute_subscribed_tickers(self) -> None:
        channel_members: list[set[str]] = []
        for channel in ("orderbook_delta", "trade", "ticker"):
            members: set[str] = set()
            for sid in self.subscription_sids.get(channel, set()):
                members.update(self.subscription_membership.get(sid, set()))
            channel_members.append(members)
        self.subscribed_tickers = set.intersection(*channel_members) if channel_members else set()

    async def _send_subscription_update(
        self, ws: Any, sid: int, tickers: set[str], action: str
    ) -> None:
        command_id = self._next_ws_id()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        self.pending_updates[command_id] = (sid, action, set(tickers))
        self.pending_update_futures[command_id] = future
        await ws.send(
            json.dumps(
                {
                    "id": command_id,
                    "cmd": "update_subscription",
                    "params": {
                        "sids": [sid],
                        "market_tickers": sorted(tickers),
                        "action": action,
                    },
                }
            )
        )
        await asyncio.wait_for(future, timeout=15)

    async def _update_ws_subscriptions(self, ws: Any) -> None:
        while not self.stopping.is_set():
            desired = await self.subscription_updates.get()
            try:
                removals = self.subscribed_tickers - desired
                additions = desired - self.subscribed_tickers
                for ticker in sorted(removals):
                    self.books.pop(ticker, None)
                    self.book_received_monotonic.pop(ticker, None)
                    for sid, members in list(self.subscription_membership.items()):
                        if ticker in members:
                            await self._send_subscription_update(ws, sid, {ticker}, "delete_markets")
                # After deletes, each market is absent from all channel memberships.
                for ticker in sorted(additions):
                    if not any(self.subscription_sids.get(channel) for channel in ("orderbook_delta", "trade", "ticker")):
                        await self._subscribe_markets(ws, [ticker])
                        continue
                    for channel in ("orderbook_delta", "trade", "ticker"):
                        candidates = sorted(
                            sid
                            for sid in self.subscription_sids.get(channel, set())
                            if len(self.subscription_membership.get(sid, set())) < 100
                        )
                        if candidates:
                            await self._send_subscription_update(
                                ws, candidates[0], {ticker}, "add_markets"
                            )
                        else:
                            futures = await self._send_market_subscription(
                                ws, channel, [ticker], initial=False
                            )
                            await asyncio.wait_for(asyncio.gather(*futures), timeout=15)
                self._recompute_subscribed_tickers()
            finally:
                self.subscription_updates.task_done()

    def _handle_subscription_response(self, outer: dict[str, Any]) -> bool:
        kind = str(outer.get("type", ""))
        command_id = outer.get("id")
        if not isinstance(command_id, int):
            return False
        message = outer.get("msg") if isinstance(outer.get("msg"), dict) else {}
        if kind == "error":
            exc = RuntimeError(str(message.get("msg") or message.get("code") or "subscription rejected"))
            future = self.pending_subscription_futures.pop(command_id, None)
            self.pending_subscriptions.pop(command_id, None)
            if future and not future.done():
                future.set_exception(exc)
            future = self.pending_update_futures.pop(command_id, None)
            self.pending_updates.pop(command_id, None)
            if future and not future.done():
                future.set_exception(exc)
            return True
        if kind == "subscribed" and command_id in self.pending_subscriptions:
            expected_channel, tickers = self.pending_subscriptions.pop(command_id)
            sid = message.get("sid")
            channel = str(message.get("channel") or expected_channel)
            future = self.pending_subscription_futures.pop(command_id, None)
            if not isinstance(sid, int) or channel != expected_channel:
                if future and not future.done():
                    future.set_exception(RuntimeError("subscription acknowledgement mismatch"))
                return True
            self.subscription_channel_by_sid[sid] = channel
            self.subscription_sids.setdefault(channel, set()).add(sid)
            self.subscription_membership.setdefault(sid, set()).update(tickers)
            if channel == "orderbook_delta":
                self.orderbook_sequence_by_sid.setdefault(
                    sid,
                    {
                        "state": "AWAITING_SNAPSHOT",
                        "last_seq": None,
                        "snapshot_count": 0,
                        "delta_count": 0,
                        "last_message_utc": None,
                    },
                )
                self._publish_orderbook_sequence_state(connected=True)
            self._recompute_subscribed_tickers()
            self.initial_subscription_commands.discard(command_id)
            if future and not future.done():
                future.set_result(None)
            if not self.initial_subscription_commands:
                self.ws_subscriptions_ready.set()
            return True
        if kind == "ok" and command_id in self.pending_updates:
            sid, action, tickers = self.pending_updates.pop(command_id)
            future = self.pending_update_futures.pop(command_id, None)
            members = self.subscription_membership.setdefault(sid, set())
            if action == "add_markets":
                members.update(tickers)
            elif action == "delete_markets":
                members.difference_update(tickers)
            else:
                if future and not future.done():
                    future.set_exception(RuntimeError(f"unexpected subscription action: {action}"))
                return True
            self._recompute_subscribed_tickers()
            if future and not future.done():
                future.set_result(None)
            return True
        return kind in {"subscribed", "unsubscribed", "ok", "error"}

    async def _receive_ws(self, ws: Any) -> None:
        async for message in ws:
            try:
                outer = json.loads(message)
            except json.JSONDecodeError:
                await self.emit("websocket", "malformed_message", {"raw": str(message)[:2048]})
                continue
            kind = str(outer.get("type", "unknown"))
            if kind in {"subscribed", "unsubscribed", "ok", "error"}:
                self._handle_subscription_response(outer)
                if kind == "error":
                    details = outer.get("msg") if isinstance(outer.get("msg"), dict) else {}
                    self.status.update(
                        websocket_state="SUBSCRIPTION_ERROR",
                        last_error=(
                            "WebSocket subscription rejected: "
                            f"{details.get('msg') or details.get('code') or 'unknown error'}"
                        ),
                    )
                    await ws.close(code=1011, reason="subscription rejected")
                    return
                continue
            payload = outer.get("msg") if isinstance(outer.get("msg"), dict) else outer
            ticker = payload.get("market_ticker") or payload.get("ticker")
            channel = "websocket_" + kind
            exchange_ts = payload.get("ts") or payload.get("timestamp") or payload.get("created_time")
            sid, seq = outer.get("sid"), outer.get("seq")
            if kind == "orderbook_snapshot" and ticker:
                prior = self.last_seq.get(sid) if isinstance(sid, int) else None
                invalid_sequence = (
                    not isinstance(sid, int)
                    or not isinstance(seq, int)
                    or self.subscription_channel_by_sid.get(sid) != "orderbook_delta"
                    or (prior is not None and seq != prior + 1)
                )
                if invalid_sequence:
                    self._invalidate_subscription_books(sid)
                    reason = f"invalid snapshot sequence: prior={prior}, current={seq}"
                    self._record_orderbook_sequence_failure(sid, reason)
                    self.status.update(
                        sequence_gap_count=self.status.data.get("sequence_gap_count", 0) + 1,
                        websocket_state="RESYNC_REQUIRED",
                        last_error=(
                            f"orderbook snapshot sequence invalid on subscription {sid}: {reason}"
                        ),
                    )
                    await self.emit(
                        "websocket_orderbook",
                        "sequence_gap",
                        {"sid": sid, "prior_seq": prior, "current_seq": seq, "market_ticker": ticker},
                        str(ticker),
                        exchange_ts,
                        f"snapshot-gap|{self.websocket_session_id}|{sid}|{prior}|{seq}",
                    )
                    await ws.close(code=1011, reason="snapshot missing sequence metadata")
                    return
                self.last_seq[sid] = seq
                self._record_orderbook_sequence(sid, seq, snapshot=True)
                if str(ticker) in self.open_tickers:
                    book = self.books.setdefault(str(ticker), OrderBook())
                    book.snapshot(payload)
                    self.book_received_monotonic[str(ticker)] = time.monotonic()
                    self._mark_orderbook_quote(str(ticker))
                    await self.emit(
                        "websocket_orderbook",
                        kind,
                        {"raw": outer, "synchronized": book.synchronized, "executable_quotes": book.quotes()},
                        str(ticker),
                        exchange_ts,
                        f"ws|{self.websocket_session_id}|{channel}|{sid}|{seq}",
                    )
            elif kind == "orderbook_delta" and ticker:
                prior = self.last_seq.get(sid) if isinstance(sid, int) else None
                if (
                    not isinstance(sid, int)
                    or not isinstance(seq, int)
                    or self.subscription_channel_by_sid.get(sid) != "orderbook_delta"
                    or prior is None
                    or seq != prior + 1
                ):
                    self._invalidate_subscription_books(sid)
                    reason = f"noncontiguous delta: prior={prior}, current={seq}"
                    self._record_orderbook_sequence_failure(sid, reason)
                    self.status.update(
                        sequence_gap_count=self.status.data.get("sequence_gap_count", 0) + 1,
                        websocket_state="RESYNC_REQUIRED",
                        last_error=(
                            f"orderbook sequence invalid on subscription {sid}: {reason}"
                        ),
                    )
                    await self.emit(
                        "websocket_orderbook",
                        "sequence_gap",
                        {"sid": sid, "prior_seq": prior, "current_seq": seq, "market_ticker": ticker},
                        str(ticker),
                        exchange_ts,
                        f"gap|{self.websocket_session_id}|{sid}|{prior}|{seq}",
                    )
                    await ws.close(code=1011, reason="sequence invalid; reconnect for fresh snapshots")
                    return
                self.last_seq[sid] = seq
                if str(ticker) not in self.open_tickers:
                    self._record_orderbook_sequence(sid, seq, snapshot=False)
                    continue
                book = self.books.setdefault(str(ticker), OrderBook())
                if not book.synchronized:
                    self._invalidate_subscription_books(sid)
                    reason = f"delta arrived before a fresh snapshot for {ticker}"
                    self._record_orderbook_sequence_failure(sid, reason)
                    self.status.update(
                        sequence_gap_count=self.status.data.get("sequence_gap_count", 0) + 1,
                        websocket_state="RESYNC_REQUIRED",
                        last_error=reason,
                    )
                    await ws.close(code=1011, reason="delta arrived before snapshot")
                    return
                try:
                    book.delta(payload)
                except ValueError as exc:
                    self._invalidate_subscription_books(sid)
                    reason = f"invalid orderbook delta: {exc}"
                    self._record_orderbook_sequence_failure(sid, reason)
                    self.status.update(
                        sequence_gap_count=self.status.data.get("sequence_gap_count", 0) + 1,
                        websocket_state="RESYNC_REQUIRED",
                        last_error=reason,
                    )
                    await ws.close(code=1011, reason="invalid book; reconnect for fresh snapshots")
                    return
                self._record_orderbook_sequence(sid, seq, snapshot=False)
                self.book_received_monotonic[str(ticker)] = time.monotonic()
                self._mark_orderbook_quote(str(ticker))
                await self.emit(
                    "websocket_orderbook",
                    kind,
                    {
                        "raw": outer,
                        "synchronized": book.synchronized,
                        "executable_quotes": book.quotes(),
                    },
                    str(ticker),
                    exchange_ts,
                    f"ws|{self.websocket_session_id}|{channel}|{sid}|{seq}" if sid is not None and seq is not None else None,
                )
            else:
                await self.emit(
                    channel,
                    kind,
                    outer,
                    str(ticker) if ticker else None,
                    exchange_ts,
                    f"ws|{self.websocket_session_id}|{channel}|{sid}|{seq}" if sid is not None and seq is not None else None,
                )
            self.status.mark_channel(channel, exchange_ts)
            if kind == "trade":
                self.status.update(trades_observed=self.status.data.get("trades_observed", 0) + 1)
            if kind in {"market_lifecycle_v2", "event_lifecycle", "market_lifecycle"}:
                self.status.update(
                    lifecycle_events_observed=self.status.data.get("lifecycle_events_observed", 0) + 1
                )
                action = str(payload.get("event_type", payload.get("action", ""))).lower()
                if action in {"created", "activated", "close_date_updated", "determined", "settled"}:
                    self.refresh_requested.set()
                if action == "settled":
                    self.status.update(
                        settlements_observed=self.status.data.get("settlements_observed", 0) + 1
                    )

    def _invalidate_subscription_books(self, sid: Any) -> None:
        invalidated_tickers: set[str] = set()
        if isinstance(sid, int):
            entry = self.orderbook_sequence_by_sid.setdefault(
                sid,
                {
                    "state": "AWAITING_SNAPSHOT",
                    "last_seq": self.last_seq.get(sid),
                    "snapshot_count": 0,
                    "delta_count": 0,
                    "last_message_utc": None,
                },
            )
            entry["state"] = "INVALIDATED"
            for ticker in self.subscription_membership.get(sid, set()):
                book = self.books.get(ticker)
                if book:
                    book.invalidate()
                self.book_received_monotonic.pop(ticker, None)
                invalidated_tickers.add(ticker)
        for ticker, book in self.books.items():
            if ticker in self.open_tickers and self.subscription_channel_by_sid.get(sid) is None:
                book.invalidate()
                invalidated_tickers.add(ticker)
        if invalidated_tickers:
            states = dict(self.status.data.get("orderbook_quote_freshness_by_ticker", {}))
            for ticker in invalidated_tickers:
                states[ticker] = "INVALIDATED"
            self.status.update(orderbook_quote_freshness_by_ticker=states)
        self._publish_orderbook_sequence_state(connected=True)

    async def run(self) -> None:
        self.cfg.ensure_dirs()
        configure_logging(self.cfg)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.writer_pool, self.storage.open)
        storage_status = await loop.run_in_executor(
            self.writer_pool, self.storage.connection_status
        )
        self.status.update(
            **storage_status,
            parquet_archive_recovery_state=self.storage.parquet_recovery_state,
            parquet_archive_recovery_rows=self.storage.parquet_recovery_rows,
        )
        if self.strategy_profile_id:
            try:
                await self.restore_paper_state()
            except Exception as exc:
                self.status.update(
                    strategy_state="DEGRADED_PAPER_LEDGER_RECOVERY",
                    error_count=self.status.data.get("error_count", 0) + 1,
                    last_error=f"paper ledger recovery failed: {type(exc).__name__}",
                )
                LOG.exception("paper ledger recovery failed")
        if self.crossmarket_profile_id:
            try:
                await self.restore_crossmarket_paper_state()
            except Exception as exc:
                self.status.update(
                    crossmarket_strategy_state="DEGRADED_PAPER_LEDGER_RECOVERY",
                    error_count=self.status.data.get("error_count", 0) + 1,
                    last_error=f"cross-market paper ledger recovery failed: {type(exc).__name__}",
                )
                LOG.exception("cross-market paper ledger recovery failed")
        self.status_http = status_server(self.status, self.cfg)
        self.status.update(status_url=f"http://{self.cfg.status_host}:{self.cfg.status_port}/api/v1/status")
        self.stop_file.unlink(missing_ok=True)
        writer = asyncio.create_task(self.writer_loop(), name="sqlite-parquet-writer")
        heartbeat = asyncio.create_task(self.heartbeat_loop(), name="runtime-heartbeat")
        await self.refresh_inventory()
        ws_task = asyncio.create_task(self.websocket_loop(), name="kalshi-websocket")
        await self.emit(
            "runtime",
            "worker_started",
            {
                "pid": os.getpid(),
                "mode": "PAPER_ONLY",
                "source": "Kalshi public REST and optional authenticated read-market-data WSS",
                "exchange_write_calls": 0,
            },
            dedupe_key=f"start|{os.getpid()}|{self.status.started_utc}",
        )
        next_reconcile = time.monotonic() + self._next_inventory_reconcile_delay()
        next_depth = time.monotonic()
        next_trades = time.monotonic()
        next_strategy = time.monotonic()
        next_capture_resume_check = time.monotonic()
        inventory_task: asyncio.Task[None] | None = None
        trades_task: asyncio.Task[None] | None = None
        try:
            while not self.stopping.is_set():
                if self.stop_file.exists():
                    self.stopping.set()
                    break
                now = time.monotonic()
                rollover_check_due = (
                    self.pending_rollover_check is not None
                    and datetime.now(timezone.utc) >= self.pending_rollover_check[1]
                )
                if rollover_check_due and self.pending_rollover_check is not None:
                    detected_open = parse_time(
                        self.status.data.get("last_rollover_scheduled_open_utc")
                    )
                    if detected_open == self.pending_rollover_check[1]:
                        self.pending_rollover_check = None
                        self.pending_rollover_tickers.clear()
                        rollover_check_due = False
                if self.refresh_requested.is_set() or now >= next_reconcile or rollover_check_due:
                    self.refresh_requested.clear()
                    if inventory_task is None or inventory_task.done():
                        if (
                            self.pending_rollover_check is not None
                            and datetime.now(timezone.utc) >= self.pending_rollover_check[1]
                        ):
                            opening = parse_time(self.pending_rollover_check[0])
                            if opening is not None:
                                self.rollover_priority_tickers = (
                                    self.pending_rollover_tickers
                                    | self._rollover_group_tickers(opening)
                                )
                                self.pending_rollover_tickers.clear()
                                self.rollover_priority_retry_count = 0
                            self.status.update(
                                rollover_postopen_check_started_utc=utc_now(),
                                rollover_postopen_check_open_utc=self.pending_rollover_check[0],
                                rollover_priority_reconcile_ticker_count=len(
                                    self.rollover_priority_tickers
                                ),
                                rollover_priority_reconcile_state=(
                                    "POSTOPEN_PRIORITY_READY"
                                    if self.rollover_priority_tickers
                                    else "POSTOPEN_PRIORITY_UNAVAILABLE"
                                ),
                            )
                            self.pending_rollover_check = None
                            self.rollover_postopen_reconcile_pending = True
                        inventory_task = asyncio.create_task(
                            self.refresh_inventory(), name="scheduled-inventory-reconcile"
                        )
                        next_reconcile = time.monotonic() + self._next_inventory_reconcile_delay()
                    else:
                        # Do not queue overlapping discovery calls; retry as soon as
                        # the existing rate-limited reconciliation has completed.
                        next_reconcile = time.monotonic() + 1.0
                if rollover_check_due and inventory_task is not None:
                    try:
                        await inventory_task
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        LOG.exception("rollover priority inventory reconcile failed")
                        self.status.update(
                            error_count=self.status.data.get("error_count", 0) + 1,
                            last_error="rollover priority inventory reconcile failed",
                        )
                    finally:
                        inventory_task = None
                    next_reconcile = time.monotonic() + self._next_inventory_reconcile_delay()
                    continue
                # Keep the frozen strategy poll ahead of broad, slower collection.
                if now >= next_strategy:
                    await self.collect_strategy_inputs()
                    await self.collect_crossmarket_strategy_inputs()
                    next_strategy = time.monotonic() + self.cfg.strategy_poll_interval_seconds
                    if (
                        self.pending_rollover_check is not None
                        and datetime.now(timezone.utc) >= self.pending_rollover_check[1]
                    ):
                        continue
                if now >= next_depth:
                    await self.collect_rest_depth()
                    next_depth = time.monotonic() + self.cfg.book_poll_interval_seconds
                    if (
                        self.pending_rollover_check is not None
                        and datetime.now(timezone.utc) >= self.pending_rollover_check[1]
                    ):
                        continue
                if now >= next_trades and (trades_task is None or trades_task.done()):
                    rollover_deferral = self._public_trade_poll_rollover_deferral()
                    if rollover_deferral is not None:
                        delay, scheduled_open = rollover_deferral
                        next_trades = time.monotonic() + delay
                        self.status.update(
                            public_trade_rollover_deferrals=(
                                self.status.data.get("public_trade_rollover_deferrals", 0) + 1
                            ),
                            public_trade_poll_deferred_until_utc=(
                                scheduled_open + timedelta(
                                    seconds=self.cfg.rollover_trade_quiet_after_seconds
                                )
                            ).isoformat(),
                            public_trade_poll_deferred_for_open_utc=scheduled_open.isoformat(),
                            public_trade_poll_deferred_seconds=round(delay, 3),
                        )
                        await self.emit(
                            "runtime",
                            "public_trade_poll_deferred_for_rollover",
                            {
                                "scheduled_open_utc": scheduled_open.isoformat(),
                                "deferred_until_utc": (
                                    scheduled_open
                                    + timedelta(seconds=self.cfg.rollover_trade_quiet_after_seconds)
                                ).isoformat(),
                                "delay_seconds": round(delay, 3),
                                "quiet_before_seconds": self.cfg.rollover_trade_quiet_before_seconds,
                                "quiet_after_seconds": self.cfg.rollover_trade_quiet_after_seconds,
                                "exchange_write_calls": 0,
                            },
                            dedupe_key=f"public-trade-poll-deferred|{scheduled_open.isoformat()}",
                        )
                    else:
                        trades_task = asyncio.create_task(
                            self.collect_recent_trades(), name="background-public-trade-poll"
                        )
                        next_trades = time.monotonic() + self.cfg.trades_poll_interval_seconds
                disk_free = shutil.disk_usage(self.cfg.home).free
                low_disk = disk_free < self.cfg.min_free_bytes
                if low_disk:
                    self.storage.raw_capture_enabled = False
                elif (
                    not self.storage.raw_capture_enabled
                    and now >= next_capture_resume_check
                ):
                    capture_resumed = await loop.run_in_executor(
                        self.writer_pool,
                        self.storage.resume_raw_capture_if_safe,
                    )
                    next_capture_resume_check = time.monotonic() + 30.0
                    self.status.update(
                        parquet_archive_recovery_state=self.storage.parquet_recovery_state,
                        parquet_archive_recovery_rows=self.storage.parquet_recovery_rows,
                    )
                    if capture_resumed:
                        LOG.info(
                            "Parquet capture resumed after archive recovery rows=%d",
                            self.storage.parquet_recovery_rows,
                        )
                self.status.update(
                    disk_free_bytes=disk_free,
                    disk_state="THROTTLED_LOW_DISK" if low_disk else "OK",
                    raw_capture_state="ENABLED" if self.storage.raw_capture_enabled else "THROTTLED",
                    parquet_buffered_rows=len(self.storage.parquet_buffer),
                    parquet_buffer_estimated_bytes=self.storage.parquet_buffer_bytes_estimate,
                    queue_depth=self.queue.qsize(),
                    events_dropped=self.queue_dropped,
                )
                self.status.refresh_ages()
                sleep_seconds = 1.0
                if self.pending_rollover_check is not None:
                    seconds_until_open = (
                        self.pending_rollover_check[1] - datetime.now(timezone.utc)
                    ).total_seconds()
                    sleep_seconds = (
                        0.0
                        if seconds_until_open <= 0.0
                        else min(sleep_seconds, max(0.01, seconds_until_open))
                    )
                await asyncio.sleep(sleep_seconds)
        finally:
            self.stopping.set()
            heartbeat.cancel()
            try:
                await heartbeat
            except asyncio.CancelledError:
                pass
            ws_task.cancel()
            try:
                await ws_task
            except asyncio.CancelledError:
                pass
            background_tasks = [
                task for task in (inventory_task, trades_task) if task is not None
            ]
            for task in background_tasks:
                if not task.done():
                    task.cancel()
            if background_tasks:
                await asyncio.gather(*background_tasks, return_exceptions=True)
            await self.queue.join()
            await writer
            metrics: dict[str, Any] = {}
            storage_error = None
            try:
                await loop.run_in_executor(
                    self.writer_pool,
                    lambda: self.storage.flush_parquet(reason="SHUTDOWN"),
                )
                await loop.run_in_executor(self.writer_pool, self.storage.integrity_check)
                metrics = await loop.run_in_executor(self.writer_pool, self.storage.database_metrics)
            except Exception as exc:
                storage_error = f"{type(exc).__name__}: {exc}"
                LOG.exception("final storage verification failed")
            finally:
                try:
                    await loop.run_in_executor(self.writer_pool, self.storage.close)
                except Exception:
                    LOG.exception("storage close failed")
                    if storage_error is None:
                        storage_error = "Parquet or SQLite close failed"
                self.status.update(
                    service_state="STOPPED",
                    final_storage_metrics=metrics,
                    final_storage_error=storage_error,
                    parquet_archive_recovery_state=self.storage.parquet_recovery_state,
                    parquet_archive_recovery_rows=self.storage.parquet_recovery_rows,
                    parquet_buffered_rows=len(self.storage.parquet_buffer),
                    parquet_buffer_estimated_bytes=self.storage.parquet_buffer_bytes_estimate,
                )
                if self.status_http:
                    self.status_http.shutdown()
                    self.status_http.server_close()
                self.writer_pool.shutdown(wait=True)
                self.stop_file.unlink(missing_ok=True)


async def async_main() -> None:
    await Collector().run()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
