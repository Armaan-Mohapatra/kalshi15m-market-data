import os
from dataclasses import dataclass
from pathlib import Path

REST_BASE = "https://external-api.kalshi.com/trade-api/v2"
WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"


def default_home() -> Path:
    base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    return Path(os.environ.get("KALSHI15M_HOME", base / "Kalshi15mNative"))


@dataclass(frozen=True)
class Settings:
    home: Path = default_home()
    status_host: str = "127.0.0.1"
    status_port: int = int(os.environ.get("KALSHI15M_STATUS_PORT", "8768"))
    min_free_bytes: int = 20 * 1024**3
    max_response_bytes: int = 64 * 1024**2
    max_queue_events: int = 25000
    # Leave headroom below one REST request per second; forward polling at
    # 1.0 s observed recoverable 429 responses on /markets discovery.
    rest_min_interval_seconds: float = 2.0
    # Keep event-boundary market status and rollover discovery reasonably fresh
    # while staying within the shared public REST request budget.
    reconcile_interval_seconds: int = 15
    # The four-series rotating scan completes a full 34-series pass in about
    # 135 seconds at the normal 15-second reconciliation cadence. Five minutes
    # leaves time to discover and warm upcoming books before their opening.
    rollover_preopen_seconds: int = 300
    # A four-series rotating scan takes about 135 seconds at the normal
    # reconciliation cadence. Begin the pre-open inventory pass three minutes
    # ahead, leaving roughly 45 seconds for rate-limited book preparation.
    rollover_preflight_lead_seconds: int = 180
    # Let scheduled inventory reconciliation use the shared REST budget around
    # openings before starting the multi-market public-trades batch.
    rollover_trade_quiet_before_seconds: int = 45
    rollover_trade_quiet_after_seconds: int = 35
    # Rotate discovery across the known candidate series without a per-pass burst.
    rollover_series_scan_batch_size: int = 4
    # The exchange accepted a 26-ticker read-only rollover reconciliation in
    # one request; retain room for the current binary and MVE inventory.
    rollover_priority_batch_size: int = 50
    book_poll_interval_seconds: int = 15
    strategy_poll_interval_seconds: int = 2
    trades_poll_interval_seconds: int = 120
    # Recheck provisional market outcomes without adding a settlement GET to
    # every 15-second inventory reconciliation.
    paper_settlement_poll_interval_seconds: int = 30
    # Recheck the complete series catalog cheaply; full event discovery only
    # runs when the set of confirmed-frequency candidates changes.
    series_catalog_refresh_seconds: int = 900
    discovery_refresh_seconds: int = 21600
    raw_data_budget_bytes: int = 1500 * 1024**2
    log_budget_bytes: int = 40 * 1024**2
    log_file_max_bytes: int = 8 * 1024**2
    log_backup_count: int = 5

    @property
    def data_dir(self) -> Path:
        return self.home / "data"

    @property
    def parquet_dir(self) -> Path:
        return self.data_dir / "market_data"

    @property
    def ledger_path(self) -> Path:
        return self.data_dir / "paper_ledger.sqlite3"

    @property
    def runtime_dir(self) -> Path:
        return self.home / "runtime"

    @property
    def log_dir(self) -> Path:
        return self.home / "logs"

    def ensure_dirs(self) -> None:
        self.home.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
