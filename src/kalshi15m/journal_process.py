"""Run the event journal + Parquet archive in a second process (GIL relief).

The collector is CPU-bound in one Python process: the event loop and the journal
writer's row preparation share a single GIL. ProcessJournalStorage keeps the same
interface the worker already uses, but the heavy path (row prepare, SQLite insert,
Parquet flush, manifest/checkpoint, recovery, retention pruning) runs in a child
process that owns a normal Storage. The parent keeps its own connection for
paper_ledger only (its commits stay FULL and in-process).

Opt-in: KALSHI15M_JOURNAL_PROCESS=1. If the child dies the parent raises from
append_batch, so the worker's existing retry path holds the batch and the status
shows a degraded journal; nothing is silently dropped.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import sys
import threading
import time
from concurrent.futures import Future
from typing import Any

from .config import Settings
from .storage import Storage

LOG = logging.getLogger(__name__)

# Plain attributes the parent mirrors (cheap reads, safe while recovery is running).
_MIRRORED = (
    "events_inserted",
    "events_duplicate",
    "raw_capture_enabled",
    "parquet_dropped_rows",
    "parquet_recovery_state",
    "parquet_recovery_rows",
    "parquet_recovery_mode",
    "parquet_recovery_reason",
    "parquet_recovery_files_scanned",
    "parquet_recovery_rows_scanned",
    "parquet_recovery_seconds",
    "parquet_bytes",
    "sqlite_bytes",
    "parquet_buffer_bytes_estimate",
)
STATE_INTERVAL_SECONDS = 1.0
CALL_TIMEOUT_SECONDS = 3600.0  # shutdown flush of a large buffer on the USB volume


def storage_enabled() -> bool:
    return os.environ.get("KALSHI15M_JOURNAL_PROCESS", "") == "1"


def make_storage(cfg: Settings) -> Storage:
    return ProcessJournalStorage(cfg) if storage_enabled() else Storage(cfg)


def _lock_file(path):
    """Exclusive, non-blocking lock so an orphaned child can never share the journal."""
    handle = open(path, "a+b")
    try:
        if sys.platform == "win32":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        return None
    return handle


def _snapshot(store: Storage, *, full: bool) -> dict[str, Any]:
    state: dict[str, Any] = {name: getattr(store, name) for name in _MIRRORED}
    state["buffered_rows"] = len(store.parquet_buffer)
    state["latest_event"] = store.latest_event
    if full:
        state["status"] = store.connection_status()
    return state


def _child_main(cfg: Settings, conn) -> None:
    send_lock = threading.Lock()

    def send(message) -> None:
        with send_lock:
            conn.send(message)

    lock = None
    store = None
    try:
        cfg.runtime_dir.mkdir(parents=True, exist_ok=True)
        lock = _lock_file(cfg.runtime_dir / "journal-process.lock")
        if lock is None:
            send(("ready", False, "JOURNAL_PROCESS_ALREADY_RUNNING"))
            return
        # Same reason as the worker: the append thread competes with the flush thread for the GIL and
        # the default 5 ms hand-off made it wait; 0.5 ms keeps both moving.
        sys.setswitchinterval(0.0005)
        store = Storage(cfg)
        # Parquet/manifest/prune I/O (seconds per flush on the USB volume) must not stall
        # appends: measured flush ~6 s vs ~0.2 s for everything else in an append.
        store.async_flush = True
        done = threading.Event()

        def progress() -> None:  # recovery can take minutes: keep the parent informed
            while not done.wait(STATE_INTERVAL_SECONDS):
                try:
                    send(("state", _snapshot(store, full=False)))
                except Exception:
                    return

        reporter = threading.Thread(target=progress, name="journal-progress", daemon=True)
        reporter.start()
        try:
            store.open()
        except Exception as exc:
            done.set()
            send(("ready", False, f"{type(exc).__name__}: {exc}"))
            return
        done.set()
        reporter.join(timeout=5)
        send(("ready", True, _snapshot(store, full=True)))
        last_state = time.monotonic()
        while True:
            try:
                has_message = conn.poll(0.5)
                message = conn.recv() if has_message else None
            except (EOFError, OSError):
                break  # parent is gone: flush and close below
            if message is None:
                kind = None
            else:
                kind = message[0]
            if kind == "append":
                _, request_id, events = message
                try:
                    wall0, cpu0 = time.perf_counter(), time.thread_time()
                    inserted = store.append_batch(events)
                    send((
                        "ack", request_id, inserted,
                        (time.perf_counter() - wall0) * 1000, (time.thread_time() - cpu0) * 1000,
                    ))
                except Exception as exc:
                    LOG.exception("journal process append failed")
                    send(("err", request_id, f"{type(exc).__name__}: {exc}"))
            elif kind == "call":
                _, request_id, method, args, kwargs = message
                try:
                    send(("rpc", request_id, getattr(store, method)(*args, **kwargs)))
                except Exception as exc:
                    LOG.exception("journal process call %s failed", method)
                    send(("err", request_id, f"{type(exc).__name__}: {exc}"))
                if method == "close":
                    send(("state", _snapshot(store, full=False)))
                    return
            elif kind == "set_raw":
                store.raw_capture_enabled = bool(message[1])
            try:
                store.flush_parquet_if_due()
            except Exception:
                LOG.exception("journal process age flush failed")
            if time.monotonic() - last_state >= STATE_INTERVAL_SECONDS:
                send(("state", _snapshot(store, full=True)))
                last_state = time.monotonic()
                # On-demand profiling of THIS process: runtime/journal_stack_sample.txt holds
                # '<interval> <duration>'; samples go to logs/journal_stack_samples.log.
                if (cfg.runtime_dir / "journal_stack_sample.txt").exists():
                    from .worker import start_stack_sampling

                    start_stack_sampling(
                        cfg.runtime_dir, cfg.log_dir, "journal_stack_sample", "journal_stack_samples.log"
                    )
        try:
            store.close()
        except Exception:
            LOG.exception("journal process close after parent exit failed")
    finally:
        if lock is not None:
            lock.close()


class _BufferView:
    """len()-only stand-in for Storage.parquet_buffer (the rows live in the child)."""

    def __init__(self, rows: int) -> None:
        self.rows = rows

    def __len__(self) -> int:
        return self.rows


class ProcessJournalStorage(Storage):
    """Storage whose journal/archive side runs in a child process."""

    def __init__(self, cfg: Settings) -> None:
        self._process = None
        self._conn = None
        self._send_lock = threading.Lock()
        self._pending: dict[int, Future] = {}
        self._next_id = 0
        self._id_lock = threading.Lock()
        self._cache_status: dict[str, Any] = {}
        self._timing_ema: dict[str, float] = {}
        self._last_ack_monotonic: float | None = None
        self._inflight: tuple[Future, list[dict[str, Any]]] | None = None
        self._failed: list[list[dict[str, Any]]] = []
        self._buffered_rows = 0
        self._raw_flag = True
        self._ready: Future = Future()
        self.child_alive = False
        super().__init__(cfg)

    # -- mirrored state ---------------------------------------------------------
    @property
    def raw_capture_enabled(self) -> bool:
        return self._raw_flag

    @raw_capture_enabled.setter
    def raw_capture_enabled(self, value: bool) -> None:
        value = bool(value)
        changed = value != self._raw_flag
        self._raw_flag = value
        if changed and self._conn is not None and self.child_alive:
            self._send(("set_raw", value))

    @property
    def parquet_buffer(self) -> _BufferView:
        return _BufferView(self._buffered_rows)

    @parquet_buffer.setter
    def parquet_buffer(self, _value: Any) -> None:  # Storage.__init__ assigns [] once
        return

    # -- plumbing ---------------------------------------------------------------
    def _send(self, message) -> None:
        with self._send_lock:
            self._conn.send(message)

    def _request(self, kind: str, *payload) -> Future:
        future: Future = Future()
        with self._id_lock:
            self._next_id += 1
            request_id = self._next_id
            self._pending[request_id] = future
        if not self.child_alive:
            self._pending.pop(request_id, None)
            future.set_exception(RuntimeError("journal process is not running"))
            return future
        try:
            self._send((kind, request_id, *payload))
        except (OSError, ValueError) as exc:
            self._pending.pop(request_id, None)
            future.set_exception(RuntimeError(f"journal process pipe failed: {exc}"))
        return future

    def _note(self, key: str, value: float) -> None:
        """EMA (alpha 0.2) of one pipe-path timing, in ms: where does a batch's time go?"""
        ema = self._timing_ema
        ema[key] = value if key not in ema else 0.8 * ema[key] + 0.2 * value

    def _apply_state(self, state: dict[str, Any]) -> None:
        for name in _MIRRORED:
            if name == "raw_capture_enabled":
                self._raw_flag = bool(state[name])
            elif name in state:
                setattr(self, name, state[name])
        self._buffered_rows = int(state.get("buffered_rows", 0))
        if state.get("latest_event") is not None:
            self.latest_event = tuple(state["latest_event"])
        if "status" in state:
            self._cache_status = state["status"]

    def _reader(self) -> None:
        conn = self._conn
        while True:
            try:
                message = conn.recv()
            except (EOFError, OSError):
                break
            kind = message[0]
            if kind == "state":
                self._apply_state(message[1])
            elif kind == "ready":
                if message[1]:
                    self._apply_state(message[2])
                    self._ready.set_result(True)
                else:
                    self._ready.set_exception(RuntimeError(f"journal process failed to start: {message[2]}"))
            else:  # ack / rpc / err
                if kind == "ack" and len(message) >= 5:
                    self._note("child_wall", message[3])
                    self._note("child_cpu", message[4])
                future = self._pending.pop(message[1], None)
                if future is None:
                    continue
                if kind == "err":
                    future.set_exception(RuntimeError(message[2]))
                else:
                    future.set_result(message[2])
        self.child_alive = False
        error = RuntimeError("journal process exited")
        if not self._ready.done():
            self._ready.set_exception(error)
        for request_id in list(self._pending):
            future = self._pending.pop(request_id, None)
            if future is not None and not future.done():
                future.set_exception(error)

    # -- Storage interface ------------------------------------------------------
    def open(self, **_ignored: Any) -> None:
        context = multiprocessing.get_context("spawn")
        parent_conn, child_conn = context.Pipe(duplex=True)
        self._conn = parent_conn
        self._process = context.Process(
            target=_child_main, args=(self.cfg, child_conn), name="journal-process", daemon=True
        )
        self._process.start()
        child_conn.close()
        self.child_alive = True
        threading.Thread(target=self._reader, name="journal-reader", daemon=True).start()
        self._ready.result()  # raises if the child could not take the lock or recover
        recovery_state = self.parquet_recovery_state  # mirrored from the child
        super().open(paper_only=True)
        self.parquet_recovery_state = recovery_state

    def append_batch(self, events: list[dict[str, Any]]) -> int:
        """Pipelined: send this batch, then collect the PREVIOUS batch's ack.

        The child works on batch N-1 while the parent gathers and sends batch N, so it is
        not idle during the parent's per-cycle work (measured ~1.3 s of a ~3 s cycle).
        A batch whose ack failed is kept in _failed and re-sent, in order, before anything
        new is sent; durability points (_settle: flush/close/metrics) wait for all of it.
        """
        started = time.perf_counter()
        self._resend_failed()
        if not self.child_alive:  # fail fast: the worker holds this batch and retries
            raise RuntimeError("journal process is not running")
        if self._last_ack_monotonic is not None:  # parent-side time between two batches
            self._note("gap", (time.monotonic() - self._last_ack_monotonic) * 1000)
        future = self._request("append", events)
        self._note("send", (time.perf_counter() - started) * 1000)  # pickle + pipe write
        previous, self._inflight = self._inflight, (future, events)
        if previous is not None:
            self._collect(previous)
        self._note("rpc", (time.perf_counter() - started) * 1000)
        self._last_ack_monotonic = time.monotonic()
        return len(events)

    def _collect(self, inflight: tuple[Future, list[dict[str, Any]]]) -> None:
        future, events = inflight
        try:
            future.result(timeout=CALL_TIMEOUT_SECONDS)
        except Exception:
            LOG.exception("journal batch failed; it will be re-sent before new batches")
            self._failed.append(events)

    def _resend_failed(self) -> None:
        """Re-send failed batches one by one; raises (keeping them queued) if one fails again."""
        while self._failed:
            events = self._failed[0]
            self._request("append", events).result(timeout=CALL_TIMEOUT_SECONDS)
            self._failed.pop(0)

    def _settle(self) -> None:
        """Everything handed to the child is acknowledged (or raises)."""
        inflight, self._inflight = self._inflight, None
        if inflight is not None:
            self._collect(inflight)
        self._resend_failed()

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        if method != "close" or self.child_alive:
            self._settle()
        return self._request("call", method, args, kwargs).result(timeout=CALL_TIMEOUT_SECONDS)

    def flush_parquet(self, *, reason: str = "MANUAL") -> int:
        return int(self._call("flush_parquet", reason=reason))

    def flush_parquet_if_due(self, reason: str = "AGED_BUFFER") -> int:
        return 0  # the child ages its own buffer after every command and when idle

    def resume_raw_capture_if_safe(self) -> bool:
        return bool(self._call("resume_raw_capture_if_safe"))

    def database_metrics(self, *, full_integrity_check: bool = False) -> dict[str, Any]:
        return self._call("database_metrics", full_integrity_check=full_integrity_check)

    def connection_status(self) -> dict[str, Any]:
        status = dict(self._cache_status)
        disk_paper = status.get("storage_latest_disk_paper_record_utc")
        latest_paper = None
        if self.connection is not None:
            row = self.connection.execute(
                "SELECT created_utc FROM paper_ledger ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            latest_paper = str(row[0]) if row else None
        status.update(
            storage_paper_ledger_record_count=self.paper_ledger_record_count,
            storage_latest_paper_record_utc=self.latest_paper_record_utc,
            # recomputed against our own fresh read: the child's probe predates our commits
            storage_paper_watermark_matches_disk=(latest_paper == self.latest_paper_record_utc),
            journal_process_alive=self.child_alive,
            journal_pipe_ms_ema={k: round(v, 1) for k, v in self._timing_ema.items()},
            journal_process_pid=self._process.pid if self._process is not None else None,
        )
        if disk_paper is None:
            status.setdefault("storage_latest_disk_paper_record_utc", latest_paper)
        return status

    def close(self) -> None:
        try:
            if self.child_alive:
                self._call("close")
        finally:
            if self._process is not None:
                self._process.join(timeout=60)
                if self._process.is_alive():
                    LOG.error("journal process did not exit after close; terminating")
                    self._process.terminate()
            if self.connection is not None:
                self.connection.commit()
                self.connection.close()
                self.connection = None
