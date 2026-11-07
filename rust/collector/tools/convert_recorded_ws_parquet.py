#!/usr/bin/env python3
"""Convert a finalized Phoenix Parquet segment to bounded raw-WS NDJSON replay input.

This is a one-way staging adapter: the Parquet source and its sidecar are read-only;
the output is an explicit, provenance-manifested NDJSON copy for the Rust offline
replay command. It preserves all messages from numeric WebSocket subscriptions in
source order and excludes rows such as REST discovery with subscription_id=REST:window.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

CONVERTER_VERSION = "1"
PROTECTED_OUTPUT_MARKERS = (
    "tradingnextdata",
    "kalshidata",
    "kalshi15mnative",
    "paper_ledger.sqlite3",
)
REQUIRED_COLUMNS = {
    "subscription_id",
    "connection_generation",
    "event_type",
    "received_at",
    "raw_payload",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp has no UTC offset: {value}")
    return parsed


def _decode_payload(value: bytes | str) -> dict[str, Any]:
    text = value.decode("utf-8") if isinstance(value, bytes) else value
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("raw_payload must decode to a JSON object")
    return payload


def _normalize_frame(
    *,
    raw_payload: bytes | str,
    received_at: str,
    connection_generation: str | int,
    subscription_id: str,
    expected_event_type: str,
    source_id: str,
) -> tuple[dict[str, Any], int, str]:
    message = _decode_payload(raw_payload)
    _parse_time(received_at)
    sid = message.get("sid")
    seq = message.get("seq")
    if isinstance(sid, bool) or not isinstance(sid, int):
        raise ValueError("numeric WebSocket subscription row has no integer sid")
    if isinstance(seq, bool) or not isinstance(seq, int):
        raise ValueError("numeric WebSocket subscription row has no integer seq")
    if sid != int(subscription_id):
        raise ValueError(
            f"source subscription_id {subscription_id} does not match raw sid {sid}"
        )
    if message.get("type") != expected_event_type:
        raise ValueError(
            f"source event_type {expected_event_type} does not match raw type {message.get('type')}"
        )
    if "_fixture_session" in message or "_collector_received_utc" in message:
        raise ValueError("source payload already contains reserved replay metadata")
    message["_fixture_session"] = f"segment-{source_id[:16]}-connection-{connection_generation}"
    message["_collector_received_utc"] = received_at
    return message, sid, expected_event_type


def convert_segment(
    input_path: Path,
    output_path: Path,
    *,
    start_utc: str | None = None,
    end_utc: str | None = None,
    subscription_ids: set[str] | None = None,
    require_verified_sidecar: bool = False,
    max_source_rows: int = 500_000,
) -> dict[str, Any]:
    input_path = input_path.resolve(strict=True)
    output_path = output_path.resolve(strict=False)
    if input_path == output_path:
        raise ValueError("input and output paths must differ")
    output_text = str(output_path).casefold().replace("/", "\\")
    if any(marker in output_text for marker in PROTECTED_OUTPUT_MARKERS):
        raise ValueError("refusing to write converter output in a protected data/runtime path")
    sidecar_path = Path(f"{input_path}.json")
    output_manifest_path = Path(f"{output_path}.manifest.json")
    partial_path = Path(f"{output_path}.partial")
    manifest_partial_path = Path(f"{output_manifest_path}.partial")
    for candidate in (output_path, output_manifest_path, partial_path, manifest_partial_path):
        if candidate.exists():
            raise FileExistsError(f"refusing to overwrite existing converter output: {candidate}")

    try:
        import pyarrow.parquet as pq
    except ImportError as error:  # pragma: no cover - installation depends on the caller
        raise RuntimeError("PyArrow is required for Parquet conversion") from error

    if max_source_rows <= 0 or max_source_rows > 1_000_000:
        raise ValueError("max_source_rows must be between 1 and 1,000,000")
    parquet_file = pq.ParquetFile(input_path)
    if parquet_file.metadata.num_rows > max_source_rows:
        raise ValueError(
            "source Parquet exceeds bounded row limit; select a smaller finalized segment"
        )
    missing = REQUIRED_COLUMNS.difference(parquet_file.schema_arrow.names)
    if missing:
        raise ValueError(f"source Parquet is missing columns: {sorted(missing)}")
    sidecar: dict[str, Any] | None = None
    source_sha256 = _sha256(input_path)
    sidecar_verified = False
    if sidecar_path.is_file():
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
        if not isinstance(sidecar, dict):
            raise ValueError("source sidecar must be a JSON object")
        expected_hash = sidecar.get("outputSha256")
        expected_rows = sidecar.get("readbackRowsVerified")
        # A sidecar without verifiable hash and row count must never be
        # reported as authoritative provenance.
        if not isinstance(expected_hash, str) or len(expected_hash) != 64:
            raise ValueError("source sidecar missing valid outputSha256")
        if expected_hash.casefold() != source_sha256:
            raise ValueError("source Parquet SHA-256 does not match its sidecar")
        if isinstance(expected_rows, bool) or not isinstance(expected_rows, int):
            raise ValueError("source sidecar missing integer readbackRowsVerified")
        if expected_rows != parquet_file.metadata.num_rows:
            raise ValueError("source Parquet row count does not match its sidecar")
        sidecar_verified = True
    if require_verified_sidecar and not sidecar_verified:
        raise ValueError("verified finalized sidecar required but not present")
    # Restrict both initial metadata inspection and materialization. The
    # caller must provide bounded segments rather than one huge raw archive.
    table = parquet_file.read(columns=sorted(REQUIRED_COLUMNS))

    start = _parse_time(start_utc) if start_utc else None
    end = _parse_time(end_utc) if end_utc else None
    if start and end and end < start:
        raise ValueError("end_utc precedes start_utc")
    requested_subscriptions = subscription_ids
    counts: Counter[str] = Counter()
    event_counts: Counter[str] = Counter()
    # A Kalshi subscription ID may be reused after reconnection. Sequence
    # continuity is meaningful only within the same connection generation.
    seq_state: dict[tuple[str, int], int] = {}
    seq_breaks: Counter[str] = Counter()
    first_received: str | None = None
    last_received: str | None = None
    previous_source_time: datetime | None = None
    written = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with partial_path.open("x", encoding="utf-8", newline="\n") as output:
            for row in table.select(sorted(REQUIRED_COLUMNS)).to_pylist():
                subscription = str(row["subscription_id"])
                if not subscription.isdecimal():
                    counts["excluded_non_websocket_subscription"] += 1
                    continue
                if requested_subscriptions is not None and subscription not in requested_subscriptions:
                    counts["excluded_subscription_filter"] += 1
                    continue
                received_at = str(row["received_at"])
                received_time = _parse_time(received_at)
                if previous_source_time is not None and received_time < previous_source_time:
                    raise ValueError("source rows are not ordered by received_at")
                previous_source_time = received_time
                if start and received_time < start:
                    counts["excluded_before_time_range"] += 1
                    continue
                if end and received_time > end:
                    counts["excluded_after_time_range"] += 1
                    continue
                message, sid, event_type = _normalize_frame(
                    raw_payload=row["raw_payload"],
                    received_at=received_at,
                    connection_generation=row["connection_generation"],
                    subscription_id=subscription,
                    expected_event_type=str(row["event_type"]),
                    source_id=source_sha256,
                )
                sequence_identity = (str(row["connection_generation"]), sid)
                prior = seq_state.get(sequence_identity)
                if prior is not None and message["seq"] != prior + 1:
                    seq_breaks[str(sid)] += 1
                seq_state[sequence_identity] = message["seq"]
                output.write(json.dumps(message, separators=(",", ":"), ensure_ascii=False))
                output.write("\n")
                written += 1
                counts["included_websocket_frames"] += 1
                counts[f"subscription_{sid}"] += 1
                event_counts[event_type] += 1
                first_received = first_received or received_at
                last_received = received_at
        if written == 0:
            raise ValueError("conversion selection produced no WebSocket frames")
        os.replace(partial_path, output_path)
        manifest = {
            "converter": "convert_recorded_ws_parquet.py",
            "converter_version": CONVERTER_VERSION,
            "input_path": str(input_path),
            "input_sha256": source_sha256,
            "input_sidecar_path": str(sidecar_path) if sidecar else None,
            "input_sidecar_verified": sidecar_verified,
            "max_source_rows": max_source_rows,
            "input_rows": table.num_rows,
            "output_path": str(output_path),
            "output_sha256": _sha256(output_path),
            "output_lines": written,
            "received_at_first": first_received,
            "received_at_last": last_received,
            "start_utc_filter": start_utc,
            "end_utc_filter": end_utc,
            "subscription_id_filter": sorted(requested_subscriptions)
            if requested_subscriptions is not None
            else None,
            "row_counts": dict(sorted(counts.items())),
            "event_type_counts": dict(sorted(event_counts.items())),
            "subscription_sequence_break_counts": dict(sorted(seq_breaks.items())),
            "transformation_rules": [
                "Preserve numeric-subscription raw_payload JSON and source row order.",
                "Exclude nonnumeric subscription_id rows such as REST:window.",
                "Inject _fixture_session from source SHA-256 and connection_generation.",
                "Check subscription sequence continuity only within the same source connection_generation.",
                "Inject _collector_received_utc from source received_at.",
                "Apply optional time and subscription filters without rewriting source data.",
            ],
        }
        with manifest_partial_path.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(manifest_partial_path, output_manifest_path)
        return manifest
    except Exception:
        # Preserve partial output as explicit recovery evidence for a failed conversion.
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="finalized Parquet segment")
    parser.add_argument("--output", type=Path, required=True, help="new staging NDJSON path")
    parser.add_argument("--start-utc", help="inclusive RFC3339 received_at bound")
    parser.add_argument("--end-utc", help="inclusive RFC3339 received_at bound")
    parser.add_argument(
        "--subscription-id",
        action="append",
        dest="subscription_ids",
        help="include only this numeric WebSocket subscription; repeatable",
    )
    parser.add_argument(
        "--require-verified-sidecar", action="store_true",
        help="fail closed unless the source sidecar has a matching SHA-256 and row count",
    )
    parser.add_argument(
        "--max-source-rows", type=int, default=500_000,
        help="maximum Parquet rows to materialize (default 500000, absolute max 1000000)",
    )
    args = parser.parse_args()
    manifest = convert_segment(
        args.input,
        args.output,
        start_utc=args.start_utc,
        end_utc=args.end_utc,
        subscription_ids=set(args.subscription_ids) if args.subscription_ids else None,
        require_verified_sidecar=args.require_verified_sidecar,
        max_source_rows=args.max_source_rows,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
