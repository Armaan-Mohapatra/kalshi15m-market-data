from __future__ import annotations

import hashlib
import json
import struct
import tempfile
import unittest
import zlib
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from convert_tnevent_v2_segments import SegmentError, convert_segments


def _record(
    *,
    received: str,
    ticker: str,
    sid: int | None,
    seq: int | None,
    generation: int = 5,
    event_type: str = "orderbook_delta",
) -> bytes:
    if sid is None:
        identity = None
        metadata_sid = "REST:window"
        payload = {"markets": [{"ticker": ticker, "open_time": "2026-10-31T12:00:00Z"}]}
    else:
        metadata_sid = str(sid)
        identity = (
            f"kalshi-trade:{ticker}:trade-{seq}"
            if event_type == "trade"
            else f"run-test:g{generation}:sid{sid}:seq{seq}:type{event_type}"
        )
        payload = {
            "type": event_type,
            "sid": sid,
            "seq": seq,
            "msg": {"market_ticker": ticker, "price_dollars": "0.50"},
        }
    metadata = [
        1,
        "kalshi",
        ticker,
        metadata_sid,
        event_type if sid is not None else "market_discovery",
        None,
        received,
        received,
        seq,
        generation if sid is not None else 0,
        identity,
    ]
    meta_bytes = json.dumps(metadata, separators=(",", ":")).encode()
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode()
    remainder = 4 + len(meta_bytes) + len(payload_bytes) + 1
    return (
        struct.pack(">III", remainder, 123456, len(meta_bytes))
        + meta_bytes
        + payload_bytes
        + b"\n"
    )


def _write_segment(root: Path, segment_id: int, records: list[bytes]) -> Path:
    directory = root / f"segment-{segment_id:08}.ready"
    directory.mkdir()
    # A first block uses the compact marker-0 header; subsequent blocks use
    # marker 1. Keeping one record per block exercises both block paths.
    content = bytearray(b"TNEVENT\0" + struct.pack(">HBB", 2, 1, 0))
    for index, record in enumerate(records):
        compressed = zlib.compress(record)
        crc = zlib.crc32(record) & 0xFFFFFFFF
        if index == 0:
            content.extend(b"\x00" + struct.pack(">HII", len(compressed), len(record), crc))
        else:
            content.extend(b"\x01" + struct.pack(">III", len(compressed), len(record), crc))
        content.extend(compressed)
    events = directory / "events.bin"
    events.write_bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    manifest = {
        "schema_version": 1,
        "segment_format_version": 2,
        "segment_id": segment_id,
        "event_count": len(records),
        "file_bytes": len(content),
        "max_event_bytes": 4_194_304,
        "sha256": digest,
        "created_at": "2026-10-31T11:59:00Z",
        "finalized_at": "2026-10-31T12:01:00Z",
    }
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (directory / "segment.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "segment_format_version": 2,
                "segment_id": segment_id,
                "created_at": manifest["created_at"],
            }
        ),
        encoding="utf-8",
    )
    return directory


class TneventV2ConverterTests(unittest.TestCase):
    def test_verified_multisegment_window_preserves_source_connection(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            target = "KXBTC15M-26OCT310645-45"
            other = "KXETH15M-26OCT310645-45"
            first = _write_segment(
                root,
                462,
                [
                    _record(
                        received="2026-10-31T11:59:59.900Z",
                        ticker=target,
                        sid=1,
                        seq=99,
                    ),
                    _record(
                        received="2026-10-31T12:00:00.100Z",
                        ticker=target,
                        sid=1,
                        seq=100,
                    ),
                    _record(
                        received="2026-10-31T12:00:00.200Z",
                        ticker=other,
                        sid=1,
                        seq=101,
                    ),
                    _record(
                        received="2026-10-31T12:00:00.300Z",
                        ticker=target,
                        sid=None,
                        seq=None,
                    ),
                    _record(
                        received="2026-10-31T12:00:00.400Z",
                        ticker=target,
                        sid=3,
                        seq=1,
                        event_type="trade",
                    ),
                ],
            )
            second = _write_segment(
                root,
                463,
                [
                    _record(
                        received="2026-10-31T12:00:59.900Z",
                        ticker=target,
                        sid=1,
                        seq=102,
                    )
                ],
            )
            output = root / "staging" / "opening.ndjson"
            result = convert_segments(
                [first, second],
                output,
                start_utc="2026-10-31T12:00:00Z",
                end_utc="2026-10-31T12:03:00Z",
                target_market=target,
            )

            rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(rows), 4)
            self.assertEqual({row["_fixture_session"] for row in rows}, {"run-test:g5"})
            self.assertEqual([row["seq"] for row in rows if row["sid"] == 1], [100, 101, 102])
            self.assertEqual(result["source_event_count_verified"], 6)
            self.assertEqual(result["websocket_replay"]["frames_in_window"], 4)
            self.assertEqual(result["websocket_replay"]["sequence_gaps_in_window"], {})
            self.assertEqual(result["window"]["target_event_types"], {"orderbook_delta": 2, "trade": 1})
            self.assertEqual(result["excluded_non_websocket_records"]["discovery_rows"], 1)
            provenance = Path(f"{output}.provenance.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(provenance), 4)
            self.assertEqual(json.loads(provenance[-1])["source_segment_id"], 463)
            discovery = Path(f"{output}.discovery.jsonl").read_text(encoding="utf-8")
            self.assertIn('"open_time":"2026-10-31T12:00:00Z"', discovery)
            saved_manifest = json.loads(Path(f"{output}.manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(saved_manifest["outputs"]["ndjson"]["readbackRowsVerified"], 4)

    def test_invalid_block_crc_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            segment = _write_segment(
                root,
                1,
                [
                    _record(
                        received="2026-10-31T12:00:00.100Z",
                        ticker="KXBTC15M-TEST",
                        sid=1,
                        seq=1,
                    )
                ],
            )
            events_path = segment / "events.bin"
            raw = bytearray(events_path.read_bytes())
            raw[19] ^= 0x01  # First block CRC-32; compressed bytes remain intact.
            events_path.write_bytes(raw)
            manifest_path = segment / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["sha256"] = hashlib.sha256(raw).hexdigest()
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(SegmentError, "CRC-32"):
                convert_segments(
                    [segment],
                    root / "stage" / "output.ndjson",
                    start_utc="2026-10-31T12:00:00Z",
                    end_utc="2026-10-31T12:03:00Z",
                    target_market="KXBTC15M-TEST",
                )

    def test_oversized_source_is_rejected_before_decode(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            segment = _write_segment(
                root,
                1,
                [
                    _record(
                        received="2026-10-31T12:00:00.100Z",
                        ticker="KXBTC15M-TEST",
                        sid=1,
                        seq=1,
                    )
                ],
            )
            manifest_path = segment / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["event_count"] = 2
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(SegmentError, "over bounded limit"):
                convert_segments(
                    [segment],
                    root / "stage" / "output.ndjson",
                    start_utc="2026-10-31T12:00:00Z",
                    end_utc="2026-10-31T12:03:00Z",
                    target_market="KXBTC15M-TEST",
                    max_source_events=1,
                )

    def test_trade_without_source_connection_mapping_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            segment = _write_segment(
                root,
                1,
                [
                    _record(
                        received="2026-10-31T12:00:00.100Z",
                        ticker="KXBTC15M-TEST",
                        sid=3,
                        seq=1,
                        event_type="trade",
                    )
                ],
            )
            with self.assertRaisesRegex(SegmentError, "cannot resolve trade record"):
                convert_segments(
                    [segment],
                    root / "stage" / "output.ndjson",
                    start_utc="2026-10-31T12:00:00Z",
                    end_utc="2026-10-31T12:03:00Z",
                    target_market="KXBTC15M-TEST",
                )


if __name__ == "__main__":
    unittest.main()
