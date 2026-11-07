import json
import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from convert_recorded_ws_parquet import _normalize_frame, convert_segment  # noqa: E402


class RecordedWebSocketParquetConverterTests(unittest.TestCase):
    def test_normalize_preserves_protocol_and_qualifies_source_session(self):
        raw = json.dumps(
            {"type": "orderbook_snapshot", "sid": 7, "seq": 19, "msg": {"market_ticker": "T"}}
        ).encode()
        message, sid, event_type = _normalize_frame(
            raw_payload=raw,
            received_at="2026-10-30T11:45:00.123Z",
            connection_generation=2,
            subscription_id="7",
            expected_event_type="orderbook_snapshot",
            source_id="a" * 64,
        )
        self.assertEqual(message["type"], "orderbook_snapshot")
        self.assertEqual(message["seq"], 19)
        self.assertEqual(message["_collector_received_utc"], "2026-10-30T11:45:00.123Z")
        self.assertEqual(message["_fixture_session"], f"segment-{'a' * 16}-connection-2")
        self.assertEqual((sid, event_type), (7, "orderbook_snapshot"))

    def test_conversion_filters_rest_rows_and_records_sequence_provenance(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "segment.parquet"
            output = root / "stage" / "segment.ndjson"
            messages = [
                {"type": "orderbook_snapshot", "sid": 1, "seq": 10, "msg": {"market_ticker": "T"}},
                {"type": "orderbook_delta", "sid": 1, "seq": 12, "msg": {"market_ticker": "T"}},
                {"type": "market_discovery", "msg": {"markets": []}},
            ]
            table = pa.table(
                {
                    "subscription_id": ["1", "1", "REST:window"],
                    "connection_generation": ["2", "2", "0"],
                    "event_type": ["orderbook_snapshot", "orderbook_delta", "market_discovery"],
                    "received_at": [
                        "2026-10-30T11:45:00.100Z",
                        "2026-10-30T11:45:00.200Z",
                        "2026-10-30T11:45:00.300Z",
                    ],
                    "raw_payload": [json.dumps(item).encode() for item in messages],
                }
            )
            pq.write_table(table, source)

            manifest = convert_segment(source, output)

            lines = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(lines), 2)
            self.assertEqual(manifest["row_counts"]["excluded_non_websocket_subscription"], 1)
            self.assertEqual(manifest["subscription_sequence_break_counts"], {"1": 1})
            self.assertEqual(manifest["output_lines"], 2)
            self.assertTrue(Path(f"{output}.manifest.json").is_file())
            with self.assertRaises(FileExistsError):
                convert_segment(source, output)

    def test_subscription_mismatch_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "does not match raw sid"):
            _normalize_frame(
                raw_payload=b'{"type":"trade","sid":4,"seq":2}',
                received_at="2026-10-30T11:45:00Z",
                connection_generation=1,
                subscription_id="3",
                expected_event_type="trade",
                source_id="b" * 64,
            )


if __name__ == "__main__":
    unittest.main()
