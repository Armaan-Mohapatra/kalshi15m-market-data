#!/usr/bin/env python3
"""Convert finalized Phoenix TNEVENT v2 segments to bounded Rust WS replay NDJSON.

The adapter is intentionally fail-closed. It verifies each finalized segment's
manifest SHA-256, byte and event counts, versioned block framing, zlib lengths,
block CRCs, record framing, source metadata, and source connection identities.
Original `.ready` inputs are only read. Output is written to a new staging path.
"""

from __future__ import annotations

import argparse
import binascii
import hashlib
import json
import os
import re
import struct
import zlib
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

FORMAT_MAGIC = b"TNEVENT\0"
FORMAT_VERSION = 2
SCHEMA_VERSION = 1
MAX_BLOCK_BYTES = 16 * 1024 * 1024
MAX_METADATA_BYTES = 64 * 1024
MAX_SOURCE_EVENTS = 1_000_000
PROTECTED_OUTPUT_MARKERS = (
    "tradingnextdata",
    "kalshidata",
    "kalshi15mnative",
    "paper_ledger.sqlite3",
)
CONNECTION_ID = re.compile(
    r"(?P<root>.+):g(?P<generation>\d+):sid(?P<sid>\d+):"
    r"seq(?P<seq>\d+):type(?P<event_type>.+)"
)


class SegmentError(ValueError):
    """A finalized source does not meet the supported, verified v2 contract."""


@dataclass(frozen=True)
class Segment:
    directory: Path
    segment_id: int
    event_count: int
    file_bytes: int
    max_event_bytes: int
    sha256: str
    created_at: str
    finalized_at: str


@dataclass(frozen=True)
class Record:
    metadata: list[Any]
    payload: dict[str, Any]
    payload_raw: bytes
    opaque_word: int
    ordinal: int


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SegmentError(f"cannot read valid JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise SegmentError(f"expected a JSON object in {path}")
    return value


def _verified_segment(path: Path, *, max_source_events: int) -> Segment:
    directory = path.resolve(strict=True)
    if not directory.is_dir() or directory.suffix.casefold() != ".ready":
        raise SegmentError(f"input must be a finalized .ready directory: {directory}")
    manifest = _read_json(directory / "manifest.json")
    segment_meta = _read_json(directory / "segment.json")
    events_path = directory / "events.bin"
    try:
        stat = events_path.stat()
    except OSError as error:
        raise SegmentError(f"cannot stat finalized event file {events_path}: {error}") from error

    def integer(name: str, value: Any, *, minimum: int = 0) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise SegmentError(f"invalid {name} in {directory / 'manifest.json'}")
        return value

    segment_id = integer("segment_id", manifest.get("segment_id"), minimum=0)
    count = integer("event_count", manifest.get("event_count"), minimum=1)
    if count > max_source_events:
        raise SegmentError(
            f"source segment {segment_id} has {count} events, over bounded limit "
            f"{max_source_events}; select smaller finalized segments"
        )
    file_bytes = integer("file_bytes", manifest.get("file_bytes"), minimum=1)
    max_event_bytes = integer(
        "max_event_bytes", manifest.get("max_event_bytes"), minimum=1
    )
    expected_hash = manifest.get("sha256")
    if (
        not isinstance(expected_hash, str)
        or re.fullmatch(r"[0-9a-fA-F]{64}", expected_hash) is None
    ):
        raise SegmentError(f"invalid SHA-256 claim in {directory / 'manifest.json'}")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise SegmentError(f"unsupported manifest schema in {directory}")
    if manifest.get("segment_format_version") != FORMAT_VERSION:
        raise SegmentError(f"unsupported segment format in {directory}")
    if segment_meta.get("segment_id") != segment_id:
        raise SegmentError(f"segment.json id does not match manifest in {directory}")
    if segment_meta.get("segment_format_version") != FORMAT_VERSION:
        raise SegmentError(f"segment.json format does not match manifest in {directory}")
    if stat.st_size != file_bytes:
        raise SegmentError(
            f"event file size mismatch for segment {segment_id}: "
            f"manifest={file_bytes}, actual={stat.st_size}"
        )
    actual_hash = _sha256(events_path)
    if actual_hash.casefold() != expected_hash.casefold():
        raise SegmentError(f"event file SHA-256 mismatch for segment {segment_id}")
    return Segment(
        directory=directory,
        segment_id=segment_id,
        event_count=count,
        file_bytes=file_bytes,
        max_event_bytes=max_event_bytes,
        sha256=actual_hash,
        created_at=str(manifest.get("created_at", "")),
        finalized_at=str(manifest.get("finalized_at", "")),
    )


def _utc(value: Any, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise SegmentError(f"{label} must be a timestamp string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise SegmentError(f"invalid {label}: {value!r}") from error
    if parsed.tzinfo is None:
        raise SegmentError(f"{label} has no UTC offset: {value!r}")
    return parsed.astimezone(timezone.utc)


def _decode_record_block(raw: bytes, segment: Segment, first_ordinal: int) -> Iterator[Record]:
    offset = 0
    ordinal = first_ordinal
    while offset < len(raw):
        if len(raw) - offset < 12:
            raise SegmentError(f"short record header in segment {segment.segment_id}")
        remainder = struct.unpack_from(">I", raw, offset)[0]
        if remainder < 5 or remainder > segment.max_event_bytes + MAX_METADATA_BYTES + 5:
            raise SegmentError(
                f"invalid bounded record length {remainder} in segment {segment.segment_id}"
            )
        record_bytes = 8 + remainder
        if offset + record_bytes > len(raw):
            raise SegmentError(f"truncated record in segment {segment.segment_id}")
        metadata_length = struct.unpack_from(">I", raw, offset + 8)[0]
        payload_length = remainder - 4 - metadata_length - 1
        if (
            metadata_length <= 0
            or metadata_length > MAX_METADATA_BYTES
            or payload_length <= 0
            or payload_length > segment.max_event_bytes
        ):
            raise SegmentError(f"invalid metadata or payload length in segment {segment.segment_id}")
        frame = raw[offset : offset + record_bytes]
        if frame[-1] != 0x0A:
            raise SegmentError(f"record is missing its newline terminator in segment {segment.segment_id}")
        metadata_raw = frame[12 : 12 + metadata_length]
        payload_raw = frame[12 + metadata_length : -1]
        try:
            metadata = json.loads(metadata_raw)
            payload = json.loads(payload_raw)
        except (UnicodeError, json.JSONDecodeError) as error:
            raise SegmentError(
                f"invalid record JSON in segment {segment.segment_id}, record {ordinal + 1}"
            ) from error
        if (
            not isinstance(metadata, list)
            or len(metadata) < 11
            or not isinstance(payload, dict)
            or metadata[0] != SCHEMA_VERSION
            or metadata[1] != "kalshi"
        ):
            raise SegmentError(
                f"unsupported record schema in segment {segment.segment_id}, record {ordinal + 1}"
            )
        _utc(metadata[7], label="record receive timestamp")
        opaque_word = struct.unpack_from(">I", frame, 4)[0]
        yield Record(metadata, payload, payload_raw, opaque_word, ordinal + 1)
        ordinal += 1
        offset += record_bytes


def _records(segment: Segment) -> Iterator[Record]:
    path = segment.directory / "events.bin"
    with path.open("rb") as source:
        prefix = source.read(12)
        if (
            len(prefix) != 12
            or prefix[:8] != FORMAT_MAGIC
            or struct.unpack_from(">H", prefix, 8)[0] != FORMAT_VERSION
            or prefix[10] != SCHEMA_VERSION
            or prefix[11] != 0
        ):
            raise SegmentError(f"unsupported TNEVENT file header in segment {segment.segment_id}")
        block_index = 0
        event_count = 0
        while True:
            marker = source.read(1)
            if not marker:
                break
            if marker == b"\x00" and block_index == 0:
                block_header = source.read(10)
                if len(block_header) != 10:
                    raise SegmentError(f"truncated first block header in segment {segment.segment_id}")
                compressed_length = struct.unpack_from(">H", block_header, 0)[0]
                raw_length = struct.unpack_from(">I", block_header, 2)[0]
                expected_crc = struct.unpack_from(">I", block_header, 6)[0]
            elif marker == b"\x01" and block_index > 0:
                block_header = source.read(12)
                if len(block_header) != 12:
                    raise SegmentError(
                        f"truncated continuation block header in segment {segment.segment_id}"
                    )
                compressed_length, raw_length, expected_crc = struct.unpack(">III", block_header)
            else:
                raise SegmentError(
                    f"unexpected block marker {marker.hex()} at block {block_index} "
                    f"in segment {segment.segment_id}"
                )
            if (
                compressed_length <= 0
                or raw_length <= 0
                or raw_length > MAX_BLOCK_BYTES
                or compressed_length > segment.file_bytes
            ):
                raise SegmentError(f"invalid bounded block length in segment {segment.segment_id}")
            compressed = source.read(compressed_length)
            if len(compressed) != compressed_length:
                raise SegmentError(f"truncated compressed block in segment {segment.segment_id}")
            decompressor = zlib.decompressobj()
            try:
                raw = decompressor.decompress(compressed, raw_length + 1)
                if len(raw) > raw_length or decompressor.unconsumed_tail:
                    raise SegmentError(
                        f"compressed block expands beyond declared bound in segment {segment.segment_id}"
                    )
                raw += decompressor.flush(raw_length + 1 - len(raw))
            except zlib.error as error:
                raise SegmentError(
                    f"invalid compressed block {block_index} in segment {segment.segment_id}"
                ) from error
            if (
                not decompressor.eof
                or decompressor.unused_data
                or decompressor.unconsumed_tail
                or len(raw) != raw_length
            ):
                raise SegmentError(
                    f"compressed block length mismatch in segment {segment.segment_id}, block {block_index}"
                )
            if (binascii.crc32(raw) & 0xFFFFFFFF) != expected_crc:
                raise SegmentError(
                    f"block CRC-32 mismatch in segment {segment.segment_id}, block {block_index}"
                )
            for record in _decode_record_block(raw, segment, event_count):
                event_count += 1
                yield record
            block_index += 1
        if block_index == 0 or event_count != segment.event_count:
            raise SegmentError(
                f"segment {segment.segment_id} event count mismatch: "
                f"manifest={segment.event_count}, decoded={event_count}, blocks={block_index}"
            )


def _parse_ws_identity(record: Record) -> tuple[str, int, int] | None:
    metadata = record.metadata
    payload = record.payload
    sid = payload.get("sid")
    seq = payload.get("seq")
    event_type = metadata[4]
    if sid is None and seq is None:
        return None
    if (
        isinstance(sid, bool)
        or not isinstance(sid, int)
        or isinstance(seq, bool)
        or not isinstance(seq, int)
        or not isinstance(event_type, str)
        or payload.get("type") != event_type
    ):
        raise SegmentError("WebSocket raw message is missing valid sid, seq, or type")
    if str(metadata[3]) != str(sid) or metadata[8] != seq:
        raise SegmentError("WebSocket message sid/seq does not match source metadata")
    generation = metadata[9]
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
        raise SegmentError("WebSocket record has an invalid connection generation")
    identity = metadata[10]
    if not isinstance(identity, str):
        raise SegmentError("WebSocket record has no source identity")
    return identity, sid, seq


def convert_segments(
    inputs: list[Path],
    output: Path,
    *,
    start_utc: str,
    end_utc: str,
    target_market: str,
    max_source_events: int = 500_000,
) -> dict[str, Any]:
    if not inputs:
        raise SegmentError("specify at least one finalized .ready segment")
    if max_source_events <= 0 or max_source_events > MAX_SOURCE_EVENTS:
        raise SegmentError(f"max_source_events must be between 1 and {MAX_SOURCE_EVENTS}")
    start = _utc(start_utc, label="start_utc")
    end = _utc(end_utc, label="end_utc")
    if end <= start:
        raise SegmentError("end_utc must be later than start_utc")
    if not target_market or len(target_market) > 128:
        raise SegmentError("target_market must be a nonempty ticker of at most 128 characters")

    # Validate bounded manifest counts before hashing or decoding payloads.
    segments = [_verified_segment(path, max_source_events=max_source_events) for path in inputs]
    total_source_events = sum(segment.event_count for segment in segments)
    if total_source_events > max_source_events:
        raise SegmentError(
            f"selected segments total {total_source_events} events, over bounded limit "
            f"{max_source_events}; select smaller finalized segments"
        )
    if [segment.segment_id for segment in segments] != sorted(
        {segment.segment_id for segment in segments}
    ):
        raise SegmentError("input segments must have unique IDs in ascending source order")

    output = output.resolve(strict=False)
    normalized_output = str(output).casefold().replace("/", "\\")
    if any(marker in normalized_output for marker in PROTECTED_OUTPUT_MARKERS):
        raise SegmentError(f"refusing to write converter outputs under a protected path: {output}")
    discovery_path = Path(f"{output}.discovery.jsonl")
    provenance_path = Path(f"{output}.provenance.jsonl")
    manifest_path = Path(f"{output}.manifest.json")
    finals = (output, discovery_path, provenance_path, manifest_path)
    partials = tuple(Path(f"{path}.partial") for path in finals)
    for path in (*finals, *partials):
        if path.exists():
            raise FileExistsError(f"refusing to overwrite converter output: {path}")
    output.parent.mkdir(parents=True, exist_ok=True)

    source_rows = 0
    window_rows = 0
    discovery_rows = 0
    ws_types: Counter[str] = Counter()
    non_ws_types: Counter[str] = Counter()
    target_types: Counter[str] = Counter()
    target_minutes: Counter[str] = Counter()
    full_seq_gaps: Counter[str] = Counter()
    full_seq_duplicates: Counter[str] = Counter()
    full_seq_regressions: Counter[str] = Counter()
    window_seq_gaps: Counter[str] = Counter()
    window_seq_duplicates: Counter[str] = Counter()
    window_seq_regressions: Counter[str] = Counter()
    seq_previous: dict[tuple[str, int], int] = {}
    window_seq_previous: dict[tuple[str, int], int] = {}
    connection_by_generation: dict[int, str] = {}
    previous_receive: datetime | None = None
    target_first: datetime | None = None
    target_last: datetime | None = None
    raw_digest = hashlib.sha256()
    provenance_digest = hashlib.sha256()
    discovery_digest = hashlib.sha256()

    try:
        with (
            partials[0].open("x", encoding="utf-8", newline="\n") as raw_out,
            partials[1].open("x", encoding="utf-8", newline="\n") as discovery_out,
            partials[2].open("x", encoding="utf-8", newline="\n") as provenance_out,
        ):
            for segment in segments:
                for record in _records(segment):
                    source_rows += 1
                    metadata = record.metadata
                    payload = record.payload
                    received = _utc(metadata[7], label="record receive timestamp")
                    if previous_receive is not None and received < previous_receive:
                        raise SegmentError("source receive timestamps regress across finalized segment order")
                    previous_receive = received
                    event_type = str(metadata[4])
                    identity = _parse_ws_identity(record)
                    if identity is None:
                        non_ws_types[event_type] += 1
                        discovery_row = {
                            "source_segment_id": segment.segment_id,
                            "source_record_ordinal": record.ordinal,
                            "received_utc": str(metadata[7]),
                            "source_metadata": metadata,
                            "payload": payload,
                        }
                        encoded = json.dumps(
                            discovery_row, ensure_ascii=False, separators=(",", ":")
                        ) + "\n"
                        discovery_out.write(encoded)
                        discovery_digest.update(encoded.encode("utf-8"))
                        discovery_rows += 1
                        continue

                    source_identity, sid, seq = identity
                    generation = int(metadata[9])
                    match = CONNECTION_ID.fullmatch(source_identity)
                    if match:
                        if (
                            int(match.group("generation")) != generation
                            or int(match.group("sid")) != sid
                            or int(match.group("seq")) != seq
                            or match.group("event_type") != event_type
                        ):
                            raise SegmentError("source connection identity conflicts with raw frame")
                        connection = f"{match.group('root')}:g{generation}"
                        previous_connection = connection_by_generation.setdefault(
                            generation, connection
                        )
                        if previous_connection != connection:
                            raise SegmentError(
                                "ambiguous source run identity for a connection generation"
                            )
                    elif event_type == "trade" and source_identity.startswith("kalshi-trade:"):
                        connection = connection_by_generation.get(generation, "")
                        if not connection:
                            raise SegmentError(
                                f"cannot resolve trade record to source connection generation {generation}"
                            )
                    else:
                        raise SegmentError(f"unsupported WebSocket source identity: {source_identity}")

                    sequence_key = (connection, sid)
                    previous_seq = seq_previous.get(sequence_key)
                    if previous_seq is not None:
                        label = f"{connection}|sid={sid}"
                        if seq == previous_seq:
                            full_seq_duplicates[label] += 1
                        elif seq < previous_seq:
                            full_seq_regressions[label] += 1
                        elif seq > previous_seq + 1:
                            full_seq_gaps[label] += seq - previous_seq - 1
                    seq_previous[sequence_key] = seq

                    if not (start <= received < end):
                        continue
                    window_rows += 1
                    ws_types[event_type] += 1
                    window_previous = window_seq_previous.get(sequence_key)
                    if window_previous is not None:
                        label = f"{connection}|sid={sid}"
                        if seq == window_previous:
                            window_seq_duplicates[label] += 1
                        elif seq < window_previous:
                            window_seq_regressions[label] += 1
                        elif seq > window_previous + 1:
                            window_seq_gaps[label] += seq - window_previous - 1
                    window_seq_previous[sequence_key] = seq

                    ticker = str(metadata[2]) if metadata[2] is not None else ""
                    if ticker == target_market:
                        target_types[event_type] += 1
                        target_minutes[received.strftime("%H:%M")] += 1
                        target_first = received if target_first is None else min(target_first, received)
                        target_last = received if target_last is None else max(target_last, received)
                    replay_payload = dict(payload)
                    replay_payload["_fixture_session"] = connection
                    replay_payload["_collector_received_utc"] = str(metadata[7])
                    encoded = json.dumps(
                        replay_payload, ensure_ascii=False, separators=(",", ":")
                    ) + "\n"
                    raw_out.write(encoded)
                    raw_digest.update(encoded.encode("utf-8"))
                    provenance_row = {
                        "output_line": window_rows,
                        "source_segment_id": segment.segment_id,
                        "source_segment_sha256": segment.sha256,
                        "source_record_ordinal": record.ordinal,
                        "opaque_record_word": record.opaque_word,
                        "source_connection": connection,
                        "sid": sid,
                        "seq": seq,
                        "event_type": event_type,
                        "market_ticker": ticker,
                        "received_utc": str(metadata[7]),
                        "raw_message_sha256": hashlib.sha256(
                            record.payload_raw
                        ).hexdigest(),
                    }
                    provenance_encoded = json.dumps(
                        provenance_row, ensure_ascii=False, separators=(",", ":")
                    ) + "\n"
                    provenance_out.write(provenance_encoded)
                    provenance_digest.update(provenance_encoded.encode("utf-8"))
            for handle in (raw_out, discovery_out, provenance_out):
                handle.flush()
                os.fsync(handle.fileno())
        if source_rows != total_source_events:
            raise SegmentError(
                f"decoded source row count {source_rows} does not match manifest total {total_source_events}"
            )
        if window_rows == 0 or target_first is None or target_last is None:
            raise SegmentError("selected segments contain no target-market records in the requested window")

        # The three data files were flushed and synced before their handles closed.
        output_sha = _sha256(partials[0])
        provenance_sha = provenance_digest.hexdigest()
        discovery_sha = discovery_digest.hexdigest()
        with partials[0].open("r", encoding="utf-8") as readback:
            readback_rows = sum(1 for _ in readback)
        if readback_rows != window_rows:
            raise SegmentError("output NDJSON readback row count does not match conversion count")
        manifest = {
            "schema": "phoenix.tnevent_v2_ws_replay.v1",
            "converter_version": "1",
            "format_contract": {
                "file": "TNEVENT NUL, big-endian version 2, schema 1, reserved byte 0",
                "first_block": "marker 0, u16 compressed length, u32 raw length, u32 CRC-32, zlib bytes",
                "continuation_block": "marker 1, u32 compressed length, u32 raw length, u32 CRC-32, zlib bytes",
                "record": "u32 remaining length after first 8 bytes, opaque u32, u32 metadata length, metadata JSON, raw JSON, LF",
                "validation_basis": "observed and verified against each finalized input header, block CRC, zlib length, record count, and SHA-256 manifest",
            },
            "source_segments": [
                {
                    "path": str(segment.directory),
                    "segment_id": segment.segment_id,
                    "sha256": segment.sha256,
                    "file_bytes": segment.file_bytes,
                    "event_count": segment.event_count,
                    "created_at": segment.created_at,
                    "finalized_at": segment.finalized_at,
                }
                for segment in segments
            ],
            "source_event_count_verified": source_rows,
            "window": {
                "start_utc_inclusive": start.isoformat().replace("+00:00", "Z"),
                "end_utc_exclusive": end.isoformat().replace("+00:00", "Z"),
                "target_market": target_market,
                "target_first_received_utc": target_first.isoformat().replace("+00:00", "Z"),
                "target_last_received_utc": target_last.isoformat().replace("+00:00", "Z"),
                "target_event_types": dict(sorted(target_types.items())),
                "target_rows_by_utc_minute": dict(sorted(target_minutes.items())),
            },
            "websocket_replay": {
                "frames_in_window": window_rows,
                "readback_rows_verified": readback_rows,
                "frame_types": dict(sorted(ws_types.items())),
                "source_connection_generations": {
                    str(generation): identity
                    for generation, identity in sorted(connection_by_generation.items())
                },
                "sequence_gaps_by_connection_sid_full_segments": dict(sorted(full_seq_gaps.items())),
                "duplicate_frames_by_connection_sid_full_segments": dict(sorted(full_seq_duplicates.items())),
                "sequence_regressions_by_connection_sid_full_segments": dict(sorted(full_seq_regressions.items())),
                "sequence_gaps_in_window": dict(sorted(window_seq_gaps.items())),
                "duplicate_frames_in_window": dict(sorted(window_seq_duplicates.items())),
                "sequence_regressions_in_window": dict(sorted(window_seq_regressions.items())),
            },
            "excluded_non_websocket_records": {
                "count": sum(non_ws_types.values()),
                "types": dict(sorted(non_ws_types.items())),
                "preserved_discovery_path": str(discovery_path),
                "discovery_rows": discovery_rows,
            },
            "outputs": {
                "ndjson": {
                    "path": str(output),
                    "sha256": output_sha,
                    "readbackRowsVerified": readback_rows,
                },
                "provenance": {"path": str(provenance_path), "sha256": provenance_sha, "rows": window_rows},
                "discovery": {"path": str(discovery_path), "sha256": discovery_sha, "rows": discovery_rows},
            },
        }
        with partials[3].open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        # Publish data and provenance first; the manifest is the completion marker.
        os.replace(partials[1], discovery_path)
        os.replace(partials[2], provenance_path)
        os.replace(partials[0], output)
        os.replace(partials[3], manifest_path)
        return manifest
    except Exception:
        for partial in partials:
            try:
                partial.unlink(missing_ok=True)
            except OSError:
                pass
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", action="append", required=True, type=Path, help="finalized .ready directory; repeat in source order")
    parser.add_argument("--output", required=True, type=Path, help="new staging NDJSON path")
    parser.add_argument("--start-utc", required=True)
    parser.add_argument("--end-utc", required=True, help="exclusive end time")
    parser.add_argument("--target-market", required=True)
    parser.add_argument("--max-source-events", type=int, default=500_000)
    args = parser.parse_args()
    try:
        result = convert_segments(
            args.input,
            args.output,
            start_utc=args.start_utc,
            end_utc=args.end_utc,
            target_market=args.target_market,
            max_source_events=args.max_source_events,
        )
    except Exception as error:
        parser.exit(2, f"conversion failed closed: {error}\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
