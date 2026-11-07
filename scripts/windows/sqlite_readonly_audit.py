from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: sqlite_readonly_audit.py LEDGER_PATH")
    path = Path(sys.argv[1]).resolve()
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)
    try:
        event = connection.execute(
            "SELECT rowid, received_utc FROM market_events ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        paper = connection.execute(
            "SELECT rowid, created_utc FROM paper_ledger ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        paper_count = connection.execute("SELECT count(*) FROM paper_ledger").fetchone()[0]
        main = next(row for row in connection.execute("PRAGMA database_list") if row[1] == "main")
        print(
            json.dumps(
                {
                    "sqlite_open_path": main[2],
                    "latest_event_rowid": event[0] if event else None,
                    "latest_event_utc": event[1] if event else None,
                    "latest_paper_rowid": paper[0] if paper else None,
                    "latest_paper_utc": paper[1] if paper else None,
                    "paper_ledger_rows": paper_count,
                }
            )
        )
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
