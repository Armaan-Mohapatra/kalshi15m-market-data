from __future__ import annotations

import argparse
import asyncio
import json

from .api import PublicKalshiAPI
from .config import settings
from .discovery import discover


async def _discover() -> int:
    snapshot = await discover(PublicKalshiAPI(), settings.home / "reports")
    print(
        json.dumps(
            {
                "snapshot_utc": snapshot["snapshot_utc"],
                "series_api_rows": snapshot["series_api_rows"],
                "fifteen_min_series": snapshot["fifteen_min_series"],
                "active_15m_market_rows": snapshot["active_15m_market_rows"],
                "market_event_rows": snapshot["market_event_rows"],
                "api_requests": snapshot["api_requests"],
                "api_errors": snapshot["api_errors"],
                "artifact_paths": snapshot.get("artifact_paths"),
            },
            indent=2,
        )
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="kalshi15m")
    parser.add_argument("command", choices=("discover", "worker"))
    args = parser.parse_args()
    if args.command == "discover":
        raise SystemExit(asyncio.run(_discover()))
    from .worker import main as worker_main

    worker_main()


if __name__ == "__main__":
    main()
