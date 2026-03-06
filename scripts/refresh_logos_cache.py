#!/usr/bin/env python3
"""Refresh institution logo cache independently of scrape ingestion.

Usage (inside app container):
    python scripts/refresh_logos_cache.py

Force refresh existing logos:
    python scripts/refresh_logos_cache.py --force
"""

from __future__ import annotations

import argparse

from app import app
from json_fetcher import refresh_logo_cache


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh cached institution logos")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch logos even when cache files already exist",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    def progress(_event_type, data):
        msg = data.get("message")
        if msg:
            print(msg)

    result = refresh_logo_cache(app, progress_callback=progress, force=args.force)
    print(
        "Logo refresh complete:",
        f"total={result['total']}",
        f"saved={result['saved']}",
        f"skipped={result['skipped']}",
        f"errors={result['errors']}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
