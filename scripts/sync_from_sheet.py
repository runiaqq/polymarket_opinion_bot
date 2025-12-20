#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
import json

from utils.config_loader import ConfigLoader
from utils.google_sheets import GoogleSheetsClient, parse_sheet_pairs
from utils.logger import BotLogger


async def run(once: bool) -> None:
    loader = ConfigLoader()
    settings = loader.load_settings()
    if not settings.google_sheets.enabled:
        raise SystemExit("google_sheets integration is disabled in settings.yaml")
    logger = BotLogger("sheet_sync")
    client = GoogleSheetsClient(settings.google_sheets, logger=logger)
    try:
        specs = await client.fetch_specs()
        output = {
            "pair_count": len(specs),
            "pairs": [
                {
                    "pair_id": spec.pair_cfg.event_id,
                    "primary": spec.pair_cfg.primary_market_id,
                    "secondary": spec.pair_cfg.secondary_market_id,
                    "size_limit": spec.size_limit,
                }
                for spec in specs.values()
            ],
        }
        print(json.dumps(output, indent=2))
    finally:
        await client.close()
    if not once:
        print("sync_from_sheet.py only supports --once mode currently.")


def main() -> None:
    parser = argparse.ArgumentParser(description="One-off Google Sheets sync for market pairs")
    parser.add_argument("--once", action="store_true", help="Fetch once and print results")
    args = parser.parse_args()
    if not args.once:
        parser.error("--once flag is required")
    asyncio.run(run(once=True))


if __name__ == "__main__":
    main()



