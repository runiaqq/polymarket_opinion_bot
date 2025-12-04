from __future__ import annotations

import argparse
import asyncio
from typing import Optional

from aiohttp import web

from utils.config_loader import ConfigLoader
from utils.google_sheets import GoogleSheetsSync, MarketPairStore
from utils.logger import BotLogger


def _pair_to_dict(pair):
    return {
        "event_id": pair.event_id,
        "primary_market_id": pair.primary_market_id,
        "secondary_market_id": pair.secondary_market_id,
        "primary_account_id": pair.primary_account_id,
        "secondary_account_id": pair.secondary_account_id,
        "strategy": pair.strategy,
    }


async def create_app(
    pair_store: MarketPairStore,
    syncer: Optional[GoogleSheetsSync],
    admin_token: str,
) -> web.Application:
    app = web.Application()
    app["pair_store"] = pair_store
    app["syncer"] = syncer
    app["admin_token"] = admin_token

    async def require_token(request: web.Request):
        token = request.headers.get("X-ADMIN-TOKEN")
        if not admin_token or token != admin_token:
            raise web.HTTPUnauthorized(text="missing or invalid admin token")

    async def list_pairs(request: web.Request):
        await require_token(request)
        store: MarketPairStore = app["pair_store"]
        pairs = await store.list_pairs()
        payload = [_pair_to_dict(pair) for pair in pairs]
        return web.json_response({"pairs": payload, "count": len(payload)})

    async def trigger_sync(request: web.Request):
        await require_token(request)
        syncer: Optional[GoogleSheetsSync] = app["syncer"]
        if not syncer:
            raise web.HTTPBadRequest(text="google_sheets sync disabled")
        result = await syncer.sync(app["pair_store"])
        return web.json_response(result)

    app.router.add_get("/pairs", list_pairs)
    app.router.add_post("/sync", trigger_sync)
    return app


async def _run_server(host: str, port: int, app: web.Application):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner


async def main_async(args) -> None:
    loader = ConfigLoader()
    settings = loader.load_settings()
    logger = BotLogger("webhook")
    store = MarketPairStore(settings.market_pairs)
    syncer = GoogleSheetsSync(settings.google_sheets, logger) if settings.google_sheets.enabled else None
    app = await create_app(store, syncer, args.token or settings.webhook.admin_token)
    runner = await _run_server(args.host or settings.webhook.host, args.port or settings.webhook.port, app)
    logger.info("webhook server started", host=args.host or settings.webhook.host, port=args.port or settings.webhook.port)
    stop = asyncio.Event()
    try:
        await stop.wait()
    finally:
        await runner.cleanup()
        if syncer:
            await syncer.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Webhook server for manual Google Sheets sync")
    parser.add_argument("--host", help="Host to bind")
    parser.add_argument("--port", type=int, help="Port to bind")
    parser.add_argument("--token", help="Admin token override")
    return parser.parse_args()


def main():
    asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    main()

