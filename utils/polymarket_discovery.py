from __future__ import annotations

import asyncio
import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp
from difflib import SequenceMatcher
import json as pyjson

DEFAULT_GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"


def normalize_title(title: str) -> str:
    return " ".join((title or "").lower().split())


def slugify(value: str) -> str:
    """Simple slug builder for event ids and titles."""
    normalized = normalize_title(value)
    return normalized.replace(" ", "-").replace("/", "-")


def parse_slug_from_url(url_or_slug: str) -> str:
    """
    Accepts a full Polymarket URL or plain slug and returns the slug portion.
    Examples:
      https://polymarket.com/event/fed-decision-in-january?tid=... -> fed-decision-in-january
      fed-decision-in-january -> fed-decision-in-january
    """
    if not url_or_slug:
        return ""
    if "://" not in url_or_slug:
        return url_or_slug.split("?")[0].split("#")[0]
    parsed = urlparse(url_or_slug)
    parts = parsed.path.strip("/").split("/")
    if "event" in parts:
        idx = parts.index("event")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    if parts:
        return parts[-1].split("?")[0].split("#")[0]
    return ""


def _market_to_row(payload: Dict[str, Any]) -> Dict[str, Any]:
    token_ids = extract_token_ids(payload)
    outcomes = payload.get("outcomes") or payload.get("outcomeNames") or payload.get("outcome_ids") or ""
    clob_tokens = payload.get("clobTokenIds") or payload.get("clob_token_ids") or []
    return {
        "id": str(payload.get("id") or payload.get("market_id") or payload.get("marketId")),
        "slug": payload.get("slug") or payload.get("marketSlug") or payload.get("market_slug") or "",
        "condition_id": payload.get("conditionId") or payload.get("condition_id") or "",
        "question": payload.get("question") or payload.get("title") or payload.get("name") or payload.get("marketTitle") or "",
        "active": payload.get("active"),
        "closed": payload.get("closed"),
        "accepting_orders": payload.get("acceptingOrders") or payload.get("enableOrderBook"),
        "primary_token_id": token_ids[0] if token_ids else "",
        "created_at": payload.get("createdAt") or payload.get("startDate"),
        "end_date": payload.get("endDate") or payload.get("endDateIso"),
        "outcomes": json.dumps(outcomes, ensure_ascii=False) if not isinstance(outcomes, str) else outcomes,
        "clob_token_ids": json.dumps(clob_tokens) if not isinstance(clob_tokens, str) else clob_tokens,
        "normalized_title": normalize_title(payload.get("question") or payload.get("title") or payload.get("name") or ""),
    }


async def fetch_markets_page(
    session: aiohttp.ClientSession,
    offset: int = 0,
    limit: int = 100,
    proxy: Optional[str] = None,
    base_url: str = DEFAULT_GAMMA_URL,
    extra_params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    url = f"{base_url}/markets"
    params = {"offset": offset, "limit": limit}
    if extra_params:
        params.update(extra_params)
    async with session.get(url, params=params, proxy=proxy, timeout=30) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"polymarket markets failed ({resp.status}): {text[:200]}")
        try:
            payload = await resp.json()
        except Exception:
            text = await resp.text()
            raise RuntimeError(f"polymarket markets non-json: {text[:200]}")
    if isinstance(payload, dict):
        if "markets" in payload:
            return payload.get("markets", [])
        if "data" in payload:
            return payload.get("data", [])
    if isinstance(payload, list):
        return payload
    return []


async def paginate_markets(
    session: aiohttp.ClientSession,
    proxy: Optional[str] = None,
    base_url: str = DEFAULT_GAMMA_URL,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    extra_params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    offset = 0
    collected: List[Dict[str, Any]] = []
    page = 0
    while True:
        page += 1
        markets = await fetch_markets_page(
            session, offset=offset, limit=page_size, proxy=proxy, base_url=base_url, extra_params=extra_params
        )
        if not markets:
            break
        collected.extend(markets)
        offset += page_size
        if max_pages and page >= max_pages:
            break
        if len(markets) < page_size:
            break
    return collected


async def fetch_clob_markets_page(
    session: aiohttp.ClientSession,
    offset: int = 0,
    limit: int = 100,
    proxy: Optional[str] = None,
    base_url: str = CLOB_URL,
    extra_params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch a single page of CLOB markets."""
    url = f"{base_url}/markets"
    params = {"offset": offset, "limit": limit}
    if extra_params:
        params.update(extra_params)
    async with session.get(url, params=params, proxy=proxy, timeout=30) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"clob markets failed ({resp.status}): {text[:200]}")
        try:
            payload = await resp.json()
        except Exception:
            text = await resp.text()
            raise RuntimeError(f"clob markets non-json: {text[:200]}")
    if isinstance(payload, dict):
        if "markets" in payload:
            return payload.get("markets", [])
        if "data" in payload:
            return payload.get("data", [])
    if isinstance(payload, list):
        return payload
    return []


async def paginate_clob_markets(
    session: aiohttp.ClientSession,
    proxy: Optional[str] = None,
    base_url: str = CLOB_URL,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    extra_params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Iterate through all CLOB markets, following the reference pagination approach."""
    offset = 0
    collected: List[Dict[str, Any]] = []
    page = 0
    while True:
        page += 1
        markets = await fetch_clob_markets_page(
            session, offset=offset, limit=page_size, proxy=proxy, base_url=base_url, extra_params=extra_params
        )
        if not markets:
            break
        collected.extend(markets)
        offset += page_size
        if max_pages and page >= max_pages:
            break
        if len(markets) < page_size:
            break
    return collected


def extract_token_ids(market: Dict[str, Any]) -> List[str]:
    tokens: List[str] = []
    raw_clob_ids = market.get("clobTokenIds") or market.get("clob_token_ids")
    if isinstance(raw_clob_ids, str):
        try:
            raw_clob_ids = pyjson.loads(raw_clob_ids)
        except Exception:
            raw_clob_ids = []
    if isinstance(raw_clob_ids, (list, tuple)):
        tokens.extend([str(t) for t in raw_clob_ids if t])
    token_objs = market.get("tokens") or []
    for tok in token_objs:
        tok_id = tok.get("token_id") or tok.get("id")
        if tok_id:
            tokens.append(str(tok_id))
    return tokens


def extract_primary_token_id(market: Dict[str, Any]) -> Optional[str]:
    tokens = extract_token_ids(market)
    return tokens[0] if tokens else None


async def check_clob_orderbook(
    session: aiohttp.ClientSession,
    market_token_id: str,
    proxy: Optional[str] = None,
    base_url: str = CLOB_URL,
) -> Tuple[bool, int]:
    """
    Return True if any Polymarket orderbook endpoint responds with 200.

    We check both `/markets/{id}/orderbook` (legacy path used by the bot)
    and the documented `/book?token_id=` endpoint.
    """
    candidates = [
        (f"{base_url}/markets/{market_token_id}/orderbook", None),
        (f"{base_url}/book", {"token_id": market_token_id}),
    ]
    last_status = 0
    for url, params in candidates:
        async with session.get(url, params=params, proxy=proxy, timeout=15) as resp:
            last_status = resp.status
            if resp.status == 200:
                return True, resp.status
            try:
                await resp.text()
            except Exception:
                pass
    return False, last_status


async def fetch_valid_clob_markets(
    session: aiohttp.ClientSession,
    proxy: Optional[str] = None,
    base_url: str = CLOB_URL,
    orderbook_base_url: str = CLOB_URL,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    concurrency: int = 10,
    source: str = "clob",
    query_params: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], List[Tuple[Dict[str, Any], int]]]:
    """
    Fetch all CLOB markets and filter out any whose /orderbook returns non-200.

    Returns (valid_markets, excluded_with_status).
    """
    if source == "gamma":
        markets = await paginate_markets(
            session=session,
            proxy=proxy,
            base_url=base_url,
            page_size=page_size,
            max_pages=max_pages,
            extra_params=query_params,
        )
    else:
        markets = await paginate_clob_markets(
            session=session,
            proxy=proxy,
            base_url=base_url,
            page_size=page_size,
            max_pages=max_pages,
            extra_params=query_params,
        )
    valid: List[Dict[str, Any]] = []
    excluded: List[Tuple[Dict[str, Any], int]] = []
    sem = asyncio.Semaphore(concurrency)

    async def validate(market: Dict[str, Any]):
        market_id = extract_primary_token_id(market)
        if not market_id:
            excluded.append((market, -1))
            return
        async with sem:
            ok, status = await check_clob_orderbook(session, market_id, proxy=proxy, base_url=orderbook_base_url)
        if ok:
            market["validated_token_id"] = market_id
            valid.append(market)
        else:
            excluded.append((market, status))

    await asyncio.gather(*(validate(m) for m in markets))
    return valid, excluded


def write_csv(markets: Iterable[Dict[str, Any]], path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [_market_to_row(m) for m in markets]
    if not rows:
        return path
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def load_cache(path: str | Path) -> List[Dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader]


def resolve_market(cache: List[Dict[str, Any]], slug: Optional[str] = None, market_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    slug_norm = slug or ""
    slug_norm = slug_norm.strip()
    if market_id:
        for row in cache:
            if str(row.get("id")) == str(market_id):
                return row
    if slug_norm:
        for row in cache:
            if row.get("slug") == slug_norm:
                return row
            if row.get("normalized_title") == normalize_title(slug_norm):
                return row
    return None


def build_yaml_snippet(
    event_id: str,
    opinion_market_id: str,
    polymarket_market_id: str,
    contract_type: str = "BINARY",
    strategy_direction: str = "AUTO",
) -> str:
    return (
        f"- event_id: \"{event_id}\"\n"
        f"  primary_market_id: \"{opinion_market_id}\"\n"
        f"  secondary_market_id: \"{polymarket_market_id}\"\n"
        f"  contract_type: \"{contract_type}\"\n"
        f"  strategy_direction: \"{strategy_direction}\"\n"
    )


def score_title_match(opinion_title: str, polymarket_title: str) -> float:
    """Heuristic similarity score for mapping Opinion ↔ Polymarket markets."""
    op_norm = normalize_title(opinion_title)
    pm_norm = normalize_title(polymarket_title)
    base = SequenceMatcher(None, op_norm, pm_norm).ratio()
    keywords = ["fed", "rate", "rates", "bps", "inflation", "cpi", "unemployment", "gdp"]
    bonus = 0.0
    for kw in keywords:
        if kw in op_norm and kw in pm_norm:
            bonus += 0.05
    return min(1.0, base + bonus)




