from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Dict, List, Optional

import aiohttp

from core.models import ContractType, ExchangeName, StrategyDirection
from utils.config_loader import GoogleSheetsConfig, MarketPairConfig
from utils.logger import BotLogger


@dataclass(slots=True)
class SheetPairSpec:
    """Representation of a Sheet-sourced market pair."""

    pair_cfg: MarketPairConfig
    size_limit: float | None
    fingerprint: str


class MarketPairStore:
    """Thread-safe container tracking currently active market pairs."""

    def __init__(self, initial: Optional[List[MarketPairConfig]] = None):
        self._pairs: Dict[str, MarketPairConfig] = {}
        self._lock = asyncio.Lock()
        if initial:
            for pair in initial:
                self._pairs[self._pair_id(pair)] = pair

    def _pair_id(self, pair: MarketPairConfig) -> str:
        if pair.pair_id:
            return pair.pair_id
        return pair.event_id or f"{pair.primary_market_id}:{pair.secondary_market_id}"

    async def list_pairs(self) -> List[MarketPairConfig]:
        async with self._lock:
            return list(self._pairs.values())

    async def update_pairs(self, pairs: List[MarketPairConfig]) -> Dict[str, int]:
        new_map = {self._pair_id(pair): pair for pair in pairs}
        async with self._lock:
            old_keys = set(self._pairs.keys())
            new_keys = set(new_map.keys())
            added = len(new_keys - old_keys)
            removed = len(old_keys - new_keys)
            self._pairs = new_map
        return {"added": added, "removed": removed, "total": len(new_map)}


class GoogleSheetsClient:
    """
    Lightweight client that fetches raw rows and parses them into SheetPairSpec
    structures consumable by PairController and scripts.
    """

    def __init__(
        self,
        config: GoogleSheetsConfig,
        logger: BotLogger | None = None,
        session: aiohttp.ClientSession | None = None,
    ):
        self.sync = GoogleSheetsSync(config, logger=logger, session=session)

    async def fetch_rows(self) -> List[List[str]]:
        return await self.sync._fetch_rows()  # noqa: SLF001 - internal reuse is intentional

    async def fetch_specs(self) -> Dict[str, SheetPairSpec]:
        rows = await self.fetch_rows()
        return parse_sheet_pairs(rows)

    async def close(self) -> None:
        await self.sync.close()


class GoogleSheetsSync:
    """Fetches market pair definitions from Google Sheets."""

    def __init__(
        self,
        config: GoogleSheetsConfig,
        logger: BotLogger | None = None,
        session: aiohttp.ClientSession | None = None,
    ):
        self.config = config
        self.logger = logger or BotLogger(__name__)
        self._session = session
        self._session_owner = session is None

    async def close(self) -> None:
        if self._session_owner and self._session:
            await self._session.close()

    async def fetch_pairs(self) -> List[MarketPairConfig]:
        values = await self._fetch_rows()
        return self._parse_pairs(values)

    async def sync(self, store: MarketPairStore) -> Dict[str, int]:
        if not self.config.enabled:
            raise RuntimeError("google sheets sync is disabled")
        pairs = await self.fetch_pairs()
        result = await store.update_pairs(pairs)
        self.logger.info(
            "google sheets sync completed",
            added=result["added"],
            removed=result["removed"],
            total=result["total"],
        )
        return result

    async def _fetch_rows(self) -> List[List[str]]:
        if not self.config.sheet_id or not self.config.range:
            raise ValueError("google_sheets requires sheet_id and range")
        session = self._session
        if session is None:
            session = aiohttp.ClientSession()
            self._session = session
            self._session_owner = True
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{self.config.sheet_id}/values/{self.config.range}"
        headers: Dict[str, str] = {}
        params: Dict[str, str] = {}
        if self.config.mode == "api_key":
            if not self.config.api_key:
                raise ValueError("google_sheets.api_key required for api_key mode")
            params["key"] = self.config.api_key
        else:
            token = await asyncio.get_event_loop().run_in_executor(None, self._load_service_account_token)
            headers["Authorization"] = f"Bearer {token}"
        async with session.get(url, headers=headers, params=params, timeout=30) as response:
            if response.status != 200:
                text = await response.text()
                raise RuntimeError(f"google sheets fetch failed ({response.status}): {text}")
            payload = await response.json()
            return payload.get("values", [])

    def _load_service_account_token(self) -> str:
        if not self.config.credentials_path:
            raise ValueError("google_sheets.credentials_path required for service_account mode")
        try:
            from google.oauth2.service_account import Credentials
            from google.auth.transport.requests import Request
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("google-auth library required for service_account mode") from exc
        with open(self.config.credentials_path, "r", encoding="utf-8") as handle:
            info = json.load(handle)
        creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
        request = Request()
        creds.refresh(request)
        if not creds.token:
            raise RuntimeError("failed to obtain Google Sheets token")
        return creds.token

    def _parse_pairs(self, values: List[List[str]]) -> List[MarketPairConfig]:
        if not values:
            return []
        headers = [_normalize_header(cell) for cell in values[0]]
        pairs: List[MarketPairConfig] = []
        for row_values in values[1:]:
            row = {headers[idx]: row_values[idx] for idx in range(min(len(headers), len(row_values)))}
            pair = _row_to_pair(row)
            if pair:
                pairs.append(pair)
        return pairs


def parse_sheet_pairs(values: List[List[str]]) -> Dict[str, SheetPairSpec]:
    """
    Parse a two-column (Polymarket, Opinion) sheet into SheetPairSpec entries.
    Accepts optional headers like `size_limit` to cap per-market exposure.
    """
    if not values:
        return {}
    headers = [_normalize_header(cell) for cell in values[0]]
    specs: Dict[str, SheetPairSpec] = {}
    for row_values in values[1:]:
        row = {headers[idx]: row_values[idx] for idx in range(min(len(headers), len(row_values)))}
        primary_market = row.get("polymarket") or row.get("primary_market_id") or row.get("marketa_url")
        secondary_market = row.get("opinion") or row.get("secondary_market_id") or row.get("marketb_url")
        if not primary_market or not secondary_market:
            continue
        event_id = row.get("event_id") or row.get("pair_id") or f"{primary_market}:{secondary_market}"
        size_raw = row.get("size_limit") or row.get("max_position_size_per_market")
        try:
            size_limit = float(size_raw) if size_raw not in (None, "", "None") else None
        except (TypeError, ValueError):
            size_limit = None
        contract_raw = str(row.get("contract_type", ContractType.BINARY.value)).upper()
        strategy_dir_raw = str(row.get("strategy_direction", StrategyDirection.AUTO.value)).upper()
        try:
            contract_type = ContractType(contract_raw)
        except ValueError:
            contract_type = ContractType.BINARY
        try:
            strategy_direction = StrategyDirection(strategy_dir_raw)
        except ValueError:
            strategy_direction = StrategyDirection.AUTO
        pair_cfg = MarketPairConfig(
            event_id=event_id,
            primary_market_id=primary_market,
            secondary_market_id=secondary_market,
            primary_account_id=row.get("primary_account_id"),
            secondary_account_id=row.get("secondary_account_id"),
            pair_id=row.get("pair_id") or event_id,
            strategy=row.get("strategy"),
            max_position_size_per_market=size_limit,
            primary_exchange=ExchangeName.POLYMARKET,
            secondary_exchange=ExchangeName.OPINION,
            contract_type=contract_type,
            strategy_direction=strategy_direction,
        )
        fingerprint = _fingerprint(pair_cfg, size_limit)
        specs[event_id] = SheetPairSpec(pair_cfg=pair_cfg, size_limit=size_limit, fingerprint=fingerprint)
    return specs


def _row_to_pair(row: Dict[str, str]) -> Optional[MarketPairConfig]:
    primary_market = row.get("primary_market_id") or row.get("marketa_url")
    secondary_market = row.get("secondary_market_id") or row.get("marketb_url")
    if not primary_market or not secondary_market:
        return None
    event_id = row.get("event_id") or row.get("pair_id") or f"{primary_market}:{secondary_market}"
    primary_exchange = _parse_exchange(row.get("primary_exchange"))
    secondary_exchange = _parse_exchange(row.get("secondary_exchange"))
    max_size_raw = row.get("max_position_size_per_market") or row.get("size_limit")
    pair = MarketPairConfig(
        event_id=event_id,
        primary_market_id=primary_market,
        secondary_market_id=secondary_market,
        primary_account_id=row.get("primary_account_id"),
        secondary_account_id=row.get("secondary_account_id"),
        pair_id=row.get("pair_id") or event_id,
        strategy=row.get("strategy"),
        max_position_size_per_market=float(max_size_raw) if max_size_raw else None,
        primary_exchange=primary_exchange,
        secondary_exchange=secondary_exchange,
    )
    return pair


def _parse_exchange(value: Optional[str]) -> Optional[ExchangeName]:
    if not value:
        return None
    try:
        return ExchangeName(value)
    except ValueError:
        return None


def _normalize_header(header: str) -> str:
    return header.strip().lower().replace(" ", "_")


def _fingerprint(pair_cfg: MarketPairConfig, size_override: Optional[float]) -> str:
    payload = {
        "primary": pair_cfg.primary_market_id,
        "secondary": pair_cfg.secondary_market_id,
        "primary_exchange": pair_cfg.primary_exchange.value if pair_cfg.primary_exchange else "",
        "secondary_exchange": pair_cfg.secondary_exchange.value if pair_cfg.secondary_exchange else "",
        "size": size_override,
        "contract_type": pair_cfg.contract_type.value if pair_cfg.contract_type else "",
        "strategy_direction": pair_cfg.strategy_direction.value if pair_cfg.strategy_direction else "",
    }
    return json.dumps(payload, sort_keys=True)

