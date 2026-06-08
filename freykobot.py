#!/usr/bin/env python3
"""FreykoBot: momentum + Polymarket repricing lag + hedge inventory.

The bot is intentionally standalone and dry-run first. It consumes fast exchange
ticks, watches the Polymarket CLOB, builds two-sided inventory, and records
every decision for calibration before any live trading is enabled.
"""

from __future__ import annotations

import asyncio
import csv
import json
import math
import os
import time
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    import websockets
except ImportError:  # pragma: no cover
    websockets = None

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

try:
    from telegram_notify import TelegramNotifier
except ImportError:  # pragma: no cover
    TelegramNotifier = None


FAMILY_CONFIG = {
    "btc5m": {"prefix": "btc-updown-5m", "period": 300, "symbol": "BTCUSDT", "coinbase": "BTC-USD"},
    "eth5m": {"prefix": "eth-updown-5m", "period": 300, "symbol": "ETHUSDT", "coinbase": "ETH-USD"},
    "btc15m": {"prefix": "btc-updown-15m", "period": 900, "symbol": "BTCUSDT", "coinbase": "BTC-USD"},
    "eth15m": {"prefix": "eth-updown-15m", "period": 900, "symbol": "ETHUSDT", "coinbase": "ETH-USD"},
}

BINANCE_TO_COINBASE = {"BTCUSDT": "BTC-USD", "ETHUSDT": "ETH-USD"}
COINBASE_TO_BINANCE = {v: k for k, v in BINANCE_TO_COINBASE.items()}


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except ValueError:
        return default


def utc_iso(ts: Optional[float] = None) -> str:
    if ts is None:
        ts = time.time()
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def exchange_ts_to_seconds(value: Any) -> float:
    ts = as_float(value, 0.0)
    if ts <= 0:
        return 0.0
    if ts > 1_000_000_000_000_000:
        return ts / 1_000_000.0
    if ts > 1_000_000_000_000:
        return ts / 1000.0
    return ts


def parse_coinbase_time(value: str) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def current_window_start(period_seconds: int, now: Optional[int] = None) -> int:
    if now is None:
        now = int(time.time())
    return now - (now % period_seconds)


def parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def http_json(
    url: str,
    params: Optional[dict[str, Any]] = None,
    timeout: float = 5.0,
) -> tuple[Any, float]:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json,text/plain,*/*", "User-Agent": "FreykoBot/0.1"},
    )
    started = time.perf_counter()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    elapsed_ms = (time.perf_counter() - started) * 1000
    if not raw.strip():
        return {}, elapsed_ms
    try:
        return json.loads(raw), elapsed_ms
    except json.JSONDecodeError:
        return raw, elapsed_ms


def ensure_csv(path: Path, fieldnames: list[str]) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writeheader()


def append_csv(path: Path, fieldnames: list[str], row: dict[str, Any]) -> None:
    ensure_csv(path, fieldnames)
    with path.open("a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore").writerow(row)


@dataclass
class Config:
    dry_run: bool = True
    allow_live: bool = False
    price_primary: str = "binance"
    families: list[str] = field(default_factory=lambda: ["btc5m", "eth5m", "btc15m"])
    poll_seconds: float = 0.25
    http_timeout: float = 4.0
    max_loops: int = 0
    log_dir: Path = Path("logs")

    binance_ws_url: str = (
        "wss://data-stream.binance.vision/stream?"
        "streams=btcusdt@trade/ethusdt@trade&timeUnit=MICROSECOND"
    )
    coinbase_enabled: bool = True
    coinbase_ws_url: str = "wss://ws-feed.exchange.coinbase.com"
    max_price_lag_ms: float = 700.0

    gamma_api_url: str = "https://gamma-api.polymarket.com"
    clob_api_url: str = "https://clob.polymarket.com"
    poly_ws_enabled: bool = True
    poly_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    rest_book_fallback_seconds: float = 2.0

    lot_shares: int = 10
    max_market_cost: float = 160.0
    max_side_cost: float = 110.0
    max_daily_loss: float = 150.0
    max_buy_price: float = 0.92
    cooldown_seconds: float = 3.0

    min_momentum_5: float = 0.010
    min_momentum_10: float = 0.015
    momentum_to_prob: float = 6.0
    min_edge: float = 0.035
    strong_momentum: float = 0.040

    pair_arb_max: float = 0.99
    hedge_pair_max: float = 1.03
    hedge_max_price: float = 0.38
    hedge_target_cost_ratio: float = 0.58
    avoid_pair_sum_gt: float = 1.14

    entry_start_5m: float = 285.0
    entry_start_15m: float = 840.0
    entry_end_seconds: float = 7.0
    resolve_delay_seconds: float = 2.0
    require_open_seen: bool = True
    open_capture_grace_seconds: float = 6.0

    @classmethod
    def from_env(cls) -> "Config":
        families = [
            f.strip().lower()
            for f in os.getenv("FREYKO_FAMILIES", "btc5m,eth5m,btc15m").split(",")
            if f.strip().lower() in FAMILY_CONFIG
        ]
        return cls(
            dry_run=env_bool("DRY_RUN", True),
            allow_live=env_bool("FREYKO_ALLOW_LIVE", False),
            price_primary=os.getenv("PRICE_PRIMARY", "binance").strip().lower(),
            families=families or ["btc5m", "eth5m", "btc15m"],
            poll_seconds=env_float("FREYKO_POLL_SECONDS", 0.25),
            http_timeout=env_float("FREYKO_HTTP_TIMEOUT", 4.0),
            max_loops=env_int("FREYKO_MAX_LOOPS", 0),
            log_dir=Path(os.getenv("FREYKO_LOG_DIR", "logs")),
            binance_ws_url=os.getenv(
                "BINANCE_WS_URL",
                "wss://data-stream.binance.vision/stream?streams=btcusdt@trade/ethusdt@trade&timeUnit=MICROSECOND",
            ),
            coinbase_enabled=env_bool("COINBASE_ENABLED", True),
            coinbase_ws_url=os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com"),
            max_price_lag_ms=env_float("MAX_PRICE_LAG_MS", 700.0),
            gamma_api_url=os.getenv("GAMMA_API_URL", "https://gamma-api.polymarket.com"),
            clob_api_url=os.getenv("CLOB_API_URL", "https://clob.polymarket.com"),
            poly_ws_enabled=env_bool("POLY_WS_ENABLED", True),
            poly_ws_url=os.getenv("POLY_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
            rest_book_fallback_seconds=env_float("REST_BOOK_FALLBACK_SECONDS", 2.0),
            lot_shares=env_int("FREYKO_LOT_SHARES", 10),
            max_market_cost=env_float("FREYKO_MAX_MARKET_COST", 160.0),
            max_side_cost=env_float("FREYKO_MAX_SIDE_COST", 110.0),
            max_daily_loss=env_float("FREYKO_DAILY_LOSS_LIMIT", 150.0),
            max_buy_price=env_float("FREYKO_MAX_BUY_PRICE", 0.92),
            cooldown_seconds=env_float("FREYKO_COOLDOWN_SECONDS", 3.0),
            min_momentum_5=env_float("FREYKO_MIN_MOMENTUM_5", 0.010),
            min_momentum_10=env_float("FREYKO_MIN_MOMENTUM_10", 0.015),
            momentum_to_prob=env_float("FREYKO_MOMENTUM_TO_PROB", 6.0),
            min_edge=env_float("FREYKO_MIN_EDGE", 0.035),
            strong_momentum=env_float("FREYKO_STRONG_MOMENTUM", 0.040),
            pair_arb_max=env_float("FREYKO_PAIR_ARB_MAX", 0.99),
            hedge_pair_max=env_float("FREYKO_HEDGE_PAIR_MAX", 1.03),
            hedge_max_price=env_float("FREYKO_HEDGE_MAX_PRICE", 0.38),
            hedge_target_cost_ratio=env_float("FREYKO_HEDGE_TARGET_COST_RATIO", 0.58),
            avoid_pair_sum_gt=env_float("FREYKO_AVOID_PAIR_SUM_GT", 1.14),
            entry_start_5m=env_float("FREYKO_ENTRY_START_5M", 285.0),
            entry_start_15m=env_float("FREYKO_ENTRY_START_15M", 840.0),
            entry_end_seconds=env_float("FREYKO_ENTRY_END_SECONDS", 7.0),
            resolve_delay_seconds=env_float("FREYKO_RESOLVE_DELAY_SECONDS", 2.0),
            require_open_seen=env_bool("FREYKO_REQUIRE_OPEN_SEEN", True),
            open_capture_grace_seconds=env_float("FREYKO_OPEN_CAPTURE_GRACE_SECONDS", 6.0),
        )


@dataclass
class PriceTick:
    source: str
    symbol: str
    price: float
    exchange_ts: float
    local_ts: float

    @property
    def lag_ms(self) -> float:
        if self.exchange_ts <= 0:
            return 0.0
        return max(0.0, (self.local_ts - self.exchange_ts) * 1000)


class PriceStore:
    def __init__(self, max_age_seconds: float = 90.0) -> None:
        self.max_age_seconds = max_age_seconds
        self.history: dict[tuple[str, str], deque[PriceTick]] = {}
        self.latest: dict[tuple[str, str], PriceTick] = {}

    def update(self, tick: PriceTick) -> None:
        key = (tick.source, tick.symbol)
        hist = self.history.setdefault(key, deque())
        hist.append(tick)
        self.latest[key] = tick
        cutoff = tick.local_ts - self.max_age_seconds
        while hist and hist[0].local_ts < cutoff:
            hist.popleft()

    def tick(self, source: str, symbol: str) -> Optional[PriceTick]:
        return self.latest.get((source, symbol))

    def price(self, source: str, symbol: str) -> float:
        tick = self.tick(source, symbol)
        return tick.price if tick else 0.0

    def lag_ms(self, source: str, symbol: str) -> float:
        tick = self.tick(source, symbol)
        return tick.lag_ms if tick else 999_999.0

    def change_pct(self, source: str, symbol: str, seconds: float) -> float:
        key = (source, symbol)
        hist = self.history.get(key)
        latest = self.latest.get(key)
        if not hist or not latest:
            return 0.0
        target = latest.local_ts - seconds
        base = hist[0]
        for item in hist:
            if item.local_ts <= target:
                base = item
            else:
                break
        if base.price <= 0:
            return 0.0
        return ((latest.price - base.price) / base.price) * 100.0


@dataclass
class BookQuote:
    bid: float = 0.0
    ask: float = 0.0
    bid_size: float = 0.0
    ask_size: float = 0.0
    ts: float = 0.0
    source: str = ""

    @property
    def age_ms(self) -> float:
        if self.ts <= 0:
            return 999_999.0
        return max(0.0, (time.time() - self.ts) * 1000)


class BookStore:
    def __init__(self) -> None:
        self.quotes: dict[str, BookQuote] = {}
        self.wanted_assets: set[str] = set()

    def want(self, *asset_ids: str) -> None:
        for asset_id in asset_ids:
            if asset_id:
                self.wanted_assets.add(str(asset_id))

    def quote(self, asset_id: str) -> BookQuote:
        return self.quotes.get(str(asset_id), BookQuote())

    def update_quote(
        self,
        asset_id: str,
        bid: float,
        ask: float,
        bid_size: float = 0.0,
        ask_size: float = 0.0,
        source: str = "rest",
    ) -> None:
        if bid <= 0 and ask <= 0:
            return
        self.quotes[str(asset_id)] = BookQuote(bid, ask, bid_size, ask_size, time.time(), source)

    def apply_poly_message(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        messages = data if isinstance(data, list) else [data]
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            event_type = msg.get("event_type")
            if event_type == "book":
                asset_id = str(msg.get("asset_id", ""))
                bids = msg.get("bids") or []
                asks = msg.get("asks") or []
                bid_price, bid_size = self._best_bid(bids)
                ask_price, ask_size = self._best_ask(asks)
                self.update_quote(asset_id, bid_price, ask_price, bid_size, ask_size, "poly_ws")
            elif event_type == "best_bid_ask":
                asset_id = str(msg.get("asset_id", ""))
                self.update_quote(asset_id, as_float(msg.get("best_bid")), as_float(msg.get("best_ask")), source="poly_ws")
            elif event_type == "price_change":
                for change in msg.get("price_changes") or []:
                    asset_id = str(change.get("asset_id", ""))
                    old = self.quote(asset_id)
                    bid = as_float(change.get("best_bid"), old.bid)
                    ask = as_float(change.get("best_ask"), old.ask)
                    self.update_quote(asset_id, bid, ask, old.bid_size, old.ask_size, "poly_ws")

    @staticmethod
    def _best_bid(levels: list[dict[str, Any]]) -> tuple[float, float]:
        best_price = 0.0
        best_size = 0.0
        for level in levels:
            price = as_float(level.get("price"))
            if price > best_price:
                best_price = price
                best_size = as_float(level.get("size"))
        return best_price, best_size

    @staticmethod
    def _best_ask(levels: list[dict[str, Any]]) -> tuple[float, float]:
        best_price = 0.0
        best_size = 0.0
        for level in levels:
            price = as_float(level.get("price"))
            if price > 0 and (best_price <= 0 or price < best_price):
                best_price = price
                best_size = as_float(level.get("size"))
        return best_price, best_size


@dataclass
class MarketInfo:
    family: str
    slug: str
    symbol: str
    period_seconds: int
    window_start: int
    window_end: int
    up_token: str
    down_token: str
    title: str = ""
    condition_id: str = ""
    gamma_ms: float = 0.0

    @property
    def seconds_to_close(self) -> float:
        return self.window_end - time.time()


@dataclass
class Inventory:
    slug: str
    family: str
    symbol: str
    open_price: float
    open_estimated: bool
    window_end: int
    up_shares: float = 0.0
    up_cost: float = 0.0
    down_shares: float = 0.0
    down_cost: float = 0.0
    last_buy_ts: dict[str, float] = field(default_factory=dict)
    resolved: bool = False

    @property
    def total_cost(self) -> float:
        return self.up_cost + self.down_cost

    def shares(self, side: str) -> float:
        return self.up_shares if side == "UP" else self.down_shares

    def side_cost(self, side: str) -> float:
        return self.up_cost if side == "UP" else self.down_cost

    def avg(self, side: str) -> float:
        shares = self.shares(side)
        return self.side_cost(side) / shares if shares > 0 else 0.0

    def add(self, side: str, shares: float, price: float) -> None:
        amount = shares * price
        if side == "UP":
            self.up_shares += shares
            self.up_cost += amount
        else:
            self.down_shares += shares
            self.down_cost += amount
        self.last_buy_ts[side] = time.time()

    def pair_avg(self) -> float:
        paired = min(self.up_shares, self.down_shares)
        if paired <= 0:
            return 0.0
        return (self.avg("UP") + self.avg("DOWN"))

    def hedge_ratio_cost(self) -> float:
        if self.up_cost <= 0 or self.down_cost <= 0:
            return 0.0
        return min(self.up_cost, self.down_cost) / max(self.up_cost, self.down_cost)


@dataclass
class Snapshot:
    market: MarketInfo
    primary_source: str
    underlying: float
    price_lag_ms: float
    d5: float
    d10: float
    d30: float
    up: BookQuote
    down: BookQuote

    @property
    def ask_pair_sum(self) -> float:
        if self.up.ask <= 0 or self.down.ask <= 0:
            return 0.0
        return self.up.ask + self.down.ask

    @property
    def buy_sum(self) -> float:
        if self.up.bid <= 0 or self.down.bid <= 0:
            return 0.0
        return self.up.bid + self.down.bid


class FeedLogger:
    def __init__(self, log_dir: Path) -> None:
        self.path = log_dir / "feed_latency.csv"
        self.fields = ["seen_at", "source", "symbol", "price", "lag_ms"]
        self.last_written: dict[tuple[str, str], float] = {}

    def write(self, tick: PriceTick) -> None:
        key = (tick.source, tick.symbol)
        if tick.local_ts - self.last_written.get(key, 0.0) < 1.0:
            return
        self.last_written[key] = tick.local_ts
        append_csv(
            self.path,
            self.fields,
            {
                "seen_at": utc_iso(tick.local_ts),
                "source": tick.source,
                "symbol": tick.symbol,
                "price": tick.price,
                "lag_ms": round(tick.lag_ms, 3),
            },
        )


class FreykoBot:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.prices = PriceStore()
        self.books = BookStore()
        self.feed_logger = FeedLogger(cfg.log_dir)
        self.telegram = TelegramNotifier.from_env() if TelegramNotifier else None
        self.markets: dict[str, MarketInfo] = {}
        self.open_prices: dict[str, tuple[float, bool]] = {}
        self.inventory: dict[str, Inventory] = {}
        self.paper_pnl = 0.0
        self.closed_markets = 0
        self.last_status = 0.0
        self.last_discovery: dict[str, str] = {}
        self.real_live = (not cfg.dry_run) and cfg.allow_live
        if not cfg.dry_run and not cfg.allow_live:
            print("[safety] DRY_RUN=false ignored because FREYKO_ALLOW_LIVE is not true.")
            self.cfg.dry_run = True

    def notify(self, text: str) -> None:
        if not self.telegram or not self.telegram.configured:
            return
        self.telegram.send(text)

    async def binance_feed(self) -> None:
        if websockets is None:
            print("[binance] websockets package missing; install requirements.txt")
            return
        while True:
            try:
                async with websockets.connect(self.cfg.binance_ws_url, ping_interval=15, ping_timeout=10) as ws:
                    print("[binance] connected")
                    async for raw in ws:
                        data = json.loads(raw)
                        payload = data.get("data", data)
                        event = payload.get("e")
                        if event not in {"trade", "aggTrade"}:
                            continue
                        symbol = str(payload.get("s", "")).upper()
                        price = as_float(payload.get("p"))
                        exchange_ts = exchange_ts_to_seconds(payload.get("T") or payload.get("E"))
                        if symbol and price > 0:
                            tick = PriceTick("binance", symbol, price, exchange_ts, time.time())
                            self.prices.update(tick)
                            self.feed_logger.write(tick)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[binance] reconnect after error: {exc}")
                await asyncio.sleep(2)

    async def coinbase_feed(self) -> None:
        if not self.cfg.coinbase_enabled:
            return
        if websockets is None:
            return
        products = sorted({FAMILY_CONFIG[f]["coinbase"] for f in self.cfg.families})
        sub = {"type": "subscribe", "product_ids": products, "channels": ["ticker", "matches"]}
        while True:
            try:
                async with websockets.connect(self.cfg.coinbase_ws_url, ping_interval=15, ping_timeout=10) as ws:
                    await ws.send(json.dumps(sub))
                    print("[coinbase] connected")
                    async for raw in ws:
                        data = json.loads(raw)
                        if data.get("type") not in {"ticker", "match", "last_match"}:
                            continue
                        product = data.get("product_id", "")
                        symbol = COINBASE_TO_BINANCE.get(product)
                        price = as_float(data.get("price"))
                        exchange_ts = parse_coinbase_time(data.get("time", ""))
                        if symbol and price > 0:
                            tick = PriceTick("coinbase", symbol, price, exchange_ts, time.time())
                            self.prices.update(tick)
                            self.feed_logger.write(tick)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[coinbase] reconnect after error: {exc}")
                await asyncio.sleep(2)

    async def polymarket_ws(self) -> None:
        if not self.cfg.poly_ws_enabled:
            return
        if websockets is None:
            return
        while True:
            try:
                async with websockets.connect(self.cfg.poly_ws_url, ping_interval=10, ping_timeout=10) as ws:
                    print("[poly_ws] connected")
                    subscribed: set[str] = set()
                    while True:
                        wanted = set(self.books.wanted_assets)
                        if wanted and wanted != subscribed:
                            await ws.send(json.dumps({
                                "assets_ids": sorted(wanted),
                                "type": "market",
                                "custom_feature_enabled": True,
                            }))
                            subscribed = wanted
                            print(f"[poly_ws] subscribed assets={len(subscribed)}")
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            self.books.apply_poly_message(raw)
                        except asyncio.TimeoutError:
                            continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[poly_ws] reconnect after error: {exc}")
                await asyncio.sleep(2)

    async def discover_market(self, family: str) -> Optional[MarketInfo]:
        cfg = FAMILY_CONFIG[family]
        period = int(cfg["period"])
        start = current_window_start(period)
        slug = f"{cfg['prefix']}-{start}"
        cached = self.markets.get(slug)
        if cached:
            return cached

        params = {"slug": slug}
        try:
            data, elapsed_ms = await asyncio.to_thread(
                http_json,
                f"{self.cfg.gamma_api_url}/markets",
                params,
                self.cfg.http_timeout,
            )
        except Exception:
            return None

        items = data if isinstance(data, list) else data.get("markets", []) if isinstance(data, dict) else []
        if not items:
            return None
        item = items[0]
        token_ids = parse_json_list(item.get("clobTokenIds") or item.get("clob_token_ids"))
        outcomes = parse_json_list(item.get("outcomes"))
        if len(token_ids) < 2:
            return None

        up_index = 0
        down_index = 1
        for i, outcome in enumerate(outcomes):
            label = str(outcome).lower()
            if "up" in label or label == "yes":
                up_index = i
            if "down" in label or label == "no":
                down_index = i
        if up_index >= len(token_ids) or down_index >= len(token_ids):
            return None

        market = MarketInfo(
            family=family,
            slug=slug,
            symbol=str(cfg["symbol"]),
            period_seconds=period,
            window_start=start,
            window_end=start + period,
            up_token=str(token_ids[up_index]),
            down_token=str(token_ids[down_index]),
            title=str(item.get("question") or item.get("title") or ""),
            condition_id=str(item.get("conditionId") or item.get("condition_id") or ""),
            gamma_ms=elapsed_ms,
        )
        self.markets[slug] = market
        self.books.want(market.up_token, market.down_token)
        if self.last_discovery.get(family) != slug:
            self.last_discovery[family] = slug
            print(f"[market] {family} {slug} T-{market.seconds_to_close:.0f}s")
        return market

    async def refresh_book_rest(self, token_id: str) -> None:
        old = self.books.quote(token_id)
        if old.ts > 0 and old.age_ms < self.cfg.rest_book_fallback_seconds * 1000:
            return
        try:
            data, _elapsed = await asyncio.to_thread(
                http_json,
                f"{self.cfg.clob_api_url}/book",
                {"token_id": token_id},
                self.cfg.http_timeout,
            )
        except Exception:
            return
        if not isinstance(data, dict):
            return
        bids = data.get("bids") or []
        asks = data.get("asks") or []
        bid, bid_size = BookStore._best_bid(bids)
        ask, ask_size = BookStore._best_ask(asks)
        self.books.update_quote(token_id, bid, ask, bid_size, ask_size, "rest")

    async def snapshot(self, market: MarketInfo) -> Optional[Snapshot]:
        source = "coinbase" if self.cfg.price_primary == "coinbase" else "binance"
        tick = self.prices.tick(source, market.symbol)
        if not tick and source == "coinbase":
            source = "binance"
            tick = self.prices.tick(source, market.symbol)
        if not tick:
            return None
        await asyncio.gather(
            self.refresh_book_rest(market.up_token),
            self.refresh_book_rest(market.down_token),
        )
        up = self.books.quote(market.up_token)
        down = self.books.quote(market.down_token)
        if up.ask <= 0 or down.ask <= 0:
            return None
        return Snapshot(
            market=market,
            primary_source=source,
            underlying=tick.price,
            price_lag_ms=tick.lag_ms,
            d5=self.prices.change_pct(source, market.symbol, 5),
            d10=self.prices.change_pct(source, market.symbol, 10),
            d30=self.prices.change_pct(source, market.symbol, 30),
            up=up,
            down=down,
        )

    def inventory_for(self, snap: Snapshot) -> Inventory:
        inv = self.inventory.get(snap.market.slug)
        if inv:
            return inv
        open_price, open_estimated = self.capture_open_price(snap)
        inv = Inventory(
            slug=snap.market.slug,
            family=snap.market.family,
            symbol=snap.market.symbol,
            open_price=open_price,
            open_estimated=open_estimated,
            window_end=snap.market.window_end,
        )
        self.inventory[snap.market.slug] = inv
        return inv

    def in_entry_window(self, market: MarketInfo) -> bool:
        t = market.seconds_to_close
        start = self.cfg.entry_start_15m if market.period_seconds >= 900 else self.cfg.entry_start_5m
        return self.cfg.entry_end_seconds <= t <= start

    def momentum_score(self, snap: Snapshot) -> float:
        return 0.55 * snap.d5 + 0.30 * snap.d10 + 0.15 * snap.d30

    def fair_probability(self, score: float) -> float:
        return clamp(0.5 + abs(score) * self.cfg.momentum_to_prob, 0.02, 0.98)

    def capture_open_price(self, snap: Snapshot) -> tuple[float, bool]:
        stored = self.open_prices.get(snap.market.slug)
        if stored:
            return stored
        age = time.time() - snap.market.window_start
        estimated = age > self.cfg.open_capture_grace_seconds
        self.open_prices[snap.market.slug] = (snap.underlying, estimated)
        label = "estimated" if estimated else "captured"
        print(f"[open] {snap.market.family} {label} open={snap.underlying:.2f} age={age:.1f}s")
        return snap.underlying, estimated

    def can_buy(self, inv: Inventory, side: str, price: float) -> tuple[bool, str]:
        if price <= 0:
            return False, "no ask"
        if price > self.cfg.max_buy_price:
            return False, f"ask {price:.3f} > max {self.cfg.max_buy_price:.3f}"
        if self.paper_pnl <= -abs(self.cfg.max_daily_loss):
            return False, "daily paper loss limit"
        if inv.total_cost + self.cfg.lot_shares * price > self.cfg.max_market_cost:
            return False, "market cap"
        if inv.side_cost(side) + self.cfg.lot_shares * price > self.cfg.max_side_cost:
            return False, "side cap"
        last = inv.last_buy_ts.get(side, 0.0)
        if time.time() - last < self.cfg.cooldown_seconds:
            return False, "cooldown"
        return True, ""

    def place_buy(self, snap: Snapshot, inv: Inventory, side: str, price: float, reason: str, fair: float, edge: float) -> None:
        ok, error = self.can_buy(inv, side, price)
        if not ok:
            return
        shares = float(self.cfg.lot_shares)
        amount = shares * price
        status = "PAPER"
        if self.real_live:
            status = "LIVE_BLOCKED"
            error = "live execution is not enabled in this first standalone build"
        else:
            inv.add(side, shares, price)
        self.log_trade(snap, inv, side, price, shares, amount, reason, fair, edge, status, error)
        icon = "BUY" if status == "PAPER" else status
        print(
            f"  {icon} {snap.market.family} {side} {shares:.0f}@{price:.3f} "
            f"${amount:.2f} {reason} T-{snap.market.seconds_to_close:.0f}s "
            f"d5={snap.d5:+.3f}% d10={snap.d10:+.3f}% pair={snap.ask_pair_sum:.3f}"
        )
        self.notify(
            f"{status} BUY {snap.market.family} {side}\n"
            f"{shares:.0f} shares @ {price:.3f} = ${amount:.2f}\n"
            f"Reason: {reason} | T-{snap.market.seconds_to_close:.0f}s\n"
            f"d5={snap.d5:+.3f}% d10={snap.d10:+.3f}% pair={snap.ask_pair_sum:.3f}\n"
            f"Paper PnL: ${self.paper_pnl:+.2f}"
        )

    def evaluate(self, snap: Snapshot) -> None:
        inv = self.inventory_for(snap)
        decision = "SKIP"
        if self.cfg.require_open_seen and inv.open_estimated:
            self.log_snapshot(snap, inv, "OPEN_NOT_CAPTURED")
            return
        if not self.in_entry_window(snap.market):
            self.log_snapshot(snap, inv, "OUTSIDE_WINDOW")
            return
        if snap.price_lag_ms > self.cfg.max_price_lag_ms:
            self.log_snapshot(snap, inv, "STALE_PRICE_FEED")
            return
        if snap.ask_pair_sum > self.cfg.avoid_pair_sum_gt:
            self.log_snapshot(snap, inv, "PAIR_SUM_TOO_HIGH")
            return

        if 0 < snap.ask_pair_sum <= self.cfg.pair_arb_max:
            self.place_buy(snap, inv, "UP", snap.up.ask, "PAIR_ARB", 0.5, self.cfg.pair_arb_max - snap.ask_pair_sum)
            self.place_buy(snap, inv, "DOWN", snap.down.ask, "PAIR_ARB", 0.5, self.cfg.pair_arb_max - snap.ask_pair_sum)
            decision = "PAIR_ARB"

        score = self.momentum_score(snap)
        target = "UP" if score >= 0 else "DOWN"
        weak = "DOWN" if inv.side_cost("UP") > inv.side_cost("DOWN") else "UP"
        target_ask = snap.up.ask if target == "UP" else snap.down.ask
        weak_ask = snap.down.ask if weak == "DOWN" else snap.up.ask
        fair = self.fair_probability(score)
        edge = fair - target_ask

        if inv.total_cost > 0:
            dominant = "UP" if inv.up_cost >= inv.down_cost else "DOWN"
            weak = "DOWN" if dominant == "UP" else "UP"
            weak_ask = snap.down.ask if weak == "DOWN" else snap.up.ask
            dominant_avg = inv.avg(dominant)
            projected_pair = dominant_avg + weak_ask if dominant_avg > 0 else 0.0
            if (
                inv.side_cost(weak) < inv.side_cost(dominant) * self.cfg.hedge_target_cost_ratio
                and (weak_ask <= self.cfg.hedge_max_price or (0 < projected_pair <= self.cfg.hedge_pair_max))
            ):
                self.place_buy(
                    snap,
                    inv,
                    weak,
                    weak_ask,
                    "CHEAP_HEDGE",
                    1.0 - dominant_avg if dominant_avg else 0.5,
                    self.cfg.hedge_pair_max - projected_pair if projected_pair else 0.0,
                )
                decision = "CHEAP_HEDGE"

        enough_momentum = (
            abs(snap.d5) >= self.cfg.min_momentum_5
            or abs(snap.d10) >= self.cfg.min_momentum_10
            or abs(score) >= self.cfg.min_momentum_10
        )
        strong = abs(score) >= self.cfg.strong_momentum
        if enough_momentum and (edge >= self.cfg.min_edge or (strong and edge >= 0.0)):
            self.place_buy(snap, inv, target, target_ask, "MOMENTUM_LAG", fair, edge)
            decision = "MOMENTUM_LAG"

        self.log_snapshot(snap, inv, decision)

    def resolve_finished(self) -> None:
        source = "coinbase" if self.cfg.price_primary == "coinbase" else "binance"
        now = time.time()
        for inv in list(self.inventory.values()):
            if inv.resolved or now < inv.window_end + self.cfg.resolve_delay_seconds:
                continue
            final_price = self.prices.price(source, inv.symbol) or self.prices.price("binance", inv.symbol)
            if final_price <= 0 or inv.open_price <= 0:
                continue
            outcome = "UP" if final_price >= inv.open_price else "DOWN"
            payout = inv.up_shares if outcome == "UP" else inv.down_shares
            pnl = payout - inv.total_cost
            inv.resolved = True
            self.paper_pnl += pnl
            self.closed_markets += 1
            self.log_resolution(inv, outcome, final_price, payout, pnl)
            print(
                f"  RESOLVE {inv.family} {outcome} open={inv.open_price:.2f} "
                f"final={final_price:.2f} cost=${inv.total_cost:.2f} "
                f"payout=${payout:.2f} pnl=${pnl:+.2f} total=${self.paper_pnl:+.2f}"
            )
            self.notify(
                f"RESOLVE {inv.family} {outcome}\n"
                f"Open: {inv.open_price:.2f} | Final: {final_price:.2f}\n"
                f"Cost: ${inv.total_cost:.2f} | Payout: ${payout:.2f}\n"
                f"PnL: ${pnl:+.2f} | Total: ${self.paper_pnl:+.2f}"
            )

    async def strategy_loop(self) -> None:
        loops = 0
        while True:
            started = time.perf_counter()
            try:
                for family in self.cfg.families:
                    market = await self.discover_market(family)
                    if not market:
                        continue
                    snap = await self.snapshot(market)
                    if snap:
                        self.evaluate(snap)
                self.resolve_finished()
                self.print_status()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[strategy] error: {exc}")

            loops += 1
            if self.cfg.max_loops > 0 and loops >= self.cfg.max_loops:
                print(f"Reached FREYKO_MAX_LOOPS={self.cfg.max_loops}; stopping.")
                return
            elapsed = time.perf_counter() - started
            await asyncio.sleep(max(0.05, self.cfg.poll_seconds - elapsed))

    def log_snapshot(self, snap: Snapshot, inv: Inventory, decision: str) -> None:
        fields = [
            "seen_at", "family", "slug", "seconds_to_close", "source", "symbol",
            "underlying", "open_price", "open_estimated", "price_lag_ms",
            "d5", "d10", "d30", "score", "up_bid", "up_ask", "up_age_ms",
            "down_bid", "down_ask", "down_age_ms",
            "ask_pair_sum", "buy_sum", "up_shares", "up_cost", "up_avg",
            "down_shares", "down_cost", "down_avg", "pair_avg",
            "hedge_ratio_cost", "total_cost", "decision",
        ]
        append_csv(self.cfg.log_dir / "snapshots.csv", fields, {
            "seen_at": utc_iso(),
            "family": snap.market.family,
            "slug": snap.market.slug,
            "seconds_to_close": round(snap.market.seconds_to_close, 3),
            "source": snap.primary_source,
            "symbol": snap.market.symbol,
            "underlying": round(snap.underlying, 4),
            "open_price": round(inv.open_price, 4),
            "open_estimated": inv.open_estimated,
            "price_lag_ms": round(snap.price_lag_ms, 3),
            "d5": round(snap.d5, 6),
            "d10": round(snap.d10, 6),
            "d30": round(snap.d30, 6),
            "score": round(self.momentum_score(snap), 6),
            "up_bid": snap.up.bid,
            "up_ask": snap.up.ask,
            "up_age_ms": round(snap.up.age_ms, 3),
            "down_bid": snap.down.bid,
            "down_ask": snap.down.ask,
            "down_age_ms": round(snap.down.age_ms, 3),
            "ask_pair_sum": round(snap.ask_pair_sum, 6),
            "buy_sum": round(snap.buy_sum, 6),
            "up_shares": round(inv.up_shares, 4),
            "up_cost": round(inv.up_cost, 4),
            "up_avg": round(inv.avg("UP"), 6),
            "down_shares": round(inv.down_shares, 4),
            "down_cost": round(inv.down_cost, 4),
            "down_avg": round(inv.avg("DOWN"), 6),
            "pair_avg": round(inv.pair_avg(), 6),
            "hedge_ratio_cost": round(inv.hedge_ratio_cost(), 6),
            "total_cost": round(inv.total_cost, 4),
            "decision": decision,
        })

    def log_trade(
        self,
        snap: Snapshot,
        inv: Inventory,
        side: str,
        price: float,
        shares: float,
        amount: float,
        reason: str,
        fair: float,
        edge: float,
        status: str,
        error: str,
    ) -> None:
        fields = [
            "seen_at", "status", "family", "slug", "side", "price", "shares",
            "notional", "reason", "seconds_to_close", "source", "symbol",
            "underlying", "open_price", "open_estimated", "price_lag_ms",
            "d5", "d10", "d30", "score", "fair", "edge", "up_ask",
            "down_ask", "ask_pair_sum", "up_cost", "down_cost", "pair_avg",
            "total_cost", "error",
        ]
        append_csv(self.cfg.log_dir / "trades.csv", fields, {
            "seen_at": utc_iso(),
            "status": status,
            "family": snap.market.family,
            "slug": snap.market.slug,
            "side": side,
            "price": round(price, 4),
            "shares": round(shares, 4),
            "notional": round(amount, 4),
            "reason": reason,
            "seconds_to_close": round(snap.market.seconds_to_close, 3),
            "source": snap.primary_source,
            "symbol": snap.market.symbol,
            "underlying": round(snap.underlying, 4),
            "open_price": round(inv.open_price, 4),
            "open_estimated": inv.open_estimated,
            "price_lag_ms": round(snap.price_lag_ms, 3),
            "d5": round(snap.d5, 6),
            "d10": round(snap.d10, 6),
            "d30": round(snap.d30, 6),
            "score": round(self.momentum_score(snap), 6),
            "fair": round(fair, 6),
            "edge": round(edge, 6),
            "up_ask": snap.up.ask,
            "down_ask": snap.down.ask,
            "ask_pair_sum": round(snap.ask_pair_sum, 6),
            "up_cost": round(inv.up_cost, 4),
            "down_cost": round(inv.down_cost, 4),
            "pair_avg": round(inv.pair_avg(), 6),
            "total_cost": round(inv.total_cost, 4),
            "error": error,
        })

    def log_resolution(self, inv: Inventory, outcome: str, final_price: float, payout: float, pnl: float) -> None:
        fields = [
            "resolved_at", "family", "slug", "symbol", "outcome", "open_price",
            "open_estimated", "final_price", "up_shares", "up_cost", "up_avg",
            "down_shares", "down_cost", "down_avg", "pair_avg", "total_cost",
            "payout", "pnl", "total_pnl",
        ]
        append_csv(self.cfg.log_dir / "resolutions.csv", fields, {
            "resolved_at": utc_iso(),
            "family": inv.family,
            "slug": inv.slug,
            "symbol": inv.symbol,
            "outcome": outcome,
            "open_price": round(inv.open_price, 4),
            "open_estimated": inv.open_estimated,
            "final_price": round(final_price, 4),
            "up_shares": round(inv.up_shares, 4),
            "up_cost": round(inv.up_cost, 4),
            "up_avg": round(inv.avg("UP"), 6),
            "down_shares": round(inv.down_shares, 4),
            "down_cost": round(inv.down_cost, 4),
            "down_avg": round(inv.avg("DOWN"), 6),
            "pair_avg": round(inv.pair_avg(), 6),
            "total_cost": round(inv.total_cost, 4),
            "payout": round(payout, 4),
            "pnl": round(pnl, 4),
            "total_pnl": round(self.paper_pnl, 4),
        })

    def print_status(self) -> None:
        now = time.time()
        if now - self.last_status < 30:
            return
        self.last_status = now
        open_inv = [inv for inv in self.inventory.values() if not inv.resolved and inv.total_cost > 0]
        btc = self.prices.tick("binance", "BTCUSDT")
        eth = self.prices.tick("binance", "ETHUSDT")
        if btc:
            print(
                f"[{utc_iso()}] open={len(open_inv)} closed={self.closed_markets} "
                f"paper_pnl=${self.paper_pnl:+.2f} BTC_lag={btc.lag_ms:.0f}ms"
            )
        else:
            print(f"[{utc_iso()}] waiting BTC")
        if eth:
            print(f"  ETH_lag={eth.lag_ms:.0f}ms")
        for inv in sorted(open_inv, key=lambda x: x.window_end)[:6]:
            print(
                f"  {inv.family} T-{max(0, inv.window_end-time.time()):.0f}s "
                f"UP {inv.up_shares:.0f}@{inv.avg('UP'):.3f} "
                f"DN {inv.down_shares:.0f}@{inv.avg('DOWN'):.3f} "
                f"cost=${inv.total_cost:.2f} pair={inv.pair_avg():.3f}"
            )

    async def run(self) -> None:
        self.cfg.log_dir.mkdir(parents=True, exist_ok=True)
        mode = "LIVE" if self.real_live else "DRY RUN"
        print("=" * 72)
        print("FreykoBot - momentum + Polymarket repricing lag + hedge inventory")
        print(f"Mode: {mode}")
        print(f"Families: {', '.join(self.cfg.families)}")
        print(f"Primary feed: {self.cfg.price_primary}")
        print(f"Lot: {self.cfg.lot_shares} shares | Max market cost: ${self.cfg.max_market_cost:.2f}")
        print(f"Logs: {self.cfg.log_dir.resolve()}")
        print("=" * 72)
        if self.telegram and self.telegram.configured:
            self.notify(
                f"FreykoBot started\n"
                f"Mode: {mode}\n"
                f"Families: {', '.join(self.cfg.families)}\n"
                f"Primary feed: {self.cfg.price_primary}"
            )
            print("[telegram] notifications enabled")
        else:
            print("[telegram] disabled")

        strategy_task = asyncio.create_task(self.strategy_loop(), name="strategy")
        tasks = [
            asyncio.create_task(self.binance_feed(), name="binance"),
            asyncio.create_task(self.polymarket_ws(), name="polymarket_ws"),
            strategy_task,
        ]
        if self.cfg.coinbase_enabled:
            tasks.append(asyncio.create_task(self.coinbase_feed(), name="coinbase"))
        try:
            await strategy_task
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def async_main() -> None:
    if load_dotenv:
        load_dotenv()
    cfg = Config.from_env()
    bot = FreykoBot(cfg)
    await bot.run()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\nStopping FreykoBot.")


if __name__ == "__main__":
    main()
