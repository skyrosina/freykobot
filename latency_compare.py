#!/usr/bin/env python3
"""Compare Binance and Coinbase live price feed latency from this server.

Run on the droplet:
    python latency_compare.py --duration 20

The script measures:
    - connect time
    - first message time
    - exchange timestamp lag
    - local receive-to-receive gap

Use the recommended feed in .env as PRICE_PRIMARY.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import websockets


BINANCE_STREAMS = "btcusdt@trade/ethusdt@trade"
BINANCE_URLS = {
    "binance.data_vision": f"wss://data-stream.binance.vision/stream?streams={BINANCE_STREAMS}&timeUnit=MICROSECOND",
    "binance.443": f"wss://stream.binance.com:443/stream?streams={BINANCE_STREAMS}&timeUnit=MICROSECOND",
    "binance.9443": f"wss://stream.binance.com:9443/stream?streams={BINANCE_STREAMS}&timeUnit=MICROSECOND",
}
COINBASE_URL = "wss://ws-feed.exchange.coinbase.com"


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


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return ordered[idx]


def grade(avg_lag: float, avg_gap: float) -> str:
    score = max(avg_lag, avg_gap)
    if score <= 150:
        return "FAST"
    if score <= 350:
        return "OK"
    if score <= 700:
        return "SLOW"
    return "BAD"


@dataclass
class Result:
    name: str
    connect_ms: float = 0.0
    first_msg_ms: float = 0.0
    lags: list[float] = field(default_factory=list)
    gaps: list[float] = field(default_factory=list)
    messages: int = 0
    error: str = ""

    @property
    def avg_lag(self) -> float:
        return statistics.fmean(self.lags) if self.lags else 0.0

    @property
    def p95_lag(self) -> float:
        return pct(self.lags, 0.95)

    @property
    def avg_gap(self) -> float:
        return statistics.fmean(self.gaps) if self.gaps else 0.0

    @property
    def p95_gap(self) -> float:
        return pct(self.gaps, 0.95)


async def measure_binance(name: str, url: str, duration: float) -> Result:
    result = Result(name=name)
    started = time.perf_counter()
    last_local = 0.0
    try:
        async with websockets.connect(url, ping_interval=15, ping_timeout=10) as ws:
            result.connect_ms = (time.perf_counter() - started) * 1000
            end = time.perf_counter() + duration
            while time.perf_counter() < end:
                timeout = max(0.2, end - time.perf_counter())
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                local = time.time()
                if result.messages == 0:
                    result.first_msg_ms = (time.perf_counter() - started) * 1000
                data = json.loads(raw)
                payload = data.get("data", data)
                if payload.get("e") not in {"trade", "aggTrade"}:
                    continue
                exchange_ts = exchange_ts_to_seconds(payload.get("T") or payload.get("E"))
                if exchange_ts > 0:
                    result.lags.append(max(0.0, (local - exchange_ts) * 1000))
                if last_local > 0:
                    result.gaps.append((local - last_local) * 1000)
                last_local = local
                result.messages += 1
    except Exception as exc:
        result.error = str(exc)
    return result


async def measure_coinbase(duration: float) -> Result:
    result = Result(name="coinbase.ws_feed")
    sub = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channels": ["ticker", "matches"],
    }
    started = time.perf_counter()
    last_local = 0.0
    try:
        async with websockets.connect(COINBASE_URL, ping_interval=15, ping_timeout=10) as ws:
            result.connect_ms = (time.perf_counter() - started) * 1000
            await ws.send(json.dumps(sub))
            end = time.perf_counter() + duration
            while time.perf_counter() < end:
                timeout = max(0.2, end - time.perf_counter())
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                local = time.time()
                data = json.loads(raw)
                if data.get("type") not in {"ticker", "match", "last_match"}:
                    continue
                price = as_float(data.get("price"))
                if price <= 0:
                    continue
                if result.messages == 0:
                    result.first_msg_ms = (time.perf_counter() - started) * 1000
                exchange_ts = parse_coinbase_time(data.get("time", ""))
                if exchange_ts > 0:
                    result.lags.append(max(0.0, (local - exchange_ts) * 1000))
                if last_local > 0:
                    result.gaps.append((local - last_local) * 1000)
                last_local = local
                result.messages += 1
    except Exception as exc:
        result.error = str(exc)
    return result


def print_results(results: list[Result]) -> None:
    print()
    print("Results")
    print("-" * 112)
    print(
        f"{'feed':24} {'connect':>9} {'first':>9} {'lag_avg':>9} {'lag_p95':>9} "
        f"{'gap_avg':>9} {'gap_p95':>9} {'msgs':>8} {'grade':>7}  note"
    )
    print("-" * 112)
    for r in results:
        note = r.error[:40] if r.error else ""
        print(
            f"{r.name:24} {r.connect_ms:8.0f}ms {r.first_msg_ms:8.0f}ms "
            f"{r.avg_lag:8.0f}ms {r.p95_lag:8.0f}ms {r.avg_gap:8.0f}ms "
            f"{r.p95_gap:8.0f}ms {r.messages:8d} {grade(r.avg_lag, r.avg_gap):>7}  {note}"
        )
    print("-" * 112)

    usable = [r for r in results if not r.error and r.messages > 5 and r.avg_lag > 0]
    if not usable:
        print("No usable result. Check server networking/firewall.")
        return
    best = min(usable, key=lambda r: (max(r.avg_lag, r.avg_gap), r.first_msg_ms))
    primary = "coinbase" if best.name.startswith("coinbase") else "binance"
    print(f"Recommended PRICE_PRIMARY={primary}")
    if best.name.startswith("binance"):
        print(f"Recommended BINANCE_WS_URL={BINANCE_URLS[best.name]}")


async def async_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=20.0, help="seconds per feed")
    parser.add_argument("--only", choices=["all", "binance", "coinbase"], default="all")
    args = parser.parse_args()

    tasks = []
    if args.only in {"all", "binance"}:
        tasks.extend(measure_binance(name, url, args.duration) for name, url in BINANCE_URLS.items())
    if args.only in {"all", "coinbase"}:
        tasks.append(measure_coinbase(args.duration))

    print(f"Measuring feeds for {args.duration:.1f}s from this server...")
    results = await asyncio.gather(*tasks)
    print_results(list(results))


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
