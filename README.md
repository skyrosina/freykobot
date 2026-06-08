# FreykoBot

FreykoBot is a separate experimental Polymarket crypto up/down bot based on the
JetFadil research pass:

- Binance/Coinbase short-window momentum
- Polymarket CLOB repricing lag
- two-sided UP/DOWN inventory and cheap hedge ladder
- small repeated lots held to resolution
- risk-first order approval using worst-case loss and net-share caps

It starts in dry-run by default. Live mode requires both:

```env
DRY_RUN=false
FREYKO_ALLOW_LIVE=true
```

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

On Windows PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

## Smoke Test

One loop only:

```bash
FREYKO_MAX_LOOPS=1 DRY_RUN=true python freykobot.py
```

## Run

```bash
DRY_RUN=true python freykobot.py
```

## Telegram

Create a bot with BotFather, get your chat id, then set:

```env
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=123456:ABC...
TELEGRAM_CHAT_ID=123456789
```

Test the notifier:

```bash
python telegram_notify.py "FreykoBot test"
```

When enabled, FreykoBot sends start, trade, and resolution messages.

With screen on the droplet:

```bash
screen -S freykobot
cd ~/freykobot_
source .venv/bin/activate
DRY_RUN=true python freykobot.py
```

Detach with `Ctrl+A`, then `D`. Reattach:

```bash
screen -r freykobot
```

## Data Sources

- Binance WebSocket is the default primary momentum feed.
- Coinbase WebSocket runs as a comparator/failover when `COINBASE_ENABLED=true`.
- Polymarket CLOB WebSocket updates best bid/ask when available.
- Polymarket REST book calls are kept as a fallback so the bot can recover after
  WebSocket reconnects.

Measure Binance vs Coinbase from the droplet:

```bash
python latency_compare.py --duration 20
```

Use the printed `Recommended PRICE_PRIMARY=...` value in `.env`. If Binance
wins, also copy the printed `Recommended BINANCE_WS_URL=...` line.

Logs are written to `logs/`:

```text
trades.csv
snapshots.csv
resolutions.csv
feed_latency.csv
```

If the bot starts after the current market has already opened, it may log
`OPEN_NOT_CAPTURED` and skip that market. This is intentional: paper PnL is only
trusted when the bot captured the real window open price near market start.

Paper entries also enforce `FREYKO_MIN_ORDER_NOTIONAL=5` by default so dry-run
orders are closer to live CLOB constraints. Very cheap prices will use more
shares to reach the notional minimum, then market/side caps decide whether to
allow or skip the entry.

The `reason` field is the final combined decision label, not a separate bot
module. For example, `MOMENTUM_LAG_RISK_OK` means momentum and lag were present
and the risk engine approved the resulting inventory. `RISK_HEDGE_MOMENTUM_AWARE`
means the hedge reduced worst-case loss under the current market state.

Do not switch to live mode before reviewing several hours of dry-run logs.
