# FreykoBot

FreykoBot is a separate experimental Polymarket crypto up/down bot based on the
JetFadil research pass:

- Binance/Coinbase short-window momentum
- Polymarket CLOB repricing lag
- two-sided UP/DOWN inventory and cheap hedge ladder
- small repeated lots held to resolution

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

Do not switch to live mode before reviewing several hours of dry-run logs.
