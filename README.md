# CF Scanner

Personal crypto USDT futures pattern scanner built with FastAPI. It uses Binance Futures public market data only, stores signals in memory, and can send Telegram alerts.

This app does not place trades, does not use private Binance API keys, and does not implement auto trading.

## Features

- Binance USDT Futures public kline data
- Auto watchlist: top USDT perpetual futures by 24h quote volume
- Fallback symbols: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `BNBUSDT`
- Timeframes: `15m`, `30m`
- Indicators: EMA 9, EMA 21, EMA 200, RSI 14, volume average 20
- LONG and SHORT signal detection
- In-memory latest signal storage
- Telegram alert for each new signal
- Simple HTML dashboard
- Signal reason details and dashboard filters
- Docker-ready deployment

## Signal Rules

LONG:

- Price > EMA 200
- EMA 9 crosses above EMA 21
- RSI > 50
- Current volume > average volume over 20 candles

SHORT:

- Price < EMA 200
- EMA 9 crosses below EMA 21
- RSI < 50
- Current volume > average volume over 20 candles

## Setup

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create a `.env` file if you want Telegram alerts:

```env
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
AUTO_WATCHLIST_ENABLED=true
AUTO_WATCHLIST_SIZE=20
WATCHLIST_REFRESH_SECONDS=900
DASHBOARD_PASSWORD=choose_a_private_password
SESSION_SECRET=change_me_to_a_long_random_string
SESSION_COOKIE_SECURE=true
```

Run locally:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Open the dashboard:

```text
http://localhost:8000
```

## API

- `GET /health`
- `GET /signals`
- `GET /symbols`
- `GET /status`
- `POST /telegram/test`

`/status` shows scanner runtime state, websocket state, Telegram status, and latest closed candle time for every configured symbol/timeframe.

`/telegram/test` sends a test Telegram message when `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are configured.

## Dashboard Login

Set `DASHBOARD_PASSWORD` to require login for the dashboard and scanner APIs. Set `SESSION_SECRET` to a long random string so login cookies stay valid across redeploys.

`GET /health` remains public for uptime checks.

## Watchlist

By default the scanner builds an automatic watchlist from Binance USDT perpetual futures. It ranks markets by 24h `quoteVolume` and scans the top `AUTO_WATCHLIST_SIZE` symbols.

Set `AUTO_WATCHLIST_ENABLED=false` to scan only the fixed fallback list in `app/config.py`.

## Docker

Build and run:

```bash
docker build -t cf-scanner .
docker run --env-file .env -p 8000:8000 cf-scanner
```

## GitHub

Initialize and push to GitHub:

```bash
git init
git add .
git commit -m "Initial crypto futures scanner"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/cf-scanner.git
git push -u origin main
```

Do not commit `.env`. Use `.env.example` as the template.

## Railway

1. Create a new Railway project.
2. Choose "Deploy from GitHub repo".
3. Select the `cf-scanner` repository.
4. Add environment variables if Telegram alerts are needed:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `AUTO_WATCHLIST_ENABLED`
- `AUTO_WATCHLIST_SIZE`
- `WATCHLIST_REFRESH_SECONDS`
- `DASHBOARD_PASSWORD`
- `SESSION_SECRET`

Railway provides `PORT` automatically. The Dockerfile uses that value and falls back to `8000` locally.

## Deployment Notes

For Fly.io or Render, set the same Telegram environment variables in the platform dashboard if alerts are needed.

The app uses websocket reconnect logic with exponential backoff.
