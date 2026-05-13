import asyncio
import logging
from contextlib import suppress
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.auth import ApiAuth, create_session_token, redirect_to_dashboard, require_page_auth, verify_password
from app.config import get_settings
from app.models import Signal, SymbolInfo
from app.news import CryptoNewsService
from app.scanner import FuturesScanner
from app.store import SignalStore
from app.telegram import TelegramAlerter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

settings = get_settings()
store = SignalStore(limit=settings.signal_limit, db_path=settings.signal_db_path)
alerter = TelegramAlerter(settings.telegram_bot_token, settings.telegram_chat_id)
scanner = FuturesScanner(settings=settings, store=store, alerter=alerter)
news_service = CryptoNewsService(
    refresh_seconds=settings.news_refresh_seconds,
    item_limit=settings.news_item_limit,
)
scanner_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global scanner_task
    scanner_task = asyncio.create_task(scanner.start())
    try:
        yield
    finally:
        await scanner.stop()
        if scanner_task:
            scanner_task.cancel()
            with suppress(asyncio.CancelledError):
                await scanner_task


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory="static"), name="static")


def no_store_file_response(path: str) -> FileResponse:
    response = FileResponse(path)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.get("/", include_in_schema=False)
async def dashboard(_: None = Depends(require_page_auth)) -> FileResponse:
    return no_store_file_response("static/index.html")


@app.get("/login", include_in_schema=False)
async def login_page(request: Request):
    if verify_password("") and not settings.dashboard_password:
        return redirect_to_dashboard()
    return no_store_file_response("static/login.html")


@app.post("/login", include_in_schema=False)
async def login(password: str = Form(...)) -> RedirectResponse:
    if not verify_password(password):
        return RedirectResponse("/login?error=1", status_code=303)

    response = redirect_to_dashboard()
    response.set_cookie(
        key=settings.session_cookie_name,
        value=create_session_token(),
        max_age=60 * 60 * 24 * 7,
        httponly=True,
        secure=settings.session_cookie_secure,
        samesite="lax",
    )
    return response


@app.post("/logout", include_in_schema=False)
async def logout() -> RedirectResponse:
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(settings.session_cookie_name)
    return response


@app.get("/health")
async def health() -> dict:
    scanner_status = scanner.status()
    latest_signal_at = store.latest_created_at()
    healthy = scanner_status["running"] and scanner_status["market_data_status"] in {"OK", "Loading"}
    return {
        "status": "ok" if healthy else "warning",
        "running": scanner_status["running"],
        "market_data_status": scanner_status["market_data_status"],
        "websocket_connected": scanner_status["websocket_connected"],
        "loaded_pair_count": scanner_status["loaded_pair_count"],
        "total_pair_count": scanner_status["total_pair_count"],
        "stale_pair_count": scanner_status["stale_pair_count"],
        "signal_count": store.count(),
        "latest_signal_at": latest_signal_at.isoformat() if latest_signal_at else None,
        "last_error": scanner_status["last_error"],
    }


@app.get("/signals", response_model=list[Signal])
async def signals(_: None = ApiAuth) -> list[Signal]:
    news_payload = await news_service.latest(scanner.symbols())
    return [_with_news_context(signal, news_payload) for signal in store.list()]


def _with_news_context(signal: Signal, news_payload: dict) -> Signal:
    contexts = []
    for item in news_payload.get("items", []):
        impacts = item.get("coin_impacts") or []
        matched = next((impact for impact in impacts if impact.get("symbol") == signal.symbol), None)
        if not matched:
            matched = next((impact for impact in impacts if impact.get("symbol") == "Market"), None)
        if not matched:
            continue

        direction = matched.get("direction")
        if direction not in {"Bullish", "Bearish"}:
            continue
        signal_bias = "Bullish" if signal.signal_type == "LONG" else "Bearish"
        relation = "supports" if direction == signal_bias else "conflicts"
        contexts.append(
            {
                "relation": relation,
                "symbol": matched.get("symbol"),
                "direction": direction,
                "strength": matched.get("strength"),
                "headline": item.get("title"),
                "source": item.get("source"),
                "url": item.get("url"),
                "explanation": matched.get("explanation"),
                "trade_note": matched.get("trade_note"),
            }
        )

    conflict = next((context for context in contexts if context["relation"] == "conflicts"), None)
    support = next((context for context in contexts if context["relation"] == "supports"), None)
    selected = conflict or support
    if not selected:
        selected = {
            "relation": "neutral",
            "headline": None,
            "source": None,
            "explanation": "ยังไม่พบข่าวล่าสุดที่กระทบสัญญาณนี้โดยตรง",
            "trade_note": "ใช้ technical signal เป็นหลัก และตรวจข่าวก่อนเข้าไม้จริง",
        }
    update = {"news_context": selected}
    update.update(scanner.enrich_signal_context(signal))
    if signal.quality_score is None:
        update.update(_legacy_quality(signal))
    update["trader_summary"] = scanner._trader_summary(
        signal_type=signal.signal_type,
        quality_label=update.get("quality_label") or signal.quality_label,
        quality_score=update.get("quality_score") or signal.quality_score,
        rsi=float(signal.rsi or 0),
        volume_ratio=float((signal.indicators or {}).get("volume_ratio") or 0),
        trade_plan=update.get("trade_plan") or signal.trade_plan or {},
        backtest=update.get("backtest") or signal.backtest or {},
        status=update.get("status") or signal.status or {},
        news_context=selected,
    )
    return signal.model_copy(update=update)


def _legacy_quality(signal: Signal) -> dict:
    indicators = signal.indicators or {}
    price = float(signal.price or 0)
    ema_9 = float(indicators.get("ema_9") or 0)
    ema_21 = float(indicators.get("ema_21") or 0)
    ema_200 = float(indicators.get("ema_200") or 0)
    rsi = float(indicators.get("rsi_14") or signal.rsi or 0)
    volume_ratio = float(indicators.get("volume_ratio") or 0)
    trend_distance_pct = abs(price - ema_200) / price * 100 if price > 0 and ema_200 > 0 else 0
    cross_gap_pct = abs(ema_9 - ema_21) / price * 100 if price > 0 and ema_9 > 0 and ema_21 > 0 else 0
    trend_score = _clamp(trend_distance_pct / 1.2 * 100, 35, 100)
    cross_score = _clamp(cross_gap_pct / 0.35 * 100, 35, 100)
    volume_score = _clamp(volume_ratio / 1.8 * 100, 35, 100)
    rsi_score = _rsi_quality_score(signal.signal_type, rsi)
    score = _clamp(trend_score * 0.28 + cross_score * 0.2 + rsi_score * 0.24 + volume_score * 0.28, 0, 100)
    if score >= 85:
        label = "Excellent"
    elif score >= 70:
        label = "Strong"
    elif score >= 55:
        label = "Caution"
    else:
        label = "Weak"
    return {
        "quality_score": score,
        "quality_label": label,
        "quality_reasons": [
            f"Trend distance {trend_distance_pct:.2f}% from EMA200",
            f"EMA cross gap {cross_gap_pct:.3f}%",
            f"RSI quality {rsi_score:.0f}/100",
            f"Volume strength {volume_ratio:.2f}x avg20",
        ],
    }


def _rsi_quality_score(side: str, rsi: float) -> float:
    if side == "LONG":
        if 50 <= rsi <= 68:
            return 100
        if 45 <= rsi < 50:
            return 70
        if 68 < rsi <= 75:
            return 65
        return 35
    if 32 <= rsi <= 50:
        return 100
    if 50 < rsi <= 55:
        return 70
    if 25 <= rsi < 32:
        return 65
    return 35


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


@app.get("/symbols", response_model=SymbolInfo)
async def symbols(_: None = ApiAuth) -> SymbolInfo:
    return SymbolInfo(symbols=scanner.symbols(), timeframes=settings.timeframes)


@app.get("/status")
async def status(_: None = ApiAuth) -> dict:
    return scanner.status()


@app.get("/indicators")
async def indicators(symbol: str, timeframe: str, _: None = ApiAuth) -> dict:
    return scanner.indicator_series(symbol, timeframe)


@app.get("/news")
async def news(_: None = ApiAuth) -> dict:
    return await news_service.latest(scanner.symbols())


@app.post("/telegram/test")
async def test_telegram(_: None = ApiAuth) -> dict[str, str]:
    if not alerter.enabled:
        return {"status": "disabled", "message": "Telegram environment variables are not configured."}

    sent = await alerter.send_text("CF Scanner test alert: Telegram is connected.")
    if not sent:
        return {"status": "failed", "message": "Telegram API request failed."}
    return {"status": "sent"}
