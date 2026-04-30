import asyncio
import logging
from contextlib import suppress
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import get_settings
from app.models import Signal, SymbolInfo
from app.scanner import FuturesScanner
from app.store import SignalStore
from app.telegram import TelegramAlerter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

settings = get_settings()
store = SignalStore(limit=settings.signal_limit)
alerter = TelegramAlerter(settings.telegram_bot_token, settings.telegram_chat_id)
scanner = FuturesScanner(settings=settings, store=store, alerter=alerter)
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


@app.get("/", include_in_schema=False)
async def dashboard() -> FileResponse:
    return FileResponse("static/index.html")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/signals", response_model=list[Signal])
async def signals() -> list[Signal]:
    return store.list()


@app.get("/symbols", response_model=SymbolInfo)
async def symbols() -> SymbolInfo:
    return SymbolInfo(symbols=scanner.symbols(), timeframes=settings.timeframes)


@app.get("/status")
async def status() -> dict:
    return scanner.status()


@app.post("/telegram/test")
async def test_telegram() -> dict[str, str]:
    if not alerter.enabled:
        return {"status": "disabled", "message": "Telegram environment variables are not configured."}

    sent = await alerter.send_text("CF Scanner test alert: Telegram is connected.")
    if not sent:
        return {"status": "failed", "message": "Telegram API request failed."}
    return {"status": "sent"}
