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
async def dashboard(_: None = Depends(require_page_auth)) -> FileResponse:
    return FileResponse("static/index.html")


@app.get("/login", include_in_schema=False)
async def login_page(request: Request):
    if verify_password("") and not settings.dashboard_password:
        return redirect_to_dashboard()
    return FileResponse("static/login.html")


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
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/signals", response_model=list[Signal])
async def signals(_: None = ApiAuth) -> list[Signal]:
    return store.list()


@app.get("/symbols", response_model=SymbolInfo)
async def symbols(_: None = ApiAuth) -> SymbolInfo:
    return SymbolInfo(symbols=scanner.symbols(), timeframes=settings.timeframes)


@app.get("/status")
async def status(_: None = ApiAuth) -> dict:
    return scanner.status()


@app.post("/telegram/test")
async def test_telegram(_: None = ApiAuth) -> dict[str, str]:
    if not alerter.enabled:
        return {"status": "disabled", "message": "Telegram environment variables are not configured."}

    sent = await alerter.send_text("CF Scanner test alert: Telegram is connected.")
    if not sent:
        return {"status": "failed", "message": "Telegram API request failed."}
    return {"status": "sent"}
