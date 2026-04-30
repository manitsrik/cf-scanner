import hmac
import time
from hashlib import sha256

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse

from app.config import get_settings

SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 7


def auth_enabled() -> bool:
    return bool(get_settings().dashboard_password)


def create_session_token() -> str:
    settings = get_settings()
    timestamp = str(int(time.time()))
    signature = hmac.new(settings.session_secret.encode(), timestamp.encode(), sha256).hexdigest()
    return f"{timestamp}.{signature}"


def is_valid_session_token(token: str | None) -> bool:
    if not token or "." not in token:
        return False

    settings = get_settings()
    timestamp, signature = token.split(".", 1)
    expected = hmac.new(settings.session_secret.encode(), timestamp.encode(), sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return False

    try:
        issued_at = int(timestamp)
    except ValueError:
        return False

    return time.time() - issued_at <= SESSION_MAX_AGE_SECONDS


def verify_password(password: str) -> bool:
    configured = get_settings().dashboard_password
    if not configured:
        return True
    return hmac.compare_digest(password, configured)


def require_auth(request: Request) -> None:
    settings = get_settings()
    if not settings.dashboard_password:
        return
    token = request.cookies.get(settings.session_cookie_name)
    if not is_valid_session_token(token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Login required")


def require_page_auth(request: Request) -> None:
    settings = get_settings()
    if not settings.dashboard_password:
        return
    token = request.cookies.get(settings.session_cookie_name)
    if not is_valid_session_token(token):
        raise HTTPException(
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
            headers={"Location": "/login"},
            detail="Login required",
        )


ApiAuth = Depends(require_auth)


def redirect_to_dashboard() -> RedirectResponse:
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
