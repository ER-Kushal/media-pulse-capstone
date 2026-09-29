"""Shared plumbing for the MediaPulse API: DB engine, logging, JWT auth, RBAC, rate limiting."""
import hashlib
import hmac
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from sqlalchemy import create_engine

# ---------------------------------------------------------------- logging
LOG_DIR = Path(os.environ.get("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_DIR / "api.log")],
)
log = logging.getLogger("mediapulse.api")

# ---------------------------------------------------------------- database
# Credentials come from environment variables only (never hardcoded in git).
def _db_url() -> str:
    if os.environ.get("DATABASE_URL"):
        return os.environ["DATABASE_URL"].replace("postgres://", "postgresql+psycopg2://", 1)
    return "postgresql+psycopg2://{u}:{p}@{h}:{port}/{d}".format(
        u=os.environ.get("PGUSER", "mediapulse"),
        p=os.environ.get("PGPASSWORD", "mediapulse_dev_pw"),
        h=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        d=os.environ.get("PGDATABASE", "mediapulse"),
    )

engine = create_engine(_db_url(), pool_pre_ping=True, pool_size=5, max_overflow=5)

# ---------------------------------------------------------------- auth / RBAC
JWT_SECRET = os.environ.get("JWT_SECRET", "dev-only-change-me")
JWT_ALG = "HS256"
TOKEN_MINUTES = int(os.environ.get("TOKEN_MINUTES", "120"))


def _hash(pw: str) -> str:
    return hashlib.pbkdf2_hmac("sha256", pw.encode(), b"mediapulse-salt", 100_000).hex()


# Demo users (roles: admin > analyst > viewer). Passwords can be overridden via env vars.
USERS = {
    "admin":   {"hash": _hash(os.environ.get("ADMIN_PASSWORD", "admin123")),   "role": "admin"},
    "analyst": {"hash": _hash(os.environ.get("ANALYST_PASSWORD", "analyst123")), "role": "analyst"},
    "viewer":  {"hash": _hash(os.environ.get("VIEWER_PASSWORD", "viewer123")),  "role": "viewer"},
}


def authenticate(username: str, password: str):
    user = USERS.get(username)
    if user and hmac.compare_digest(user["hash"], _hash(password)):
        return {"username": username, "role": user["role"]}
    return None


def create_token(username: str, role: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=TOKEN_MINUTES)
    return jwt.encode({"sub": username, "role": role, "exp": exp}, JWT_SECRET, algorithm=JWT_ALG)


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


def current_user(token: str = Depends(oauth2_scheme)) -> dict:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
        return {"username": payload["sub"], "role": payload["role"]}
    except (JWTError, KeyError):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired token",
                            headers={"WWW-Authenticate": "Bearer"})


def require_role(*roles: str):
    def checker(user: dict = Depends(current_user)) -> dict:
        if user["role"] not in roles:
            log.warning("authz denied: %s (%s) needs %s", user["username"], user["role"], roles)
            raise HTTPException(status.HTTP_403_FORBIDDEN, f"Requires role: {' or '.join(roles)}")
        return user
    return checker


# ---------------------------------------------------------------- rate limiting
class RateLimiter:
    """Tiny in-memory sliding-window limiter (per client IP) - abuse protection for a demo.
    In production use a gateway/Redis-backed limiter instead."""
    def __init__(self, limit: int, window_s: int = 60):
        self.limit, self.window, self.hits = limit, window_s, defaultdict(deque)

    def allow(self, key: str) -> bool:
        now, q = time.time(), self.hits[key]
        while q and now - q[0] > self.window:
            q.popleft()
        if len(q) >= self.limit:
            return False
        q.append(now)
        return True


limiter = RateLimiter(int(os.environ.get("RATE_LIMIT_PER_MIN", "300")))
login_limiter = RateLimiter(10)  # stricter for login (brute-force protection)


def client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"
