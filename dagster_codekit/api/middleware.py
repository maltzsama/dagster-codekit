"""Rate limiting middleware for ingestion endpoints.

Supports in-memory (single-pod) and Redis-backed (multi-pod) backends.
Applied to /deploy and /webhooks/* routes only.
"""

import hashlib
import time
from collections import defaultdict

import structlog
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

logger = structlog.get_logger(__name__)


class RateLimiter:
    def __init__(self, requests_per_minute: int):
        self.requests_per_minute = requests_per_minute
        self.window_seconds = 60

    def is_allowed(self, key: str) -> bool:
        raise NotImplementedError


class MemoryRateLimiter(RateLimiter):
    def __init__(self, requests_per_minute: int):
        super().__init__(requests_per_minute)
        self._windows: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, key: str) -> bool:
        now = time.time()
        window_start = now - self.window_seconds

        timestamps = self._windows[key]
        timestamps[:] = [t for t in timestamps if t > window_start]

        if len(timestamps) >= self.requests_per_minute:
            return False

        timestamps.append(now)

        if len(self._windows) > 10000:
            self._windows.clear()

        return True


class RedisRateLimiter(RateLimiter):
    def __init__(self, requests_per_minute: int, redis_url: str):
        super().__init__(requests_per_minute)
        self._redis_url = redis_url
        self._redis = None

    def _get_redis(self):
        if self._redis is None:
            import redis
            self._redis = redis.from_url(self._redis_url)
        return self._redis

    def is_allowed(self, key: str) -> bool:
        r = self._get_redis()
        window_key = f"codekit:ratelimit:{key}"
        current = r.incr(window_key)
        if current == 1:
            r.expire(window_key, self.window_seconds)
        return current <= self.requests_per_minute


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, limiter: RateLimiter, key_by: str = "ip"):
        super().__init__(app)
        self.limiter = limiter
        self.key_by = key_by

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path
        if not (path == "/deploy" or path.startswith("/webhooks/")):
            return await call_next(request)

        if self.key_by == "token":
            auth = request.headers.get("Authorization", "")
            key = _hash_key(auth if auth else "anonymous")
        else:
            forwarded = request.headers.get("X-Forwarded-For")
            ip = forwarded.split(",")[0].strip() if forwarded else (
                request.client.host if request.client else "unknown"
            )
            key = _hash_key(ip)

        if not self.limiter.is_allowed(key):
            logger.warning("rate_limit_breached", key=key[:12])
            return Response(
                content='{"detail":"Too Many Requests"}',
                status_code=429,
                headers={"Retry-After": "60", "Content-Type": "application/json"},
            )

        return await call_next(request)


def _hash_key(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()
