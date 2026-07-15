"""Tests for rate limiting middleware."""

import time

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from dagster_codekit.api.middleware import (
    MemoryRateLimiter,
    RateLimitMiddleware,
    RedisRateLimiter,
)


@pytest.fixture
def rate_limited_app():
    app = FastAPI()
    limiter = MemoryRateLimiter(requests_per_minute=3)
    app.add_middleware(RateLimitMiddleware, limiter=limiter, key_by="ip")

    @app.post("/deploy")
    async def deploy():
        return {"status": "ok"}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


class TestMemoryRateLimiter:
    def test_under_limit(self):
        limiter = MemoryRateLimiter(requests_per_minute=10)
        for _ in range(5):
            assert limiter.is_allowed("key1")

    def test_over_limit(self):
        limiter = MemoryRateLimiter(requests_per_minute=3)
        for _ in range(3):
            assert limiter.is_allowed("key1")
        assert not limiter.is_allowed("key1")

    def test_independent_keys(self):
        limiter = MemoryRateLimiter(requests_per_minute=2)
        for _ in range(2):
            assert limiter.is_allowed("key1")
        assert not limiter.is_allowed("key1")
        assert limiter.is_allowed("key2")

    def test_window_expiry(self):
        limiter = MemoryRateLimiter(requests_per_minute=2)
        assert limiter.is_allowed("key1")
        assert limiter.is_allowed("key1")
        assert not limiter.is_allowed("key1")

        limiter._windows["key1"] = [time.time() - 120]

        assert limiter.is_allowed("key1")


class TestMiddlewareIntegration:
    def test_rate_limited_endpoint(self, rate_limited_app):
        client = TestClient(rate_limited_app)

        for i in range(3):
            response = client.post("/deploy", json={"location_name": "test", "image_tag": "img:v1"})
            assert response.status_code == 200, f"Request {i} should succeed"

        response = client.post("/deploy", json={"location_name": "test", "image_tag": "img:v1"})
        assert response.status_code == 429
        assert "Retry-After" in response.headers

    def test_health_not_rate_limited(self, rate_limited_app):
        client = TestClient(rate_limited_app)

        for _ in range(10):
            response = client.get("/health")
            assert response.status_code == 200


class TestRedisRateLimiter:
    def test_redis_backend_smoke(self):
        try:
            import fakeredis
        except ImportError:
            pytest.skip("fakeredis not installed")

        limiter = RedisRateLimiter(requests_per_minute=5, redis_url="redis://localhost")
        limiter._redis = fakeredis.FakeStrictRedis()

        for _ in range(5):
            assert limiter.is_allowed("key1")
        assert not limiter.is_allowed("key1")
