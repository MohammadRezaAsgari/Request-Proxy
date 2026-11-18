from contextlib import asynccontextmanager

import httpx
import redis.asyncio as redis
from fastapi import FastAPI

from gateway.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    limits = httpx.Limits(
        max_connections=settings.upstream_max_connections,
        max_keepalive_connections=(settings.upstream_max_keepalive_connections),
    )

    timeout = httpx.Timeout(
        settings.upstream_timeout_seconds,
        connect=5.0,
    )

    app.state.http_client = httpx.AsyncClient(
        limits=limits,
        timeout=timeout,
        follow_redirects=False,
    )

    app.state.redis = redis.from_url(
        settings.redis_url,
        decode_responses=True,
    )

    yield

    await app.state.http_client.aclose()
    await app.state.redis.aclose()
