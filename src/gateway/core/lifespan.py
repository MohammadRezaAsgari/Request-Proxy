from contextlib import asynccontextmanager

import httpx
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

    yield

    await app.state.http_client.aclose()
