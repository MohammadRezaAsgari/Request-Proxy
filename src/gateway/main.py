from fastapi import FastAPI

from gateway.api.routes import health, process
from gateway.config import settings
from gateway.core.lifespan import lifespan
from gateway.core.logging import configure_logging
from gateway.core.middleware import RequestLoggingMiddleware

configure_logging()

app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(RequestLoggingMiddleware)

app.include_router(
    health.router,
)

app.include_router(
    process.router,
    prefix=settings.api_v1_prefix,
)


@app.get("/")
async def root():
    return {
        "application": settings.app_name,
        "environment": settings.app_env,
        "version": app.version,
    }
