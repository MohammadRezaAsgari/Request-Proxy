from fastapi import FastAPI

from gateway.api.routes import health
from gateway.config import settings
from gateway.core.lifespan import lifespan


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(
    health.router,
    prefix=f"{settings.api_v1_prefix}/health",
)


@app.get("/")
async def root():
    return {
        "application": settings.app_name,
        "environment": settings.app_env,
        "version": app.version,
    }
