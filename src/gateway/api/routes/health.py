from fastapi import APIRouter, Request

router = APIRouter(tags=["Health"])


@router.get("/health")
async def liveness():
    return {
        "status": "alive",
    }


@router.get("/ready")
async def readiness(request: Request):
    http_client = request.app.state.http_client

    return {
        "status": "ready",
        "http_client_initialized": not http_client.is_closed,
    }
