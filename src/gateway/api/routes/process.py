import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from gateway.routing.selector import selector

router = APIRouter()


@router.post("/process")
async def process(request: Request):
    provider = selector.select()

    url = f"{provider.base_url}/v1/process"

    if request.url.query:
        url += f"?{request.url.query}"

    try:
        client: httpx.AsyncClient = request.app.state.http_client

        upstream_response = await client.request(
            method=request.method,
            url=url,
            content=await request.body(),
            headers={
                key: value
                for key, value in request.headers.items()
                if key.lower()
                not in {
                    "host",
                    "content-length",
                    "connection",
                }
            },
        )
    except httpx.TimeoutException as exc:
        raise HTTPException(
            status_code=504,
            detail="Upstream provider timed out",
        ) from exc
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail="Upstream provider unavailable",
        ) from exc

    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers={
            key: value
            for key, value in upstream_response.headers.items()
            if key.lower()
            not in {
                "content-length",
                "transfer-encoding",
                "connection",
                "content-encoding",
            }
        },
        media_type=upstream_response.headers.get("content-type"),
    )
