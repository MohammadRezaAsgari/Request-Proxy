import logging

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from gateway.routing.selector import selector

logger = logging.getLogger("gateway")

router = APIRouter()

# Idempotent by HTTP semantics, so replaying them is always safe.
REPLAY_SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}

# Provider rejected the request or declared itself unavailable,
# so it was not processed: replaying is safe.
REJECTED_STATUS_CODES = {429, 503}

# Outcome is unknown: the provider may or may not have processed
# the request. Only replay when the request is idempotent.
UNKNOWN_OUTCOME_STATUS_CODES = {502, 504}

EXCLUDED_RESPONSE_HEADERS = {
    "content-length",
    "content-encoding",
    "transfer-encoding",
    "connection",
}


def is_replay_safe(method: str, headers: dict[str, str]) -> bool:
    if method.upper() in REPLAY_SAFE_METHODS:
        return True

    return any(
        key.lower() == "idempotency-key"
        for key in headers
    )


def proxy_response(upstream_response: httpx.Response) -> Response:
    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers={
            key: value
            for key, value in upstream_response.headers.items()
            if key.lower() not in EXCLUDED_RESPONSE_HEADERS
        },
    )


@router.post("/process")
async def process(request: Request):
    request_id = request.state.request_id

    body = await request.body()

    excluded_request_headers = {
        "host",
        "content-length",
        "connection",
    }

    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in excluded_request_headers
    }

    replay_safe = is_replay_safe(request.method, headers)

    providers = selector.providers
    start_provider = selector.select()
    start_index = providers.index(start_provider)

    client: httpx.AsyncClient = request.app.state.http_client

    for attempt in range(len(providers)):
        provider = providers[
            (start_index + attempt) % len(providers)
        ]

        logger.info(
            "request_id=%s selected_provider=%s attempt=%s",
            request_id,
            provider.name,
            attempt + 1,
        )

        url = f"{provider.base_url}/v1/process"

        if request.url.query:
            url += f"?{request.url.query}"

        try:
            upstream_response = await client.request(
                method=request.method,
                url=url,
                content=body,
                headers=headers,
            )
        except httpx.ConnectTimeout:
            logger.warning(
                "request_id=%s provider=%s connect timed out attempt=%s",
                request_id,
                provider.name,
                attempt + 1,
            )
            continue
        except httpx.ConnectError:
            logger.warning(
                "request_id=%s provider=%s connect failed attempt=%s",
                request_id,
                provider.name,
                attempt + 1,
            )
            continue
        except httpx.TimeoutException as exc:
            logger.warning(
                "request_id=%s provider=%s timed out attempt=%s replay_safe=%s",
                request_id,
                provider.name,
                attempt + 1,
                replay_safe,
            )

            if not replay_safe:
                raise HTTPException(
                    status_code=504,
                    detail="Upstream provider timed out",
                ) from exc

            continue
        except httpx.RequestError as exc:
            logger.warning(
                "request_id=%s provider=%s is unavailable attempt=%s replay_safe=%s",
                request_id,
                provider.name,
                attempt + 1,
                replay_safe,
            )

            if not replay_safe:
                raise HTTPException(
                    status_code=502,
                    detail="Upstream provider unavailable",
                ) from exc

            continue

        status_code = upstream_response.status_code

        if status_code in REJECTED_STATUS_CODES:
            logger.warning(
                "request_id=%s provider=%s returned status=%s attempt=%s",
                request_id,
                provider.name,
                status_code,
                attempt + 1,
            )
            continue

        if status_code in UNKNOWN_OUTCOME_STATUS_CODES:
            logger.warning(
                "request_id=%s provider=%s returned status=%s attempt=%s replay_safe=%s",
                request_id,
                provider.name,
                status_code,
                attempt + 1,
                replay_safe,
            )

            if not replay_safe:
                return proxy_response(upstream_response)

            continue

        return proxy_response(upstream_response)

    raise HTTPException(
        status_code=502,
        detail="All attempted providers failed",
    )
