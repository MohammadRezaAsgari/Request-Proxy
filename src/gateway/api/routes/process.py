import asyncio
import hashlib
import json
import uuid

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from gateway.core.rate_limit import (
    check_rate_limit,
    client_identity,
    rate_limit_headers,
)
from gateway.tasks import process_request

router = APIRouter()

IDEMPOTENCY_TTL = 24 * 60 * 60

DELETE_IF_OWNER = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


@router.post("/process")
async def process(request: Request):
    idempotency_key = request.headers.get("Idempotency-Key")

    if not idempotency_key:
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key header is required",
        )

    redis = request.app.state.redis

    rate_limit = await check_rate_limit(
        redis,
        client_identity(request),
    )

    if not rate_limit.allowed:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={
                **rate_limit_headers(rate_limit),
                "Retry-After": str(rate_limit.retry_after),
            },
        )

    body = await request.body()
    query_string = request.url.query

    payload_hash = hashlib.sha256(
        b"\n".join(
            [
                request.method.encode(),
                request.url.path.encode(),
                query_string.encode(),
                body,
            ]
        )
    ).hexdigest()

    idempotency_redis_key = (
        f"gateway:idempotency:{idempotency_key}"
    )

    job_id = str(uuid.uuid4())

    reservation = json.dumps(
        {
            "job_id": job_id,
            "payload_hash": payload_hash,
        }
    )

    acquired = await redis.set(
        idempotency_redis_key,
        reservation,
        nx=True,
        ex=IDEMPOTENCY_TTL,
    )

    if not acquired:
        existing_raw = await redis.get(idempotency_redis_key)

        if existing_raw is None:
            raise HTTPException(
                status_code=409,
                detail="Idempotency key conflict, retry the request",
            )

        existing = json.loads(existing_raw)

        if existing["payload_hash"] != payload_hash:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Idempotency key was already used "
                    "with a different payload"
                ),
            )

        return JSONResponse(
            status_code=202,
            content={
                "job_id": existing["job_id"],
                "status": "accepted",
                "duplicate": True,
            },
            headers=rate_limit_headers(rate_limit),
        )

    job_metadata_key = f"gateway:job:{job_id}"

    await redis.set(
        job_metadata_key,
        json.dumps({"payload_hash": payload_hash}),
        ex=IDEMPOTENCY_TTL,
    )

    try:
        await asyncio.to_thread(
            process_request.apply_async,
            kwargs={
                "body": body.decode(),
                "query_string": query_string,
                "attempt": 1,
                "excluded_providers": [],
            },
            task_id=job_id,
            queue="gateway.process",
        )
    except Exception:
        # Delete only our own reservation.
        await redis.eval(
            DELETE_IF_OWNER,
            1,
            idempotency_redis_key,
            reservation,
        )
        await redis.delete(job_metadata_key)

        raise HTTPException(
            status_code=503,
            detail="Could not enqueue job",
        )

    return JSONResponse(
        status_code=202,
        content={
            "job_id": job_id,
            "status": "accepted",
            "duplicate": False,
        },
        headers={
            **rate_limit_headers(rate_limit),
            "Location": f"/jobs/{job_id}",
        },
    )
