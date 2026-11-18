import asyncio
import os
import random
import time
from collections import deque

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

PROVIDER_NAME = os.getenv("PROVIDER_NAME", "provider_a")
MIN_LATENCY = float(os.getenv("MIN_LATENCY", "0.3"))
MAX_LATENCY = float(os.getenv("MAX_LATENCY", "1.0"))
ERROR_RATE = float(os.getenv("ERROR_RATE", "0"))
RATE_LIMIT = int(os.getenv("RATE_LIMIT", "100"))

requests: deque[float] = deque()


@app.post("/v1/process")
async def process(request: Request):
    now = time.monotonic()

    # Keep only requests from the last second
    while requests and now - requests[0] > 1:
        requests.popleft()

    if len(requests) >= RATE_LIMIT:
        return JSONResponse(
            status_code=429,
            content={
                "error": "Rate limit exceeded",
                "provider": PROVIDER_NAME,
            },
            headers={"Retry-After": "1"},
        )

    requests.append(now)

    await asyncio.sleep(random.uniform(MIN_LATENCY, MAX_LATENCY))

    if random.random() < ERROR_RATE:
        return JSONResponse(
            status_code=503,
            content={
                "error": "Provider temporarily unavailable",
                "provider": PROVIDER_NAME,
            },
        )

    body = (await request.body()).decode(errors="replace")

    return {
        "provider": PROVIDER_NAME,
        "received_body": body,
        "message": "Request processed successfully",
    }
