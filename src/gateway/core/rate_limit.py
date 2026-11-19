import hashlib
import time
from dataclasses import dataclass

from fastapi import Request

from gateway.config import settings

# Atomic token bucket. Missing state starts full; idle buckets expire so the
# key space does not grow forever.
TOKEN_BUCKET_SCRIPT = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local state = redis.call('HMGET', key, 'tokens', 'updated_at')
local tokens = tonumber(state[1])
local updated_at = tonumber(state[2])

if tokens == nil or updated_at == nil then
    tokens = capacity
    updated_at = now
end

tokens = math.min(capacity, tokens + (now - updated_at) * refill_rate)

local allowed = 0
local retry_after = 0

if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
else
    retry_after = math.ceil((1 - tokens) / refill_rate)
end

redis.call('HSET', key, 'tokens', tokens, 'updated_at', now)
redis.call('EXPIRE', key, math.ceil(capacity / refill_rate) + 1)

return {allowed, retry_after, math.floor(tokens)}
"""


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    remaining: int
    retry_after: int


def client_identity(request: Request) -> str:
    api_key = request.headers.get("X-API-Key")

    if api_key:
        return f"key:{api_key}"

    if request.client:
        return f"ip:{request.client.host}"

    return "ip:unknown"


async def check_rate_limit(redis, identity: str) -> RateLimitResult:
    key = "gateway:ratelimit:" + hashlib.sha256(
        identity.encode()
    ).hexdigest()

    allowed, retry_after, remaining = await redis.eval(
        TOKEN_BUCKET_SCRIPT,
        1,
        key,
        settings.rate_limit_capacity,
        settings.rate_limit_refill_per_second,
        time.time(),
    )

    return RateLimitResult(
        allowed=bool(allowed),
        remaining=int(remaining),
        retry_after=int(retry_after),
    )


def rate_limit_headers(result: RateLimitResult) -> dict[str, str]:
    return {
        "X-RateLimit-Limit": str(settings.rate_limit_capacity),
        "X-RateLimit-Remaining": str(result.remaining),
    }
