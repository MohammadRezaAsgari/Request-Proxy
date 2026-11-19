import time

# Prune stale members, then atomically check the limit and reserve a slot.
# Returning 1 for an existing member makes retries idempotent.
ACQUIRE_SLOT_SCRIPT = """
local key = KEYS[1]
local member = ARGV[1]
local limit = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local max_age = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', key, 0, now - max_age)

if redis.call('ZSCORE', key, member) then
    return 1
end

if redis.call('ZCARD', key) >= limit then
    return 0
end

redis.call('ZADD', key, now, member)
return 1
"""

RELEASE_SLOT_SCRIPT = """
return redis.call('ZREM', KEYS[1], ARGV[1])
"""


def inflight_key() -> str:
    return "gateway:inflight"


def provider_slot_key(provider_name: str) -> str:
    return f"gateway:provider:{provider_name}:active"


def provider_health_key(provider_name: str) -> str:
    return f"gateway:provider:{provider_name}:health"


def now() -> float:
    return time.time()
