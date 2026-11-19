import logging
import os

import httpx
import redis
from celery.exceptions import Ignore

from gateway.celery_app import celery_app
from gateway.config import settings
from gateway.core.limits import (
    ACQUIRE_SLOT_SCRIPT,
    RELEASE_SLOT_SCRIPT,
    inflight_key,
    now,
    provider_health_key,
    provider_slot_key,
)
from gateway.providers.registry import Provider
from gateway.routing.selector import selector

logger = logging.getLogger("gateway")

MAX_ATTEMPTS = 3
RETRY_QUEUES = {
    1: "gateway.process.retry",
    2: "gateway.process.retry.10s",
    3: "gateway.process.retry.20s",
}

PROVIDER_LIMITS = {
    "provider_a": settings.provider_a_concurrency,
    "provider_b": settings.provider_b_concurrency,
    "provider_c": settings.provider_c_concurrency,
}

# Redis clients must not be shared across prefork children.
_redis_clients: dict[int, redis.Redis] = {}


def get_redis() -> redis.Redis:
    pid = os.getpid()

    if pid not in _redis_clients:
        _redis_clients[pid] = redis.Redis.from_url(
            settings.redis_url,
            decode_responses=True,
        )

    return _redis_clients[pid]


def acquire_slot(
    client: redis.Redis,
    key: str,
    member: str,
    limit: int,
    max_age: int,
) -> bool:
    return bool(
        client.eval(
            ACQUIRE_SLOT_SCRIPT,
            1,
            key,
            member,
            limit,
            now(),
            max_age,
        )
    )


def release_slot(client: redis.Redis, key: str, member: str) -> None:
    client.eval(RELEASE_SLOT_SCRIPT, 1, key, member)


def provider_available(client: redis.Redis, name: str) -> bool:
    cooldown_until = client.hget(
        provider_health_key(name),
        "cooldown_until",
    )

    return not cooldown_until or float(cooldown_until) <= now()


def record_provider_failure(client: redis.Redis, name: str) -> None:
    key = provider_health_key(name)
    failures = client.hincrby(key, "failures", 1)

    if failures >= settings.provider_failure_threshold:
        client.hset(
            key,
            "cooldown_until",
            now() + settings.provider_cooldown_seconds,
        )
        client.hset(key, "failures", 0)
        client.expire(
            key,
            int(settings.provider_cooldown_seconds) + 60,
        )
    else:
        client.expire(key, settings.inflight_job_ttl_seconds)


def record_provider_success(client: redis.Redis, name: str) -> None:
    client.delete(provider_health_key(name))


def select_provider(
    excluded_providers: list[str],
    client: redis.Redis,
) -> Provider | None:
    for _ in range(len(selector.providers)):
        provider = selector.select()

        if (
            provider.name not in excluded_providers
            and provider_available(client, provider.name)
        ):
            return provider

    return None


def schedule_retry(
    task,
    retry_kwargs: dict,
    attempt: int,
) -> None:
    queue = RETRY_QUEUES.get(attempt, RETRY_QUEUES[MAX_ATTEMPTS])

    process_request.apply_async(
        kwargs=retry_kwargs,
        task_id=task.request.id,
        queue=queue,
    )

    # Custom (non-exception) state so clients see "retrying" while the
    # message waits in the TTL queue. Celery's built-in RETRY state is
    # an exception state and cannot hold plain metadata.
    task.update_state(
        state="RETRYING",
        meta={
            "attempt": retry_kwargs["attempt"],
            "queue": queue,
        },
    )


def retry_kwargs(
    body: str,
    query_string: str,
    attempt: int,
    excluded_providers: list[str],
    wait_count: int,
) -> dict:
    return {
        "body": body,
        "query_string": query_string,
        "attempt": attempt,
        "excluded_providers": excluded_providers,
        "wait_count": wait_count,
    }


@celery_app.task(bind=True, name="gateway.tasks.process_request")
def process_request(
    self,
    body: str,
    query_string: str = "",
    attempt: int = 1,
    excluded_providers: list[str] | None = None,
    wait_count: int = 0,
):
    job_id = self.request.id
    client = get_redis()
    excluded = excluded_providers or []

    provider = select_provider(excluded, client)

    if provider is None:
        if wait_count >= settings.max_provider_waits:
            release_slot(client, inflight_key(), job_id)
            return {
                "status": "failed",
                "error": "No providers available",
                "attempts": attempt,
            }

        logger.warning(
            "job_id=%s all providers cooling down, waiting",
            job_id,
        )
        schedule_retry(
            self,
            retry_kwargs(body, query_string, attempt, [], wait_count + 1),
            attempt,
        )
        raise Ignore()

    slot_key = provider_slot_key(provider.name)

    if not acquire_slot(
        client,
        slot_key,
        job_id,
        PROVIDER_LIMITS[provider.name],
        int(settings.provider_timeout_seconds) + 10,
    ):
        if wait_count >= settings.max_provider_waits:
            release_slot(client, inflight_key(), job_id)
            return {
                "status": "failed",
                "error": f"{provider.name} is busy",
                "attempts": attempt,
            }

        logger.warning(
            "job_id=%s provider=%s at concurrency limit, trying next",
            job_id,
            provider.name,
        )
        schedule_retry(
            self,
            retry_kwargs(
                body,
                query_string,
                attempt,
                excluded + [provider.name],
                wait_count + 1,
            ),
            attempt,
        )
        raise Ignore()

    url = f"{provider.base_url}/v1/process"

    if query_string:
        url += f"?{query_string}"

    logger.info(
        "job_id=%s provider=%s attempt=%s calling provider",
        job_id,
        provider.name,
        attempt,
    )

    try:
        with httpx.Client(
            timeout=settings.provider_timeout_seconds,
        ) as http_client:
            response = http_client.post(
                url,
                content=body.encode(),
            )

        if response.status_code == 429 or response.status_code >= 500:
            raise httpx.HTTPStatusError(
                f"Provider returned {response.status_code}",
                request=response.request,
                response=response,
            )

        record_provider_success(client, provider.name)

        logger.info(
            "job_id=%s provider=%s attempt=%s status=%s",
            job_id,
            provider.name,
            attempt,
            response.status_code,
        )

        result = {
            "status": "completed",
            "provider": provider.name,
            "status_code": response.status_code,
            "body": response.json(),
        }
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning(
            "job_id=%s provider=%s attempt=%s failed: %s",
            job_id,
            provider.name,
            attempt,
            exc,
        )
        record_provider_failure(client, provider.name)

        if attempt < MAX_ATTEMPTS:
            schedule_retry(
                self,
                retry_kwargs(
                    body,
                    query_string,
                    attempt + 1,
                    excluded + [provider.name],
                    0,
                ),
                attempt,
            )
            raise Ignore()

        result = {
            "status": "failed",
            "error": str(exc),
            "attempts": attempt,
        }
    except Exception as exc:
        logger.exception(
            "job_id=%s provider=%s attempt=%s unexpected failure",
            job_id,
            provider.name,
            attempt,
        )
        record_provider_failure(client, provider.name)

        if attempt < MAX_ATTEMPTS:
            schedule_retry(
                self,
                retry_kwargs(
                    body,
                    query_string,
                    attempt + 1,
                    excluded + [provider.name],
                    0,
                ),
                attempt,
            )
            raise Ignore()

        result = {
            "status": "failed",
            "error": str(exc),
            "attempts": attempt,
        }
    finally:
        release_slot(client, slot_key, job_id)

    # Only terminal outcomes reach here; retries raise Ignore above.
    release_slot(client, inflight_key(), job_id)
    return result
