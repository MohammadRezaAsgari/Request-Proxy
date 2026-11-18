import logging

import httpx
from celery.exceptions import Ignore

from gateway.celery_app import celery_app
from gateway.providers.registry import Provider
from gateway.routing.selector import selector

logger = logging.getLogger("gateway")

MAX_ATTEMPTS = 3
PROVIDER_TIMEOUT_SECONDS = 15.0


def select_provider(excluded_providers: list[str]) -> Provider:
    for _ in range(len(selector.providers)):
        provider = selector.select()

        if provider.name not in excluded_providers:
            return provider

    raise RuntimeError("No providers available for this job")


def schedule_retry(task, retry_kwargs: dict) -> None:
    process_request.apply_async(
        kwargs=retry_kwargs,
        task_id=task.request.id,
        queue="gateway.process.retry",
    )

    # Custom (non-exception) state so clients see "retrying" while the
    # message waits in the TTL queue. Celery's built-in RETRY state is
    # an exception state and cannot hold plain metadata.
    task.update_state(
        state="RETRYING",
        meta={"attempt": retry_kwargs["attempt"]},
    )


@celery_app.task(bind=True, name="gateway.tasks.process_request")
def process_request(
    self,
    body: str,
    query_string: str = "",
    attempt: int = 1,
    excluded_providers: list[str] | None = None,
):
    excluded_providers = excluded_providers or []

    try:
        provider = select_provider(excluded_providers)
    except RuntimeError as exc:
        return {
            "status": "failed",
            "error": str(exc),
            "attempts": attempt,
        }

    url = f"{provider.base_url}/v1/process"

    if query_string:
        url += f"?{query_string}"

    logger.info(
        "job_id=%s provider=%s attempt=%s calling provider",
        self.request.id,
        provider.name,
        attempt,
    )

    try:
        with httpx.Client(
            timeout=PROVIDER_TIMEOUT_SECONDS,
        ) as client:
            response = client.post(
                url,
                content=body.encode(),
            )

        if response.status_code == 429 or response.status_code >= 500:
            raise httpx.HTTPStatusError(
                f"Provider returned {response.status_code}",
                request=response.request,
                response=response,
            )

        logger.info(
            "job_id=%s provider=%s attempt=%s status=%s",
            self.request.id,
            provider.name,
            attempt,
            response.status_code,
        )

        return {
            "status": "completed",
            "provider": provider.name,
            "status_code": response.status_code,
            "body": response.json(),
        }
    except (
        httpx.TimeoutException,
        httpx.RequestError,
        httpx.HTTPStatusError,
    ) as exc:
        logger.warning(
            "job_id=%s provider=%s attempt=%s failed: %s",
            self.request.id,
            provider.name,
            attempt,
            exc,
        )

        if attempt < MAX_ATTEMPTS:
            schedule_retry(
                self,
                {
                    "body": body,
                    "query_string": query_string,
                    "attempt": attempt + 1,
                    "excluded_providers": (
                        excluded_providers + [provider.name]
                    ),
                },
            )

            raise Ignore()

        return {
            "status": "failed",
            "error": str(exc),
            "attempts": attempt,
        }
