"""Benchmark the async gateway: submit jobs, then poll them to completion.

Usage:
    python test_gateway.py
    python test_gateway.py --requests 200 --concurrency 40
"""

import argparse
import asyncio
import os
import time
import uuid
from collections import Counter

import httpx

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8000")
PROCESS_URL = f"{GATEWAY_URL}/v1/process"
POLL_INTERVAL = 0.5
MAX_RATE_LIMIT_RETRIES = 30

# Every run uses fresh idempotency keys so repeated runs measure real work
# instead of hitting the previous run's 24h idempotency records.
RUN_ID = uuid.uuid4().hex[:8]


def percentile(values: list[float], p: float) -> float:
    index = int((len(values) - 1) * p)
    return values[index]


async def submit_job(client, semaphore, request_id: int) -> dict:
    async with semaphore:
        start = time.perf_counter()
        rate_limited = 0
        response = None
        error = None

        for _ in range(MAX_RATE_LIMIT_RETRIES + 1):
            try:
                response = await client.post(
                    PROCESS_URL,
                    json={
                        "request_id": request_id,
                        "message": "Hello Gateway",
                    },
                    headers={
                        "Idempotency-Key": (
                            f"bench-{RUN_ID}-{request_id}"
                        )
                    },
                )
            except httpx.RequestError as exc:
                error = str(exc)
                break

            if response.status_code == 429:
                rate_limited += 1
                await asyncio.sleep(
                    float(response.headers.get("Retry-After", "1"))
                )
                continue

            break

        if response is None or response.status_code == 429:
            return {
                "id": request_id,
                "status_code": (
                    response.status_code if response else None
                ),
                "elapsed": time.perf_counter() - start,
                "submitted_at": time.perf_counter(),
                "job_id": None,
                "duplicate": None,
                "rate_limited": rate_limited,
                "error": error or "rate limit retries exhausted",
            }

        body = response.json() if response.status_code < 500 else {}

        return {
            "id": request_id,
            "status_code": response.status_code,
            "elapsed": time.perf_counter() - start,
            "submitted_at": time.perf_counter(),
            "job_id": body.get("job_id"),
            "duplicate": body.get("duplicate"),
            "rate_limited": rate_limited,
            "error": None,
        }


async def poll_job(
    client,
    semaphore,
    job_id: str,
    deadline: float,
) -> dict:
    async with semaphore:
        saw_retry = False

        while time.perf_counter() < deadline:
            try:
                response = await client.get(
                    f"{GATEWAY_URL}/jobs/{job_id}",
                )
            except httpx.RequestError:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            if response.status_code != 200:
                return {
                    "status": "error",
                    "saw_retry": saw_retry,
                    "status_code": response.status_code,
                    "finished_at": time.perf_counter(),
                }

            data = response.json()
            status = data.get("status")

            if status == "retrying":
                saw_retry = True

            if status in ("completed", "failed"):
                return {
                    "status": status,
                    "saw_retry": saw_retry,
                    "result": data.get("result") or {},
                    "error": data.get("error"),
                    "finished_at": time.perf_counter(),
                }

            await asyncio.sleep(POLL_INTERVAL)

        return {
            "status": "timeout",
            "saw_retry": saw_retry,
            "finished_at": time.perf_counter(),
        }


async def report_progress(
    counter: dict,
    total: int,
    done: asyncio.Event,
) -> None:
    start = time.perf_counter()

    while not done.is_set():
        try:
            await asyncio.wait_for(done.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            pass

        if done.is_set():
            return

        done_count = counter["done"]
        elapsed = time.perf_counter() - start
        print(f"  ... {done_count}/{total} jobs finished ({elapsed:.0f}s)")


async def idempotency_check(client) -> None:
    headers = {"Idempotency-Key": f"bench-idempotency-{RUN_ID}"}

    async def post(payload):
        for _ in range(MAX_RATE_LIMIT_RETRIES + 1):
            response = await client.post(
                PROCESS_URL,
                json=payload,
                headers=headers,
            )
            if response.status_code != 429:
                return response
            await asyncio.sleep(
                float(response.headers.get("Retry-After", "1"))
            )
        return response

    first = await post({"message": "idempotency"})
    first_body = first.json()

    same = await post({"message": "idempotency"})
    same_body = same.json()

    different = await post({"message": "different"})

    ok = (
        first.status_code == 202
        and same.status_code == 202
        and same_body.get("job_id") == first_body.get("job_id")
        and same_body.get("duplicate") is True
        and different.status_code == 409
    )

    print(f"Idempotency check:  {'PASS' if ok else 'FAIL'}")
    print(
        f"  submit: {first.status_code}, "
        f"repeat: {same.status_code} "
        f"(duplicate={same_body.get('duplicate')}), "
        f"changed body: {different.status_code}"
    )


async def main(
    total_requests: int,
    concurrency: int,
    timeout: float,
) -> None:
    submit_semaphore = asyncio.Semaphore(concurrency)
    poll_semaphore = asyncio.Semaphore(concurrency)

    limits = httpx.Limits(
        max_connections=concurrency + 10,
        max_keepalive_connections=concurrency,
    )

    async with httpx.AsyncClient(
        timeout=30.0,
        limits=limits,
    ) as client:
        print(f"Submitting {total_requests} jobs "
              f"(concurrency={concurrency})...")

        started_at = time.perf_counter()

        submissions = await asyncio.gather(
            *[
                submit_job(client, submit_semaphore, i)
                for i in range(1, total_requests + 1)
            ]
        )

        submit_time = time.perf_counter() - started_at
        print(f"Submit phase: {submit_time:.2f}s")

        accepted = [s for s in submissions if s["job_id"]]
        rejected = [s for s in submissions if not s["job_id"]]

        counter = {"done": 0}
        done = asyncio.Event()
        progress = asyncio.create_task(
            report_progress(counter, len(accepted), done),
        )

        deadline = time.perf_counter() + timeout

        async def tracked_poll(submission):
            outcome = await poll_job(
                client,
                poll_semaphore,
                submission["job_id"],
                deadline,
            )
            outcome["id"] = submission["id"]
            outcome["submitted_at"] = submission["submitted_at"]
            counter["done"] += 1
            return outcome

        print(f"Polling {len(accepted)} jobs "
              f"(timeout={timeout:.0f}s)...")

        outcomes = await asyncio.gather(
            *[tracked_poll(s) for s in accepted],
        )

        done.set()
        await progress

        total_time = time.perf_counter() - started_at

        await idempotency_check(client)

    submit_latencies = sorted(s["elapsed"] for s in submissions)

    job_statuses = Counter(o["status"] for o in outcomes)
    providers = Counter(
        o["result"].get("provider")
        for o in outcomes
        if o["result"].get("provider")
    )
    provider_statuses = Counter(
        o["result"].get("status_code")
        for o in outcomes
        if o["result"].get("status_code")
    )
    retried = sum(1 for o in outcomes if o["saw_retry"])
    with_body = sum(
        1 for o in outcomes
        if o["result"].get("body")
    )

    end_to_end = sorted(
        o["finished_at"] - o["submitted_at"] for o in outcomes
    )

    print("\n========== ASYNC GATEWAY BENCHMARK ==========")
    print(f"Run id:              {RUN_ID}")
    print(f"Jobs submitted:      {total_requests}")
    print(f"Accepted (202):      {len(accepted)}")
    print(f"Rejected:            {len(rejected)}")
    print(f"Duplicate jobs:      "
          f"{sum(1 for s in accepted if s.get('duplicate'))}")
    print(f"Rate-limited retries:"
          f" {sum(s.get('rate_limited', 0) for s in submissions)}")
    print(f"Concurrency:         {concurrency}")
    print(f"Total time:          {total_time:.2f}s")
    print(f"Submit throughput:   "
          f"{total_requests / submit_time:.2f} jobs/s")
    print(f"Job throughput:      "
          f"{len(outcomes) / total_time:.2f} jobs/s")

    print("\nSubmit status codes:")
    for status, count in Counter(
        s["status_code"] for s in submissions
    ).items():
        print(f"  {status}: {count}")

    print("\nJob statuses:")
    for status, count in job_statuses.items():
        print(f"  {status}: {count}")

    print(f"\nJobs that retried:   {retried}")
    print(f"Jobs with a body:    {with_body}")

    print("\nProvider distribution:")
    for provider, count in providers.most_common():
        print(f"  {provider}: {count}")

    print("\nProvider response status codes:")
    for status, count in sorted(provider_statuses.items()):
        print(f"  {status}: {count}")

    if submit_latencies:
        print("\nSubmit latency:")
        print(f"  Average: "
              f"{sum(submit_latencies) / len(submit_latencies):.3f}s")
        print(f"  P50:     "
              f"{percentile(submit_latencies, 0.50):.3f}s")
        print(f"  P95:     "
              f"{percentile(submit_latencies, 0.95):.3f}s")
        print(f"  P99:     "
              f"{percentile(submit_latencies, 0.99):.3f}s")

    if end_to_end:
        print("\nEnd-to-end latency (submit -> terminal):")
        print(f"  Average: "
              f"{sum(end_to_end) / len(end_to_end):.3f}s")
        print(f"  P50:     {percentile(end_to_end, 0.50):.3f}s")
        print(f"  P95:     {percentile(end_to_end, 0.95):.3f}s")
        print(f"  P99:     {percentile(end_to_end, 0.99):.3f}s")

    failures = [o for o in outcomes if o["status"] != "completed"]

    if failures:
        print("\nFailed jobs (first 10):")
        for outcome in failures[:10]:
            print(
                f"  Job {outcome['id']}: "
                f"status={outcome['status']}, "
                f"error={outcome.get('error')}"
            )

    if rejected:
        print("\nRejected submissions (first 10):")
        for submission in rejected[:10]:
            print(
                f"  Request {submission['id']}: "
                f"status={submission['status_code']}, "
                f"error={submission['error']}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=600.0)
    args = parser.parse_args()

    asyncio.run(
        main(
            total_requests=args.requests,
            concurrency=args.concurrency,
            timeout=args.timeout,
        )
    )
