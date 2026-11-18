
import asyncio
import time
from collections import Counter

import httpx


GATEWAY_URL = "http://localhost:8000/v1/process"

TOTAL_REQUESTS = 100
CONCURRENCY = 20


async def send_request(client, request_id):
    start = time.perf_counter()

    try:
        response = await client.post(
            GATEWAY_URL,
            json={"request_id": request_id, "message": "Hello Gateway"},
        )

        elapsed = time.perf_counter() - start

        try:
            body = response.json()
        except ValueError:
            body = response.text

        return {
            "id": request_id,
            "status": response.status_code,
            "elapsed": elapsed,
            "provider": body.get("provider")
            if isinstance(body, dict) else None,
            "error": None,
        }

    except httpx.RequestError as exc:
        return {
            "id": request_id,
            "status": None,
            "elapsed": time.perf_counter() - start,
            "provider": None,
            "error": str(exc),
        }


async def main():
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient(timeout=15.0) as client:

        async def limited_request(request_id):
            async with semaphore:
                return await send_request(client, request_id)

        start = time.perf_counter()

        results = await asyncio.gather(
            *[
                limited_request(i)
                for i in range(1, TOTAL_REQUESTS + 1)
            ]
        )

        total_time = time.perf_counter() - start

    # Statistics
    statuses = Counter(r["status"] for r in results)
    providers = Counter(
        r["provider"] for r in results if r["provider"]
    )

    successful = [
        r for r in results
        if r["status"] is not None and 200 <= r["status"] < 300
    ]

    failed = [
        r for r in results
        if r["status"] is None or r["status"] >= 400
    ]

    latencies = sorted(r["elapsed"] for r in results)

    def percentile(values, p):
        index = int((len(values) - 1) * p)
        return values[index]

    print("\n========== GATEWAY TEST RESULTS ==========")
    print(f"Total requests:     {TOTAL_REQUESTS}")
    print(f"Concurrency:        {CONCURRENCY}")
    print(f"Total time:         {total_time:.2f}s")
    print(f"Throughput:         {TOTAL_REQUESTS / total_time:.2f} req/s")
    print(f"Successful:         {len(successful)}")
    print(f"Failed:             {len(failed)}")

    print("\nStatus codes:")
    for status, count in statuses.items():
        print(f"  {status}: {count}")

    print("\nProvider distribution:")
    for provider, count in providers.items():
        print(f"  {provider}: {count}")

    if latencies:
        print("\nLatency:")
        print(f"  Average: {sum(latencies) / len(latencies):.3f}s")
        print(f"  P50:     {percentile(latencies, 0.50):.3f}s")
        print(f"  P95:     {percentile(latencies, 0.95):.3f}s")
        print(f"  P99:     {percentile(latencies, 0.99):.3f}s")

    if failed:
        print("\nFailed requests (first 10):")
        for result in failed[:10]:
            print(
                f"  Request {result['id']}: "
                f"status={result['status']}, "
                f"error={result['error']}"
            )


if __name__ == "__main__":
    asyncio.run(main())
