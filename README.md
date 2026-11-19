# Request Proxy Gateway

An async HTTP gateway that accepts requests, queues them, and proxies them to one of three upstream providers via Celery workers. Clients submit work and poll for results instead of waiting on a provider response.

Built with FastAPI, RabbitMQ, Celery (prefork workers), Redis, and MongoDB.

## How it works

1. **Submit** — `POST /v1/process` accepts a JSON body and requires an `Idempotency-Key` header. The gateway reserves the idempotency key in Redis, enforces rate limiting and a global in-flight cap, enqueues a Celery task to RabbitMQ, and returns `202 Accepted` with a `job_id`.
2. **Process** — Celery workers consume `gateway.process`, pick a provider (round-robin, skipping providers in cooldown or at their concurrency limit), and POST the body to `{provider}/v1/process` (preserving the original query string).
3. **Retry/failover** — On retryable failures (429, 5xx, timeouts, transport errors) the job is retried on a *different* provider (max 3 attempts total) through TTL retry queues with exponential backoff (5s → 10s → 20s). After repeated failures a provider is put into a short cooldown. Non-retryable 4xx responses are returned as-is.
4. **Poll** — `GET /jobs/{job_id}` returns `queued | processing | retrying | completed | failed` and, when terminal, the provider result.
5. **Persist** — On terminal completion/failure, workers write a job document (request, response, provider, attempt, timestamps) to MongoDB (`gateway.job_results`) with a TTL index for retention.

### Key behaviors

- **Idempotency (24h, Redis):** same key + same payload → `202` with the original `job_id` and `duplicate: true`; same key + different payload → `409`; missing key → `400`.
- **Ingress rate limiting (Redis token bucket):** per client (`X-API-Key` if present, otherwise client IP). Exceeded → `429` with `Retry-After` and `X-RateLimit-*` headers.
- **Backpressure:** global cap on queued+in-flight jobs. At capacity → `429 Gateway at capacity`.
- **Provider controls:** per-provider concurrency caps, failure-threshold cooldown, and exponential retry delays. Provider timeout is configurable.
- **Reliability:** durable RabbitMQ exchanges/queues, a delayed retry topology, a dead-letter queue, late acks, and `worker_prefetch_multiplier=1`. Task/result state is tracked in Redis (Celery result backend).

## Project layout

```
src/gateway/
  main.py                  FastAPI app and router wiring
  config.py                Settings loaded from .env
  celery_app.py            Celery app, exchanges/queues, retry topology
  tasks.py                 Provider call, retry/failover, persistence
  storage/                 MongoDB client, indexes, result writes
  api/routes/
    process.py             POST /v1/process (ingest, idempotency, enqueue)
    jobs.py                GET  /jobs/{job_id} (status/result polling)
    health.py              GET  /health, GET /ready
  core/
    rate_limit.py          Redis token-bucket rate limiting
    limits.py              In-flight/provider slot primitives (Lua scripts)
    lifespan.py            HTTP/Redis client lifecycle
    middleware.py, logging.py
  providers/registry.py    Provider definitions (from settings)
  routing/selector.py      Round-robin provider selection

mock_providers/            Simulated upstream providers (latency/errors/rate limits)
docker/Dockerfile          Gateway/worker image
docker-compose.yml         Full stack
test_gateway.py            Async submit/poll benchmark + idempotency check
.env                       Configuration (gitignored)
```

## Prerequisites

- Docker and Docker Compose (v2)
- A `.env` file in the project root (see [Configuration](#configuration)). The repository `.gitignore` excludes `.env`, so create/provide it locally.

## Running

Start the full stack (gateway, 3 mock providers, RabbitMQ, Redis, MongoDB, Mongo Express, and Celery workers):

```bash
docker compose up --build -d
```

Check status:

```bash
docker compose ps
```

Stop:

```bash
docker compose down
```

To remove persisted data as well (RabbitMQ/Redis/Mongo volumes):

```bash
docker compose down -v
```

### Services and ports (defaults, configurable in `.env`)

| Service | Port | Notes |
| --- | --- | --- |
| Gateway (FastAPI) | `8000` | `POST /v1/process`, `GET /jobs/{id}`, `GET /health`, `GET /ready` |
| RabbitMQ | `5672` | AMQP broker |
| RabbitMQ Management | `15672` | UI — login with `RABBITMQ_USER` / `RABBITMQ_PASSWORD` |
| Redis | `6379` | Idempotency, rate limits, inflight, Celery result backend |
| MongoDB | `27017` | Persistent job request/response storage |
| Mongo Express | `8081` | Web UI for MongoDB → database `gateway`, collection `job_results` |
| Mock providers A/B/C | internal | Reached by the gateway inside the Docker network |

## API

### `POST /v1/process`

Submit a job for async processing.

- **Header:** `Idempotency-Key: <unique-key>` (required). Same key + same payload within 24h returns the original job.
- **Body:** arbitrary JSON (forwarded as-is to the selected provider).
- **Query string:** preserved and forwarded to the provider.

```bash
curl -i -X POST "http://localhost:8000/v1/process" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-1" \
  -d '{"message":"hello"}'
```

Response — `202 Accepted`:

```json
{
  "job_id": "<uuid>",
  "status": "accepted",
  "duplicate": false
}
```

`Location: /jobs/<job_id>` is included. A repeated identical request returns the same `job_id` with `"duplicate": true`.

Common errors: `400` missing `Idempotency-Key`, `409` key reused with a different payload, `429` rate limited or gateway at capacity, `503` could not enqueue.

### `GET /jobs/{job_id}`

Poll job status/result.

```bash
curl "http://localhost:8000/jobs/<job_id>"
```

Statuses: `queued`, `processing`, `retrying`, `completed`, `failed`. When `completed`/`failed`, a `result` (and/or `error`) is included with provider details and the provider response body.

### `GET /health` and `GET /ready`

Liveness and readiness probes.

### `GET /`

Basic service info.

## Testing

### Benchmark / end-to-end check (`test_gateway.py`)

Submits jobs, polls them to completion, runs an idempotency check, and prints throughput/latency percentiles and provider distribution. Requires the stack to be running.

```bash
# from the host, using a local venv with httpx installed
python test_gateway.py
python test_gateway.py --requests 200 --concurrency 40 --timeout 600
```

Options:
- `--requests` total jobs to submit (default 100)
- `--concurrency` concurrent submit/poll operations (default 20)
- `--timeout` overall polling timeout in seconds (default 600)

The script targets `GATEWAY_URL` from the environment (defaults to `http://localhost:8000`) and uses a unique run id so repeated runs don't collide with prior idempotency records. Note: mock providers simulate latency/errors/rate limits, so a small percentage of jobs may fail after exhausting retries and end-to-end latency reflects provider behavior.

### Manual checks

- **RabbitMQ queues/topology:** http://localhost:15672
- **Stored job documents:** http://localhost:8081 → `gateway` → `job_results`
- **Logs:** `docker compose logs -f gateway` / `docker compose logs -f celery-worker`
- **Queue state:** `docker exec gateway-rabbitmq rabbitmqctl list_queues name messages`

## Configuration

All runtime configuration lives in `.env` (loaded by the app and Compose). Key groups:

| Group | Examples |
| --- | --- |
| App/gateway | `APP_NAME`, `APP_ENV`, `LOG_LEVEL`, `GATEWAY_PORT`, `GATEWAY_URL` |
| Upstream HTTP client | `UPSTREAM_TIMEOUT_SECONDS`, `UPSTREAM_MAX_CONNECTIONS`, `UPSTREAM_MAX_KEEPALIVE_CONNECTIONS` |
| Providers | `PROVIDER_A_URL`, `PROVIDER_B_URL`, `PROVIDER_C_URL` |
| Mock provider behavior | `PROVIDER_{A,B,C}_{MIN_LATENCY,MAX_LATENCY,ERROR_RATE,RATE_LIMIT}` |
| Broker / results | `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `CELERY_BROKER_URL`, `CELERY_RESULT_BACKEND` |
| Redis / MongoDB | `REDIS_URL`, `MONGODB_URL`, `MONGODB_DB_NAME`, `MONGODB_RETENTION_SECONDS` |
| Workers | `CELERY_WORKERS` (containers/replicas), `CELERY_CONCURRENCY` (prefork processes per container) |
| Rate limiting | `RATE_LIMIT_CAPACITY`, `RATE_LIMIT_REFILL_PER_SECOND` |
| Provider controls | `PROVIDER_TIMEOUT_SECONDS`, `PROVIDER_{A,B,C}_CONCURRENCY`, `PROVIDER_FAILURE_THRESHOLD`, `PROVIDER_COOLDOWN_SECONDS` |
| Backpressure | `MAX_INFLIGHT_JOBS`, `INFLIGHT_JOB_TTL_SECONDS`, `MAX_PROVIDER_WAITS` |
| Exposed ports | `GATEWAY_PORT`, `RABBITMQ_PORT`, `RABBITMQ_MANAGEMENT_PORT`, `REDIS_PORT`, `MONGODB_PORT`, `MONGO_EXPRESS_PORT` |

By default `PROVIDER_*_URL` targets the local mock containers. To run against external providers, point these URLs at them and (if desired) remove or stop the mock services.

## Notes / limitations

- **At-least-once execution:** jobs are acked late and may be reprocessed if a worker is lost; the system is not exactly-once end-to-end.
- **Crash window:** idempotency reservation (Redis) and task publish are not atomic — a crash between them can leave a reserved key without a running job until the reservation expires.
- **Persistence is best-effort:** a MongoDB outage is logged but does not fail an otherwise completed job.
- **No auth:** the gateway does not authenticate callers; `X-API-Key` is only a rate-limit identity, and rate limiting by IP trusts the direct peer (no `X-Forwarded-For` handling).
- **`/v1` prefix:** the process endpoint is mounted at `/v1/process` (as implemented); `GET /jobs/{id}` is unprefixed.

## Development (without full rebuild cycles)

The images bake in the source. For iterative changes you can rebuild a specific service, e.g.:

```bash
docker compose up -d --build gateway celery-worker
```

Or run the app locally against the containerized infra by pointing `.env` URLs at `localhost` ports and starting uvicorn/celery from a venv (ensure `PYTHONPATH=src` and dependencies from `requirements.txt`).
