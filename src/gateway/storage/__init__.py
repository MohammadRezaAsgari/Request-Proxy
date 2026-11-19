import logging
import os
from datetime import datetime, timezone

from pymongo import ASCENDING, MongoClient

from gateway.config import settings

logger = logging.getLogger("gateway")

# Prefork workers must not share a MongoClient across processes.
_clients: dict[int, MongoClient] = {}


def get_collection():
    pid = os.getpid()

    if pid not in _clients:
        _clients[pid] = MongoClient(settings.mongodb_url)

    return _clients[pid][settings.mongodb_db_name]["job_results"]


def ensure_indexes() -> None:
    """Create indexes once at process startup (not per request)."""
    collection = get_collection()
    collection.create_index([("job_id", ASCENDING)], unique=True)
    collection.create_index([("idempotency_key", ASCENDING)])
    collection.create_index(
        [("created_at", ASCENDING)],
        expireAfterSeconds=settings.mongodb_retention_seconds,
    )


def store_result(
    job_id: str,
    result: dict,
    *,
    idempotency_key: str,
    request_body: str,
    query_string: str,
    attempt: int,
) -> None:
    now = datetime.now(timezone.utc)

    document = {
        "job_id": job_id,
        "idempotency_key": idempotency_key,
        "request": {
            "body": request_body,
            "query_string": query_string,
        },
        "response": {
            "status": result.get("status"),
            "status_code": result.get("status_code"),
            "body": result.get("body"),
            "error": result.get("error"),
        },
        "provider": result.get("provider"),
        "attempt": attempt,
        "created_at": now,
    }

    try:
        get_collection().update_one(
            {"job_id": job_id},
            {"$set": document},
            upsert=True,
        )
    except Exception:
        logger.exception(
            "job_id=%s failed to persist result to MongoDB",
            job_id,
        )
