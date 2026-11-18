import asyncio

from celery.result import AsyncResult
from fastapi import APIRouter, HTTPException, Request

from gateway.celery_app import celery_app

router = APIRouter(tags=["Jobs"])


@router.get("/jobs/{job_id}")
async def get_job(job_id: str, request: Request):
    redis = request.app.state.redis

    exists = await redis.exists(f"gateway:job:{job_id}")

    if not exists:
        raise HTTPException(
            status_code=404,
            detail="Job not found",
        )

    result = AsyncResult(job_id, app=celery_app)

    state = await asyncio.to_thread(lambda: result.state)

    if state == "PENDING":
        return {
            "job_id": job_id,
            "status": "queued",
        }

    if state == "STARTED":
        return {
            "job_id": job_id,
            "status": "processing",
        }

    if state in ("RETRY", "RETRYING"):
        return {
            "job_id": job_id,
            "status": "retrying",
        }

    if state == "SUCCESS":
        task_result = await asyncio.to_thread(
            lambda: result.result
        )

        if task_result.get("status") == "failed":
            return {
                "job_id": job_id,
                "status": "failed",
                "error": task_result.get("error"),
                "result": task_result,
            }

        return {
            "job_id": job_id,
            "status": "completed",
            "result": task_result,
        }

    if state == "FAILURE":
        return {
            "job_id": job_id,
            "status": "failed",
            "error": "Task execution failed",
        }

    return {
        "job_id": job_id,
        "status": state.lower(),
    }
