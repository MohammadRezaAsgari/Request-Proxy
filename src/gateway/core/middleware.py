import logging
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger("gateway")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id

        start = time.perf_counter()

        try:
            response = await call_next(request)
            duration = time.perf_counter() - start

            logger.info(
                "request_id=%s method=%s path=%s status=%s duration=%.3f",
                request_id,
                request.method,
                request.url.path,
                response.status_code,
                duration,
            )

            response.headers["X-Request-ID"] = request_id
            return response
        except Exception:
            duration = time.perf_counter() - start

            logger.exception(
                "request_id=%s method=%s path=%s duration=%.3f",
                request_id,
                request.method,
                request.url.path,
                duration,
            )
            raise
