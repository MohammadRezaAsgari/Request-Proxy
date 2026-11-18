import logging
import time
import uuid

from starlette.datastructures import MutableHeaders

logger = logging.getLogger("gateway")


class RequestLoggingMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = str(uuid.uuid4())
        scope.setdefault("state", {})["request_id"] = request_id

        start = time.perf_counter()
        status_code = 500

        async def send_with_request_id(message):
            nonlocal status_code

            if message["type"] == "http.response.start":
                status_code = message["status"]
                headers = MutableHeaders(scope=message)
                headers["X-Request-ID"] = request_id

            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        except Exception:
            duration = time.perf_counter() - start

            logger.exception(
                "request_id=%s method=%s path=%s duration=%.3f",
                request_id,
                scope["method"],
                scope["path"],
                duration,
            )
            raise

        duration = time.perf_counter() - start

        logger.info(
            "request_id=%s method=%s path=%s status=%s duration=%.3f",
            request_id,
            scope["method"],
            scope["path"],
            status_code,
            duration,
        )
