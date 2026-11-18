import os

from celery import Celery
from celery.signals import worker_ready
from kombu import Exchange, Queue

jobs_exchange = Exchange("gateway.jobs", type="direct", durable=True)
retry_exchange = Exchange("gateway.retry", type="direct", durable=True)
dead_exchange = Exchange("gateway.dead", type="direct", durable=True)

celery_app = Celery(
    "gateway",
    broker=os.environ["CELERY_BROKER_URL"],
    backend=os.environ["CELERY_RESULT_BACKEND"],
    include=["gateway.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # Default routing
    task_default_queue="gateway.process",
    task_default_exchange="gateway.jobs",
    task_default_exchange_type="direct",
    task_default_routing_key="process",
    # Explicit RabbitMQ topology
    task_queues=(
        Queue(
            "gateway.process",
            exchange=jobs_exchange,
            routing_key="process",
            durable=True,
            queue_arguments={
                "x-dead-letter-exchange": "gateway.dead",
                "x-dead-letter-routing-key": "process.dead",
            },
        ),
        Queue(
            "gateway.process.retry",
            exchange=retry_exchange,
            routing_key="process.retry",
            durable=True,
            queue_arguments={
                "x-message-ttl": 5000,
                "x-dead-letter-exchange": "gateway.jobs",
                "x-dead-letter-routing-key": "process",
            },
        ),
        Queue(
            "gateway.process.dead",
            exchange=dead_exchange,
            routing_key="process.dead",
            durable=True,
        ),
    ),
    task_routes={
        "gateway.tasks.process_request": {
            "queue": "gateway.process",
        },
    },
    # Reliability
    task_track_started=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_default_delivery_mode=2,
    broker_connection_retry_on_startup=True,
    # Results
    result_expires=86400,
)


@worker_ready.connect
def declare_topology(**_kwargs):
    with celery_app.connection_for_write() as connection:
        channel = connection.default_channel

        for queue in celery_app.amqp.queues.values():
            queue.declare(channel=channel)
