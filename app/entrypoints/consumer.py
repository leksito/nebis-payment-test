"""Composition root of the consumer: `python -m app.entrypoints.consumer`."""

import asyncio
from functools import partial

import httpx
from faststream import FastStream

from app.adapters.gateway import EmulatedPaymentGateway
from app.adapters.postgres.uow import PgUnitOfWork, create_engine
from app.adapters.rabbitmq.consumer import register_consumer
from app.adapters.rabbitmq.topology import RetryPolicy, create_broker, declare_topology
from app.adapters.webhooks import HttpWebhookNotifier
from app.application.processing import PaymentProcessingService
from app.config import Settings, get_settings
from app.log import setup_logging


def build_app(settings: Settings) -> FastStream:
    engine = create_engine(settings.database_url)
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)
    processing = PaymentProcessingService(
        uow=partial(PgUnitOfWork, engine),
        gateway=EmulatedPaymentGateway(
            min_delay=settings.gateway_min_delay,
            max_delay=settings.gateway_max_delay,
            success_rate=settings.gateway_success_rate,
        ),
        notifier=HttpWebhookNotifier(http_client),
    )
    retry_policy = RetryPolicy(settings.max_attempts, settings.retry_base_delay_ms)

    broker = create_broker(settings.rabbitmq_url, prefetch_count=settings.consumer_concurrency)
    register_consumer(broker, processing.process, retry_policy)
    app = FastStream(broker)

    @app.on_startup
    async def setup_topology() -> None:
        await broker.connect()
        await declare_topology(broker, retry_policy)

    @app.after_shutdown
    async def close_resources() -> None:
        await http_client.aclose()
        await engine.dispose()

    return app


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    asyncio.run(build_app(settings).run())


if __name__ == "__main__":
    main()
