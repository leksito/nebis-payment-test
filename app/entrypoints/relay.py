"""Composition root of the outbox relay: `python -m app.entrypoints.relay`.

Several replicas can run side by side: outbox rows are claimed with SKIP LOCKED.
"""

import asyncio
import contextlib
import logging
import signal

from app.adapters.outbox_relay import OutboxRelay
from app.adapters.postgres.uow import create_engine
from app.adapters.rabbitmq.topology import RetryPolicy, create_broker, declare_topology
from app.config import Settings, get_settings
from app.log import setup_logging

logger = logging.getLogger(__name__)


async def run_forever(relay: OutboxRelay, stop: asyncio.Event, poll_interval: float) -> None:
    while not stop.is_set():
        try:
            published = await relay.publish_batch()
        except Exception:
            logger.exception("Outbox relay iteration failed")
            published = 0
        # A full batch means there is probably more waiting: go again right away
        if published < relay.batch_size:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=poll_interval)


async def run(settings: Settings) -> None:
    engine = create_engine(settings.database_url)
    broker = create_broker(settings.rabbitmq_url)
    relay = OutboxRelay(engine, broker, settings.outbox_batch_size)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await broker.start()
    try:
        await declare_topology(
            broker, RetryPolicy(settings.max_attempts, settings.retry_base_delay_ms)
        )
        logger.info("Outbox relay started")
        await run_forever(relay, stop, settings.outbox_poll_interval)
    finally:
        await broker.stop()
        await engine.dispose()
        logger.info("Outbox relay stopped")


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
