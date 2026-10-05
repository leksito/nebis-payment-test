"""Outbox relay: moves committed domain events from the `outbox` table to RabbitMQ."""

import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker, RabbitExchange
from sqlalchemy import Row, select, update
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.tables import outbox
from app.adapters.rabbitmq.topology import PAYMENTS_NEW, payments_exchange
from app.domain.events import PaymentCreated

logger = logging.getLogger(__name__)

ROUTES: dict[str, tuple[RabbitExchange, str]] = {
    PaymentCreated.event_type: (payments_exchange, PAYMENTS_NEW),
}


class UnroutableEventError(Exception):
    pass


class OutboxRelay:
    """At-least-once delivery from the outbox table to the broker."""

    def __init__(
        self,
        engine: AsyncEngine,
        broker: RabbitBroker,
        batch_size: int,
    ) -> None:
        self._engine = engine
        self._broker = broker
        self.batch_size = batch_size

    async def publish_batch(self) -> int:
        """Publish up to batch_size pending events; returns how many were published."""
        published = 0
        async with self._engine.begin() as conn:
            rows = await conn.execute(
                select(outbox)
                .where(outbox.c.published_at.is_(None), outbox.c.failed_at.is_(None))
                .order_by(outbox.c.created_at)
                .limit(self.batch_size)
                .with_for_update(skip_locked=True)
            )

            for row in rows.all():
                mark = update(outbox).where(outbox.c.id == row.id)
                attempts = outbox.c.publish_attempts + 1

                try:
                    await self._publish(row)
                except UnroutableEventError as exc:
                    # Retrying cannot help: set it aside so it does not block the events after it
                    await conn.execute(
                        mark.values(
                            publish_attempts=attempts,
                            failed_at=datetime.now(UTC),
                            last_error=f"{type(exc).__name__}: {exc}",
                        )
                    )
                    logger.error("Outbox event %s can never be published: %s", row.id, exc)
                    continue
                except Exception as exc:
                    # Most likely the broker is unavailable: stop and retry on the next poll
                    error = f"{type(exc).__name__}: {exc}"
                    await conn.execute(mark.values(publish_attempts=attempts, last_error=error))
                    logger.warning("Failed to publish outbox event %s: %s", row.id, exc)
                    break

                await conn.execute(
                    mark.values(
                        publish_attempts=attempts,
                        published_at=datetime.now(UTC),
                        last_error=None,
                    )
                )
                published += 1

        if published:
            logger.info("Published %d outbox event(s)", published)

        return published

    async def _publish(self, row: Row) -> None:
        route = ROUTES.get(row.event_type)
        if route is None:
            raise UnroutableEventError(f"no route for event type {row.event_type!r}")

        exchange, routing_key = route
        await self._broker.publish(
            row.payload,
            exchange=exchange,
            routing_key=routing_key,
            message_id=str(row.id),
            headers={"x-event-type": row.event_type},
            persist=True,
        )
