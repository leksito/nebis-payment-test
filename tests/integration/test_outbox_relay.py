import uuid
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

from faststream.rabbit import RabbitMessage, TestRabbitBroker
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.outbox_relay import OutboxRelay
from app.adapters.postgres.tables import outbox
from app.adapters.rabbitmq.topology import create_broker, payments_exchange, payments_new_queue
from app.application.ports import UnitOfWorkFactory
from app.domain.payment import Currency, Money, Payment
from tests.integration.conftest import fetch_all


async def create_payments(uow: UnitOfWorkFactory, n: int) -> list[Payment]:
    payments = []
    for _ in range(n):
        payment = Payment.create(
            money=Money(Decimal("10.00"), Currency.USD),
            description="",
            metadata={},
            webhook_url="https://example.com/hook",
            idempotency_key=str(uuid.uuid4()),
            request_fingerprint="fp",
            at=datetime.now(UTC),
        )
        async with uow() as tx:
            await tx.payments.add(payment)
            await tx.commit()
        payments.append(payment)
    return payments


async def outbox_rows(engine: AsyncEngine) -> list:
    return await fetch_all(engine, outbox)


async def test_publishes_pending_events_in_batches_and_marks_them(
    uow: UnitOfWorkFactory, engine: AsyncEngine
) -> None:
    payments = await create_payments(uow, 3)
    broker = create_broker("amqp://unused")
    received: list[RabbitMessage] = []

    @broker.subscriber(payments_new_queue, payments_exchange)
    async def handler(message: RabbitMessage) -> None:
        received.append(message)

    async with TestRabbitBroker(broker):
        relay = OutboxRelay(engine, broker, batch_size=2)
        assert await relay.publish_batch() == 2
        assert await relay.publish_batch() == 1
        assert await relay.publish_batch() == 0

    records = await outbox_rows(engine)
    assert all(r.published_at is not None and r.publish_attempts == 1 for r in records)
    assert [m.message_id for m in received] == [str(r.id) for r in records]
    assert [m.headers["x-event-type"] for m in received] == ["payment.created"] * 3
    assert [await m.decode() for m in received] == [{"payment_id": str(p.id)} for p in payments]


async def test_keeps_events_when_broker_is_unavailable(
    uow: UnitOfWorkFactory, engine: AsyncEngine
) -> None:
    await create_payments(uow, 2)
    broker = AsyncMock()
    broker.publish.side_effect = ConnectionError("broker is down")
    relay = OutboxRelay(engine, broker, batch_size=10)

    assert await relay.publish_batch() == 0

    first, second = await outbox_rows(engine)
    assert first.published_at is None
    assert (first.publish_attempts, first.last_error) == (1, "ConnectionError: broker is down")
    # Stops at the first failure instead of hammering a dead broker with the whole batch
    assert (second.publish_attempts, second.last_error) == (0, None)

    broker.publish.side_effect = None
    assert await relay.publish_batch() == 2
    records = await outbox_rows(engine)
    assert all(r.published_at is not None and r.last_error is None for r in records)


async def test_unroutable_event_is_set_aside_without_blocking_the_rest(
    engine: AsyncEngine, uow: UnitOfWorkFactory
) -> None:
    async with engine.begin() as conn:
        await conn.execute(insert(outbox).values(id=uuid.uuid4(), event_type="unknown", payload={}))
    [payment] = await create_payments(uow, 1)  # its event comes after the poisoned one
    broker = AsyncMock()
    relay = OutboxRelay(engine, broker, batch_size=10)

    assert await relay.publish_batch() == 1
    assert await relay.publish_batch() == 0, "the poisoned event is not retried"

    poisoned, published = await outbox_rows(engine)
    assert poisoned.failed_at is not None
    assert poisoned.published_at is None
    assert poisoned.last_error == "UnroutableEventError: no route for event type 'unknown'"
    assert published.published_at is not None
    broker.publish.assert_awaited_once()
    assert broker.publish.await_args.args[0] == {"payment_id": str(payment.id)}


async def test_concurrent_relays_do_not_publish_the_same_event_twice(
    uow: UnitOfWorkFactory, engine: AsyncEngine
) -> None:
    await create_payments(uow, 1)
    blocked, other = AsyncMock(), AsyncMock()
    first = OutboxRelay(engine, blocked, batch_size=10)
    second = OutboxRelay(engine, other, batch_size=10)

    # While the first relay holds the row lock, the second one must skip the row
    async def publish_while_other_relay_runs(*args: object, **kwargs: object) -> None:
        assert await second.publish_batch() == 0

    blocked.publish.side_effect = publish_while_other_relay_runs

    assert await first.publish_batch() == 1
    other.publish.assert_not_awaited()
