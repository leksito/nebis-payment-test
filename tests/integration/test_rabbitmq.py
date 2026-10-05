"""RabbitMQ adapters against FastStream's in-memory TestRabbitBroker."""

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from unittest.mock import AsyncMock

import pytest
from faststream.rabbit import RabbitBroker, RabbitMessage, TestRabbitBroker
from faststream.rabbit.message import RabbitMessage as RabbitMessageImpl

from app.adapters.rabbitmq.consumer import ATTEMPT_HEADER, ERROR_HEADER, register_consumer
from app.adapters.rabbitmq.topology import (
    PAYMENTS_NEW,
    RetryPolicy,
    create_broker,
    dead_letter_exchange,
    dead_letter_queue,
    payments_exchange,
    retry_exchange,
)
from app.application.errors import NotificationError, PaymentNotFoundError

RETRY = RetryPolicy(max_attempts=3, base_delay_ms=1000)


@dataclass
class StubProcess:
    error: Exception | None = None
    calls: list[uuid.UUID] = field(default_factory=list)

    async def __call__(self, payment_id: uuid.UUID) -> None:
        self.calls.append(payment_id)
        if self.error:
            raise self.error


@dataclass
class ConsumerHarness:
    broker: RabbitBroker
    process: StubProcess
    retried: list[RabbitMessage]
    dead: list[RabbitMessage]

    async def deliver(self, payment_id: uuid.UUID, attempt: int | None = None) -> None:
        await self.broker.publish(
            {"payment_id": str(payment_id)},
            exchange=payments_exchange,
            routing_key=PAYMENTS_NEW,
            headers={ATTEMPT_HEADER: attempt} if attempt else None,
        )


@pytest.fixture
async def consumer() -> AsyncIterator[ConsumerHarness]:
    broker = create_broker("amqp://unused")
    process = StubProcess()
    register_consumer(broker, process, RETRY)

    # Capture what the consumer sends to the retry queues and the DLQ
    retried: list[RabbitMessage] = []
    dead: list[RabbitMessage] = []
    for queue in RETRY.queues():

        @broker.subscriber(queue, retry_exchange)
        async def on_retry(message: RabbitMessage) -> None:
            retried.append(message)

    @broker.subscriber(dead_letter_queue, dead_letter_exchange)
    async def on_dead(message: RabbitMessage) -> None:
        dead.append(message)

    async with TestRabbitBroker(broker):
        yield ConsumerHarness(broker, process, retried, dead)


async def test_successful_processing_is_acked(consumer: ConsumerHarness) -> None:
    payment_id = uuid.uuid4()

    await consumer.deliver(payment_id)

    assert consumer.process.calls == [payment_id]
    assert consumer.retried == consumer.dead == []


@pytest.mark.parametrize(
    ("attempt", "queue"),
    [(1, "payments.new.retry.1000ms"), (2, "payments.new.retry.2000ms")],
)
async def test_failure_is_retried_with_exponential_backoff(
    consumer: ConsumerHarness, attempt: int, queue: str
) -> None:
    consumer.process.error = NotificationError("webhook responded with HTTP 503")

    await consumer.deliver(uuid.uuid4(), attempt=attempt)

    [retry] = consumer.retried
    assert retry.raw_message.routing_key == queue
    assert retry.headers[ATTEMPT_HEADER] == attempt + 1
    assert retry.headers[ERROR_HEADER] == "NotificationError: webhook responded with HTTP 503"
    assert consumer.dead == []


async def test_last_failed_attempt_goes_to_dlq(consumer: ConsumerHarness) -> None:
    payment_id = uuid.uuid4()
    consumer.process.error = NotificationError("webhook responded with HTTP 500")

    await consumer.deliver(payment_id, attempt=3)

    assert consumer.retried == []
    [dead] = consumer.dead
    assert await dead.decode() == {"payment_id": str(payment_id)}
    assert dead.headers[ATTEMPT_HEADER] == 3
    assert dead.headers[ERROR_HEADER] == "NotificationError: webhook responded with HTTP 500"
    assert "x-failed-at" in dead.headers


async def test_non_retryable_error_goes_straight_to_dlq(consumer: ConsumerHarness) -> None:
    consumer.process.error = PaymentNotFoundError("gone")

    await consumer.deliver(uuid.uuid4())

    assert consumer.retried == []
    [dead] = consumer.dead
    assert dead.headers[ATTEMPT_HEADER] == 1


async def test_failed_retry_publish_requeues_instead_of_dead_lettering(
    consumer: ConsumerHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    consumer.process.error = NotificationError("webhook responded with HTTP 503")
    publish = consumer.broker.publish

    async def publish_failing_retries(*args: object, **kwargs: object) -> object:
        if kwargs.get("exchange") is retry_exchange:
            raise ConnectionError("channel closed")
        return await publish(*args, **kwargs)

    monkeypatch.setattr(consumer.broker, "publish", publish_failing_retries)
    nack = AsyncMock()
    monkeypatch.setattr(RabbitMessageImpl, "nack", nack)

    await consumer.deliver(uuid.uuid4())

    assert consumer.retried == consumer.dead == [], "not routed to the DLQ, attempts are kept"
    nack.assert_awaited_once_with(requeue=True)
