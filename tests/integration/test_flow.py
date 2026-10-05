"""The whole pipeline with real Postgres, the in-memory broker and a mocked webhook:
PaymentService.create -> outbox -> OutboxRelay -> consumer
-> PaymentProcessingService.process -> webhook.
"""

import json
import uuid
from decimal import Decimal

import httpx
from faststream.rabbit import TestRabbitBroker
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.outbox_relay import OutboxRelay
from app.adapters.rabbitmq.consumer import register_consumer
from app.adapters.rabbitmq.topology import RetryPolicy, create_broker
from app.adapters.webhooks import HttpWebhookNotifier
from app.application.payments import CreatePaymentCommand, PaymentService
from app.application.ports import UnitOfWorkFactory
from app.application.processing import PaymentProcessingService
from app.domain.payment import ChargeOutcome, Currency, Money, PaymentStatus


class DecliningGateway:
    async def charge(self, idempotency_key: uuid.UUID, money: Money) -> ChargeOutcome:
        return ChargeOutcome.DECLINED


async def test_payment_goes_through_the_whole_pipeline(
    uow: UnitOfWorkFactory, engine: AsyncEngine
) -> None:
    webhooks: list[httpx.Request] = []
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: webhooks.append(r) or httpx.Response(200))
    )
    broker = create_broker("amqp://unused")
    register_consumer(
        broker,
        PaymentProcessingService(uow, DecliningGateway(), HttpWebhookNotifier(http)).process,
        RetryPolicy(max_attempts=3, base_delay_ms=1000),
    )
    relay = OutboxRelay(engine, broker, batch_size=10)

    payments = PaymentService(uow)
    created = await payments.create(
        CreatePaymentCommand(
            idempotency_key=str(uuid.uuid4()),
            amount=Decimal("15.00"),
            currency=Currency.USD,
            webhook_url="https://client.example/hook",
        )
    )
    async with TestRabbitBroker(broker):
        assert await relay.publish_batch() == 1

    payment = await payments.get(created.payment.id)
    assert payment.status is PaymentStatus.FAILED
    assert payment.processed_at is not None

    [request] = webhooks
    assert str(request.url) == "https://client.example/hook"
    assert request.headers["X-Payment-Id"] == str(payment.id)
    body = json.loads(request.content)
    assert body["event"] == "payment.processed"
    assert body["payment"]["status"] == "failed"
    assert body["payment"]["amount"] == "15.00"
    # Same serialization as the API: datetimes in UTC with a "Z" suffix
    assert body["payment"]["processed_at"].endswith("Z")
    assert body["payment"]["created_at"].endswith("Z")
    await http.aclose()
