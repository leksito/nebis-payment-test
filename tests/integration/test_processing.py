import asyncio
import uuid
from datetime import UTC, datetime
from decimal import Decimal

from app.application.ports import UnitOfWorkFactory
from app.application.processing import PaymentProcessingService
from app.domain.payment import ChargeOutcome, Currency, Money, Payment
from tests.unit.fakes import FakeGateway, FakeNotifier


class SlowGateway(FakeGateway):
    """Slow enough that two concurrent deliveries both see the payment as pending."""

    async def charge(self, idempotency_key: uuid.UUID, money: Money) -> ChargeOutcome:
        await asyncio.sleep(0.1)
        return await super().charge(idempotency_key, money)


async def test_duplicate_deliveries_charge_once(uow: UnitOfWorkFactory) -> None:
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

    gateway = SlowGateway(outcome=ChargeOutcome.DECLINED)
    notifier = FakeNotifier()
    service = PaymentProcessingService(uow, gateway, notifier)

    # The same message delivered twice at once (e.g. the relay re-published it)
    await asyncio.gather(service.process(payment.id), service.process(payment.id))

    assert gateway.calls == [payment.id, payment.id], "both deliveries reach the gateway"
    assert gateway.charges == {payment.id: ChargeOutcome.DECLINED}, "but it charges once"
    async with uow() as tx:
        stored = await tx.payments.get(payment.id)
    assert stored is not None
    assert [p.status for p in notifier.notified] == [stored.status, stored.status]
