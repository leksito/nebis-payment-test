import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.application.errors import NotificationError, PaymentNotFoundError
from app.application.processing import PaymentProcessingService
from app.domain.payment import ChargeOutcome, Currency, Money, Payment, PaymentStatus
from tests.unit.fakes import FakeDatabase, FakeGateway, FakeNotifier, FakeUnitOfWork


@pytest.fixture
def db() -> FakeDatabase:
    return FakeDatabase()


@pytest.fixture
def payment(db: FakeDatabase) -> Payment:
    payment = Payment.create(
        money=Money(Decimal("10.00"), Currency.USD),
        description="",
        metadata={},
        webhook_url="https://example.com/hook",
        idempotency_key="key",
        request_fingerprint="fp",
        at=datetime.now(UTC),
    )
    payment.pull_events()
    db.payments[payment.id] = payment
    return payment


@dataclass
class Setup:
    service: PaymentProcessingService
    gateway: FakeGateway
    notifier: FakeNotifier


@pytest.fixture
def setup(db: FakeDatabase) -> Setup:
    gateway, notifier = FakeGateway(), FakeNotifier()
    return Setup(
        PaymentProcessingService(lambda: FakeUnitOfWork(db), gateway, notifier), gateway, notifier
    )


@pytest.mark.parametrize(
    ("outcome", "status"),
    [
        (ChargeOutcome.APPROVED, PaymentStatus.SUCCEEDED),
        (ChargeOutcome.DECLINED, PaymentStatus.FAILED),
    ],
)
async def test_charges_saves_result_and_notifies(
    db: FakeDatabase,
    setup: Setup,
    payment: Payment,
    outcome: ChargeOutcome,
    status: PaymentStatus,
) -> None:
    setup.gateway.outcome = outcome

    await setup.service.process(payment.id)

    assert setup.gateway.calls == [payment.id]
    stored = db.payments[payment.id]
    assert stored.status is status
    assert stored.processed_at is not None
    [notified] = setup.notifier.notified
    assert notified.status is status


async def test_already_processed_payment_is_not_charged_again(
    db: FakeDatabase, setup: Setup, payment: Payment
) -> None:
    await setup.service.process(payment.id)
    await setup.service.process(payment.id)

    assert setup.gateway.calls == [payment.id]
    assert len(setup.notifier.notified) == 2


async def test_failed_notification_keeps_the_charge_result(
    db: FakeDatabase, setup: Setup, payment: Payment
) -> None:
    setup.notifier.fail = True

    with pytest.raises(NotificationError):
        await setup.service.process(payment.id)

    assert db.payments[payment.id].status is PaymentStatus.SUCCEEDED


async def test_unknown_payment(setup: Setup) -> None:
    with pytest.raises(PaymentNotFoundError):
        await setup.service.process(uuid.uuid4())
    assert setup.gateway.calls == []
    assert setup.notifier.notified == []


async def test_concurrent_duplicate_does_not_overwrite_first_result(
    db: FakeDatabase, setup: Setup, payment: Payment
) -> None:
    class SlowGateway(FakeGateway):
        async def charge(self, idempotency_key: uuid.UUID, money: Money) -> ChargeOutcome:
            outcome = await super().charge(idempotency_key, money)
            # While we wait for the gateway, a duplicate delivery finishes first
            db.payments[idempotency_key].apply_charge(outcome, at=datetime.now(UTC))
            return outcome

    gateway = SlowGateway(outcome=ChargeOutcome.DECLINED)
    service = PaymentProcessingService(lambda: FakeUnitOfWork(db), gateway, setup.notifier)

    await service.process(payment.id)

    assert gateway.charges == {payment.id: ChargeOutcome.DECLINED}
    assert db.payments[payment.id].status is PaymentStatus.FAILED
    [notified] = setup.notifier.notified
    assert notified.status is PaymentStatus.FAILED
