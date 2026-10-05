import uuid
from dataclasses import replace
from decimal import Decimal
from typing import Self
from unittest.mock import ANY

import pytest

from app.application.errors import IdempotencyConflictError, PaymentNotFoundError
from app.application.payments import CreatePaymentCommand, PaymentService
from app.domain.events import PaymentCreated
from app.domain.payment import Currency, Money, PaymentStatus
from tests.unit.fakes import FakeDatabase, FakeUnitOfWork

COMMAND = CreatePaymentCommand(
    idempotency_key="key-1",
    amount=Decimal("100.50"),
    currency=Currency.RUB,
    description="Order #42",
    metadata={"order_id": 42},
    webhook_url="https://example.com/hook",
)


@pytest.fixture
def db() -> FakeDatabase:
    return FakeDatabase()


@pytest.fixture
def service(db: FakeDatabase) -> PaymentService:
    return PaymentService(lambda: FakeUnitOfWork(db))


async def test_creates_pending_payment_and_raises_payment_created(
    db: FakeDatabase, service: PaymentService
) -> None:
    result = await service.create(COMMAND)

    assert result.created
    payment = db.payments[result.payment.id]
    assert payment.status is PaymentStatus.PENDING
    assert payment.money == Money(Decimal("100.50"), Currency.RUB)
    assert payment.idempotency_key == "key-1"
    assert db.events == [PaymentCreated(payment_id=payment.id, event_id=ANY, occurred_at=ANY)]


async def test_same_request_returns_existing_payment(
    db: FakeDatabase, service: PaymentService
) -> None:
    first = await service.create(COMMAND)
    # 100.5 and 100.50 are the same amount, so this is the same request
    second = await service.create(replace(COMMAND, amount=Decimal("100.5")))

    assert not second.created
    assert second.payment == first.payment
    assert len(db.payments) == 1
    assert len(db.events) == 1


@pytest.mark.parametrize(
    "change",
    [
        {"amount": Decimal("1.00")},
        {"currency": Currency.USD},
        {"description": "other"},
        {"metadata": {}},
        {"webhook_url": "https://other.example/hook"},
    ],
)
async def test_same_key_with_different_request_conflicts(
    db: FakeDatabase, service: PaymentService, change: dict
) -> None:
    await service.create(COMMAND)

    with pytest.raises(IdempotencyConflictError):
        await service.create(replace(COMMAND, **change))
    assert len(db.payments) == 1


async def test_lost_race_returns_the_winning_payment(db: FakeDatabase) -> None:
    # The same request, created by a concurrent call
    winner = (await PaymentService(lambda: FakeUnitOfWork(FakeDatabase())).create(COMMAND)).payment

    class RacingUnitOfWork(FakeUnitOfWork):
        async def __aenter__(self) -> Self:
            await super().__aenter__()
            # The concurrent call commits after our snapshot was taken: we don't see it
            # on lookup, but the unique key rejects our insert
            db.payments.setdefault(winner.id, winner)
            return self

    result = await PaymentService(lambda: RacingUnitOfWork(db)).create(COMMAND)

    assert not result.created
    assert result.payment == winner
    assert list(db.payments) == [winner.id]


async def test_get_returns_the_payment(service: PaymentService) -> None:
    created = await service.create(COMMAND)

    assert await service.get(created.payment.id) == created.payment


async def test_get_unknown_payment(service: PaymentService) -> None:
    with pytest.raises(PaymentNotFoundError):
        await service.get(uuid.uuid4())
