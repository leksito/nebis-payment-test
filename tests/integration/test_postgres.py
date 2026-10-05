from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.tables import outbox
from app.application.errors import DuplicateIdempotencyKeyError
from app.application.ports import UnitOfWorkFactory
from app.domain.payment import ChargeOutcome, Currency, Money, Payment, PaymentStatus
from tests.integration.conftest import fetch_all


def new_payment(key: str = "key-1") -> Payment:
    return Payment.create(
        money=Money(Decimal("42.10"), Currency.EUR),
        description="Order",
        metadata={"nested": {"a": [1, 2]}},
        webhook_url="https://example.com/hook",
        idempotency_key=key,
        request_fingerprint="fp",
        at=datetime.now(UTC),
    )


async def saved_payment(uow: UnitOfWorkFactory) -> Payment:
    payment = new_payment()
    async with uow() as tx:
        await tx.payments.add(payment)
        await tx.commit()
    return payment


async def test_repository_round_trips_the_aggregate(uow: UnitOfWorkFactory) -> None:
    payment = await saved_payment(uow)

    async with uow() as tx:
        by_id = await tx.payments.get(payment.id)
        by_key = await tx.payments.get_by_idempotency_key("key-1")

    assert isinstance(by_id, Payment)
    assert by_id == by_key == payment
    assert by_id.money == payment.money
    assert by_id.metadata == payment.metadata
    assert by_id.request_fingerprint == "fp"
    assert by_id.status is PaymentStatus.PENDING
    assert by_id.created_at == payment.created_at
    assert by_id.pull_events() == [], "loading does not replay events"


async def test_add_writes_domain_events_to_outbox(
    uow: UnitOfWorkFactory, engine: AsyncEngine
) -> None:
    payment = new_payment()
    [event] = payment._events

    async with uow() as tx:
        await tx.payments.add(payment)
        await tx.commit()

    [row] = await fetch_all(engine, outbox)
    assert row.id == event.event_id
    assert row.event_type == "payment.created"
    assert row.payload == {"payment_id": str(payment.id)}
    assert row.published_at is None
    assert payment.pull_events() == [], "events are written once"


async def test_save_persists_the_changed_aggregate(uow: UnitOfWorkFactory) -> None:
    payment = await saved_payment(uow)

    async with uow() as tx:
        loaded = await tx.payments.get(payment.id, for_update=True)
        assert loaded is not None
        loaded.apply_charge(ChargeOutcome.DECLINED, at=datetime.now(UTC))
        await tx.payments.save(loaded)
        await tx.commit()

    async with uow() as tx:
        stored = await tx.payments.get(payment.id)
    assert stored is not None
    assert stored.status is PaymentStatus.FAILED
    assert stored.processed_at == loaded.processed_at


async def test_changes_without_save_are_not_persisted(uow: UnitOfWorkFactory) -> None:
    payment = await saved_payment(uow)

    async with uow() as tx:
        loaded = await tx.payments.get(payment.id)
        assert loaded is not None
        loaded.apply_charge(ChargeOutcome.APPROVED, at=datetime.now(UTC))
        await tx.commit()

    async with uow() as tx:
        stored = await tx.payments.get(payment.id)
    assert stored is not None
    assert stored.status is PaymentStatus.PENDING


async def test_nothing_is_written_without_commit(
    uow: UnitOfWorkFactory, engine: AsyncEngine
) -> None:
    payment = new_payment()
    async with uow() as tx:
        await tx.payments.add(payment)

    async with uow() as tx:
        assert await tx.payments.get(payment.id) is None
    assert await fetch_all(engine, outbox) == []


async def test_duplicate_idempotency_key_is_translated(uow: UnitOfWorkFactory) -> None:
    await saved_payment(uow)

    with pytest.raises(DuplicateIdempotencyKeyError):
        async with uow() as tx:
            await tx.payments.add(new_payment())
