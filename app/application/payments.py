import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from app.application.errors import (
    DuplicateIdempotencyKeyError,
    IdempotencyConflictError,
    PaymentNotFoundError,
)
from app.application.ports import UnitOfWorkFactory
from app.domain.payment import Currency, Money, Payment


@dataclass(frozen=True, kw_only=True)
class CreatePaymentCommand:
    idempotency_key: str
    amount: Decimal
    currency: Currency
    webhook_url: str
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    created: bool


def request_fingerprint(command: CreatePaymentCommand, money: Money) -> str:
    canonical = json.dumps(
        {
            "amount": str(money.amount),
            "currency": money.currency.value,
            "description": command.description,
            "metadata": command.metadata,
            "webhook_url": command.webhook_url,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentService:
    def __init__(self, uow: UnitOfWorkFactory) -> None:
        self._uow = uow

    async def create(self, command: CreatePaymentCommand) -> CreatePaymentResult:
        money = Money(command.amount, command.currency)
        fingerprint = request_fingerprint(command, money)

        try:
            async with self._uow() as uow:
                existing = await uow.payments.get_by_idempotency_key(command.idempotency_key)
                if existing is None:
                    payment = Payment.create(
                        money=money,
                        description=command.description,
                        metadata=command.metadata,
                        webhook_url=command.webhook_url,
                        idempotency_key=command.idempotency_key,
                        request_fingerprint=fingerprint,
                        at=datetime.now(UTC),
                    )
                    await uow.payments.add(payment)
                    await uow.commit()

                    return CreatePaymentResult(payment, created=True)
        except DuplicateIdempotencyKeyError:
            async with self._uow() as uow:
                existing = await uow.payments.get_by_idempotency_key(command.idempotency_key)

        if existing is None:
            # The unique key reported a conflict, so a committed payment must hold the key
            raise RuntimeError(f"payment with idempotency key {command.idempotency_key!r} vanished")
        if existing.request_fingerprint != fingerprint:
            raise IdempotencyConflictError(command.idempotency_key)

        return CreatePaymentResult(existing, created=False)

    async def get(self, payment_id: uuid.UUID) -> Payment:
        async with self._uow() as uow:
            payment = await uow.payments.get(payment_id)

        if payment is None:
            raise PaymentNotFoundError(str(payment_id))

        return payment
