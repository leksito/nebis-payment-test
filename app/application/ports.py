"""Ports: what the application needs from the outside world.

Adapters in `app.adapters` implement these; application services depend only on the protocols.
"""

import uuid
from collections.abc import Callable
from types import TracebackType
from typing import Protocol, Self

from app.domain.payment import ChargeOutcome, Money, Payment


class PaymentRepository(Protocol):
    async def add(self, payment: Payment) -> None: ...
    async def save(self, payment: Payment) -> None: ...
    async def get(self, payment_id: uuid.UUID, *, for_update: bool = False) -> Payment | None: ...
    async def get_by_idempotency_key(self, key: str) -> Payment | None: ...


class UnitOfWork(Protocol):
    payments: PaymentRepository

    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...
    async def commit(self) -> None: ...


UnitOfWorkFactory = Callable[[], UnitOfWork]


class PaymentGateway(Protocol):
    async def charge(self, idempotency_key: uuid.UUID, money: Money) -> ChargeOutcome:
        """Idempotent per key: a repeated call (redelivery, concurrent duplicate, retry
        after a crash) does not charge again and returns the original outcome."""


class PaymentNotifier(Protocol):
    async def notify(self, payment: Payment) -> None: ...
