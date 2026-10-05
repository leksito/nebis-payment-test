"""In-memory implementations of the application ports."""

import copy
import uuid
from dataclasses import dataclass, field
from types import TracebackType
from typing import Self

from app.application.errors import DuplicateIdempotencyKeyError, NotificationError
from app.domain.events import DomainEvent
from app.domain.payment import ChargeOutcome, Money, Payment


@dataclass
class FakeDatabase:
    """Committed state shared by all units of work."""

    payments: dict[uuid.UUID, Payment] = field(default_factory=dict)
    # Domain events released by committed units of work
    events: list[DomainEvent] = field(default_factory=list)

    def by_key(self, key: str) -> Payment | None:
        return next((p for p in self.payments.values() if p.idempotency_key == key), None)


class FakePaymentRepository:
    """Mirrors PgPaymentRepository: explicit writes, and every read returns a fresh copy,
    so a change that is never saved is never persisted.
    """

    def __init__(self, db: FakeDatabase) -> None:
        self._db = db
        # Snapshot isolation: what other transactions commit later is invisible to reads...
        self._snapshot = copy.deepcopy(db.payments)
        # ...and our writes are invisible to others until commit
        self.written: dict[uuid.UUID, Payment] = {}
        self.events: list[DomainEvent] = []

    async def add(self, payment: Payment) -> None:
        # The unique index sees rows committed by concurrent transactions
        if self._db.by_key(payment.idempotency_key) is not None:
            raise DuplicateIdempotencyKeyError(payment.idempotency_key)
        self._write(payment)

    async def save(self, payment: Payment) -> None:
        self._write(payment)

    async def get(self, payment_id: uuid.UUID, *, for_update: bool = False) -> Payment | None:
        # A locking read sees the latest committed version
        committed = self._db.payments if for_update else self._snapshot
        payment = self.written.get(payment_id) or committed.get(payment_id)
        return copy.deepcopy(payment)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        visible = {**self._snapshot, **self.written}.values()
        return copy.deepcopy(next((p for p in visible if p.idempotency_key == key), None))

    def _write(self, payment: Payment) -> None:
        self.events.extend(payment.pull_events())
        self.written[payment.id] = copy.deepcopy(payment)


class FakeUnitOfWork:
    def __init__(self, db: FakeDatabase) -> None:
        self.db = db
        self.commits = 0

    async def __aenter__(self) -> Self:
        self.payments = FakePaymentRepository(self.db)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        pass

    async def commit(self) -> None:
        self.db.payments.update(copy.deepcopy(self.payments.written))
        self.db.events.extend(self.payments.events)
        self.commits += 1


@dataclass
class FakeGateway:
    """Idempotent like the port requires: `charges` holds what was actually charged."""

    outcome: ChargeOutcome = ChargeOutcome.APPROVED
    calls: list[uuid.UUID] = field(default_factory=list)
    charges: dict[uuid.UUID, ChargeOutcome] = field(default_factory=dict)

    async def charge(self, idempotency_key: uuid.UUID, money: Money) -> ChargeOutcome:
        self.calls.append(idempotency_key)
        return self.charges.setdefault(idempotency_key, self.outcome)


@dataclass
class FakeNotifier:
    fail: bool = False
    notified: list[Payment] = field(default_factory=list)

    async def notify(self, payment: Payment) -> None:
        if self.fail:
            raise NotificationError("webhook responded with HTTP 503")
        self.notified.append(payment)
