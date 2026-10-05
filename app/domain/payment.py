import enum
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Self

from app.domain.errors import InvalidMoneyError, PaymentAlreadyProcessedError
from app.domain.events import DomainEvent, PaymentCreated

CENT = Decimal("0.01")


class Currency(enum.StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(enum.StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ChargeOutcome(enum.StrEnum):
    APPROVED = "approved"
    DECLINED = "declined"


@dataclass(frozen=True)
class Money:
    amount: Decimal
    currency: Currency

    def __post_init__(self) -> None:
        try:
            amount = Decimal(self.amount)
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise InvalidMoneyError(f"invalid amount: {self.amount!r}") from exc
        if not amount.is_finite() or amount <= 0:
            raise InvalidMoneyError("amount must be a positive number")
        if amount != amount.quantize(CENT):
            raise InvalidMoneyError("amount must have at most 2 decimal places")
        # Normalized, so 100.5 and 100.50 are the same money
        object.__setattr__(self, "amount", amount.quantize(CENT))
        object.__setattr__(self, "currency", Currency(self.currency))


@dataclass(eq=False, kw_only=True)
class Payment:
    """Aggregate root. Its status moves pending -> succeeded | failed exactly once."""

    id: uuid.UUID
    money: Money
    description: str
    metadata: dict[str, Any]
    webhook_url: str
    idempotency_key: str
    # Fingerprint of the request that created the payment: tells a client retry
    # (same key, same request) from a key reuse (same key, different request)
    request_fingerprint: str
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None = None

    _events: list[DomainEvent] = field(default_factory=list, repr=False)

    @classmethod
    def create(
        cls,
        *,
        money: Money,
        description: str,
        metadata: dict[str, Any],
        webhook_url: str,
        idempotency_key: str,
        request_fingerprint: str,
        at: datetime,
    ) -> Self:
        payment = cls(
            id=uuid.uuid4(),
            money=money,
            description=description,
            metadata=dict(metadata),
            webhook_url=webhook_url,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            status=PaymentStatus.PENDING,
            created_at=at,
        )
        payment._events.append(PaymentCreated(payment_id=payment.id, occurred_at=at))
        return payment

    @property
    def is_pending(self) -> bool:
        return self.status is PaymentStatus.PENDING

    def apply_charge(self, outcome: ChargeOutcome, at: datetime) -> None:
        if not self.is_pending:
            raise PaymentAlreadyProcessedError(f"payment {self.id} is already {self.status}")
        self.status = (
            PaymentStatus.SUCCEEDED if outcome is ChargeOutcome.APPROVED else PaymentStatus.FAILED
        )
        self.processed_at = at

    def pull_events(self) -> list[DomainEvent]:
        events, self._events = self._events, []
        return events

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Payment) and other.id == self.id

    def __hash__(self) -> int:
        return hash(self.id)
