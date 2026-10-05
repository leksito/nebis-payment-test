import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import ClassVar


@dataclass(frozen=True, kw_only=True)
class DomainEvent:
    event_type: ClassVar[str]

    event_id: uuid.UUID = field(default_factory=uuid.uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True, kw_only=True)
class PaymentCreated(DomainEvent):
    event_type: ClassVar[str] = "payment.created"

    payment_id: uuid.UUID
