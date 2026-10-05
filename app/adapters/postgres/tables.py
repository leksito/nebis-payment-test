"""Table definitions for SQLAlchemy Core (used as a query builder only, no ORM)."""

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from app.domain.payment import Currency, PaymentStatus

UQ_PAYMENTS_IDEMPOTENCY_KEY = "uq_payments_idempotency_key"

metadata = MetaData()


def _pg_enum(enum_cls: type[Currency | PaymentStatus], name: str) -> Enum:
    return Enum(enum_cls, name=name, values_callable=lambda e: [m.value for m in e])


payments = Table(
    "payments",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("amount", Numeric(18, 2), nullable=False),
    Column("currency", _pg_enum(Currency, "currency"), nullable=False),
    Column("description", Text, nullable=False),
    Column("metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column(
        "status",
        _pg_enum(PaymentStatus, "payment_status"),
        nullable=False,
        server_default=PaymentStatus.PENDING.value,
    ),
    Column("idempotency_key", String(255), nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("webhook_url", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("processed_at", DateTime(timezone=True)),
    CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
    UniqueConstraint("idempotency_key", name=UQ_PAYMENTS_IDEMPOTENCY_KEY),
)

outbox = Table(
    "outbox",
    metadata,
    # Equal to the domain event id; also sent as the AMQP message_id
    Column("id", Uuid, primary_key=True),
    Column("event_type", String(100), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("published_at", DateTime(timezone=True)),
    # Set when the event can never be published (e.g. no route): the relay skips it
    Column("failed_at", DateTime(timezone=True)),
    Column("publish_attempts", Integer, nullable=False, server_default=text("0")),
    Column("last_error", Text),
    Index(
        "ix_outbox_unpublished",
        "created_at",
        postgresql_where=text("published_at IS NULL AND failed_at IS NULL"),
    ),
)
