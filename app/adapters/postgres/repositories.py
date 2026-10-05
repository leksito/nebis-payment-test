import uuid
from typing import Any

from sqlalchemy import Row, Select, insert, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from app.adapters.postgres.outbox import insert_events
from app.adapters.postgres.tables import UQ_PAYMENTS_IDEMPOTENCY_KEY, payments
from app.application.errors import DuplicateIdempotencyKeyError
from app.domain.payment import Money, Payment


class PgPaymentRepository:
    """Explicit queries only: every write is an INSERT or UPDATE issued right here,
    together with the aggregate's pending domain events going to the outbox in the
    same transaction. Nothing is tracked; every read builds a fresh aggregate.
    """

    def __init__(self, conn: AsyncConnection) -> None:
        self._conn = conn

    async def add(self, payment: Payment) -> None:
        try:
            await self._conn.execute(insert(payments).values(_to_row(payment)))
        except IntegrityError as exc:
            if UQ_PAYMENTS_IDEMPOTENCY_KEY in str(exc.orig):
                raise DuplicateIdempotencyKeyError(payment.idempotency_key) from exc
            raise
        await insert_events(self._conn, payment.pull_events())

    async def save(self, payment: Payment) -> None:
        await self._conn.execute(
            update(payments)
            .where(payments.c.id == payment.id)
            .values(status=payment.status, processed_at=payment.processed_at)
        )
        await insert_events(self._conn, payment.pull_events())

    async def get(self, payment_id: uuid.UUID, *, for_update: bool = False) -> Payment | None:
        stmt = select(payments).where(payments.c.id == payment_id)
        if for_update:
            stmt = stmt.with_for_update()
        return await self._fetch_one(stmt)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        return await self._fetch_one(select(payments).where(payments.c.idempotency_key == key))

    async def _fetch_one(self, stmt: Select[Any]) -> Payment | None:
        row = (await self._conn.execute(stmt)).one_or_none()
        return _from_row(row) if row is not None else None


def _to_row(payment: Payment) -> dict[str, Any]:
    return {
        "id": payment.id,
        "amount": payment.money.amount,
        "currency": payment.money.currency,
        "description": payment.description,
        "metadata": payment.metadata,
        "status": payment.status,
        "idempotency_key": payment.idempotency_key,
        "request_hash": payment.request_fingerprint,
        "webhook_url": payment.webhook_url,
        "created_at": payment.created_at,
        "processed_at": payment.processed_at,
    }


def _from_row(row: Row[Any]) -> Payment:
    return Payment(
        id=row.id,
        money=Money(row.amount, row.currency),
        description=row.description,
        metadata=dict(row.metadata),
        webhook_url=row.webhook_url,
        idempotency_key=row.idempotency_key,
        request_fingerprint=row.request_hash,
        status=row.status,
        created_at=row.created_at,
        processed_at=row.processed_at,
    )
