from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from app.adapters.postgres.repositories import PgPaymentRepository


def create_engine(url: str) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True)


class PgUnitOfWork:
    """Just a database transaction: repositories run their queries on its connection."""

    payments: PgPaymentRepository

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def __aenter__(self) -> Self:
        self._conn: AsyncConnection = await self._engine.connect()
        await self._conn.begin()
        self.payments = PgPaymentRepository(self._conn)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._conn.close()  # rolls back anything not committed

    async def commit(self) -> None:
        await self._conn.commit()
