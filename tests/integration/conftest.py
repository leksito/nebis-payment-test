import os
from collections.abc import AsyncIterator, Iterator
from functools import partial
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import Row, Table, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.api.app import create_app
from app.adapters.api.deps import get_payment_service
from app.adapters.postgres.uow import PgUnitOfWork, create_engine
from app.application.payments import PaymentService
from app.application.ports import UnitOfWorkFactory
from app.config import ApiSettings, get_api_settings

API_KEY = "test-api-key"


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    """TEST_DATABASE_URL if set, otherwise a throwaway Postgres container."""
    if url := os.environ.get("TEST_DATABASE_URL"):
        yield url
        return

    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer("postgres:17-alpine", driver="asyncpg") as postgres:
        yield postgres.get_connection_url()


@pytest.fixture(scope="session")
def migrated_database_url(database_url: str) -> str:
    # Sync on purpose: Alembic's env.py runs its own event loop
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")
    return database_url


@pytest.fixture(scope="session")
async def engine(migrated_database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(migrated_database_url)
    yield engine
    await engine.dispose()


@pytest.fixture
async def uow(
    engine: AsyncEngine,
) -> AsyncIterator[UnitOfWorkFactory]:
    yield partial(PgUnitOfWork, engine)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox"))


@pytest.fixture
async def client(uow: UnitOfWorkFactory) -> AsyncIterator[AsyncClient]:
    app = create_app()
    app.dependency_overrides[get_payment_service] = lambda: PaymentService(uow)
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(api_key=API_KEY)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": API_KEY},
    ) as client:
        yield client


async def fetch_all(engine: AsyncEngine, table: Table) -> list[Row[Any]]:
    async with engine.connect() as conn:
        return list((await conn.execute(select(table).order_by(table.c.created_at))).all())
