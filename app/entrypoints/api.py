"""Composition root of the HTTP API: `uvicorn app.entrypoints.api:app`."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial

from fastapi import FastAPI

from app.adapters.api.app import create_app
from app.adapters.api.deps import AppState
from app.adapters.postgres.uow import PgUnitOfWork, create_engine
from app.application.payments import PaymentService
from app.config import get_api_settings
from app.log import setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[AppState]:
    """Resources live exactly as long as the application: nothing is created on import."""
    settings = get_api_settings()  # fails fast at startup if API_KEY is not set
    setup_logging(settings.log_level)
    engine = create_engine(settings.database_url)
    uow = partial(PgUnitOfWork, engine)
    try:
        yield {"payment_service": PaymentService(uow)}
    finally:
        await engine.dispose()


app = create_app(lifespan=lifespan)
