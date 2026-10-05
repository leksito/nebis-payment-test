import secrets
from typing import Annotated, TypedDict

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

from app.application.payments import PaymentService
from app.config import ApiSettings, get_api_settings


class AppState(TypedDict):
    """Lifespan state: what the lifespan yields is available as `request.state`."""

    payment_service: PaymentService


api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(
    api_key: Annotated[str | None, Security(api_key_header)],
    settings: Annotated[ApiSettings, Depends(get_api_settings)],
) -> None:
    if api_key is None or not secrets.compare_digest(api_key, settings.api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )


def get_payment_service(request: Request) -> PaymentService:
    return request.state.payment_service


PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
