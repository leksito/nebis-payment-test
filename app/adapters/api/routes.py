import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.adapters.api.deps import PaymentServiceDep, require_api_key
from app.adapters.api.schemas import PaymentAcceptedResponse, PaymentCreateRequest, PaymentResponse
from app.application.errors import IdempotencyConflictError, PaymentNotFoundError

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={409: {"description": "Idempotency-Key reused with a different request body"}},
)
async def create_payment(
    body: PaymentCreateRequest,
    payments: PaymentServiceDep,
    response: Response,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
) -> PaymentAcceptedResponse:
    try:
        result = await payments.create(body.to_command(idempotency_key))
    except IdempotencyConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key has already been used with a different request body",
        ) from None
    if not result.created:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAcceptedResponse.from_domain(result.payment)


@router.get("/{payment_id}")
async def get_payment(payment_id: uuid.UUID, payments: PaymentServiceDep) -> PaymentResponse:
    try:
        payment = await payments.get(payment_id)
    except PaymentNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        ) from None
    return PaymentResponse.from_domain(payment)
