import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, Self

import httpx
from pydantic import BaseModel

from app.application.errors import NotificationError
from app.domain.payment import Currency, Payment, PaymentStatus


class WebhookPayment(BaseModel):
    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None


class PaymentWebhook(BaseModel):
    event: Literal["payment.processed"] = "payment.processed"
    payment: WebhookPayment

    @classmethod
    def from_domain(cls, payment: Payment) -> Self:
        return cls(
            payment=WebhookPayment(
                id=payment.id,
                amount=payment.money.amount,
                currency=payment.money.currency,
                description=payment.description,
                metadata=payment.metadata,
                status=payment.status,
                created_at=payment.created_at,
                processed_at=payment.processed_at,
            )
        )


class HttpWebhookNotifier:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def notify(self, payment: Payment) -> None:
        try:
            response = await self._client.post(
                payment.webhook_url,
                content=PaymentWebhook.from_domain(payment).model_dump_json(),
                headers={
                    "Content-Type": "application/json",
                    # Lets the receiver deduplicate: delivery is at-least-once
                    "X-Payment-Id": str(payment.id),
                },
            )
        except httpx.HTTPError as exc:
            raise NotificationError(f"{type(exc).__name__}: {exc}") from exc

        if not response.is_success:
            raise NotificationError(f"webhook responded with HTTP {response.status_code}")
