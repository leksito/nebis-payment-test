from typing import Annotated

from fastapi import FastAPI, Header, Request, status
from fastapi.responses import JSONResponse
from starlette.types import Lifespan

from app.adapters.api.routes import router as payments_router
from app.adapters.webhooks import PaymentWebhook
from app.domain.errors import DomainError


def create_app(lifespan: Lifespan[FastAPI] | None = None) -> FastAPI:
    """Dependencies come from the lifespan state (see AppState); tests override them
    with `app.dependency_overrides` instead.
    """
    app = FastAPI(title="Payments Service", version="0.1.0", lifespan=lifespan)
    app.include_router(payments_router)

    @app.exception_handler(DomainError)
    async def domain_error(request: Request, exc: DomainError) -> JSONResponse:
        # Request schemas catch these first; this is the safety net for rules only the domain knows
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, content={"detail": str(exc)}
        )

    @app.webhooks.post("payment.processed")
    def payment_processed(
        body: PaymentWebhook,
        x_payment_id: Annotated[str, Header(description="Payment id, for deduplication")],
    ) -> None:
        """Sent to the payment's `webhook_url` once the payment is processed.

        Any non-2xx response, timeout or network error counts as a failed delivery and is
        retried; delivery is at-least-once, so deduplicate by `X-Payment-Id`.
        """

    @app.get("/health", tags=["health"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
