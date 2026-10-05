"""Driving adapter: turns `payments.new` messages into PaymentProcessingService.process calls.

Retries and dead-lettering are transport concerns and live here, not in the application service.
"""

import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from faststream import AckPolicy
from faststream.exceptions import NackMessage
from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import BaseModel

from app.adapters.rabbitmq.topology import (
    PAYMENTS_NEW,
    RetryPolicy,
    dead_letter_exchange,
    payments_exchange,
    payments_new_queue,
    retry_exchange,
    retry_queue,
)
from app.application.errors import PaymentNotFoundError

logger = logging.getLogger(__name__)

ATTEMPT_HEADER = "x-attempt"
ERROR_HEADER = "x-last-error"

# Retrying cannot fix these, they go straight to the DLQ
NON_RETRYABLE_ERRORS: tuple[type[Exception], ...] = (PaymentNotFoundError,)


class PaymentCreatedMessage(BaseModel):
    payment_id: uuid.UUID


def register_consumer(
    broker: RabbitBroker,
    process_payment: Callable[[uuid.UUID], Awaitable[None]],
    retry_policy: RetryPolicy,
) -> None:
    # REJECT_ON_ERROR only kicks in for errors the handler does not catch (e.g. an
    # undecodable body); the queue dead-letters those to the DLQ instead of looping.
    @broker.subscriber(
        payments_new_queue,
        payments_exchange,
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )
    async def handle_payment_created(body: PaymentCreatedMessage, message: RabbitMessage) -> None:
        attempt = int(message.headers.get(ATTEMPT_HEADER, 1))
        try:
            await process_payment(body.payment_id)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            retryable = not isinstance(exc, NON_RETRYABLE_ERRORS)
            if retryable and attempt < retry_policy.max_attempts:
                delay_ms = retry_policy.delay_ms(attempt)
                logger.warning(
                    "Payment %s attempt %d/%d failed (%s), retrying in %d ms",
                    body.payment_id,
                    attempt,
                    retry_policy.max_attempts,
                    error,
                    delay_ms,
                )
                exchange, routing_key = retry_exchange, retry_queue(delay_ms).routing_key
                headers: dict[str, Any] = {ATTEMPT_HEADER: attempt + 1, ERROR_HEADER: error}
            else:
                logger.error(
                    "Payment %s failed on attempt %d/%d (%s), sending to DLQ",
                    body.payment_id,
                    attempt,
                    retry_policy.max_attempts,
                    error,
                )
                exchange, routing_key = dead_letter_exchange, PAYMENTS_NEW
                headers = {
                    ATTEMPT_HEADER: attempt,
                    ERROR_HEADER: error,
                    "x-failed-at": datetime.now(UTC).isoformat(),
                }

            try:
                await broker.publish(
                    body,
                    exchange=exchange,
                    routing_key=routing_key,
                    headers=headers,
                    message_id=message.message_id,
                    correlation_id=message.correlation_id,
                    persist=True,
                )
            except Exception as publish_exc:
                # Not REJECT_ON_ERROR: that would dead-letter the message straight to the DLQ,
                # skipping its remaining attempts. Requeue it instead; x-delivery-limit bounds
                # how many times this can happen.
                logger.warning(
                    "Could not route payment %s for retry (%s), requeueing",
                    body.payment_id,
                    publish_exc,
                )
                raise NackMessage(requeue=True) from publish_exc
        # Returning normally acks the original message. The retry/DLQ copy is published
        # (with publisher confirms) before that, so a crash in between can only produce a
        # duplicate, which PaymentProcessingService handles.
