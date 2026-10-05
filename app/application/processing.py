import logging
import uuid
from datetime import UTC, datetime

from app.application.errors import PaymentNotFoundError
from app.application.ports import PaymentGateway, PaymentNotifier, UnitOfWorkFactory
from app.domain.payment import ChargeOutcome, Payment

logger = logging.getLogger(__name__)


class PaymentProcessingService:
    def __init__(
        self,
        uow: UnitOfWorkFactory,
        gateway: PaymentGateway,
        notifier: PaymentNotifier,
    ) -> None:
        self._uow = uow
        self._gateway = gateway
        self._notifier = notifier

    async def process(self, payment_id: uuid.UUID) -> None:
        payment = await self._load(payment_id)

        if payment.is_pending:
            outcome = await self._gateway.charge(payment.id, payment.money)
            payment = await self._apply(payment_id, outcome)
        else:
            logger.info("Payment %s already %s, notifying again", payment.id, payment.status)

        await self._notifier.notify(payment)
        logger.info("Client notified about payment %s", payment.id)

    async def _load(self, payment_id: uuid.UUID) -> Payment:
        async with self._uow() as uow:
            payment = await uow.payments.get(payment_id)

        if payment is None:
            raise PaymentNotFoundError(str(payment_id))

        return payment

    async def _apply(self, payment_id: uuid.UUID, outcome: ChargeOutcome) -> Payment:
        async with self._uow() as uow:
            payment = await uow.payments.get(payment_id, for_update=True)
            if payment is None:
                raise PaymentNotFoundError(str(payment_id))

            if payment.is_pending:
                payment.apply_charge(outcome, at=datetime.now(UTC))
                await uow.payments.save(payment)
                await uow.commit()
                logger.info("Payment %s processed: %s", payment.id, payment.status)
        return payment
