import asyncio
import random
import uuid
from collections import defaultdict

from app.domain.payment import ChargeOutcome, Money


class EmulatedPaymentGateway:
    def __init__(
        self,
        *,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._rng = rng or random.Random()
        # A real gateway keeps this server-side; the emulator only remembers it per process
        self._outcomes: dict[uuid.UUID, ChargeOutcome] = {}
        self._locks: defaultdict[uuid.UUID, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def charge(self, idempotency_key: uuid.UUID, money: Money) -> ChargeOutcome:
        async with self._locks[idempotency_key]:
            if (outcome := self._outcomes.get(idempotency_key)) is not None:
                return outcome

            await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
            outcome = (
                ChargeOutcome.APPROVED
                if self._rng.random() < self._success_rate
                else ChargeOutcome.DECLINED
            )
            self._outcomes[idempotency_key] = outcome
            return outcome
