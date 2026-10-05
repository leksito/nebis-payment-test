import asyncio
import random
import uuid
from decimal import Decimal

from app.adapters.gateway import EmulatedPaymentGateway
from app.domain.payment import Currency, Money

MONEY = Money(Decimal("10.00"), Currency.USD)


class CountingRandom(random.Random):
    """Counts actual charges: the emulator draws its processing delay once per charge."""

    draws = 0

    def uniform(self, a: float, b: float) -> float:
        self.draws += 1
        return super().uniform(a, b)


def gateway(rng: random.Random) -> EmulatedPaymentGateway:
    return EmulatedPaymentGateway(min_delay=0.01, max_delay=0.01, success_rate=0.5, rng=rng)


async def test_repeated_calls_with_one_key_charge_once() -> None:
    rng = CountingRandom(42)
    gw = gateway(rng)
    key = uuid.uuid4()

    outcomes = await asyncio.gather(*(gw.charge(key, MONEY) for _ in range(5)))
    again = await gw.charge(key, MONEY)

    assert len(set(outcomes)) == 1
    assert again == outcomes[0]
    assert rng.draws == 1


async def test_different_keys_are_charged_independently() -> None:
    rng = CountingRandom(42)
    gw = gateway(rng)

    await asyncio.gather(*(gw.charge(uuid.uuid4(), MONEY) for _ in range(3)))

    assert rng.draws == 3
