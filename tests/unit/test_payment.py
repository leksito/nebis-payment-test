from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.domain.errors import InvalidMoneyError, PaymentAlreadyProcessedError
from app.domain.events import PaymentCreated
from app.domain.payment import ChargeOutcome, Currency, Money, Payment, PaymentStatus

NOW = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)


def new_payment() -> Payment:
    return Payment.create(
        money=Money(Decimal("100.00"), Currency.RUB),
        description="Order #1",
        metadata={"order": 1},
        webhook_url="https://example.com/hook",
        idempotency_key="key-1",
        request_fingerprint="fp",
        at=NOW,
    )


def test_new_payment_is_pending_and_raises_payment_created() -> None:
    payment = new_payment()

    assert payment.status is PaymentStatus.PENDING
    assert payment.created_at == NOW
    assert payment.processed_at is None
    [event] = payment.pull_events()
    assert isinstance(event, PaymentCreated)
    assert event.payment_id == payment.id
    assert payment.pull_events() == [], "events are handed out once"


@pytest.mark.parametrize(
    ("outcome", "status"),
    [
        (ChargeOutcome.APPROVED, PaymentStatus.SUCCEEDED),
        (ChargeOutcome.DECLINED, PaymentStatus.FAILED),
    ],
)
def test_charge_outcome_finalizes_payment(outcome: ChargeOutcome, status: PaymentStatus) -> None:
    payment = new_payment()

    payment.apply_charge(outcome, at=NOW)

    assert payment.status is status
    assert payment.processed_at == NOW
    assert not payment.is_pending


def test_payment_is_processed_only_once() -> None:
    payment = new_payment()
    payment.apply_charge(ChargeOutcome.DECLINED, at=NOW)

    with pytest.raises(PaymentAlreadyProcessedError):
        payment.apply_charge(ChargeOutcome.APPROVED, at=NOW)
    assert payment.status is PaymentStatus.FAILED


def test_money_is_normalized_to_cents() -> None:
    assert Money(Decimal("100.5"), Currency.USD) == Money(Decimal("100.50"), Currency.USD)
    assert str(Money(Decimal("7"), Currency.USD).amount) == "7.00"


@pytest.mark.parametrize("amount", ["0", "-1", "1.001", "NaN", "Infinity", "abc"])
def test_money_rejects_invalid_amounts(amount: str) -> None:
    with pytest.raises(InvalidMoneyError):
        Money(amount, Currency.EUR)  # type: ignore[arg-type]


def test_money_rejects_unknown_currency() -> None:
    with pytest.raises(ValueError):
        Money(Decimal(1), "GBP")  # type: ignore[arg-type]
