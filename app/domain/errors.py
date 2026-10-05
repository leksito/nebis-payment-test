class DomainError(Exception):
    """A business rule was violated."""


class InvalidMoneyError(DomainError):
    pass


class PaymentAlreadyProcessedError(DomainError):
    pass
