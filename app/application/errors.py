class ApplicationError(Exception):
    pass


class PaymentNotFoundError(ApplicationError):
    pass


class IdempotencyConflictError(ApplicationError):
    pass


class DuplicateIdempotencyKeyError(ApplicationError):
    pass


class NotificationError(ApplicationError):
    pass
