"""RabbitMQ topology.

    payments (direct) --payments.new--> [payments.new] --> consumer
         ^                                   |
         |                                   | reject (poison message) via x-dead-letter-exchange
         |                                   v
         |                     payments.dlx (direct) --payments.new--> [payments.new.dlq]
         |                                   ^
         |                                   | consumer publishes after the last attempt
         |
         +-- dead-lettered on TTL expiry -- [payments.new.retry.<delay>ms] (one queue per attempt)
                                                     ^
                                                     | consumer publishes on a failed attempt
                                           payments.retry (direct)

Retry queues have no consumers: a message sits there for the queue's TTL and is then
dead-lettered back to `payments.new`. Using a queue per delay (instead of per-message TTL)
avoids head-of-line blocking, and the delay is part of the queue name so changing the
backoff settings never conflicts with already declared queues.

All queues are quorum queues:
- Retries and the DLQ are built on dead-lettering, which is at-most-once for classic
  queues: a message moved by the broker is not confirmed and can be lost. Quorum queues
  support at-least-once dead-lettering: the message stays in the source queue until the
  target queue has confirmed it (requires `x-overflow: reject-publish`).
- `x-delivery-limit` stops a message that keeps crashing the consumer before it can ack
  from being redelivered forever: after the limit it is dead-lettered to the DLQ.
- Replicated (Raft) once RabbitMQ runs as a cluster, with no code changes.
"""

from dataclasses import dataclass

from faststream.rabbit import (
    Channel,
    ExchangeType,
    QueueType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

PAYMENTS_NEW = "payments.new"

# Redeliveries of one message (consumer crashed or lost its connection before acking)
# before RabbitMQ dead-letters it. Handled failures are acked and retried via retry queues.
DELIVERY_LIMIT = 5

AT_LEAST_ONCE_DEAD_LETTERING = {
    "x-dead-letter-strategy": "at-least-once",
    "x-overflow": "reject-publish",
}

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
retry_exchange = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
dead_letter_exchange = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

payments_new_queue = RabbitQueue(
    PAYMENTS_NEW,
    queue_type=QueueType.QUORUM,
    routing_key=PAYMENTS_NEW,
    arguments={
        "x-dead-letter-exchange": dead_letter_exchange.name,
        "x-dead-letter-routing-key": PAYMENTS_NEW,
        "x-delivery-limit": DELIVERY_LIMIT,
        **AT_LEAST_ONCE_DEAD_LETTERING,
    },
)

dead_letter_queue = RabbitQueue(
    f"{PAYMENTS_NEW}.dlq",
    queue_type=QueueType.QUORUM,
    routing_key=PAYMENTS_NEW,
    # Quorum queues default to a delivery limit of 20 and the DLQ has no DLX of its own:
    # messages inspected with requeue (e.g. "Get messages" in the management UI) more than
    # 20 times would be silently dropped. Messages here wait for a human, so no limit.
    arguments={"x-delivery-limit": -1},
)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    base_delay_ms: int

    def delay_ms(self, attempt: int) -> int:
        """Delay after the failed `attempt` (1-based): base, 2*base, 4*base, ..."""
        return self.base_delay_ms * 2 ** (attempt - 1)

    def queues(self) -> list[RabbitQueue]:
        return [retry_queue(self.delay_ms(a)) for a in range(1, self.max_attempts)]


def retry_queue(delay_ms: int) -> RabbitQueue:
    name = f"{PAYMENTS_NEW}.retry.{delay_ms}ms"
    return RabbitQueue(
        name,
        queue_type=QueueType.QUORUM,
        routing_key=name,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": payments_exchange.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW,
            **AT_LEAST_ONCE_DEAD_LETTERING,
        },
    )


def create_broker(url: str, *, prefetch_count: int | None = None) -> RabbitBroker:
    """prefetch_count is also the consumer's concurrency: aio-pika handles every delivered
    message in its own task, so up to prefetch_count messages are processed at once.
    """
    return RabbitBroker(
        url,
        default_channel=Channel(
            prefetch_count=prefetch_count,
            # Per consumer: quorum queues do not support global QoS
            global_qos=False,
            # Raise if a mandatory message cannot be routed, so the outbox never marks it
            # published
            on_return_raises=True,
        ),
    )


async def declare_topology(broker: RabbitBroker, retry_policy: RetryPolicy) -> None:
    """Idempotently declare all exchanges, queues and bindings.

    Called by both the relay and the consumer, so whichever starts first leaves the broker
    ready and no published message is ever unroutable.
    """
    payments = await broker.declare_exchange(payments_exchange)
    retry = await broker.declare_exchange(retry_exchange)
    dlx = await broker.declare_exchange(dead_letter_exchange)

    main = await broker.declare_queue(payments_new_queue)
    await main.bind(payments, routing_key=PAYMENTS_NEW)

    dlq = await broker.declare_queue(dead_letter_queue)
    await dlq.bind(dlx, routing_key=PAYMENTS_NEW)

    for queue in retry_policy.queues():
        declared = await broker.declare_queue(queue)
        await declared.bind(retry, routing_key=queue.routing_key)
