"""
Single source of truth for the RabbitMQ topology.

For every route:

    exchange ──routing_key──► queue ──reject(requeue=False)──► DLX ──► DLQ
                                ▲
                                │ x-message-ttl expired
                                │
                         queue.retry.N  ◄── consumer publishes a failed message
"""

from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

from src.rabbitmq.routes import EVENT_ROUTES, EventRoute

# The default exchange delivers a message straight to the queue named by its
# routing key, so retried messages return only to their own queue instead of
# every queue bound to the route's exchange.
DEFAULT_EXCHANGE = ""


def build_exchange(route: EventRoute) -> RabbitExchange:
    """Build the exchange that events of the given route are published to."""
    return RabbitExchange(
        name=route.exchange,
        type=ExchangeType.DIRECT,
        durable=True,
    )


def build_queue(route: EventRoute) -> RabbitQueue:
    """
    Build the queue that consumers of the given route read from.

    Rejected messages are dead-lettered to the route's DLQ.
    """
    return RabbitQueue(
        name=route.queue,
        durable=True,
        routing_key=route.routing_key,
        arguments={
            "x-dead-letter-exchange": route.dead_letter_exchange,
            "x-dead-letter-routing-key": route.dead_letter_queue,
        },
    )


def build_dead_letter_exchange(route: EventRoute) -> RabbitExchange:
    """Build the exchange that receives messages rejected by consumers."""
    return RabbitExchange(
        name=route.dead_letter_exchange,
        type=ExchangeType.DIRECT,
        durable=True,
    )


def build_dead_letter_queue(route: EventRoute) -> RabbitQueue:
    """Build the queue that stores messages that could not be processed."""
    return RabbitQueue(
        name=route.dead_letter_queue,
        durable=True,
        routing_key=route.dead_letter_queue,
    )


def build_retry_queue(route: EventRoute, retry: int) -> RabbitQueue:
    """
    Build the queue that delays a message before the given retry.

    The queue has no consumers: once the message TTL expires, RabbitMQ
    dead-letters the message back to the route's main queue.
    """
    return RabbitQueue(
        name=route.retry_queue(retry),
        durable=True,
        arguments={
            "x-message-ttl": route.retry.delay_ms(retry),
            "x-dead-letter-exchange": DEFAULT_EXCHANGE,
            "x-dead-letter-routing-key": route.queue,
        },
    )


def build_retry_queues(route: EventRoute) -> list[RabbitQueue]:
    """Build one delay queue per retry, so each retry can wait longer."""
    return [
        build_retry_queue(route, retry)
        for retry in range(1, route.retry.max_retries + 1)
    ]


async def declare_topology(broker: RabbitBroker) -> None:
    """Declare all configured exchanges, queues, and their bindings."""

    for route in set(EVENT_ROUTES.values()):
        # Dead-letter destination first, so rejected messages are never lost.
        dead_letter_exchange = await broker.declare_exchange(
            build_dead_letter_exchange(route)
        )
        dead_letter_queue = await broker.declare_queue(
            build_dead_letter_queue(route)
        )
        await dead_letter_queue.bind(
            exchange=dead_letter_exchange,
            routing_key=route.dead_letter_queue,
        )

        for retry_queue in build_retry_queues(route):
            await broker.declare_queue(retry_queue)

        exchange = await broker.declare_exchange(build_exchange(route))
        queue = await broker.declare_queue(build_queue(route))
        await queue.bind(
            exchange=exchange,
            routing_key=route.routing_key,
        )
