"""
Single source of truth for the RabbitMQ topology.
"""

from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

from src.rabbitmq.routes import EVENT_ROUTES, EventRoute


def build_exchange(route: EventRoute) -> RabbitExchange:
    """Build the exchange that events of the given route are published to."""
    return RabbitExchange(
        name=route.exchange,
        type=ExchangeType.DIRECT,
        durable=True,
    )


def build_queue(route: EventRoute) -> RabbitQueue:
    """Build the queue that consumers of the given route read from."""
    return RabbitQueue(
        name=route.queue,
        durable=True,
        routing_key=route.routing_key,
    )


async def declare_topology(broker: RabbitBroker) -> None:
    """Declare all configured exchanges, queues, and their bindings."""

    for route in set(EVENT_ROUTES.values()):
        exchange = await broker.declare_exchange(build_exchange(route))
        queue = await broker.declare_queue(build_queue(route))

        await queue.bind(
            exchange=exchange,
            routing_key=route.routing_key,
        )
