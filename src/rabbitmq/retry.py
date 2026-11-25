"""
Delayed retries for consumed messages.

The number of retries already made travels with the message in the
``x-retry-count`` header; the delay comes from the TTL of the retry queue
the message is parked in (see ``build_retry_queue``).
"""

from faststream.rabbit import RabbitBroker
from faststream.rabbit.message import RabbitMessage

from src.rabbitmq.routes import EventRoute

RETRY_COUNT_HEADER = "x-retry-count"

# Headers RabbitMQ adds when it dead-letters a message. They describe the
# broker's own history of the message and must not be copied into a new one.
DEATH_HEADER_PREFIXES = ("x-death", "x-first-death-", "x-last-death-")


def get_retry_count(message: RabbitMessage) -> int:
    """Return how many times the message has already been retried."""
    return int(message.headers.get(RETRY_COUNT_HEADER, 0))


def get_attempt(message: RabbitMessage) -> int:
    """Return the 1-based number of the current processing attempt."""
    return get_retry_count(message) + 1


async def publish_retry(
    broker: RabbitBroker,
    message: RabbitMessage,
    route: EventRoute,
) -> int:
    """
    Park a copy of the message in the next retry queue and return its number.

    The caller acks the original afterwards. If the process dies in between,
    the original is redelivered as well, so handlers must be idempotent.
    """
    retry = get_retry_count(message) + 1

    headers = {
        key: value
        for key, value in message.headers.items()
        if not key.startswith(DEATH_HEADER_PREFIXES)
    }
    headers[RETRY_COUNT_HEADER] = retry

    # Publishing to a queue name goes through the default exchange.
    await broker.publish(
        message.body,
        queue=route.retry_queue(retry),
        headers=headers,
        content_type=message.content_type,
        message_id=message.message_id,
        correlation_id=message.correlation_id,
        persist=True,
    )

    return retry
