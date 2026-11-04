import asyncio
import logging
from faststream.rabbit import (
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
    ExchangeType,
)

from database.database import async_session_maker
from outbox_service.repositories.outbox import OutboxRepository
from outbox_service.services.publisher.event_router import EVENT_ROUTES
from settings import REBBITMQ_URL

logger = logging.getLogger(__name__)

POLL_INTERVAL = 1
BATCH_SIZE = 100


class OutboxPublisher:
    """Publishes pending Outbox events to their configured RabbitMQ queues."""

    def __init__(self, broker: RabbitBroker):
        self.broker = broker

    async def declare_topology(self) -> None:
        """Declare RabbitMQ exchanges, queues, and their bindings."""

        # Declare each unique exchange and queue.
        for route in set(EVENT_ROUTES.values()):
            exchange = await self.broker.declare_exchange(
                RabbitExchange(
                    name=route.exchange,
                    type=ExchangeType.DIRECT,
                    durable=True,
                )
            )
            queue = await self.broker.declare_queue(
                RabbitQueue(
                    name=route.queue,
                    durable=True,
                )
            )
            # Bind the queue to the exchange with the specified routing key.
            await queue.bind(
                exchange=exchange,
                routing_key=route.routing_key,
            )

    async def publish_pending(self) -> None:
        """Fetch and publish pending events from the Outbox."""
        # Fetch unpublished events from the Outbox.
        async with async_session_maker() as session:
            repository = OutboxRepository(session)
            events = await repository.get_unpublished(limit=BATCH_SIZE)

            # Detach event data before closing the database session.
            event_data = [
                {
                    "id": event.id,
                    "event_type": event.event_type,
                    "payload": event.payload,
                }
                for event in events
            ]

        # Process each event and publish it.
        for event in event_data:
            route = EVENT_ROUTES.get(event["event_type"])

            if route is None:
                logger.error(
                    "No route configured for event_type=%s, event_id=%s",
                    event["event_type"],
                    event["id"],
                )
                continue

            # Publish the event to RabbitMQ.
            try:
                await self.broker.publish(
                    message=event["payload"],
                    exchange=RabbitExchange(
                        name=route.exchange,
                        type=ExchangeType.DIRECT,
                        durable=True,
                    ),
                    routing_key=route.routing_key,
                    persist=True,
                    message_id=str(event["id"]),
                    headers={
                        "event_type": event["event_type"],
                    },
                )
            except Exception:
                logger.exception(
                    "Failed to publish outbox event id=%s",
                    event["id"],
                )
                continue

            # Mark the event as published after successful publication.
            async with async_session_maker() as session:
                async with session.begin():
                    repository = OutboxRepository(session)

                    result = await repository.get_by_id(event["id"])

                    if result is None or result.published_at is not None:
                        continue

                    await repository.mark_published(result)

    async def run(self) -> None:
        """Continuously poll the Outbox and publish pending events."""
        while True:
            try:
                await self.publish_pending()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Outbox publisher iteration failed")

            await asyncio.sleep(POLL_INTERVAL)


async def main() -> None:
    """Initialize RabbitMQ topology and start the publisher."""
    broker = RabbitBroker(url=REBBITMQ_URL)

    async with broker:
        publisher = OutboxPublisher(broker)

        await publisher.declare_topology()
        await publisher.run()


if __name__ == "__main__":
    asyncio.run(main())
