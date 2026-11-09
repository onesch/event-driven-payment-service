import asyncio
import logging
from dataclasses import dataclass
from sqlalchemy.exc import SQLAlchemyError
from aio_pika.exceptions import AMQPException
from faststream.rabbit import (
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
    ExchangeType,
)

from database.database import async_session_maker
from outbox_service.repositories.outbox import OutboxRepository
from outbox_service.services.publisher.event_router import EVENT_ROUTES
from settings import RABBITMQ_URL

logger = logging.getLogger(__name__)

# Polling configuration
MIN_POLL_INTERVAL = 1
MAX_POLL_INTERVAL = 30
BACKOFF_FACTOR = 2

BATCH_SIZE = 100


@dataclass(frozen=True, slots=True)
class PublishResult:
    """Result of processing an Outbox batch."""

    fetched: int = 0
    published: int = 0
    failed: int = 0


class PollingBackoff:
    """Controls adaptive polling intervals using exponential backoff."""

    def __init__(
        self,
        min_interval: int = MIN_POLL_INTERVAL,
        max_interval: int = MAX_POLL_INTERVAL,
        factor: int = BACKOFF_FACTOR,
    ):
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.factor = factor

        self.current_interval = min_interval

    def reset(self) -> None:
        """Reset polling interval after successful processing."""
        self.current_interval = self.min_interval

    def next_interval(self) -> int:
        """Return current interval and increase it for the next iteration."""
        interval = self.current_interval

        self.current_interval = min(
            self.current_interval * self.factor,
            self.max_interval,
        )

        return interval


class OutboxPublisher:
    """Publishes Outbox events to their configured RabbitMQ queues."""

    def __init__(self, broker: RabbitBroker):
        self.broker = broker
        self.backoff = PollingBackoff()

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

    async def publish_pending(self) -> PublishResult:
        """Fetch and publish a batch of unpublished Outbox events."""

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

        published_count = 0
        failed_count = 0

        for event in event_data:
            route = EVENT_ROUTES.get(event["event_type"])

            if route is None:
                logger.error(
                    "No route configured for event_type=%s, event_id=%s",
                    event["event_type"],
                    event["id"],
                )
                failed_count += 1
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
            except (AMQPException, asyncio.TimeoutError):
                logger.exception(
                    "RabbitMQ publication failed, event_id=%s",
                    event["id"],
                )
                failed_count += 1
                continue

            # Mark as published in PostgreSQL.
            try:
                async with async_session_maker() as session:
                    async with session.begin():
                        repository = OutboxRepository(session)
                        result = await repository.get_by_id(event["id"])

                        if result is not None and result.published_at is None:
                            await repository.mark_published(result)

            except SQLAlchemyError:
                logger.exception(
                    "Failed to mark event as published, event_id=%s",
                    event["id"],
                )
                failed_count += 1
                continue

            published_count += 1

        return PublishResult(
            fetched=len(event_data),
            published=published_count,
            failed=failed_count,
        )

    async def run(self) -> None:
        """Continuously publish events using adaptive polling."""

        while True:
            try:
                result = await self.publish_pending()

            except asyncio.CancelledError:
                raise

            except Exception:
                interval = self.backoff.next_interval()

                logger.exception(
                    "Outbox polling failed. Retrying in %s seconds",
                    interval,
                )

                await asyncio.sleep(interval)
                continue

            # Events were successfully published.
            if result.published > 0:
                self.backoff.reset()

                logger.debug(
                    "Outbox batch processed: fetched=%s, published=%s, failed=%s",
                    result.fetched,
                    result.published,
                    result.failed,
                )

                # Immediately fetch the next batch.
                continue

            # No progress: either the Outbox is empty or all events failed.
            interval = self.backoff.next_interval()

            if result.failed > 0:
                logger.warning(
                    "No Outbox events published: fetched=%s, failed=%s. "
                    "Retrying in %s seconds",
                    result.fetched,
                    result.failed,
                    interval,
                )
            else:
                logger.debug(
                    "No unpublished Outbox events. Next poll in %s seconds",
                    interval,
                )

            await asyncio.sleep(interval)


async def main() -> None:
    """Initialize RabbitMQ topology and start the publisher."""

    broker = RabbitBroker(url=RABBITMQ_URL)

    async with broker:
        publisher = OutboxPublisher(broker)

        await publisher.declare_topology()
        await publisher.run()


if __name__ == "__main__":
    asyncio.run(main())
