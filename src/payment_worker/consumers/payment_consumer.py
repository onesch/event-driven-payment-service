import asyncio
import logging
from faststream import AckPolicy
from faststream.rabbit import (
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
    ExchangeType,
)

from src.payment_worker.services.payment_processor import PaymentProcessor
from src.settings import EVENT_ROUTES, RABBITMQ_URL

logger = logging.getLogger(__name__)

PAYMENT_ROUTE = EVENT_ROUTES["payment.created"]

broker = RabbitBroker(
    url=RABBITMQ_URL,
    ack_policy=AckPolicy.NACK_ON_ERROR,
)


@broker.subscriber(
    queue=RabbitQueue(
        name=PAYMENT_ROUTE.queue,
        durable=True,
        routing_key=PAYMENT_ROUTE.routing_key,
    ),
    exchange=RabbitExchange(
        name=PAYMENT_ROUTE.exchange,
        type=ExchangeType.DIRECT,
        durable=True,
    ),
)
async def handle_payment(message: dict) -> None:
    """Consume payment.created events and process payments."""

    payment_id = message.get("payment_id")

    if payment_id is None:
        logger.error("Received payment event without payment_id: %s", message)
        return

    logger.info("Received payment event: payment_id=%s", payment_id)

    await PaymentProcessor.process(payment_id=payment_id)

    logger.info("Payment processed: payment_id=%s", payment_id)


async def main() -> None:
    """Start the payment consumer."""

    logger.info("Starting payment consumer...")

    await broker.start()

    try:
        await asyncio.Event().wait()
    finally:
        await broker.close()


if __name__ == "__main__":
    asyncio.run(main())
