import logging
from faststream import AckPolicy
from faststream.rabbit import RabbitBroker

from src.payment_worker.schemas.payment import PaymentCreatedEvent
from src.payment_worker.services.payment_processor import PaymentProcessor
from src.rabbitmq.routes import EVENT_ROUTES
from src.rabbitmq.topology import build_exchange, build_queue
from src.settings import RABBITMQ_URL

logger = logging.getLogger(__name__)

PAYMENT_ROUTE = EVENT_ROUTES["payment.created"]

broker = RabbitBroker(
    url=RABBITMQ_URL,
    ack_policy=AckPolicy.NACK_ON_ERROR,
)


@broker.subscriber(
    queue=build_queue(PAYMENT_ROUTE),
    exchange=build_exchange(PAYMENT_ROUTE),
)
async def handle_payment(message: PaymentCreatedEvent) -> None:
    """Consume payment.created events and process payments."""

    payment_id = message.payment_id

    logger.info("Received payment event: payment_id=%s", payment_id)

    await PaymentProcessor.process(payment_id=payment_id)

    logger.info("Payment processed: payment_id=%s", payment_id)
