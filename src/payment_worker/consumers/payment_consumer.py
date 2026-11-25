import logging
from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from src.payment_worker.schemas.payment import PaymentCreatedEvent
from src.payment_worker.services.payment_processor import PaymentProcessor
from src.rabbitmq.retry import get_attempt, publish_retry
from src.rabbitmq.routes import EVENT_ROUTES
from src.rabbitmq.topology import build_exchange, build_queue
from src.settings import RABBITMQ_URL

logger = logging.getLogger(__name__)

PAYMENT_ROUTE = EVENT_ROUTES["payment.created"]

broker = RabbitBroker(
    url=RABBITMQ_URL,
    ack_policy=AckPolicy.REJECT_ON_ERROR,
)


@broker.subscriber(
    queue=build_queue(PAYMENT_ROUTE),
    exchange=build_exchange(PAYMENT_ROUTE),
)
async def handle_payment(
    message: PaymentCreatedEvent,
    raw_message: RabbitMessage,
) -> None:
    """Consume payment.created events and process payments."""

    payment_id = message.payment_id
    attempt = get_attempt(raw_message)
    max_attempts = PAYMENT_ROUTE.retry.max_attempts

    logger.info(
        "Received payment event: payment_id=%s, attempt=%s/%s",
        payment_id,
        attempt,
        max_attempts,
    )

    try:
        await PaymentProcessor.process(payment_id=payment_id)

    except Exception:
        if attempt >= max_attempts:
            logger.exception(
                "Payment processing failed after %s attempts, "
                "moving to DLQ: payment_id=%s",
                attempt,
                payment_id,
            )
            raise

        retry = await publish_retry(broker, raw_message, PAYMENT_ROUTE)

        logger.exception(
            "Payment processing failed, retry %s scheduled in %s ms: "
            "payment_id=%s, attempt=%s/%s",
            retry,
            PAYMENT_ROUTE.retry.delay_ms(retry),
            payment_id,
            attempt,
            max_attempts,
        )
        return

    logger.info("Payment processed: payment_id=%s", payment_id)
