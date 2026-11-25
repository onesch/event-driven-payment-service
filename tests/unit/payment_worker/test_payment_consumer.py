import logging

import pytest
from faststream import AckPolicy
from pydantic import ValidationError

from src.rabbitmq.routes import EVENT_ROUTES
from src.rabbitmq.topology import build_exchange, build_queue
from src.payment_worker.consumers import payment_consumer as consumer_module
from src.payment_worker.consumers.payment_consumer import (
    PAYMENT_ROUTE,
    handle_payment,
)


async def publish_payment_event(broker, message) -> None:
    """Publish a message the same way the Outbox publisher does."""
    await broker.publish(
        message,
        exchange=build_exchange(PAYMENT_ROUTE),
        routing_key=PAYMENT_ROUTE.routing_key,
    )


# Test cases for the broker topology configured by the consumer


def test_payment_route_uses_configured_event_route():
    assert PAYMENT_ROUTE == EVENT_ROUTES["payment.created"]


def test_consumer_subscribes_using_shared_topology():
    """The consumer must declare exactly what the publisher declares."""
    (subscriber,) = consumer_module.broker.subscribers

    assert subscriber.queue == build_queue(PAYMENT_ROUTE)
    assert subscriber.exchange == build_exchange(PAYMENT_ROUTE)


def test_consumer_subscribes_to_payment_created_queue():
    (subscriber,) = consumer_module.broker.subscribers

    assert subscriber.queue.name == PAYMENT_ROUTE.queue
    assert subscriber.queue.routing_key == PAYMENT_ROUTE.routing_key
    assert subscriber.queue.durable is True

    assert subscriber.exchange.name == PAYMENT_ROUTE.exchange
    assert subscriber.exchange.durable is True


def test_consumer_nacks_failed_messages():
    """Failed deliveries must be NACKed so RabbitMQ can redeliver them."""
    (subscriber,) = consumer_module.broker.subscribers

    assert subscriber.ack_policy is AckPolicy.NACK_ON_ERROR


def test_consumer_registers_payment_handler():
    (subscriber,) = consumer_module.broker.subscribers

    assert [handler.name for handler in subscriber.calls] == ["handle_payment"]


# Test cases for handle_payment() called directly


async def test_handle_payment_processes_payment(
    payment_event,
    processor_process,
):
    result = await handle_payment(payment_event)

    assert result is None
    processor_process.assert_awaited_once_with(
        payment_id=payment_event.payment_id,
    )


async def test_handle_payment_logs_processing(
    payment_event,
    processor_process,
    caplog,
):
    caplog.set_level(logging.INFO, logger=consumer_module.logger.name)

    await handle_payment(payment_event)

    assert (
        f"Received payment event: payment_id={payment_event.payment_id}"
        in caplog.text
    )
    assert (
        f"Payment processed: payment_id={payment_event.payment_id}"
        in caplog.text
    )


async def test_handle_payment_processing_error_propagates(
    payment_event,
    processor_process,
    caplog,
):
    """Errors must escape the handler so the broker NACKs the message."""
    caplog.set_level(logging.INFO, logger=consumer_module.logger.name)
    processor_process.side_effect = RuntimeError("processing failed")

    with pytest.raises(RuntimeError, match="processing failed"):
        await handle_payment(payment_event)

    assert "Payment processed" not in caplog.text


# Test cases for messages delivered through the broker


async def test_published_message_is_processed(
    consumer_test_broker,
    payment_message,
    processor_process,
):
    await publish_payment_event(consumer_test_broker, payment_message)

    handle_payment.mock.assert_called_once_with(payment_message)
    processor_process.assert_awaited_once_with(
        payment_id=payment_message["payment_id"],
    )


async def test_published_message_extra_fields_are_ignored(
    consumer_test_broker,
    processor_process,
):
    await publish_payment_event(
        consumer_test_broker,
        {
            "payment_id": 7,
            "event_type": "payment.created",
            "amount": "10.00",
        },
    )

    processor_process.assert_awaited_once_with(payment_id=7)


async def test_published_message_numeric_string_is_coerced(
    consumer_test_broker,
    processor_process,
):
    await publish_payment_event(consumer_test_broker, {"payment_id": "7"})

    processor_process.assert_awaited_once_with(payment_id=7)


@pytest.mark.parametrize(
    "message",
    [
        {},
        {"payment_id": None},
        {"payment_id": 0},
        {"payment_id": -1},
        {"payment_id": "abc"},
        {"event_type": "payment.created"},
    ],
    ids=[
        "missing_payment_id",
        "none_payment_id",
        "zero_payment_id",
        "negative_payment_id",
        "non_numeric_payment_id",
        "unrelated_fields",
    ],
)
async def test_published_invalid_message_is_rejected(
    consumer_test_broker,
    processor_process,
    message,
):
    with pytest.raises(ValidationError):
        await publish_payment_event(consumer_test_broker, message)

    processor_process.assert_not_awaited()


async def test_published_message_processing_error_propagates(
    consumer_test_broker,
    payment_message,
    processor_process,
):
    processor_process.side_effect = RuntimeError("processing failed")

    with pytest.raises(RuntimeError, match="processing failed"):
        await publish_payment_event(consumer_test_broker, payment_message)
