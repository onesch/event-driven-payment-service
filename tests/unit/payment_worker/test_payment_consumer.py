import logging

import pytest
from faststream import AckPolicy
from pydantic import ValidationError

from src.rabbitmq.retry import RETRY_COUNT_HEADER
from src.rabbitmq.routes import EVENT_ROUTES
from src.rabbitmq.topology import build_exchange, build_queue
from src.payment_worker.consumers import payment_consumer as consumer_module
from src.payment_worker.consumers.payment_consumer import (
    PAYMENT_ROUTE,
    handle_payment,
)

MAX_ATTEMPTS = PAYMENT_ROUTE.retry.max_attempts


async def publish_payment_event(broker, message, headers=None) -> None:
    """Publish a message the same way the Outbox publisher does."""
    await broker.publish(
        message,
        exchange=build_exchange(PAYMENT_ROUTE),
        routing_key=PAYMENT_ROUTE.routing_key,
        headers=headers,
    )


# Test cases for the broker topology configured by the consumer


def test_payment_route_uses_configured_event_route():
    assert PAYMENT_ROUTE == EVENT_ROUTES["payment.created"]


def test_consumer_subscribes_using_shared_topology():
    """The consumer must declare exactly what the publisher declares."""
    (subscriber,) = consumer_module.broker.subscribers

    assert subscriber.queue == build_queue(PAYMENT_ROUTE)
    assert subscriber.queue.arguments == build_queue(PAYMENT_ROUTE).arguments
    assert subscriber.exchange == build_exchange(PAYMENT_ROUTE)


def test_consumer_subscribes_to_payment_created_queue():
    (subscriber,) = consumer_module.broker.subscribers

    assert subscriber.queue.name == PAYMENT_ROUTE.queue
    assert subscriber.queue.routing_key == PAYMENT_ROUTE.routing_key
    assert subscriber.queue.durable is True

    assert subscriber.exchange.name == PAYMENT_ROUTE.exchange
    assert subscriber.exchange.durable is True


def test_consumer_rejects_failed_messages():
    """
    Unhandled errors must reject the message without requeue,
    so RabbitMQ dead-letters it to the DLQ instead of redelivering it.
    """
    (subscriber,) = consumer_module.broker.subscribers

    assert subscriber.ack_policy is AckPolicy.REJECT_ON_ERROR


def test_consumer_registers_payment_handler():
    (subscriber,) = consumer_module.broker.subscribers

    assert [handler.name for handler in subscriber.calls] == ["handle_payment"]


# Test cases for handle_payment() called directly


async def test_handle_payment_processes_payment(
    payment_event,
    raw_payment_message,
    processor_process,
    retry_publisher,
):
    result = await handle_payment(payment_event, raw_payment_message)

    assert result is None
    processor_process.assert_awaited_once_with(
        payment_id=payment_event.payment_id,
    )
    retry_publisher.assert_not_awaited()


async def test_handle_payment_logs_processing(
    payment_event,
    raw_payment_message,
    processor_process,
    caplog,
):
    caplog.set_level(logging.INFO, logger=consumer_module.logger.name)

    await handle_payment(payment_event, raw_payment_message)

    assert (
        f"Received payment event: payment_id={payment_event.payment_id}, "
        f"attempt=1/{MAX_ATTEMPTS}"
    ) in caplog.text
    assert (
        f"Payment processed: payment_id={payment_event.payment_id}"
        in caplog.text
    )


@pytest.mark.parametrize(
    ("retry_count", "expected_retry"),
    [(0, 1), (MAX_ATTEMPTS - 2, MAX_ATTEMPTS - 1)],
    ids=["first_attempt", "second_to_last_attempt"],
)
async def test_handle_payment_failure_schedules_retry(
    payment_event,
    raw_payment_message,
    processor_process,
    retry_publisher,
    caplog,
    retry_count,
    expected_retry,
):
    """A retried message is acked: the handler must not raise."""
    raw_payment_message.headers = {RETRY_COUNT_HEADER: retry_count}
    processor_process.side_effect = RuntimeError("processing failed")

    await handle_payment(payment_event, raw_payment_message)

    retry_publisher.assert_awaited_once_with(
        consumer_module.broker,
        raw_payment_message,
        PAYMENT_ROUTE,
    )
    assert (
        f"retry {expected_retry} scheduled in "
        f"{PAYMENT_ROUTE.retry.delay_ms(expected_retry)} ms"
    ) in caplog.text
    assert "Payment processed" not in caplog.text


@pytest.mark.parametrize(
    "retry_count",
    [MAX_ATTEMPTS - 1, MAX_ATTEMPTS + 5],
    ids=["last_attempt", "beyond_last_attempt"],
)
async def test_handle_payment_failure_on_last_attempt_propagates(
    payment_event,
    raw_payment_message,
    processor_process,
    retry_publisher,
    caplog,
    retry_count,
):
    """The error must escape the handler so the broker rejects it to the DLQ."""
    raw_payment_message.headers = {RETRY_COUNT_HEADER: retry_count}
    processor_process.side_effect = RuntimeError("processing failed")

    with pytest.raises(RuntimeError, match="processing failed"):
        await handle_payment(payment_event, raw_payment_message)

    retry_publisher.assert_not_awaited()
    assert "moving to DLQ" in caplog.text


async def test_handle_payment_retry_publish_failure_propagates(
    payment_event,
    raw_payment_message,
    processor_process,
    retry_publisher,
):
    """If the retry cannot be scheduled, the message goes to the DLQ, not lost."""
    processor_process.side_effect = RuntimeError("processing failed")
    retry_publisher.side_effect = ConnectionError("rabbitmq unavailable")

    with pytest.raises(ConnectionError, match="rabbitmq unavailable"):
        await handle_payment(payment_event, raw_payment_message)


# Test cases for messages delivered through the broker


async def test_published_message_is_processed(
    consumer_test_broker,
    payment_message,
    processor_process,
    retry_publisher,
):
    await publish_payment_event(consumer_test_broker, payment_message)

    handle_payment.mock.assert_called_once_with(payment_message)
    processor_process.assert_awaited_once_with(
        payment_id=payment_message["payment_id"],
    )
    retry_publisher.assert_not_awaited()


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
async def test_published_invalid_message_is_rejected_without_retry(
    consumer_test_broker,
    processor_process,
    retry_publisher,
    message,
):
    """Invalid messages go straight to the DLQ: retrying cannot fix them."""
    with pytest.raises(ValidationError):
        await publish_payment_event(consumer_test_broker, message)

    processor_process.assert_not_awaited()
    retry_publisher.assert_not_awaited()


async def test_published_message_processing_error_schedules_retry(
    consumer_test_broker,
    payment_message,
    processor_process,
    retry_publisher,
):
    processor_process.side_effect = RuntimeError("processing failed")

    await publish_payment_event(consumer_test_broker, payment_message)

    retry_publisher.assert_awaited_once()

    _, raw_message, route = retry_publisher.await_args.args
    assert raw_message.headers.get(RETRY_COUNT_HEADER) is None
    assert route == PAYMENT_ROUTE


async def test_published_message_processing_error_on_last_attempt_propagates(
    consumer_test_broker,
    payment_message,
    processor_process,
    retry_publisher,
):
    processor_process.side_effect = RuntimeError("processing failed")

    with pytest.raises(RuntimeError, match="processing failed"):
        await publish_payment_event(
            consumer_test_broker,
            payment_message,
            headers={RETRY_COUNT_HEADER: MAX_ATTEMPTS - 1},
        )

    retry_publisher.assert_not_awaited()
