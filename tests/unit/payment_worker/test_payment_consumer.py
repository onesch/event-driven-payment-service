import asyncio
import logging

import pytest
from faststream import AckPolicy
from faststream.rabbit import ExchangeType

from src.settings import EVENT_ROUTES
from src.payment_worker.consumers import payment_consumer as consumer_module
from src.payment_worker.consumers.payment_consumer import (
    PAYMENT_ROUTE,
    handle_payment,
    main,
)


# Test cases for the broker topology configured by the consumer


def test_payment_route_uses_configured_event_route():
    assert PAYMENT_ROUTE == EVENT_ROUTES["payment.created"]


def test_consumer_subscribes_to_payment_created_queue():
    (subscriber,) = consumer_module.broker.subscribers

    assert subscriber.queue.name == PAYMENT_ROUTE.queue
    assert subscriber.queue.routing_key == PAYMENT_ROUTE.routing_key
    assert subscriber.queue.durable is True

    assert subscriber.exchange.name == PAYMENT_ROUTE.exchange
    assert subscriber.exchange.type is ExchangeType.DIRECT
    assert subscriber.exchange.durable is True


def test_consumer_nacks_failed_messages():
    """Failed deliveries must be NACKed so RabbitMQ can redeliver them."""
    (subscriber,) = consumer_module.broker.subscribers

    assert subscriber.ack_policy is AckPolicy.NACK_ON_ERROR


def test_consumer_registers_payment_handler():
    (subscriber,) = consumer_module.broker.subscribers

    assert [handler.name for handler in subscriber.calls] == ["handle_payment"]


# Test cases for handle_payment() handler


@pytest.mark.asyncio
async def test_handle_payment_processes_payment(
    payment_message,
    processor_process,
):
    result = await handle_payment(payment_message)

    assert result is None
    processor_process.assert_awaited_once_with(
        payment_id=payment_message["payment_id"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message",
    [
        {},
        {"payment_id": None},
        {"event_type": "payment.created"},
    ],
    ids=[
        "missing_payment_id",
        "none_payment_id",
        "unrelated_fields",
    ],
)
async def test_handle_payment_rejects_message_without_payment_id(
    message,
    processor_process,
    caplog,
):
    with pytest.raises(
        ValueError,
        match="Payment event must contain payment_id",
    ):
        await handle_payment(message)

    processor_process.assert_not_awaited()

    error_records = [
        record
        for record in caplog.records
        if record.levelno == logging.ERROR
    ]

    assert len(error_records) == 1
    assert error_records[0].getMessage() == (
        f"Received payment event without payment_id: {message}"
    )


@pytest.mark.asyncio
async def test_handle_payment_accepts_zero_payment_id(processor_process):
    await handle_payment({"payment_id": 0})

    processor_process.assert_awaited_once_with(payment_id=0)


@pytest.mark.asyncio
async def test_handle_payment_forwards_only_payment_id(processor_process):
    await handle_payment(
        {
            "payment_id": 7,
            "event_type": "payment.created",
            "amount": "10.00",
        }
    )

    processor_process.assert_awaited_once_with(payment_id=7)


@pytest.mark.asyncio
async def test_handle_payment_logs_processing(
    payment_message,
    processor_process,
    caplog,
):
    caplog.set_level(logging.INFO, logger=consumer_module.logger.name)

    await handle_payment(payment_message)

    assert (
        f"Received payment event: payment_id={payment_message['payment_id']}"
        in caplog.text
    )
    assert (
        f"Payment processed: payment_id={payment_message['payment_id']}"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_handle_payment_processing_error_propagates(
    payment_message,
    processor_process,
):
    """Errors must escape the handler so the broker NACKs the message."""
    processor_process.side_effect = RuntimeError("processing failed")

    with pytest.raises(RuntimeError, match="processing failed"):
        await handle_payment(payment_message)


# Test cases for main()


@pytest.mark.asyncio
async def test_main_runs_until_stopped(consumer_broker, consumer_event_wait):
    await main()

    consumer_broker.start.assert_awaited_once_with()
    consumer_event_wait.assert_awaited_once_with()
    consumer_broker.stop.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_main_stops_broker_on_cancellation(
    consumer_broker,
    consumer_event_wait,
):
    consumer_event_wait.side_effect = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await main()

    consumer_broker.start.assert_awaited_once_with()
    consumer_broker.stop.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_main_stops_broker_on_unexpected_error(
    consumer_broker,
    consumer_event_wait,
):
    consumer_event_wait.side_effect = RuntimeError("consumer crashed")

    with pytest.raises(RuntimeError, match="consumer crashed"):
        await main()

    consumer_broker.start.assert_awaited_once_with()
    consumer_broker.stop.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_main_propagates_startup_failure(
    consumer_broker,
    consumer_event_wait,
):
    consumer_broker.start.side_effect = ConnectionError("rabbitmq unavailable")

    with pytest.raises(ConnectionError, match="rabbitmq unavailable"):
        await main()

    consumer_event_wait.assert_not_awaited()
    consumer_broker.stop.assert_not_awaited()
