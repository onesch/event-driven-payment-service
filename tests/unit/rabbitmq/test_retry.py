import pytest
from unittest.mock import AsyncMock, Mock
from faststream.rabbit import RabbitBroker
from faststream.rabbit.message import RabbitMessage

from src.rabbitmq.retry import (
    RETRY_COUNT_HEADER,
    get_attempt,
    get_retry_count,
    publish_retry,
)
from src.rabbitmq.routes import EventRoute

ROUTE = EventRoute(
    exchange="payments",
    routing_key="payments.created",
    queue="payments.new",
)


def make_message(headers: dict | None = None) -> Mock:
    """Consumed RabbitMQ message with the attributes publish_retry() copies."""
    message = Mock(spec=RabbitMessage)
    message.headers = headers or {}
    message.body = b'{"payment_id":42}'
    message.content_type = "application/json"
    message.message_id = "1"
    message.correlation_id = "corr-1"
    return message


@pytest.fixture
def retry_broker() -> AsyncMock:
    return AsyncMock(spec=RabbitBroker)


# Test cases for get_retry_count() and get_attempt()


@pytest.mark.parametrize(
    ("headers", "retry_count", "attempt"),
    [
        ({}, 0, 1),
        ({RETRY_COUNT_HEADER: 1}, 1, 2),
        ({RETRY_COUNT_HEADER: "2"}, 2, 3),
    ],
    ids=["first_delivery", "after_first_retry", "string_header"],
)
def test_retry_count_and_attempt_from_headers(headers, retry_count, attempt):
    message = make_message(headers)

    assert get_retry_count(message) == retry_count
    assert get_attempt(message) == attempt


# Test cases for publish_retry()


async def test_publish_retry_parks_message_in_first_retry_queue(retry_broker):
    message = make_message({"event_type": "payment.created"})

    retry = await publish_retry(retry_broker, message, ROUTE)

    assert retry == 1
    retry_broker.publish.assert_awaited_once_with(
        message.body,
        queue="payments.new.retry.1",
        headers={
            "event_type": "payment.created",
            RETRY_COUNT_HEADER: 1,
        },
        content_type="application/json",
        message_id="1",
        correlation_id="corr-1",
        persist=True,
    )


async def test_publish_retry_uses_next_retry_queue(retry_broker):
    message = make_message({RETRY_COUNT_HEADER: 1})

    retry = await publish_retry(retry_broker, message, ROUTE)

    assert retry == 2

    call = retry_broker.publish.await_args
    assert call.kwargs["queue"] == "payments.new.retry.2"
    assert call.kwargs["headers"][RETRY_COUNT_HEADER] == 2


async def test_publish_retry_drops_broker_dead_letter_headers(retry_broker):
    message = make_message(
        {
            "event_type": "payment.created",
            RETRY_COUNT_HEADER: 1,
            "x-death": [{"count": 1, "reason": "expired"}],
            "x-first-death-queue": "payments.new.retry.1",
            "x-first-death-reason": "expired",
            "x-last-death-queue": "payments.new.retry.1",
            "x-last-death-reason": "expired",
            "x-first-death-exchange": "",
        }
    )

    await publish_retry(retry_broker, message, ROUTE)

    assert retry_broker.publish.await_args.kwargs["headers"] == {
        "event_type": "payment.created",
        RETRY_COUNT_HEADER: 2,
    }


async def test_publish_retry_does_not_mutate_original_headers(retry_broker):
    headers = {"event_type": "payment.created"}
    message = make_message(headers)

    await publish_retry(retry_broker, message, ROUTE)

    assert headers == {"event_type": "payment.created"}


async def test_publish_retry_error_propagates(retry_broker):
    retry_broker.publish.side_effect = ConnectionError("rabbitmq unavailable")

    with pytest.raises(ConnectionError):
        await publish_retry(retry_broker, make_message(), ROUTE)
