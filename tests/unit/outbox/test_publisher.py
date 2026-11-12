import pytest
import asyncio
from unittest.mock import AsyncMock, Mock
from datetime import datetime, timezone
from aio_pika.exceptions import AMQPException
from faststream.rabbit import ExchangeType
from sqlalchemy.exc import SQLAlchemyError

from src.outbox_service.services.publisher import publisher as publisher_module
from src.outbox_service.services.publisher.event_router import EVENT_ROUTES
from src.outbox_service.services.publisher.publisher import (
    BACKOFF_FACTOR,
    BATCH_SIZE,
    MAX_POLL_INTERVAL,
    MIN_POLL_INTERVAL,
    OutboxPublisher,
    PollingBackoff,
    PublishResult,
)


@pytest.fixture
def session_maker_mock(monkeypatch):
    """Mock the global async SQLAlchemy session factory."""
    session = AsyncMock()

    session.__aenter__.return_value = session

    transaction = AsyncMock()
    session.begin = Mock(return_value=transaction)

    maker = Mock(return_value=session)
    monkeypatch.setattr(
        publisher_module,
        "async_session_maker",
        maker,
    )

    return session


@pytest.fixture
def repository_mock(monkeypatch):
    """Mock OutboxRepository used by the publisher."""
    repository = AsyncMock()

    monkeypatch.setattr(
        publisher_module,
        "OutboxRepository",
        Mock(return_value=repository),
    )

    return repository


# Test cases for PollingBackoff class


def test_backoff_starts_at_min_interval():
    backoff = PollingBackoff()

    assert backoff.current_interval == MIN_POLL_INTERVAL


def test_backoff_grows_exponentially_and_respects_max_interval():
    backoff = PollingBackoff()

    intervals = [
        backoff.next_interval()
        for _ in range(7)
    ]

    assert intervals == [1, 2, 4, 8, 16, 30, 30]


def test_backoff_reset_restores_min_interval():
    backoff = PollingBackoff()

    for _ in range(4):
        backoff.next_interval()

    backoff.reset()

    assert backoff.current_interval == MIN_POLL_INTERVAL
    assert backoff.next_interval() == MIN_POLL_INTERVAL


def test_backoff_supports_custom_configuration():
    backoff = PollingBackoff(
        min_interval=1,
        max_interval=3,
        factor=BACKOFF_FACTOR,
    )

    assert [
        backoff.next_interval()
        for _ in range(4)
    ] == [1, 2, 3, 3]


# Test case for PublishResult dataclass


def test_publish_result_defaults_to_zero():
    result = PublishResult()

    assert result == PublishResult(
        fetched=0,
        published=0,
        failed=0,
    )


# Test cases for OutboxPublisher.publish_pending() method


@pytest.mark.asyncio
async def test_publish_pending_empty_batch(
    outbox_broker,
    session_maker_mock,
    repository_mock,
):
    repository_mock.get_unpublished.return_value = []

    publisher = OutboxPublisher(outbox_broker)

    result = await publisher.publish_pending()

    assert result == PublishResult()

    repository_mock.get_unpublished.assert_awaited_once_with(
        limit=BATCH_SIZE,
    )

    outbox_broker.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_pending_publishes_event_and_marks_it(
    outbox_broker,
    session_maker_mock,
    repository_mock,
    outbox_event,
):
    repository_mock.get_unpublished.return_value = [outbox_event]
    repository_mock.get_by_id.return_value = outbox_event

    publisher = OutboxPublisher(outbox_broker)

    result = await publisher.publish_pending()

    assert result == PublishResult(
        fetched=1,
        published=1,
        failed=0,
    )

    outbox_broker.publish.assert_awaited_once()

    call = outbox_broker.publish.await_args

    assert call.kwargs["message"] == outbox_event.payload
    assert call.kwargs["message_id"] == str(outbox_event.id)
    assert call.kwargs["persist"] is True
    assert call.kwargs["headers"] == {
        "event_type": outbox_event.event_type,
    }

    route = EVENT_ROUTES[outbox_event.event_type]

    assert call.kwargs["routing_key"] == route.routing_key
    assert call.kwargs["exchange"].name == route.exchange
    assert call.kwargs["exchange"].type == ExchangeType.DIRECT
    assert call.kwargs["exchange"].durable is True

    repository_mock.get_by_id.assert_awaited_once_with(
        outbox_event.id,
    )

    repository_mock.mark_published.assert_awaited_once_with(
        outbox_event,
    )

    # The published status must be updated inside a transaction.
    session_maker_mock.begin.assert_called_once()


@pytest.mark.asyncio
async def test_publish_pending_processes_multiple_events(
    outbox_broker,
    session_maker_mock,
    repository_mock,
    outbox_event,
):
    second_event = type(outbox_event)(
        id=2,
        event_type=outbox_event.event_type,
        payload={"payment_id": 2},
        created_at=outbox_event.created_at,
        published_at=None,
    )

    events = [outbox_event, second_event]

    repository_mock.get_unpublished.return_value = events
    repository_mock.get_by_id.side_effect = events

    publisher = OutboxPublisher(outbox_broker)

    result = await publisher.publish_pending()

    assert result == PublishResult(
        fetched=2,
        published=2,
        failed=0,
    )

    assert outbox_broker.publish.await_count == 2

    published_ids = [
        call.kwargs["message_id"]
        for call in outbox_broker.publish.await_args_list
    ]

    assert published_ids == ["1", "2"]


@pytest.mark.asyncio
async def test_publish_pending_unknown_event_type(
    outbox_broker,
    session_maker_mock,
    repository_mock,
    outbox_event,
):
    outbox_event.event_type = "unknown.event"

    repository_mock.get_unpublished.return_value = [outbox_event]

    publisher = OutboxPublisher(outbox_broker)

    result = await publisher.publish_pending()

    assert result == PublishResult(
        fetched=1,
        published=0,
        failed=1,
    )

    outbox_broker.publish.assert_not_awaited()
    repository_mock.get_by_id.assert_not_awaited()
    repository_mock.mark_published.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        AMQPException("broker unavailable"),
        asyncio.TimeoutError(),
    ],
)
async def test_publish_pending_broker_failure_does_not_mark_event(
    outbox_broker,
    session_maker_mock,
    repository_mock,
    outbox_event,
    error,
):
    repository_mock.get_unpublished.return_value = [outbox_event]
    outbox_broker.publish.side_effect = error

    publisher = OutboxPublisher(outbox_broker)

    result = await publisher.publish_pending()

    assert result == PublishResult(
        fetched=1,
        published=0,
        failed=1,
    )

    repository_mock.get_by_id.assert_not_awaited()
    repository_mock.mark_published.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_pending_database_failure_after_publish(
    outbox_broker,
    session_maker_mock,
    repository_mock,
    outbox_event,
):
    repository_mock.get_unpublished.return_value = [outbox_event]
    repository_mock.get_by_id.return_value = outbox_event

    repository_mock.mark_published.side_effect = SQLAlchemyError(
        "connection lost",
    )

    publisher = OutboxPublisher(outbox_broker)

    result = await publisher.publish_pending()

    assert result == PublishResult(
        fetched=1,
        published=0,
        failed=1,
    )

    outbox_broker.publish.assert_awaited_once()
    repository_mock.mark_published.assert_awaited_once()


@pytest.mark.asyncio
async def test_publish_pending_already_marked_event(
    outbox_broker,
    session_maker_mock,
    repository_mock,
    outbox_event,
):
    outbox_event.published_at = datetime.now(timezone.utc)

    repository_mock.get_unpublished.return_value = [outbox_event]
    repository_mock.get_by_id.return_value = outbox_event

    publisher = OutboxPublisher(outbox_broker)

    result = await publisher.publish_pending()

    repository_mock.mark_published.assert_not_awaited()

    assert result.published == 1


@pytest.mark.asyncio
async def test_publish_pending_fetch_failure_propagates(
    outbox_broker,
    session_maker_mock,
    repository_mock,
):
    repository_mock.get_unpublished.side_effect = SQLAlchemyError(
        "database unavailable",
    )

    publisher = OutboxPublisher(outbox_broker)

    with pytest.raises(SQLAlchemyError):
        await publisher.publish_pending()

    outbox_broker.publish.assert_not_awaited()


# Test cases for OutboxPublisher.run() method


@pytest.mark.asyncio
async def test_run_resets_backoff_after_successful_batch(
    outbox_broker,
    monkeypatch,
):
    publisher = OutboxPublisher(outbox_broker)
    publisher.backoff.current_interval = MAX_POLL_INTERVAL
    publisher.publish_pending = AsyncMock(
        side_effect=[
            PublishResult(fetched=1, published=1, failed=0),
            asyncio.CancelledError(),
        ]
    )
    sleep = AsyncMock()
    monkeypatch.setattr(publisher_module.asyncio, "sleep", sleep)

    with pytest.raises(asyncio.CancelledError):
        await publisher.run()

    sleep.assert_not_awaited()
    assert publisher.backoff.current_interval == MIN_POLL_INTERVAL


@pytest.mark.asyncio
async def test_run_backs_off_when_batch_is_empty(
    outbox_broker,
    monkeypatch,
):
    publisher = OutboxPublisher(outbox_broker)
    publisher.publish_pending = AsyncMock(
        side_effect=[PublishResult(), asyncio.CancelledError()]
    )
    sleep = AsyncMock()
    monkeypatch.setattr(publisher_module.asyncio, "sleep", sleep)

    with pytest.raises(asyncio.CancelledError):
        await publisher.run()

    sleep.assert_awaited_once_with(MIN_POLL_INTERVAL)


@pytest.mark.asyncio
async def test_run_backs_off_when_all_events_failed(
    outbox_broker,
    monkeypatch,
):
    publisher = OutboxPublisher(outbox_broker)
    publisher.publish_pending = AsyncMock(
        side_effect=[
            PublishResult(fetched=2, published=0, failed=2),
            asyncio.CancelledError(),
        ]
    )
    sleep = AsyncMock()
    monkeypatch.setattr(publisher_module.asyncio, "sleep", sleep)

    with pytest.raises(asyncio.CancelledError):
        await publisher.run()

    sleep.assert_awaited_once_with(MIN_POLL_INTERVAL)


@pytest.mark.asyncio
async def test_run_applies_exponential_backoff_on_consecutive_errors(
    outbox_broker,
    monkeypatch,
):
    publisher = OutboxPublisher(outbox_broker)
    publisher.publish_pending = AsyncMock(
        side_effect=[
            Exception("boom"),
            Exception("boom"),
            asyncio.CancelledError(),
        ]
    )
    sleep = AsyncMock()
    monkeypatch.setattr(publisher_module.asyncio, "sleep", sleep)

    with pytest.raises(asyncio.CancelledError):
        await publisher.run()

    assert [call.args[0] for call in sleep.await_args_list] == [
        MIN_POLL_INTERVAL,
        MIN_POLL_INTERVAL * BACKOFF_FACTOR,
    ]


@pytest.mark.asyncio
async def test_run_propagates_cancellation(
    outbox_broker,
    monkeypatch,
):
    publisher = OutboxPublisher(outbox_broker)
    publisher.publish_pending = AsyncMock(side_effect=asyncio.CancelledError())
    sleep = AsyncMock()
    monkeypatch.setattr(publisher_module.asyncio, "sleep", sleep)

    with pytest.raises(asyncio.CancelledError):
        await publisher.run()

    sleep.assert_not_awaited()
