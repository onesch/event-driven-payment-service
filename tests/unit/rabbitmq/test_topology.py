import pytest
from unittest.mock import AsyncMock, Mock, call
from aio_pika import RobustExchange, RobustQueue
from faststream.rabbit import ExchangeType, RabbitBroker

from src.rabbitmq import topology as topology_module
from src.rabbitmq.routes import EventRoute, RetryPolicy
from src.rabbitmq.topology import (
    DEFAULT_EXCHANGE,
    build_dead_letter_exchange,
    build_dead_letter_queue,
    build_exchange,
    build_queue,
    build_retry_queue,
    build_retry_queues,
    declare_topology,
)

ROUTE = EventRoute(
    exchange="payments",
    routing_key="payments.created",
    queue="payments.new",
    retry=RetryPolicy(max_attempts=3, base_delay_ms=1_000, backoff_factor=2),
)


@pytest.fixture
def topology_broker() -> AsyncMock:
    """Mocked broker returning a distinct aio_pika-like object per declaration."""
    broker = AsyncMock(spec=RabbitBroker)

    broker.declare_exchange.side_effect = lambda exchange: Mock(
        spec=RobustExchange, name=exchange.name
    )
    broker.declare_queue.side_effect = lambda queue: Mock(
        spec=RobustQueue, name=queue.name
    )

    return broker


@pytest.fixture
def single_route(monkeypatch):
    monkeypatch.setattr(topology_module, "EVENT_ROUTES", {"payment.created": ROUTE})


# Test cases for the main exchange and queue


def test_build_exchange_is_durable_direct_exchange():
    exchange = build_exchange(ROUTE)

    assert exchange.name == ROUTE.exchange
    assert exchange.type == ExchangeType.DIRECT
    assert exchange.durable is True


def test_build_queue_is_durable_and_bound_by_routing_key():
    queue = build_queue(ROUTE)

    assert queue.name == ROUTE.queue
    assert queue.routing_key == ROUTE.routing_key
    assert queue.durable is True


def test_build_queue_dead_letters_rejected_messages_to_dlq():
    queue = build_queue(ROUTE)

    # FastStream adds its own arguments (e.g. x-queue-type) on top of ours.
    assert queue.arguments.items() >= {
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.new.dlq",
    }.items()


def test_builders_return_equal_objects_on_each_call():
    """Publisher and consumer must declare identical topology."""
    assert build_exchange(ROUTE) == build_exchange(ROUTE)
    assert build_queue(ROUTE) == build_queue(ROUTE)


# Test cases for the dead-letter exchange and queue


def test_build_dead_letter_exchange_is_durable_direct_exchange():
    exchange = build_dead_letter_exchange(ROUTE)

    assert exchange.name == "payments.dlx"
    assert exchange.type == ExchangeType.DIRECT
    assert exchange.durable is True


def test_build_dead_letter_queue_matches_main_queue_dead_letter_key():
    queue = build_dead_letter_queue(ROUTE)

    assert queue.name == "payments.new.dlq"
    assert queue.durable is True
    assert (
        queue.routing_key
        == build_queue(ROUTE).arguments["x-dead-letter-routing-key"]
    )


# Test cases for retry queues


def test_build_retry_queue_returns_expired_messages_to_main_queue():
    queue = build_retry_queue(ROUTE, retry=1)

    assert queue.name == "payments.new.retry.1"
    assert queue.durable is True
    assert queue.arguments.items() >= {
        "x-message-ttl": 1_000,
        "x-dead-letter-exchange": DEFAULT_EXCHANGE,
        "x-dead-letter-routing-key": ROUTE.queue,
    }.items()


def test_build_retry_queues_one_per_retry_with_growing_ttl():
    queues = build_retry_queues(ROUTE)

    assert [queue.name for queue in queues] == [
        "payments.new.retry.1",
        "payments.new.retry.2",
    ]
    assert [queue.arguments["x-message-ttl"] for queue in queues] == [
        1_000,
        2_000,
    ]


def test_build_retry_queues_empty_when_retries_disabled():
    route = EventRoute(
        exchange="payments",
        routing_key="payments.created",
        queue="payments.new",
        retry=RetryPolicy(max_attempts=1),
    )

    assert build_retry_queues(route) == []


# Test cases for declare_topology()


async def test_declare_topology_declares_all_exchanges(
    topology_broker,
    single_route,
):
    await declare_topology(topology_broker)

    assert topology_broker.declare_exchange.await_args_list == [
        call(build_dead_letter_exchange(ROUTE)),
        call(build_exchange(ROUTE)),
    ]


async def test_declare_topology_declares_all_queues(
    topology_broker,
    single_route,
):
    await declare_topology(topology_broker)

    assert topology_broker.declare_queue.await_args_list == [
        call(build_dead_letter_queue(ROUTE)),
        call(build_retry_queue(ROUTE, 1)),
        call(build_retry_queue(ROUTE, 2)),
        call(build_queue(ROUTE)),
    ]


async def test_declare_topology_binds_queues_to_their_exchanges(single_route):
    broker = AsyncMock(spec=RabbitBroker)

    dead_letter_exchange = Mock(spec=RobustExchange)
    exchange = Mock(spec=RobustExchange)
    broker.declare_exchange.side_effect = [dead_letter_exchange, exchange]

    dead_letter_queue = Mock(spec=RobustQueue)
    retry_queues = [Mock(spec=RobustQueue), Mock(spec=RobustQueue)]
    queue = Mock(spec=RobustQueue)
    broker.declare_queue.side_effect = [dead_letter_queue, *retry_queues, queue]

    await declare_topology(broker)

    dead_letter_queue.bind.assert_awaited_once_with(
        exchange=dead_letter_exchange,
        routing_key=ROUTE.dead_letter_queue,
    )
    queue.bind.assert_awaited_once_with(
        exchange=exchange,
        routing_key=ROUTE.routing_key,
    )
    # Retry queues are fed through the default exchange: no bindings.
    for retry_queue in retry_queues:
        retry_queue.bind.assert_not_awaited()


async def test_declare_topology_declares_shared_route_once(
    topology_broker,
    monkeypatch,
):
    monkeypatch.setattr(
        topology_module,
        "EVENT_ROUTES",
        {"payment.created": ROUTE, "payment.alias": ROUTE},
    )

    await declare_topology(topology_broker)

    assert topology_broker.declare_exchange.await_count == 2
    assert topology_broker.declare_queue.await_count == 4
