import pytest
from unittest.mock import AsyncMock, Mock
from aio_pika import RobustExchange, RobustQueue
from faststream.rabbit import ExchangeType, RabbitBroker

from src.rabbitmq import topology as topology_module
from src.rabbitmq.topology import build_exchange, build_queue, declare_topology
from src.settings import EventRoute

ROUTE = EventRoute(
    exchange="payments",
    routing_key="payments.created",
    queue="payments.new",
)


@pytest.fixture
def topology_broker() -> AsyncMock:
    """Mocked broker whose declare_* return aio_pika-like objects."""
    broker = AsyncMock(spec=RabbitBroker)

    broker.declare_exchange.return_value = Mock(spec=RobustExchange)
    broker.declare_queue.return_value = Mock(spec=RobustQueue)

    return broker


# Test cases for build_exchange() and build_queue()


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


def test_builders_return_equal_objects_on_each_call():
    """Publisher and consumer must declare identical topology."""
    assert build_exchange(ROUTE) == build_exchange(ROUTE)
    assert build_queue(ROUTE) == build_queue(ROUTE)


# Test cases for declare_topology()


@pytest.mark.asyncio
async def test_declare_topology_declares_and_binds_route(
    topology_broker,
    monkeypatch,
):
    monkeypatch.setattr(topology_module, "EVENT_ROUTES", {"payment.created": ROUTE})

    await declare_topology(topology_broker)

    topology_broker.declare_exchange.assert_awaited_once_with(build_exchange(ROUTE))
    topology_broker.declare_queue.assert_awaited_once_with(build_queue(ROUTE))

    queue = topology_broker.declare_queue.return_value
    queue.bind.assert_awaited_once_with(
        exchange=topology_broker.declare_exchange.return_value,
        routing_key=ROUTE.routing_key,
    )


@pytest.mark.asyncio
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

    topology_broker.declare_exchange.assert_awaited_once()
    topology_broker.declare_queue.assert_awaited_once()
