import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock
from aio_pika import RobustExchange, RobustQueue
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession

from src.outbox_service.models.outbox import Outbox
from src.outbox_service.repositories.outbox import OutboxRepository


@pytest.fixture
def outbox_event_data() -> dict:
    """Canonical outbox event payload used across the test suite."""
    return {
        "id": 1,
        "event_type": "payment.created",
        "payload": {"payment_id": 42},
        "created_at": datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc),
        "published_at": None,
    }


@pytest.fixture
def outbox_event(outbox_event_data) -> Outbox:
    """An Outbox ORM instance matching ``outbox_event_data`` (no DB involved)."""
    return Outbox(**outbox_event_data)


@pytest.fixture
def outbox_repository() -> OutboxRepository:
    """OutboxRepository with a mocked database session."""
    return OutboxRepository(AsyncMock(spec=AsyncSession))


@pytest.fixture
def outbox_broker() -> AsyncMock:
    """
    Mocked RabbitMQ broker for publisher unit tests.

    Declared exchanges/queues are mocks spec'd against aio_pika's
    RobustExchange/RobustQueue (what declare_* actually return),
    including their async bind() methods.
    """
    broker = AsyncMock(spec=RabbitBroker)

    exchange = Mock(spec=RobustExchange)
    queue = Mock(spec=RobustQueue)

    broker.declare_exchange.return_value = exchange
    broker.declare_queue.return_value = queue

    return broker
