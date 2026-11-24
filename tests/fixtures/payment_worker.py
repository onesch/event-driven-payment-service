import pytest
from unittest.mock import AsyncMock, Mock
from faststream.rabbit import RabbitBroker, TestRabbitBroker

from src.payment_worker.consumers import payment_consumer as consumer_module
from src.payment_worker.schemas.payment import PaymentCreatedEvent
from src.payment_worker.services import payment_processor as processor_module


@pytest.fixture
def payment_message() -> dict:
    """``payment.created`` message body consumed by the payment worker."""
    return {"payment_id": 42}


@pytest.fixture
def payment_event(payment_message) -> PaymentCreatedEvent:
    """Validated ``payment.created`` event passed to the handler."""
    return PaymentCreatedEvent(**payment_message)


@pytest.fixture
def processor_session_maker(monkeypatch) -> Mock:
    """Mock the global async SQLAlchemy session factory used by the processor."""
    session = AsyncMock()
    session.__aenter__.return_value = session
    # Do not suppress exceptions raised inside the ``async with`` block.
    session.__aexit__.return_value = False

    maker = Mock(return_value=session)
    monkeypatch.setattr(
        processor_module,
        "async_session_maker",
        maker,
    )

    return maker


@pytest.fixture
def processor_session(processor_session_maker) -> AsyncMock:
    """Session yielded by the mocked session factory."""
    return processor_session_maker.return_value


@pytest.fixture
def processor_sleep(monkeypatch) -> AsyncMock:
    """Mock the simulated processing delay so tests do not wait for real time."""
    sleep = AsyncMock()
    monkeypatch.setattr(processor_module.asyncio, "sleep", sleep)
    return sleep


@pytest.fixture
def processing_delay(monkeypatch) -> Mock:
    """Mock the simulated processing duration."""
    uniform = Mock(return_value=3.0)
    monkeypatch.setattr(processor_module.random, "uniform", uniform)
    return uniform


@pytest.fixture
def payment_outcome(monkeypatch) -> Mock:
    """Mock the simulated payment result roll (values below 0.9 succeed)."""
    roll = Mock(return_value=0.0)
    monkeypatch.setattr(processor_module.random, "random", roll)
    return roll


@pytest.fixture
def processor_process(monkeypatch) -> AsyncMock:
    """Mock ``PaymentProcessor.process`` used by the consumer handler."""
    process = AsyncMock()

    monkeypatch.setattr(
        consumer_module,
        "PaymentProcessor",
        Mock(process=process),
    )

    return process


@pytest.fixture
async def consumer_test_broker() -> RabbitBroker:
    """
    In-memory FastStream broker for the payment consumer.

    Messages go through the real subscriber pipeline (decoding and
    pydantic validation) without a running RabbitMQ.
    """
    async with TestRabbitBroker(consumer_module.broker) as broker:
        yield broker
