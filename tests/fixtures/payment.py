import pytest
from unittest.mock import AsyncMock, Mock
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timezone
from decimal import Decimal

from src.outbox_service.repositories.outbox import OutboxRepository
from src.payment_service.models.payment import Payment, PaymentStatus
from src.payment_service.schemas.payment import PaymentCreate
from src.payment_service.repositories.payment import PaymentRepository
from src.payment_service.services.payment import PaymentService
from src.payment_service.api.v1 import payment as api_payment_module


@pytest.fixture
def payment_data() -> dict:
    """Payment payload used across the test suite."""
    return {
        "id": 1,
        "idempotency_key": "idem-key-123",
        "amount": Decimal("100.00"),
        "description": "Test payment",
        "status": PaymentStatus.PENDING,
        "created_at": datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        "processed_at": None,
    }


@pytest.fixture
def payment(payment_data) -> Payment:
    """Payment ORM instance matching ``payment_data`` (no DB involved)."""
    return Payment(**payment_data)


@pytest.fixture
def payment_create() -> PaymentCreate:
    """Valid input schema for the create-payment endpoint."""
    return PaymentCreate(
        amount=Decimal("100.00"),
        description="Test payment",
    )


@pytest.fixture
def payment_repository() -> PaymentRepository:
    """PaymentRepository with a mocked database session."""
    return PaymentRepository(AsyncMock(spec=AsyncSession))


@pytest.fixture
def payment_service() -> PaymentService:
    """PaymentService with mocked repositories."""
    service = PaymentService(AsyncMock(spec=AsyncSession))
    service.repository = AsyncMock(spec=PaymentRepository)
    service.outbox_repository = AsyncMock(spec=OutboxRepository)
    return service


@pytest.fixture
def api_payment_service(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncMock:
    """Mock PaymentService used by API-layer tests."""
    service_mock = AsyncMock(spec=PaymentService)

    monkeypatch.setattr(
        api_payment_module,
        "PaymentService",
        Mock(return_value=service_mock),
    )

    return service_mock
