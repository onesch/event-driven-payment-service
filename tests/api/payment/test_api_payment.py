import pytest
from unittest.mock import AsyncMock
from datetime import datetime, timezone
from decimal import Decimal
from fastapi import status as http_status
from httpx import AsyncClient

from src.exceptions import ConflictException, NotFoundException
from src.payment_service.models.payment import PaymentStatus
from src.payment_service.schemas.payment import PaymentCreate


# Common constants used in e2e tests


CREATED_AT = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)


# helper functions for e2e tests


def make_service_result(payment_id: int = 1) -> dict:
    """Return a mocked result for PaymentService.create()."""
    return {
        "id": payment_id,
        "status": PaymentStatus.PENDING,
        "created_at": CREATED_AT,
    }


def make_payment_get_result(payment_id: int = 1) -> dict:
    """Return a mocked result for PaymentService.get()."""
    return {
        "id": payment_id,
        "idempotency_key": "idem-key-123",
        "amount": Decimal("100.00"),
        "description": "Test payment",
        "status": PaymentStatus.PENDING,
        "created_at": CREATED_AT,
        "processed_at": None,
    }


# Test cases for the health check endpoint


@pytest.mark.asyncio
async def test_health_check(client: AsyncClient):
    response = await client.get("/health")

    assert response.status_code == http_status.HTTP_200_OK
    assert response.json() == {"status": "ok"}


# Test cases for the get-payment endpoint


@pytest.mark.asyncio
async def test_get_payment(
    client: AsyncClient,
    api_payment_service: AsyncMock,
):
    api_payment_service.get.return_value = make_payment_get_result()

    response = await client.get("/api/v1/payments/1")

    assert response.status_code == http_status.HTTP_200_OK
    assert response.json() == {
        "id": 1,
        "idempotency_key": "idem-key-123",
        "amount": "100.00",
        "description": "Test payment",
        "status": "pending",
        "created_at": CREATED_AT.isoformat().replace("+00:00", "Z"),
        "processed_at": None,
    }

    api_payment_service.get.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_get_payment_not_found(
    client: AsyncClient,
    api_payment_service: AsyncMock,
):
    api_payment_service.get.side_effect = NotFoundException(
        detail="Payment not found",
    )

    response = await client.get("/api/v1/payments/999")

    assert response.status_code == http_status.HTTP_404_NOT_FOUND
    assert response.json() == {"detail": "Payment not found"}

    api_payment_service.get.assert_awaited_once_with(999)


@pytest.mark.asyncio
async def test_get_payment_invalid_id(
    client: AsyncClient,
    api_payment_service: AsyncMock,
):
    response = await client.get("/api/v1/payments/not-an-integer")

    assert response.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT
    api_payment_service.get.assert_not_awaited()


# Test cases for the create-payment endpoint


@pytest.mark.asyncio
async def test_create_payment_accepted(
    client: AsyncClient,
    api_payment_service: AsyncMock,
):
    api_payment_service.create.return_value = make_service_result()

    response = await client.post(
        "/api/v1/payments/",
        json={
            "amount": "100.00",
            "description": "Test payment",
        },
        headers={"Idempotency-Key": "idem-key-123"},
    )

    assert response.status_code == http_status.HTTP_202_ACCEPTED
    assert response.json() == {
        "id": 1,
        "status": "pending",
        "created_at": CREATED_AT.isoformat().replace("+00:00", "Z"),
    }

    api_payment_service.create.assert_awaited_once()

    args = api_payment_service.create.await_args.args

    assert isinstance(args[0], PaymentCreate)
    assert args[0].amount == Decimal("100.00")
    assert args[0].description == "Test payment"
    assert args[1] == "idem-key-123"


@pytest.mark.asyncio
async def test_create_payment_response_contains_only_public_fields(
    client: AsyncClient,
    api_payment_service: AsyncMock,
):
    api_payment_service.create.return_value = make_service_result()

    response = await client.post(
        "/api/v1/payments/",
        json={"amount": "100.00"},
        headers={"Idempotency-Key": "idem-key-123"},
    )

    assert response.status_code == http_status.HTTP_202_ACCEPTED
    assert set(response.json()) == {"id", "status", "created_at"}

    api_payment_service.create.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_payment_missing_idempotency_key(
    client: AsyncClient,
    api_payment_service: AsyncMock,
):
    response = await client.post(
        "/api/v1/payments/",
        json={"amount": "100.00"},
    )

    assert response.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT
    api_payment_service.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_payment_missing_amount(
    client: AsyncClient,
    api_payment_service: AsyncMock,
):
    response = await client.post(
        "/api/v1/payments/",
        json={},
        headers={"Idempotency-Key": "idem-key-123"},
    )

    assert response.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT
    api_payment_service.create.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amount",
    ["0", "0.00", "-1.00", "-100.50"],
    ids=["zero", "zero_decimal", "negative", "negative_decimal"],
)
async def test_create_payment_invalid_amount(
    client: AsyncClient,
    api_payment_service: AsyncMock,
    amount: str,
):
    response = await client.post(
        "/api/v1/payments/",
        json={"amount": amount},
        headers={"Idempotency-Key": "idem-key-123"},
    )

    assert response.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT
    api_payment_service.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_payment_amount_too_many_decimal_places(
    client: AsyncClient,
    api_payment_service: AsyncMock,
):
    response = await client.post(
        "/api/v1/payments/",
        json={"amount": "10.999"},
        headers={"Idempotency-Key": "idem-key-123"},
    )

    assert response.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT
    api_payment_service.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_payment_amount_exceeds_max_digits(
    client: AsyncClient,
    api_payment_service: AsyncMock,
):
    # Numeric(12, 2) allows at most 10 digits before the decimal point.
    response = await client.post(
        "/api/v1/payments/",
        json={"amount": "12345678901.45"},
        headers={"Idempotency-Key": "idem-key-123"},
    )

    assert response.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT
    api_payment_service.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_payment_description_too_long(
    client: AsyncClient,
    api_payment_service: AsyncMock,
):
    response = await client.post(
        "/api/v1/payments/",
        json={
            "amount": "100.00",
            "description": "x" * 256,
        },
        headers={"Idempotency-Key": "idem-key-123"},
    )

    assert response.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT
    api_payment_service.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_payment_invalid_json(
    client: AsyncClient,
    api_payment_service: AsyncMock,
):
    response = await client.post(
        "/api/v1/payments/",
        content='{"amount": ',
        headers={
            "Idempotency-Key": "idem-key-123",
            "Content-Type": "application/json",
        },
    )

    assert response.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT
    api_payment_service.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_payment_conflicting_idempotency_key(
    client: AsyncClient,
    api_payment_service: AsyncMock,
):
    api_payment_service.create.side_effect = ConflictException(
        detail="Idempotency-Key already used with different payment data",
    )

    response = await client.post(
        "/api/v1/payments/",
        json={
            "amount": "100.00",
            "description": "Test payment",
        },
        headers={"Idempotency-Key": "idem-key-123"},
    )

    assert response.status_code == http_status.HTTP_409_CONFLICT
    assert response.json() == {
        "detail": "Idempotency-Key already used with different payment data",
    }

    api_payment_service.create.assert_awaited_once()
