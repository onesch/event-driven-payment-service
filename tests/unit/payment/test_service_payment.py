import pytest
from decimal import Decimal
from exceptions import ConflictException, NotFoundException
from payment_service.models.payment import PaymentStatus
from payment_service.schemas.payment import PaymentCreate


# Test cases for PaymentService.get() method


@pytest.mark.asyncio
async def test_get_payment_success(payment_service, payment):
    payment_service.repository.get_by_id.return_value = payment

    result = await payment_service.get(payment.id)

    assert result is payment
    payment_service.repository.get_by_id.assert_awaited_once_with(payment.id)



@pytest.mark.asyncio
async def test_get_payment_not_found(payment_service):
    payment_service.repository.get_by_id.return_value = None

    with pytest.raises(NotFoundException) as exc:
        await payment_service.get(999)

    assert exc.value.status_code == 404
    assert exc.value.detail == "Payment not found"
    payment_service.repository.get_by_id.assert_awaited_once_with(999)


# Test cases for PaymentService.create() method


@pytest.mark.asyncio
async def test_create_payment_success(payment_service, payment, payment_create):
    payment_service.repository.get_by_idempotency_key.return_value = None
    payment_service.repository.create.return_value = payment

    result = await payment_service.create(payment_create, payment.idempotency_key)

    assert result.id == payment.id
    assert result.status == PaymentStatus.PENDING

    payment_service.repository.get_by_idempotency_key.assert_awaited_once_with(
        payment.idempotency_key,
    )
    payment_service.repository.create.assert_awaited_once_with(
        idempotency_key=payment.idempotency_key,
        amount=payment_create.amount,
        description=payment_create.description,
    )


@pytest.mark.asyncio
async def test_create_payment_creates_outbox_event(
    payment_service,
    payment,
    payment_create,
):
    payment_service.repository.get_by_idempotency_key.return_value = None
    payment_service.repository.create.return_value = payment

    await payment_service.create(payment_create, payment.idempotency_key)

    payment_service.outbox_repository.create.assert_awaited_once_with(
        event_type="payment.created",
        payload={"payment_id": payment.id},
    )


@pytest.mark.asyncio
async def test_create_payment_repeated_request_same_data(
    payment_service,
    payment,
    payment_create,
):
    """Same idempotency key + same payload -> return the existing payment."""
    payment_service.repository.get_by_idempotency_key.return_value = payment

    result = await payment_service.create(payment_create, payment.idempotency_key)

    assert result is payment
    payment_service.repository.create.assert_not_awaited()
    payment_service.outbox_repository.create.assert_not_awaited()



@pytest.mark.asyncio
async def test_create_payment_repeated_request_equal_amount_different_scale(
    payment_service,
    payment,
    payment_create,
):
    """Decimal('100.0') and Decimal('100.00') are numerically equal."""
    payment_service.repository.get_by_idempotency_key.return_value = payment
    payment_create = PaymentCreate(
        amount=Decimal("100.0"),
        description=payment.description,
    )
    result = await payment_service.create(payment_create, payment.idempotency_key)

    assert result is payment
    payment_service.repository.create.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "stored_value", "requested_value"),
    [
        # Amount conflicts
        ("amount", Decimal("100.00"), Decimal("100.01")),
        ("amount", Decimal("100.00"), Decimal("99.99")),
        ("amount", Decimal("100.00"), Decimal("1000.00")),

        # Description conflicts
        ("description", "Original", "Different"),
        ("description", None, "Added description"),
        ("description", "Original", None),
    ],
    ids=[
        "amount_one_cent_more",
        "amount_one_cent_less",
        "amount_magnitude_change",
        "description_changed",
        "description_added",
        "description_removed",
    ],
)
async def test_create_payment_conflict_on_different_data(
    payment_service,
    payment,
    payment_create,
    field,
    stored_value,
    requested_value,
):
    setattr(payment, field, stored_value)
    payment_service.repository.get_by_idempotency_key.return_value = payment

    payment_data = {
        "amount": payment.amount,
        "description": payment.description,
    }
    payment_data[field] = requested_value

    payment_create = PaymentCreate(**payment_data)

    with pytest.raises(ConflictException) as exc:
        await payment_service.create(payment_create, payment.idempotency_key)

    assert exc.value.status_code == 409
    assert exc.value.detail == (
        "Idempotency-Key already used with different payment data"
    )

    payment_service.repository.create.assert_not_awaited()
    payment_service.outbox_repository.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_payment_repository_error_propagates(
    payment_service,
    payment_create,
):
    """If the payment insert fails, no outbox event must be queued."""
    payment_service.repository.get_by_idempotency_key.return_value = None
    payment_service.repository.create.side_effect = RuntimeError("database unavailable")

    with pytest.raises(RuntimeError, match="database unavailable"):
        await payment_service.create(payment_create, "idem-key-invalid")

    payment_service.outbox_repository.create.assert_not_awaited()
