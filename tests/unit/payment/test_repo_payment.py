import pytest
from decimal import Decimal
from sqlalchemy.exc import IntegrityError
from unittest.mock import MagicMock

from src.payment_service.models.payment import Payment


# Test cases for PaymentRepository.get_by_id() method


@pytest.mark.asyncio
async def test_get_by_id(payment_repository, payment):
    payment_repository.session.get.return_value = payment

    result = await payment_repository.get_by_id(payment.id)

    assert result is payment
    payment_repository.session.get.assert_awaited_once_with(Payment, payment.id)


@pytest.mark.asyncio
async def test_get_by_id_not_found(payment_repository):
    payment_repository.session.get.return_value = None

    result = await payment_repository.get_by_id(999)

    assert result is None
    payment_repository.session.get.assert_awaited_once_with(Payment, 999)


# Test cases for PaymentRepository.get_by_idempotency_key() method


@pytest.mark.asyncio
async def test_get_by_idempotency_key(payment_repository, payment):
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = payment
    payment_repository.session.execute.return_value = result_mock

    result = await payment_repository.get_by_idempotency_key(payment.idempotency_key)

    assert result is payment
    payment_repository.session.execute.assert_awaited_once()

    statement = payment_repository.session.execute.await_args.args[0]
    compiled = str(statement.compile(compile_kwargs={"literal_binds": True}))

    assert "FROM payments" in compiled
    assert f"idempotency_key = '{payment.idempotency_key}'" in compiled
    result_mock.scalar_one_or_none.assert_called_once_with()


@pytest.mark.asyncio
async def test_get_by_idempotency_key_not_found(payment_repository):
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    payment_repository.session.execute.return_value = result_mock

    result = await payment_repository.get_by_idempotency_key("missing-key")

    assert result is None
    payment_repository.session.execute.assert_awaited_once()



# Test cases for PaymentRepository.create() method


@pytest.mark.asyncio
async def test_create_payment(payment_repository):
    added_instances = []
    payment_repository.session.add = MagicMock(
        side_effect=added_instances.append,
    )

    result = await payment_repository.create(
        idempotency_key="idem-key-123",
        amount=Decimal("100.00"),
        description="Test payment",
    )

    assert isinstance(result, Payment)
    assert result.idempotency_key == "idem-key-123"
    assert result.amount == Decimal("100.00")
    assert result.description == "Test payment"

    assert added_instances == [result]
    payment_repository.session.flush.assert_awaited_once_with()
    payment_repository.session.refresh.assert_awaited_once_with(result)


@pytest.mark.asyncio
async def test_create_payment_flush_error_does_not_refresh(payment_repository):
    payment_repository.session.add = MagicMock()
    payment_repository.session.flush.side_effect = IntegrityError(
        "INSERT INTO payments ...",
        {},
        Exception("duplicate key value violates unique constraint"),
    )

    with pytest.raises(IntegrityError):
        await payment_repository.create(
            idempotency_key="idem-key-123",
            amount=Decimal("100.00"),
            description=None,
        )

    payment_repository.session.refresh.assert_not_awaited()
