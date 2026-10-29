from exceptions import ConflictException, NotFoundException
from payment_service.models.payment import Payment
from payment_service.schemas.payment import PaymentCreate
from payment_service.db_service.payment import (
    db_get,
    db_create,
    db_get_by_idempotency_key,
)


async def get(
    payment_id: int,
) -> Payment:
    payment = await db_get(payment_id)

    if payment is None:
        raise NotFoundException(detail="Payment not found")

    return payment


async def create(
    payment: PaymentCreate,
    idempotency_key: str,
) -> Payment:
    existing_payment = await db_get_by_idempotency_key(idempotency_key)

    if existing_payment is not None:
        if (
            existing_payment.amount != payment.amount
            or existing_payment.description != payment.description
        ):
            raise ConflictException(
                detail=(
                    "Idempotency-Key already used with different payment data"
                )
            )
        return existing_payment

    payment = await db_create(
        idempotency_key=idempotency_key,
        amount=payment.amount,
        description=payment.description,
    )
    return payment
