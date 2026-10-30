from sqlalchemy.ext.asyncio import AsyncSession

from exceptions import ConflictException, NotFoundException
from payment_service.models.payment import Payment
from payment_service.schemas.payment import PaymentCreate
from payment_service.repositories.payment import PaymentRepository


class PaymentService:
    def __init__(self, session: AsyncSession):
        self.repository = PaymentRepository(session)

    async def get(
        self, payment_id: int,
    ) -> Payment:
        payment = await self.repository.get_by_id(payment_id)

        if payment is None:
            raise NotFoundException(detail="Payment not found")

        return payment

    async def create(
        self,
        payment: PaymentCreate,
        idempotency_key: str,
    ) -> Payment:
        existing_payment = await self.repository.get_by_idempotency_key(idempotency_key)

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

        return await self.repository.create(
            idempotency_key=idempotency_key,
            amount=payment.amount,
            description=payment.description,
        )
