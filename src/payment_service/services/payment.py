from sqlalchemy.ext.asyncio import AsyncSession

from src.exceptions import ConflictException, NotFoundException
from src.payment_service.models.payment import Payment
from src.payment_service.schemas.payment import PaymentCreate
from src.payment_service.repositories.payment import PaymentRepository
from src.outbox_service.repositories.outbox import OutboxRepository


class PaymentService:
    def __init__(self, session: AsyncSession):
        self.repository = PaymentRepository(session)
        self.outbox_repository = OutboxRepository(session)

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

        new_payment = await self.repository.create(
            idempotency_key=idempotency_key,
            amount=payment.amount,
            description=payment.description,
        )

        await self.outbox_repository.create(
            event_type="payment.created",
            payload={
                "payment_id": new_payment.id,
            },
        )
        return new_payment
