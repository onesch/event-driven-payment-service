from decimal import Decimal
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.models.payment import Payment


class PaymentRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(
        self, payment_id: int,
    ) -> Payment | None:
        result = await self.session.execute(
            select(Payment).where(Payment.id == payment_id)
        )
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(
        self,
        idempotency_key: str,
    ) -> Payment | None:
        result = await self.session.execute(
            select(Payment).where(
                Payment.idempotency_key == idempotency_key
            )
        )
        return result.scalar_one_or_none()

    async def create(
        self,
        idempotency_key: str,
        amount: Decimal,
        description: str | None,
    ) -> Payment:
        payment = Payment(
            idempotency_key=idempotency_key,
            amount=amount,
            description=description,
        )
        self.session.add(payment)
        await self.session.flush()
        await self.session.refresh(payment)

        return payment
