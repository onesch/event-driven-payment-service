from decimal import Decimal
from sqlalchemy import select

from src.database.database import async_session_maker
from payment_service.models.payment import Payment


async def db_get_by_idempotency_key(
    idempotency_key: str,
) -> Payment | None:
    async with async_session_maker() as session:
        result = await session.execute(
            select(Payment).where(
                Payment.idempotency_key == idempotency_key
            )
        )
        return result.scalar_one_or_none()


async def db_get(
    id: int,
) -> Payment | None:
    async with async_session_maker() as session:
        result = await session.execute(
            select(Payment).where(
                Payment.id == id
            )
        )
        return result.scalar_one_or_none()


async def db_create(
    idempotency_key: str,
    amount: Decimal,
    description: str | None,
) -> Payment:
    async with async_session_maker() as session:
        payment = Payment(
            idempotency_key=idempotency_key,
            amount=amount,
            description=description,
        )
        session.add(payment)
        await session.commit()
        await session.refresh(payment)
    return payment
