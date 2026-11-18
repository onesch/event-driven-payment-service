import asyncio
import random

from sqlalchemy import update

from payment_service.models.payment import Payment, PaymentStatus
from src.database.database import async_session_maker


class PaymentProcessor:
    @staticmethod
    async def process(payment_id: int) -> None:
        # Simulate payment processing (2-5 seconds).
        await asyncio.sleep(random.uniform(2, 5))

        # Simulate payment result (90% success, 10% failure).
        status = (
            PaymentStatus.SUCCEEDED
            if random.random() < 0.9
            else PaymentStatus.FAILED
        )

        # Update payment data
        async with async_session_maker() as session:
            await session.execute(
                update(Payment)
                .where(
                    Payment.id == payment_id,
                    Payment.status == PaymentStatus.PENDING,
                )
                .values(status=status)
            )
            await session.commit()
