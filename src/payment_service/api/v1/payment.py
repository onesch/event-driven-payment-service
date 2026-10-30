from typing import Annotated
from fastapi import APIRouter, status, Header, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from database.database import get_db
from payment_service.services.payment import PaymentService
from payment_service.schemas.payment import (
    PaymentGet,
    PaymentCreateResponse,
    PaymentCreate,
)

router = APIRouter(
    prefix="/payments",
    tags=["Payments"],
)


@router.get(
    "/{payment_id}",
    response_model=PaymentGet,
    status_code=status.HTTP_200_OK,
)
async def get_payment(
    payment_id: int,
    session: Annotated[AsyncSession, Depends(get_db)],
):
    service = PaymentService(session)

    return await service.get(payment_id)


@router.post(
    "/",
    response_model=PaymentCreateResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_payment(
    data: PaymentCreate,
    session: Annotated[AsyncSession, Depends(get_db)],
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=100),
):
    service = PaymentService(session)

    return await service.create(data, idempotency_key)
