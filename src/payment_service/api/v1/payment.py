from fastapi import APIRouter, status, Header

from payment_service.service.payment import (
    get,
    create,
)
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
async def get_payment(payment_id: int):
    return await get(payment_id)


@router.post(
    "/",
    response_model=PaymentCreateResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=100),
):
    payment = await create(data, idempotency_key)
    return payment
