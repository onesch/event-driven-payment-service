from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, ConfigDict, Field

from payment_service.models.payment import PaymentStatus


class PaymentCreateResponse(BaseModel):
    id: int
    status: PaymentStatus
    created_at: datetime


class PaymentCreate(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    description: str | None = Field(default=None, max_length=255)


class PaymentGet(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    idempotency_key: str
    amount: Decimal
    description: str | None
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None
