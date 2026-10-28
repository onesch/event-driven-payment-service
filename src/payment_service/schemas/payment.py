from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, ConfigDict, Field

from payment_service.models.payment import PaymentStatus


class PaymentCreate(BaseModel):
    description: str | None = Field(default=None, max_length=255)
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)


class PaymentGet(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    status: PaymentStatus
    created_at: datetime
