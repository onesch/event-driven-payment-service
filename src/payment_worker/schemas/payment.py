from pydantic import BaseModel, Field


class PaymentCreatedEvent(BaseModel):
    payment_id: int = Field(gt=0)
