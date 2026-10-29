from datetime import datetime
from decimal import Decimal
from enum import Enum

from sqlalchemy import DateTime, Enum as SQLAlchemyEnum, Numeric, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from database.models.base import Base


class PaymentStatus(str, Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

payment_status_type = SQLAlchemyEnum(
    PaymentStatus,
    name="payment_status",
    values_callable=lambda enum: [item.value for item in enum],
)


class Payment(Base):
    __tablename__ = "payments"

    __table_args__ = (
        UniqueConstraint(
            "idempotency_key",
            name="uq_payments_idempotency_key",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(100), nullable=False)

    amount: Mapped[Decimal] = mapped_column(Numeric(precision=12, scale=2), nullable=False)
    description: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # currency
    # metadata_json
    # webhook_url

    status: Mapped[PaymentStatus] = mapped_column(payment_status_type, default=PaymentStatus.PENDING, nullable=False)

    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
