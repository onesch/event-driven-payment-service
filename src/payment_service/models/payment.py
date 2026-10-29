from sqlalchemy.orm import Mapped, mapped_column

from database.models.base import Base


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[int] = mapped_column(primary_key=True)
