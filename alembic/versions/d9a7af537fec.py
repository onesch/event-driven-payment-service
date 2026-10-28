"""add some fields in payment table

Revision ID: d9a7af537fec
Revises: 7ae63df6013d
Create Date: 2026-10-27 20:19:59.586148

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d9a7af537fec"
down_revision: Union[str, Sequence[str], None] = "7ae63df6013d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""

    # Create the PostgreSQL ENUM type before using it.
    payment_status = sa.Enum(
        "pending",
        "succeeded",
        "failed",
        name="payment_status",
    )
    payment_status.create(op.get_bind(), checkfirst=True)

    op.add_column(
        "payments",
        sa.Column("idempotency_key", sa.String(length=100), nullable=False),
    )
    op.add_column(
        "payments",
        sa.Column("description", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "payments",
        sa.Column("amount", sa.Numeric(precision=12, scale=2), nullable=False),
    )
    op.add_column(
        "payments",
        sa.Column("status", payment_status, nullable=False),
    )
    op.add_column(
        "payments",
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.add_column(
        "payments",
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )

    op.create_unique_constraint(
        "uq_payments_idempotency_key",
        "payments",
        ["idempotency_key"],
    )


def downgrade() -> None:
    """Downgrade schema."""

    op.drop_constraint(
        "uq_payments_idempotency_key",
        "payments",
        type_="unique",
    )

    op.drop_column("payments", "processed_at")
    op.drop_column("payments", "created_at")
    op.drop_column("payments", "status")
    op.drop_column("payments", "amount")
    op.drop_column("payments", "description")
    op.drop_column("payments", "idempotency_key")

    # Drop the ENUM type after removing the column that uses it.
    payment_status = sa.Enum(
        "pending",
        "succeeded",
        "failed",
        name="payment_status",
    )
