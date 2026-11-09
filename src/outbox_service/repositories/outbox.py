from typing import Any
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from outbox_service.models.outbox import Outbox


class OutboxRepository:
    """
    Repository for managing Outbox events in the database.
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, event_id: int) -> Outbox | None:
        return await self.session.get(Outbox, event_id)

    async def create(
        self,
        event_type: str,
        payload: dict[str, Any],
    ) -> Outbox:
        event = Outbox(
            event_type=event_type,
            payload=payload,
        )
        self.session.add(event)
        await self.session.flush()

        return event

    async def get_unpublished(
        self,
        limit: int = 100,
    ) -> list[Outbox]:
        result = await self.session.scalars(
            select(Outbox)
            .where(Outbox.published_at.is_(None))  # get unpublished
            .order_by(Outbox.created_at, Outbox.id)
            .limit(limit)
        )
        return list(result.all())

    async def mark_published(self, event: Outbox) -> None:
        event.published_at = datetime.now(timezone.utc)
        await self.session.flush()
