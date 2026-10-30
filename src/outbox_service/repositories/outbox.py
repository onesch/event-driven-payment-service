from typing import Any
from sqlalchemy.ext.asyncio import AsyncSession

from outbox_service.models.outbox import Outbox


class OutboxRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

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
