import pytest
from datetime import datetime, timezone
from sqlalchemy.exc import SQLAlchemyError
from unittest.mock import MagicMock

from src.outbox_service.models.outbox import Outbox


# Test cases for the OutboxRepository.get_by_id() method


@pytest.mark.asyncio
async def test_get_by_id(outbox_repository, outbox_event):
    outbox_repository.session.get.return_value = outbox_event

    result = await outbox_repository.get_by_id(outbox_event.id)

    assert result is outbox_event
    outbox_repository.session.get.assert_awaited_once_with(Outbox, outbox_event.id)


@pytest.mark.asyncio
async def test_get_by_id_not_found(outbox_repository):
    outbox_repository.session.get.return_value = None

    result = await outbox_repository.get_by_id(999)

    assert result is None
    outbox_repository.session.get.assert_awaited_once_with(Outbox, 999)



# Test cases for the OutboxRepository.create() method


@pytest.mark.asyncio
async def test_create_event(outbox_repository):
    added_instances = []
    outbox_repository.session.add = MagicMock(
        side_effect=added_instances.append,
    )

    result = await outbox_repository.create(
        event_type="payment.created",
        payload={"payment_id": 42},
    )

    assert isinstance(result, Outbox)
    assert result.event_type == "payment.created"
    assert result.payload == {"payment_id": 42}

    # Verify that the event was added to the session and flushed
    assert added_instances == [result]
    outbox_repository.session.flush.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_create_event_without_refresh(outbox_repository):
    """create() must not depend on a server refresh round-trip."""
    outbox_repository.session.add = MagicMock()

    await outbox_repository.create(
        event_type="payment.created",
        payload={"payment_id": 1},
    )

    outbox_repository.session.refresh.assert_not_awaited()


# Test cases for the OutboxRepository.get_unpublished() method


@pytest.mark.asyncio
async def test_get_unpublished(outbox_repository, outbox_event):
    scalars_result = MagicMock()
    scalars_result.all.return_value = [outbox_event]
    outbox_repository.session.scalars.return_value = scalars_result

    result = await outbox_repository.get_unpublished()

    assert result == [outbox_event]
    outbox_repository.session.scalars.assert_awaited_once()

    statement = outbox_repository.session.scalars.await_args.args[0]
    compiled = str(statement.compile(compile_kwargs={"literal_binds": True}))

    assert "FROM outbox" in compiled
    assert "published_at IS NULL" in compiled
    assert "ORDER BY outbox.created_at, outbox.id" in compiled
    assert "LIMIT 100" in compiled


@pytest.mark.asyncio
async def test_get_unpublished_respects_limit(outbox_repository):
    scalars_result = MagicMock()
    scalars_result.all.return_value = []
    outbox_repository.session.scalars.return_value = scalars_result

    result = await outbox_repository.get_unpublished(limit=5)

    assert result == []

    statement = outbox_repository.session.scalars.await_args.args[0]
    compiled = str(statement.compile(compile_kwargs={"literal_binds": True}))

    assert "LIMIT 5" in compiled


@pytest.mark.asyncio
async def test_get_unpublished_empty(outbox_repository):
    scalars_result = MagicMock()
    scalars_result.all.return_value = []
    outbox_repository.session.scalars.return_value = scalars_result

    result = await outbox_repository.get_unpublished()

    assert result == []


# Test cases for the OutboxRepository.mark_published() method


@pytest.mark.asyncio
async def test_mark_published(outbox_repository, outbox_event):
    assert outbox_event.published_at is None

    await outbox_repository.mark_published(outbox_event)

    assert outbox_event.published_at is not None
    assert outbox_event.published_at.tzinfo is not None
    assert outbox_event.published_at <= datetime.now(timezone.utc)
    outbox_repository.session.flush.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_mark_published_flush_error_propagates(
    outbox_repository,
    outbox_event,
):
    outbox_repository.session.flush.side_effect = SQLAlchemyError("connection lost")

    with pytest.raises(SQLAlchemyError):
        await outbox_repository.mark_published(outbox_event)
